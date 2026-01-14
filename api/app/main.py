from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Iterable, Optional

import httpx
import pymongo
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, Response

BASE_URL = os.getenv("WL_BASE_URL", "https://www.wienerlinien.at/ogd_realtime/")
BASE_URL = BASE_URL.rstrip("/") + "/"
DEFAULT_TIMEOUT = float(os.getenv("WL_TIMEOUT", "30"))
USER_AGENT = os.getenv("WL_USER_AGENT", "WeatherAndDelay/1.0")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB = os.getenv("MONGO_DB", "big_data_austria")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "wienerlinien_realtime")
HISTORICAL_URL = os.getenv(
    "HISTORICAL_URL",
    "https://cipfileshareprod.blob.core.windows.net/wlcip/WL_Incidents_2025-12-16_13-36-43.json",
)
HISTORICAL_COLLECTION = os.getenv("HISTORICAL_COLLECTION", "wienerlinien_historical")
HISTORICAL_BATCH_SIZE = int(os.getenv("HISTORICAL_BATCH_SIZE", "1000"))

FETCH_ENABLED = os.getenv("FETCH_ENABLED", "true").lower() in ("1", "true", "yes")
FETCH_INTERVAL = float(os.getenv("FETCH_INTERVAL", "30"))
FETCH_ENDPOINT = os.getenv("FETCH_ENDPOINT", "monitor")
FETCH_RBLS = os.getenv("FETCH_RBLS", "")

DEFAULT_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": USER_AGENT,
}

app = FastAPI(
    title="Wiener Linien OGD Realtime Proxy",
    description="Thin proxy for the Wiener Linien realtime endpoints (monitor, trafficInfoList, trafficInfo, newsList, news).",
    version="1.0.0",
)

_client: Optional[httpx.AsyncClient] = None
_mongo_client: Optional[pymongo.MongoClient] = None
_mongo_collection = None
_mongo_history_collection = None
_poll_task: Optional[asyncio.Task] = None


@app.on_event("startup")
async def _startup() -> None:
    global _client, _mongo_client, _mongo_collection, _poll_task
    _client = httpx.AsyncClient(timeout=DEFAULT_TIMEOUT, headers=DEFAULT_HEADERS)
    if FETCH_ENABLED:
        _ensure_mongo()
        _poll_task = asyncio.create_task(_poll_loop())


@app.on_event("shutdown")
async def _shutdown() -> None:
    global _poll_task
    if _poll_task is not None:
        _poll_task.cancel()
        try:
            await _poll_task
        except asyncio.CancelledError:
            pass
    if _client is not None:
        await _client.aclose()
    if _mongo_client is not None:
        _mongo_client.close()


def _ensure_mongo() -> None:
    global _mongo_client, _mongo_collection, _mongo_history_collection
    if _mongo_client is None:
        _mongo_client = pymongo.MongoClient(MONGO_URI)
    if _mongo_collection is None:
        _mongo_collection = _mongo_client[MONGO_DB][MONGO_COLLECTION]
    if _mongo_history_collection is None:
        _mongo_history_collection = _mongo_client[MONGO_DB][HISTORICAL_COLLECTION]


def _add_params(params: list[tuple[str, str]], key: str, values: Optional[Iterable[object]]) -> None:
    if values is None:
        return
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        params.append((key, text))


async def _proxy(endpoint: str, params: list[tuple[str, str]]) -> Response:
    if _client is None:
        raise HTTPException(status_code=503, detail="HTTP client not initialized")

    url = f"{BASE_URL}{endpoint}"
    try:
        response = await _client.get(url, params=params)
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"Upstream request failed: {exc}") from exc

    content_type = response.headers.get("content-type", "")
    if content_type.startswith("application/json"):
        return JSONResponse(content=response.json(), status_code=response.status_code)
    return Response(
        content=response.text,
        status_code=response.status_code,
        media_type=content_type or "text/plain",
    )


def _parse_rbls(value: str) -> list[int]:
    rbls: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            rbls.append(int(part))
        except ValueError:
            continue
    return rbls


async def _fetch_and_store(endpoint: str, params: list[tuple[str, str]]) -> None:
    if _client is None or _mongo_collection is None:
        return
    url = f"{BASE_URL}{endpoint}"
    try:
        response = await _client.get(url, params=params)
    except httpx.RequestError:
        return
    content_type = response.headers.get("content-type", "")
    try:
        payload = response.json() if content_type.startswith("application/json") else response.text
    except ValueError:
        payload = response.text

    doc = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "endpoint": endpoint,
        "params": [{"key": k, "value": v} for k, v in params],
        "status_code": response.status_code,
        "data": payload,
    }
    await asyncio.to_thread(_mongo_collection.insert_one, doc)


async def _ingest_historical_payload(
    payload: dict[str, object],
    source_url: str,
    parse_data: bool,
    upsert: bool,
    batch_size: int,
) -> dict[str, int]:
    if _mongo_history_collection is None:
        raise RuntimeError("MongoDB not initialized")
    entities = payload.get("entities")
    if not isinstance(entities, list):
        raise ValueError("Historical data payload missing entities list")

    export_date = payload.get("exportDate")
    table_name = payload.get("tableName")
    name_filter = payload.get("nameFilter")
    total_entities = payload.get("totalEntities")
    imported_at = datetime.now(timezone.utc).isoformat()

    counts = {
        "processed": 0,
        "skipped": 0,
        "inserted": 0,
        "upserted": 0,
        "matched": 0,
        "modified": 0,
    }
    ops = []

    for entity in entities:
        if not isinstance(entity, dict):
            counts["skipped"] += 1
            continue
        doc = dict(entity)
        doc["imported_at"] = imported_at
        doc["source_url"] = source_url
        doc["exportDate"] = export_date
        doc["tableName"] = table_name
        doc["nameFilter"] = name_filter
        doc["totalEntities"] = total_entities

        if parse_data:
            data_value = doc.get("data")
            if isinstance(data_value, str):
                try:
                    doc["data"] = json.loads(data_value)
                except json.JSONDecodeError:
                    pass

        filter_doc = None
        partition_key = doc.get("PartitionKey")
        row_key = doc.get("RowKey")
        if partition_key is not None and row_key is not None:
            filter_doc = {"PartitionKey": partition_key, "RowKey": row_key}
        elif doc.get("name") is not None:
            filter_doc = {"name": doc["name"]}
        elif doc.get("dataHash") is not None:
            filter_doc = {"dataHash": doc["dataHash"]}

        if upsert and filter_doc is not None:
            ops.append(pymongo.UpdateOne(filter_doc, {"$set": doc}, upsert=True))
        else:
            ops.append(pymongo.InsertOne(doc))
        counts["processed"] += 1

        if len(ops) >= batch_size:
            result = await asyncio.to_thread(
                _mongo_history_collection.bulk_write,
                ops,
                ordered=False,
            )
            counts["inserted"] += result.inserted_count
            counts["upserted"] += result.upserted_count
            counts["matched"] += result.matched_count
            counts["modified"] += result.modified_count
            ops = []

    if ops:
        result = await asyncio.to_thread(
            _mongo_history_collection.bulk_write,
            ops,
            ordered=False,
        )
        counts["inserted"] += result.inserted_count
        counts["upserted"] += result.upserted_count
        counts["matched"] += result.matched_count
        counts["modified"] += result.modified_count

    return counts


def _build_poll_params() -> Optional[list[tuple[str, str]]]:
    if FETCH_ENDPOINT != "monitor":
        return []
    rbls = _parse_rbls(FETCH_RBLS)
    if not rbls:
        return None
    params: list[tuple[str, str]] = []
    _add_params(params, "rbl", rbls)
    return params


async def _poll_loop() -> None:
    while True:
        params = _build_poll_params()
        if params is not None:
            await _fetch_and_store(FETCH_ENDPOINT, params)
        await asyncio.sleep(FETCH_INTERVAL)


@app.get("/")
def root() -> dict[str, str]:
    return {"service": "wiener-linien-ogd-realtime-proxy", "base_url": BASE_URL}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/monitor")
async def monitor(
    rbl: list[int] = Query(..., min_items=1),
    activateTrafficInfo: Optional[list[str]] = Query(None),
) -> Response:
    params: list[tuple[str, str]] = []
    _add_params(params, "rbl", rbl)
    _add_params(params, "activateTrafficInfo", activateTrafficInfo)
    return await _proxy("monitor", params)


@app.get("/trafficInfoList")
async def traffic_info_list(
    relatedLine: Optional[list[str]] = Query(None),
    relatedStop: Optional[list[str]] = Query(None),
    name: Optional[list[str]] = Query(None),
) -> Response:
    params: list[tuple[str, str]] = []
    _add_params(params, "relatedLine", relatedLine)
    _add_params(params, "relatedStop", relatedStop)
    _add_params(params, "name", name)
    return await _proxy("trafficInfoList", params)


@app.get("/trafficInfo")
async def traffic_info(
    name: list[str] = Query(..., min_items=1),
) -> Response:
    params: list[tuple[str, str]] = []
    _add_params(params, "name", name)
    return await _proxy("trafficInfo", params)


@app.get("/newsList")
async def news_list(
    relatedLine: Optional[list[str]] = Query(None),
    relatedStop: Optional[list[str]] = Query(None),
    name: Optional[list[str]] = Query(None),
) -> Response:
    params: list[tuple[str, str]] = []
    _add_params(params, "relatedLine", relatedLine)
    _add_params(params, "relatedStop", relatedStop)
    _add_params(params, "name", name)
    return await _proxy("newsList", params)


@app.get("/news")
async def news(
    name: list[str] = Query(..., min_items=1),
) -> Response:
    params: list[tuple[str, str]] = []
    _add_params(params, "name", name)
    return await _proxy("news", params)


@app.post("/historical/import")
async def historical_import(
    source_url: str = Query(HISTORICAL_URL),
    parse_data: bool = Query(True),
    upsert: bool = Query(True),
    batch_size: int = Query(HISTORICAL_BATCH_SIZE, ge=1, le=5000),
) -> dict[str, object]:
    if _client is None:
        raise HTTPException(status_code=503, detail="HTTP client not initialized")
    try:
        _ensure_mongo()
    except pymongo.errors.PyMongoError as exc:
        raise HTTPException(status_code=503, detail=f"MongoDB connection failed: {exc}") from exc

    try:
        response = await _client.get(source_url)
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"Upstream request failed: {exc}") from exc
    if response.status_code >= 400:
        raise HTTPException(
            status_code=response.status_code,
            detail=f"Historical fetch failed with status {response.status_code}",
        )

    try:
        payload = response.json()
    except ValueError as exc:
        raise HTTPException(
            status_code=502,
            detail="Historical data response is not valid JSON",
        ) from exc
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=502,
            detail="Historical data response must be a JSON object",
        )

    try:
        counts = await _ingest_historical_payload(
            payload,
            source_url=source_url,
            parse_data=parse_data,
            upsert=upsert,
            batch_size=batch_size,
        )
    except ValueError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except pymongo.errors.BulkWriteError as exc:
        raise HTTPException(status_code=500, detail="Bulk write failed") from exc
    except pymongo.errors.PyMongoError as exc:
        raise HTTPException(status_code=503, detail=f"MongoDB write failed: {exc}") from exc

    return {
        "source_url": source_url,
        "collection": HISTORICAL_COLLECTION,
        "exportDate": payload.get("exportDate"),
        "tableName": payload.get("tableName"),
        "totalEntities": payload.get("totalEntities"),
        "processed": counts["processed"],
        "skipped": counts["skipped"],
        "inserted": counts["inserted"],
        "upserted": counts["upserted"],
        "matched": counts["matched"],
        "modified": counts["modified"],
    }
