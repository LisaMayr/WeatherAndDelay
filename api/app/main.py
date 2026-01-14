from __future__ import annotations

import asyncio
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
_poll_task: Optional[asyncio.Task] = None


@app.on_event("startup")
async def _startup() -> None:
    global _client, _mongo_client, _mongo_collection, _poll_task
    _client = httpx.AsyncClient(timeout=DEFAULT_TIMEOUT, headers=DEFAULT_HEADERS)
    if FETCH_ENABLED:
        _mongo_client = pymongo.MongoClient(MONGO_URI)
        _mongo_collection = _mongo_client[MONGO_DB][MONGO_COLLECTION]
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
