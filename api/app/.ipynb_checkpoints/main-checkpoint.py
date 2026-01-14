from __future__ import annotations

import os
from typing import Iterable, Optional

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, Response

BASE_URL = os.getenv("WL_BASE_URL", "https://www.wienerlinien.at/ogd_realtime/")
BASE_URL = BASE_URL.rstrip("/") + "/"
DEFAULT_TIMEOUT = float(os.getenv("WL_TIMEOUT", "30"))
USER_AGENT = os.getenv("WL_USER_AGENT", "WeatherAndDelay/1.0")

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


@app.on_event("startup")
async def _startup() -> None:
    global _client
    _client = httpx.AsyncClient(timeout=DEFAULT_TIMEOUT, headers=DEFAULT_HEADERS)


@app.on_event("shutdown")
async def _shutdown() -> None:
    if _client is not None:
        await _client.aclose()


def _resolve_sender(sender: Optional[str]) -> str:
    value = sender or os.getenv("WL_SENDER")
    if not value:
        raise HTTPException(
            status_code=400,
            detail="sender is required (query param or WL_SENDER env var)",
        )
    return value


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
    sender: Optional[str] = None,
) -> Response:
    sender_value = _resolve_sender(sender)
    params: list[tuple[str, str]] = [("sender", sender_value)]
    _add_params(params, "rbl", rbl)
    _add_params(params, "activateTrafficInfo", activateTrafficInfo)
    return await _proxy("monitor", params)


@app.get("/trafficInfoList")
async def traffic_info_list(
    relatedLine: Optional[list[str]] = Query(None),
    relatedStop: Optional[list[str]] = Query(None),
    name: Optional[list[str]] = Query(None),
    sender: Optional[str] = None,
) -> Response:
    sender_value = _resolve_sender(sender)
    params: list[tuple[str, str]] = [("sender", sender_value)]
    _add_params(params, "relatedLine", relatedLine)
    _add_params(params, "relatedStop", relatedStop)
    _add_params(params, "name", name)
    return await _proxy("trafficInfoList", params)


@app.get("/trafficInfo")
async def traffic_info(
    name: list[str] = Query(..., min_items=1),
    sender: Optional[str] = None,
) -> Response:
    sender_value = _resolve_sender(sender)
    params: list[tuple[str, str]] = [("sender", sender_value)]
    _add_params(params, "name", name)
    return await _proxy("trafficInfo", params)


@app.get("/newsList")
async def news_list(
    relatedLine: Optional[list[str]] = Query(None),
    relatedStop: Optional[list[str]] = Query(None),
    name: Optional[list[str]] = Query(None),
    sender: Optional[str] = None,
) -> Response:
    sender_value = _resolve_sender(sender)
    params: list[tuple[str, str]] = [("sender", sender_value)]
    _add_params(params, "relatedLine", relatedLine)
    _add_params(params, "relatedStop", relatedStop)
    _add_params(params, "name", name)
    return await _proxy("newsList", params)


@app.get("/news")
async def news(
    name: list[str] = Query(..., min_items=1),
    sender: Optional[str] = None,
) -> Response:
    sender_value = _resolve_sender(sender)
    params: list[tuple[str, str]] = [("sender", sender_value)]
    _add_params(params, "name", name)
    return await _proxy("news", params)
