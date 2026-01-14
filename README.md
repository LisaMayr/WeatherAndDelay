# WeatherAndDelay
Final Project: Implementation of a Data Science project across all 4 layers (Data Source Layer, Data Storage Layer, Processing Layer, Data Output Layer)

## Wiener Linien Realtime API
This repo includes a small FastAPI proxy for the Wiener Linien OGD realtime endpoints.
It runs from a notebook inside the Jupyter container.

### Run via docker-compose
1) `docker-compose up --build jupyter`
2) Open Jupyter at `http://localhost:8888`, open `notebooks/wienerlinien_api.ipynb`, and run the cells.
3) The API is available at `http://localhost:8000`.
4) Use the stop cell in the notebook to shut down the API without stopping Jupyter.

### Endpoints (proxy to `WL_BASE_URL`)
- `GET /monitor?rbl=...&activateTrafficInfo=...`
- `GET /trafficInfoList?relatedLine=...&relatedStop=...&name=...`
- `GET /trafficInfo?name=...`
- `GET /newsList?relatedLine=...&relatedStop=...&name=...`
- `GET /news?name=...`

Notes:
- Repeat query params for lists, e.g. `rbl=123&rbl=124`.
- If HTTPS does not work in your environment, set `WL_BASE_URL` to `http://www.wienerlinien.at/ogd_realtime/`.
- Optional env vars: `WL_TIMEOUT` (seconds) and `WL_USER_AGENT`.
- The notebook imports the FastAPI app from `api/app/main.py` (mounted into the Jupyter container).
- Auto-fetch to MongoDB runs every 30 seconds by default. Set `FETCH_RBLS` (comma-separated RBLs) in `docker-compose.yml`.
- Data is stored in MongoDB database `big_data_austria`, collection `wienerlinien_realtime` (configurable via env vars).
