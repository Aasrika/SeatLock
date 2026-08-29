"""GET /metrics -- Prometheus text-format exposition.

Thin: the actual aggregation-across-workers logic lives in
app/infra/metrics.py (multiprocess mode is required with more than one
uvicorn worker; see that module's docstring for why).
"""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST

from app.infra.metrics import render_metrics_text

router = APIRouter()


@router.get("/metrics")
async def metrics() -> Response:
    return Response(content=render_metrics_text(), media_type=CONTENT_TYPE_LATEST)
