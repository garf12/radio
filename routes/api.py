from __future__ import annotations

from fastapi import APIRouter, Query

from config import config
from database import get_transcriptions, get_alerts, get_counts
from analyzer import fetch_models

router = APIRouter(prefix="/api")


@router.get("/transcriptions")
async def list_transcriptions(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    rows = await get_transcriptions(config.db_path, limit, offset)
    return {"transcriptions": rows}


@router.get("/alerts")
async def list_alerts(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    rows = await get_alerts(config.db_path, limit, offset)
    return {"alerts": rows}


@router.get("/config")
async def get_config():
    return config.to_dict()


@router.put("/config")
async def update_config(body: dict):
    config.update(body)
    return config.to_dict()


@router.get("/models")
async def list_models():
    models = await fetch_models(config.openrouter_api_key)
    return {"models": models}


@router.get("/status")
async def get_status():
    from main import pipeline_status
    from websocket_manager import ws_manager

    counts = await get_counts(config.db_path)
    return {
        "pipeline": pipeline_status(),
        "websocket_clients": ws_manager.connection_count,
        "counts": counts,
        "config": config.to_dict(),
    }
