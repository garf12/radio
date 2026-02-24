from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse

from config import config
from database import get_transcriptions, get_transcription, get_alerts, get_counts, get_events, get_event_with_alerts, get_events_with_location
from analyzer import fetch_models, get_base_prompt

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


@router.get("/events")
async def list_events(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    status: str = Query(None),
):
    rows = await get_events(config.db_path, limit, offset, status)
    return {"events": rows}


@router.get("/events/map")
async def list_map_events(
    status: str = Query(None),
    hours: float = Query(None, gt=0),
    limit: int = Query(200, ge=1, le=500),
):
    since = None
    if hours is not None:
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    rows = await get_events_with_location(config.db_path, limit, status if status else None, since)
    return {"events": rows}


@router.get("/events/{event_id}")
async def get_event(event_id: int):
    event = await get_event_with_alerts(config.db_path, event_id)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    return event


@router.get("/audio/{transcription_id}")
async def get_audio(transcription_id: int):
    row = await get_transcription(config.db_path, transcription_id)
    if not row:
        raise HTTPException(status_code=404, detail="Transcription not found")

    audio_file = row.get("audio_file")
    if not audio_file:
        raise HTTPException(status_code=404, detail="No audio file for this transcription")

    file_path = os.path.join(config.audio_dir, audio_file)
    if not os.path.isfile(file_path):
        raise HTTPException(status_code=404, detail="Audio file not found on disk")

    return FileResponse(file_path, media_type="audio/wav", filename=audio_file)


@router.get("/stream")
async def stream_audio():
    if not config.stream_url:
        raise HTTPException(status_code=503, detail="Stream URL not configured")

    client = httpx.AsyncClient(timeout=None, follow_redirects=True)
    try:
        upstream = await client.send(
            client.build_request("GET", config.stream_url),
            stream=True,
        )
    except Exception:
        await client.aclose()
        raise HTTPException(status_code=502, detail="Failed to connect to audio stream")

    content_type = upstream.headers.get("content-type", "audio/mpeg")

    async def generate():
        try:
            async for chunk in upstream.aiter_bytes(4096):
                yield chunk
        finally:
            await upstream.aclose()
            await client.aclose()

    return StreamingResponse(
        generate(),
        media_type=content_type,
        headers={"Cache-Control": "no-cache, no-store"},
    )


@router.get("/config/maps")
async def get_maps_config():
    return {
        "google_maps_api_key": config.google_maps_api_key,
        "map_default_lat": config.map_default_lat,
        "map_default_lng": config.map_default_lng,
    }


@router.get("/config")
async def get_config():
    result = config.to_dict()
    result["system_prompt"] = get_base_prompt()
    return result


@router.put("/config")
async def update_config(body: dict):
    config.update(body)
    from main import restart_pipeline
    await restart_pipeline()
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
