from __future__ import annotations

import asyncio
import logging
import os
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from config import config
from database import (
    init_db, insert_transcription, insert_alert,
    get_active_events, insert_event, update_event, link_alert_to_event, get_event_with_alerts,
)
from stream_capture import read_chunks, audio_to_wav_bytes
from transcriber import transcribe_chunk
from analyzer import analyze_transcript
from geocoder import geocode_location
from websocket_manager import ws_manager
from routes.api import router as api_router
from routes.ws import router as ws_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Pipeline state
_pipeline_running = False
_pipeline_error: str | None = None
_stop_event = asyncio.Event()
_pipeline_task: asyncio.Task | None = None
_last_transcription_time: str | None = None
_last_chunk_time: str | None = None
_chunks_processed: int = 0
_silent_chunks: int = 0


def pipeline_status() -> dict:
    return {
        "running": _pipeline_running,
        "error": _pipeline_error,
        "last_transcription": _last_transcription_time,
        "last_chunk": _last_chunk_time,
        "chunks_processed": _chunks_processed,
        "silent_chunks": _silent_chunks,
    }


async def _broadcast_audio_level(rms: float, peak: float) -> None:
    """Broadcast audio level to all WebSocket clients."""
    await ws_manager.broadcast({
        "type": "audio_level",
        "data": {
            "rms": round(rms, 4),
            "peak": round(peak, 4),
        },
    })


async def restart_pipeline() -> None:
    """Stop and restart the pipeline (e.g. after config changes)."""
    global _pipeline_task, _stop_event

    if _pipeline_task and not _pipeline_task.done():
        _stop_event.set()
        _pipeline_task.cancel()
        try:
            await _pipeline_task
        except asyncio.CancelledError:
            pass

    _stop_event = asyncio.Event()
    _pipeline_task = asyncio.create_task(run_pipeline())
    logger.info("Pipeline restarted with new config")


async def run_pipeline() -> None:
    global _pipeline_running, _pipeline_error, _last_transcription_time, _last_chunk_time, _chunks_processed, _silent_chunks

    _pipeline_running = True
    _pipeline_error = None
    transcript_window: deque[str] = deque(maxlen=4)  # ~4 chunks = ~2 min at 30s
    recent_alerts: deque[str] = deque(maxlen=10)  # track recent alert summaries to avoid duplicates

    # Ensure audio directory exists
    os.makedirs(config.audio_dir, exist_ok=True)

    try:
        async for timestamp, audio, raw_pcm in read_chunks(
            config.stream_url,
            config.chunk_duration_s,
            _stop_event,
            on_audio_level=_broadcast_audio_level,
        ):
            try:
                _chunks_processed += 1
                _last_chunk_time = datetime.now(timezone.utc).isoformat()

                text, duration = await transcribe_chunk(audio, config.whisper_model)
                if not text.strip():
                    _silent_chunks += 1
                    await ws_manager.broadcast({
                        "type": "status",
                        "data": {
                            "chunks_processed": _chunks_processed,
                            "silent_chunks": _silent_chunks,
                            "last_chunk": _last_chunk_time,
                        },
                    })
                    continue

                _last_transcription_time = datetime.now(timezone.utc).isoformat()

                # Save WAV file for non-silent chunks
                wav_filename = f"{timestamp.replace(':', '-')}_{_chunks_processed}.wav"
                wav_path = os.path.join(config.audio_dir, wav_filename)
                wav_data = audio_to_wav_bytes(raw_pcm)
                await asyncio.to_thread(_write_file, wav_path, wav_data)

                # Save to database
                row = await insert_transcription(config.db_path, text, duration, audio_file=wav_filename)

                # Broadcast to WebSocket clients
                await ws_manager.broadcast({
                    "type": "transcription",
                    "data": row,
                })

                # Add to rolling window for analysis
                transcript_window.append(text)

                # Analyze rolling window
                if config.openrouter_api_key:
                    combined = "\n".join(transcript_window)
                    active_events = await get_active_events(config.db_path)
                    result = await analyze_transcript(
                        combined,
                        config.openrouter_api_key,
                        config.analysis_model,
                        config.alert_sensitivity,
                        recent_alerts=list(recent_alerts),
                        custom_instructions=config.custom_instructions,
                        active_events=active_events,
                    )
                    if result:
                        recent_alerts.append(result["summary"])
                        alert = await insert_alert(
                            config.db_path,
                            row["id"],
                            result["summary"],
                            result["severity"],
                            result["category"],
                            combined,
                            config.analysis_model,
                        )

                        # Geocode location if available
                        location_text = result.get("location", "")
                        coords = None
                        logger.info("Processing alert: location=%r, geocoding=%s",
                                    location_text, bool(location_text and config.google_maps_api_key))
                        if location_text and config.google_maps_api_key:
                            coords = await geocode_location(
                                location_text, config.google_maps_api_key,
                                config.db_path, config.map_default_lat, config.map_default_lng,
                            )

                        # Event tracking: link alert to existing or new event
                        active_ids = {e["id"] for e in active_events}
                        llm_event_id = result.get("event_id")
                        if llm_event_id and llm_event_id in active_ids:
                            await update_event(
                                config.db_path,
                                llm_event_id,
                                severity=result["severity"],
                                status=result.get("event_status", "active"),
                                location_text=location_text if location_text else None,
                                latitude=coords[0] if coords else None,
                                longitude=coords[1] if coords else None,
                            )
                            await link_alert_to_event(config.db_path, alert["id"], llm_event_id)
                            event_id = llm_event_id
                        else:
                            title = result.get("event_title") or result["summary"][:80]
                            ev = await insert_event(
                                config.db_path,
                                title,
                                result["category"],
                                result["severity"],
                                location_text=location_text if location_text else None,
                                latitude=coords[0] if coords else None,
                                longitude=coords[1] if coords else None,
                            )
                            await link_alert_to_event(config.db_path, alert["id"], ev["id"])
                            event_id = ev["id"]

                        # Fetch full event with nested alerts for broadcast
                        full_event = await get_event_with_alerts(config.db_path, event_id)

                        await ws_manager.broadcast({
                            "type": "alert",
                            "data": alert,
                        })
                        await ws_manager.broadcast({
                            "type": "event",
                            "data": full_event,
                        })
                        logger.info(
                            "ALERT [%s/%s] event=%d: %s",
                            result["severity"],
                            result["category"],
                            event_id,
                            result["summary"],
                        )

            except Exception:
                logger.exception("Error processing chunk")

    except Exception as e:
        _pipeline_error = str(e)
        logger.exception("Pipeline error")
    finally:
        _pipeline_running = False


def _write_file(path: str, data: bytes) -> None:
    with open(path, "wb") as f:
        f.write(data)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pipeline_task

    init_db(config.db_path)
    config.load_saved()
    logger.info("Database initialized at %s", config.db_path)

    _pipeline_task = asyncio.create_task(run_pipeline())
    logger.info("Pipeline started")

    yield

    _stop_event.set()
    if _pipeline_task:
        _pipeline_task.cancel()
        try:
            await _pipeline_task
        except asyncio.CancelledError:
            pass
    logger.info("Pipeline stopped")


app = FastAPI(title="Scanner Monitor", lifespan=lifespan)
app.include_router(api_router)
app.include_router(ws_router)
app.mount("/", StaticFiles(directory="static", html=True), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=config.host, port=config.port)
