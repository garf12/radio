from __future__ import annotations

import asyncio
import logging
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from config import config
from database import init_db, insert_transcription, insert_alert
from stream_capture import read_chunks
from transcriber import transcribe_chunk
from analyzer import analyze_transcript
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


async def run_pipeline() -> None:
    global _pipeline_running, _pipeline_error, _last_transcription_time, _last_chunk_time, _chunks_processed, _silent_chunks

    _pipeline_running = True
    _pipeline_error = None
    transcript_window: deque[str] = deque(maxlen=4)  # ~4 chunks = ~2 min at 30s

    try:
        async for timestamp, audio in read_chunks(
            config.stream_url,
            config.chunk_duration_s,
            _stop_event,
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

                # Save to database
                row = await insert_transcription(config.db_path, text, duration)

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
                    result = await analyze_transcript(
                        combined,
                        config.openrouter_api_key,
                        config.analysis_model,
                        config.alert_sensitivity,
                    )
                    if result:
                        alert = await insert_alert(
                            config.db_path,
                            row["id"],
                            result["summary"],
                            result["severity"],
                            result["category"],
                            combined,
                            config.analysis_model,
                        )
                        await ws_manager.broadcast({
                            "type": "alert",
                            "data": alert,
                        })
                        logger.info(
                            "ALERT [%s/%s]: %s",
                            result["severity"],
                            result["category"],
                            result["summary"],
                        )

            except Exception:
                logger.exception("Error processing chunk")

    except Exception as e:
        _pipeline_error = str(e)
        logger.exception("Pipeline error")
    finally:
        _pipeline_running = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db(config.db_path)
    logger.info("Database initialized at %s", config.db_path)

    task = asyncio.create_task(run_pipeline())
    logger.info("Pipeline started")

    yield

    _stop_event.set()
    task.cancel()
    try:
        await task
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
