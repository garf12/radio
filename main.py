from __future__ import annotations

import asyncio
import logging
import os
import time as _time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from config import config
from database import (
    init_db, seed_default_stream, get_streams,
    insert_transcription, insert_alert,
    get_active_events, insert_event, update_event, link_alert_to_event, get_event_with_alerts,
    auto_resolve_stale_events, find_matching_event,
    get_recent_transcriptions, insert_summary,
)
from stream_capture import read_chunks, audio_to_wav_bytes
from transcriber import transcribe_chunk
from analyzer import analyze_transcript, generate_summary
from geocoder import geocode_location
from webhook import send_webhook_alert
from websocket_manager import ws_manager
from routes.api import router as api_router
from routes.ws import router as ws_router
import pipeline_state

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _write_file(path: str, data: bytes) -> None:
    with open(path, "wb") as f:
        f.write(data)


# ---------------------------------------------------------------------------
# StreamManager — one pipeline task per enabled stream
# ---------------------------------------------------------------------------

class StreamManager:
    def __init__(self):
        self._tasks: dict[str, asyncio.Task] = {}          # stream_id -> pipeline task
        self._stop_events: dict[str, asyncio.Event] = {}   # stream_id -> stop event
        self._periodic_task: asyncio.Task | None = None

    async def start_all(self):
        """Query enabled streams from DB and start each."""
        streams = await get_streams(config.db_path, enabled_only=True)
        for s in streams:
            await self.start_stream(s["id"], s["url"], s["name"])
        self._periodic_task = asyncio.create_task(self._run_periodic_tasks())
        logger.info("StreamManager started %d stream(s)", len(streams))

    async def start_stream(self, stream_id: str, url: str, name: str):
        if stream_id in self._tasks and not self._tasks[stream_id].done():
            return  # already running
        stop_event = asyncio.Event()
        self._stop_events[stream_id] = stop_event
        task = asyncio.create_task(
            self._run_stream_pipeline(stream_id, url, name, stop_event)
        )
        self._tasks[stream_id] = task
        logger.info("Started pipeline for stream %s (%s)", stream_id, name)

    async def stop_stream(self, stream_id: str):
        stop_event = self._stop_events.pop(stream_id, None)
        task = self._tasks.pop(stream_id, None)
        if stop_event:
            stop_event.set()
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        pipeline_state.remove_state(stream_id)
        logger.info("Stopped pipeline for stream %s", stream_id)

    async def stop_all(self):
        for sid in list(self._tasks.keys()):
            await self.stop_stream(sid)
        if self._periodic_task:
            self._periodic_task.cancel()
            try:
                await self._periodic_task
            except asyncio.CancelledError:
                pass
            self._periodic_task = None

    async def restart_stream(self, stream_id: str):
        await self.stop_stream(stream_id)
        from database import get_stream
        s = await get_stream(config.db_path, stream_id)
        if s and s["enabled"]:
            await self.start_stream(s["id"], s["url"], s["name"])

    async def sync_streams(self):
        """Diff running vs DB, start/stop as needed."""
        db_streams = await get_streams(config.db_path, enabled_only=False)
        db_map = {s["id"]: s for s in db_streams}
        enabled_ids = {s["id"] for s in db_streams if s["enabled"]}
        running_ids = {sid for sid, t in self._tasks.items() if not t.done()}

        # Stop streams that are no longer enabled or deleted
        for sid in running_ids - enabled_ids:
            await self.stop_stream(sid)

        # Start streams that should be running but aren't
        for sid in enabled_ids - running_ids:
            s = db_map[sid]
            await self.start_stream(s["id"], s["url"], s["name"])

        # Restart streams whose URL changed
        for sid in enabled_ids & running_ids:
            # We can't easily check URL without storing it, so just let it be
            pass

    # --- Per-stream pipeline ---

    async def _run_stream_pipeline(
        self, stream_id: str, stream_url: str, stream_name: str, stop_event: asyncio.Event
    ):
        ps = pipeline_state.get_state(stream_id)
        ps["running"] = True
        ps["error"] = None
        ps["chunks_processed"] = 0
        ps["silent_chunks"] = 0
        ps["last_transcription"] = None
        ps["last_chunk"] = None

        os.makedirs(config.audio_dir, exist_ok=True)

        async def broadcast_audio_level(rms: float, peak: float):
            await ws_manager.broadcast({
                "type": "audio_level",
                "data": {
                    "rms": round(rms, 4),
                    "peak": round(peak, 4),
                    "stream_id": stream_id,
                },
            })

        async def broadcast_vad_state(vad_state_name: str):
            ps["vad_state"] = vad_state_name
            await ws_manager.broadcast({
                "type": "vad_state",
                "data": {
                    "state": vad_state_name,
                    "stream_id": stream_id,
                },
            })

        restart_backoff = 5

        while not stop_event.is_set():
            # Reset per-attempt state
            TRANSCRIPT_WINDOW_S = 120.0
            transcript_window: list[tuple[float, str]] = []
            recent_alerts: list[str] = []
            COOLDOWN_S = 60.0
            last_alert_time: float = 0.0

            try:
                async for timestamp, audio, raw_pcm in read_chunks(
                    stream_url,
                    stop_event=stop_event,
                    on_audio_level=broadcast_audio_level,
                    on_vad_state=broadcast_vad_state,
                    vad_threshold=config.vad_threshold,
                    vad_grace_period_s=config.vad_grace_period_s,
                    vad_pre_roll_s=config.vad_pre_roll_s,
                    max_chunk_duration_s=config.max_chunk_duration_s,
                    min_chunk_duration_s=config.min_chunk_duration_s,
                ):
                    try:
                        restart_backoff = 5  # reset on successful chunk
                        ps["chunks_processed"] += 1
                        ps["last_chunk"] = datetime.now(timezone.utc).isoformat()

                        result = await transcribe_chunk(audio, config.whisper_model)
                        text = result["text"]
                        duration = result["duration"]
                        confidence = result["confidence"]
                        flags = result["flags"]

                        if not text.strip():
                            ps["silent_chunks"] += 1
                            await ws_manager.broadcast({
                                "type": "status",
                                "data": {
                                    "chunks_processed": ps["chunks_processed"],
                                    "silent_chunks": ps["silent_chunks"],
                                    "last_chunk": ps["last_chunk"],
                                    "stream_id": stream_id,
                                },
                            })
                            continue

                        ps["last_transcription"] = datetime.now(timezone.utc).isoformat()

                        wav_filename = f"{stream_id}_{timestamp.replace(':', '-')}_{ps['chunks_processed']}.wav"
                        wav_path = os.path.join(config.audio_dir, wav_filename)
                        wav_data = audio_to_wav_bytes(raw_pcm)
                        await asyncio.to_thread(_write_file, wav_path, wav_data)

                        from text_corrector import apply_regional_dictionary
                        corrected_text, corrections_applied = await apply_regional_dictionary(config.db_path, text)
                        if corrections_applied:
                            logger.info("[%s] Dictionary corrections applied: %s", stream_id, corrections_applied)
                        analysis_text = corrected_text

                        row = await insert_transcription(
                            config.db_path, text, duration, audio_file=wav_filename,
                            confidence=confidence, flags=flags, segment_details=result["segment_details"],
                            stream_id=stream_id,
                        )

                        await ws_manager.broadcast({
                            "type": "transcription",
                            "data": row,
                        })

                        # Time-based transcript window
                        now_mono = _time.monotonic()
                        transcript_window.append((now_mono, analysis_text))
                        # Evict entries older than window
                        cutoff = now_mono - TRANSCRIPT_WINDOW_S
                        transcript_window = [
                            (t, txt) for t, txt in transcript_window if t >= cutoff
                        ]

                        # Time-based cooldown
                        in_cooldown = (now_mono - last_alert_time) < COOLDOWN_S
                        if in_cooldown:
                            logger.debug("[%s] Skipping analysis, cooldown active (%.0fs remaining)",
                                         stream_id, COOLDOWN_S - (now_mono - last_alert_time))
                        elif config.openrouter_api_key:
                            combined = "\n".join(txt for _, txt in transcript_window)
                            active_events = await get_active_events(config.db_path, stream_id=stream_id)
                            analysis_result = await analyze_transcript(
                                combined,
                                config.openrouter_api_key,
                                config.analysis_model,
                                config.alert_sensitivity,
                                recent_alerts=recent_alerts[-10:],
                                custom_instructions=config.custom_instructions,
                                active_events=active_events,
                                db_path=config.db_path,
                            )
                            if analysis_result:
                                from scanner_vocab import detect_analysis_contradictions
                                contradictions = detect_analysis_contradictions(combined, analysis_result)
                                if contradictions:
                                    logger.warning("[%s] Analysis contradictions: %s", stream_id, [c["message"] for c in contradictions])

                                recent_alerts.append(analysis_result["summary"])
                                if len(recent_alerts) > 10:
                                    recent_alerts = recent_alerts[-10:]
                                alert = await insert_alert(
                                    config.db_path,
                                    row["id"],
                                    analysis_result["summary"],
                                    analysis_result["severity"],
                                    analysis_result["category"],
                                    combined,
                                    config.analysis_model,
                                    stream_id=stream_id,
                                )

                                location_text = analysis_result.get("location", "")
                                coords = None
                                if location_text and config.google_maps_api_key:
                                    coords = await geocode_location(
                                        location_text, config.google_maps_api_key,
                                        config.db_path, config.map_default_lat, config.map_default_lng,
                                        region_hint=config.geocode_region,
                                        max_radius_km=config.geocode_max_radius_km,
                                    )

                                active_ids = {e["id"] for e in active_events}
                                llm_event_id = analysis_result.get("event_id")
                                if llm_event_id is not None:
                                    try:
                                        llm_event_id = int(llm_event_id)
                                    except (ValueError, TypeError):
                                        llm_event_id = None
                                if llm_event_id and llm_event_id in active_ids:
                                    await update_event(
                                        config.db_path,
                                        llm_event_id,
                                        severity=analysis_result["severity"],
                                        status=analysis_result.get("event_status", "active"),
                                        location_text=location_text if location_text else None,
                                        latitude=coords[0] if coords else None,
                                        longitude=coords[1] if coords else None,
                                    )
                                    await link_alert_to_event(config.db_path, alert["id"], llm_event_id)
                                    event_id = llm_event_id
                                else:
                                    match = await find_matching_event(
                                        config.db_path, analysis_result["category"], location_text or "",
                                        alert_summary=analysis_result["summary"],
                                        stream_id=stream_id,
                                    )
                                    if match:
                                        await update_event(
                                            config.db_path,
                                            match["id"],
                                            severity=analysis_result["severity"],
                                            status=analysis_result.get("event_status", "active"),
                                            location_text=location_text if location_text else None,
                                            latitude=coords[0] if coords else None,
                                            longitude=coords[1] if coords else None,
                                        )
                                        await link_alert_to_event(config.db_path, alert["id"], match["id"])
                                        event_id = match["id"]
                                        logger.info("[%s] Fallback matched alert to existing event %d", stream_id, match["id"])
                                    else:
                                        title = analysis_result.get("event_title") or analysis_result["summary"][:80]
                                        ev = await insert_event(
                                            config.db_path,
                                            title,
                                            analysis_result["category"],
                                            analysis_result["severity"],
                                            location_text=location_text if location_text else None,
                                            latitude=coords[0] if coords else None,
                                            longitude=coords[1] if coords else None,
                                            stream_id=stream_id,
                                        )
                                        await link_alert_to_event(config.db_path, alert["id"], ev["id"])
                                        event_id = ev["id"]

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
                                    "[%s] ALERT [%s/%s] event=%d: %s",
                                    stream_id,
                                    analysis_result["severity"],
                                    analysis_result["category"],
                                    event_id,
                                    analysis_result["summary"],
                                )

                                if config.webhook_url and analysis_result["category"] == "shooting":
                                    await send_webhook_alert(config.webhook_url, alert, full_event)

                                last_alert_time = _time.monotonic()

                    except Exception:
                        logger.exception("[%s] Error processing chunk", stream_id)

                # read_chunks exited cleanly (stop_event set or stream ended)
                break

            except asyncio.CancelledError:
                break
            except Exception as e:
                ps["error"] = str(e)
                logger.exception("[%s] Pipeline error, restarting in %ds", stream_id, restart_backoff)
                await asyncio.sleep(restart_backoff)
                restart_backoff = min(restart_backoff * 2, 60)
                continue

        ps["running"] = False

    # --- Periodic tasks (shared across all streams) ---

    async def _run_periodic_tasks(self):
        last_stale_check: datetime | None = None
        last_summary_times: dict[str, datetime] = {}    # stream_id -> last 10min summary
        last_hourly_times: dict[str, datetime] = {}     # stream_id -> last hourly summary

        try:
            while True:
                await asyncio.sleep(60)  # check every minute

                # Health check: restart dead pipeline tasks
                for sid, task in list(self._tasks.items()):
                    if task.done() and sid in self._stop_events and not self._stop_events[sid].is_set():
                        logger.warning("Pipeline %s died unexpectedly, restarting...", sid)
                        exc = task.exception() if not task.cancelled() else None
                        if exc:
                            logger.warning("Pipeline %s died with: %s", sid, exc)
                        from database import get_stream
                        s = await get_stream(config.db_path, sid)
                        if s and s["enabled"]:
                            # Clean up old entries
                            self._tasks.pop(sid, None)
                            self._stop_events.pop(sid, None)
                            await self.start_stream(s["id"], s["url"], s["name"])

                # Auto-resolve stale events (global)
                try:
                    now = datetime.now(timezone.utc)
                    if config.event_timeout_minutes > 0:
                        if not last_stale_check or (now - last_stale_check).total_seconds() >= 300:
                            last_stale_check = now
                            resolved = await auto_resolve_stale_events(config.db_path, config.event_timeout_minutes)
                            for ev in resolved:
                                full_event = await get_event_with_alerts(config.db_path, ev["id"])
                                await ws_manager.broadcast({"type": "event", "data": full_event})
                                logger.info("Auto-resolved stale event %d: %s", ev["id"], ev.get("title", ""))
                except Exception:
                    logger.exception("Error in stale event check")

                # Per-stream summaries
                if not config.openrouter_api_key:
                    continue

                streams = await get_streams(config.db_path, enabled_only=True)
                now = datetime.now(timezone.utc)

                for s in streams:
                    sid = s["id"]

                    # 10-minute summary
                    try:
                        last_10 = last_summary_times.get(sid)
                        if not last_10 or (now - last_10).total_seconds() >= 300:
                            last_summary_times[sid] = now
                            transcripts = await get_recent_transcriptions(config.db_path, minutes=10, stream_id=sid)
                            if transcripts:
                                active_events = await get_active_events(config.db_path, stream_id=sid)
                                result = await generate_summary(
                                    transcripts, config.openrouter_api_key, config.analysis_model,
                                    active_events=active_events, period="10min",
                                )
                                if result:
                                    period_start = transcripts[0]["timestamp"]
                                    period_end = transcripts[-1]["timestamp"]
                                    event_refs = [e["id"] for e in active_events] if active_events else []
                                    summary = await insert_summary(
                                        config.db_path, result["summary_text"], period_start, period_end,
                                        len(transcripts), event_refs, result["key_themes"],
                                        result["activity_level"], config.analysis_model,
                                        summary_type="10min", stream_id=sid,
                                    )
                                    await ws_manager.broadcast({"type": "summary", "data": summary})
                                    logger.info("[%s] 10min summary generated: activity_level=%s", sid, result["activity_level"])
                    except Exception:
                        logger.exception("[%s] Error generating 10min summary", sid)

                    # Hourly summary
                    try:
                        last_h = last_hourly_times.get(sid)
                        if not last_h or (now - last_h).total_seconds() >= 3600:
                            last_hourly_times[sid] = now
                            transcripts = await get_recent_transcriptions(config.db_path, minutes=60, stream_id=sid)
                            if transcripts:
                                active_events = await get_active_events(config.db_path, stream_id=sid)
                                result = await generate_summary(
                                    transcripts, config.openrouter_api_key, config.analysis_model,
                                    active_events=active_events, period="hourly",
                                )
                                if result:
                                    period_start = transcripts[0]["timestamp"]
                                    period_end = transcripts[-1]["timestamp"]
                                    event_refs = [e["id"] for e in active_events] if active_events else []
                                    summary = await insert_summary(
                                        config.db_path, result["summary_text"], period_start, period_end,
                                        len(transcripts), event_refs, result["key_themes"],
                                        result["activity_level"], config.analysis_model,
                                        summary_type="hourly", stream_id=sid,
                                    )
                                    await ws_manager.broadcast({"type": "summary", "data": summary})
                                    logger.info("[%s] Hourly summary generated: activity_level=%s", sid, result["activity_level"])
                    except Exception:
                        logger.exception("[%s] Error generating hourly summary", sid)

        except asyncio.CancelledError:
            pass


# Singleton
stream_manager = StreamManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db(config.db_path)
    config.load_saved()
    await seed_default_stream(config.db_path, config._seed_stream_url)

    pipeline_state.restart_pipeline = stream_manager.sync_streams
    pipeline_state.restart_stream = stream_manager.restart_stream
    logger.info("Database initialized at %s", config.db_path)

    await stream_manager.start_all()
    logger.info("StreamManager started")

    yield

    await stream_manager.stop_all()
    logger.info("StreamManager stopped")


app = FastAPI(title="Scanner Monitor", lifespan=lifespan)
app.include_router(api_router)
app.include_router(ws_router)
app.mount("/", StaticFiles(directory="static", html=True), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=config.host, port=config.port)
