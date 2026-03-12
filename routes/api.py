from __future__ import annotations

import asyncio
import os
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from starlette.background import BackgroundTask

import re

from config import config
from database import (
    get_transcriptions, get_transcription, get_alerts, get_counts,
    get_events, get_event_with_alerts, get_events_with_location,
    get_summaries, get_latest_summaries,
    get_review_queue, submit_transcription_correction, confirm_transcription,
    insert_alert_feedback, get_feedback_stats,
    get_dictionary_entries, upsert_dictionary_entry, delete_dictionary_entry,
    get_training_data, get_alert_training_data,
    get_streams, get_stream, create_stream, update_stream, delete_stream,
)
from analyzer import fetch_models, get_base_prompt

router = APIRouter(prefix="/api")


@router.get("/transcriptions")
async def list_transcriptions(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    stream_id: str = Query(None),
):
    rows = await get_transcriptions(config.db_path, limit, offset, stream_id=stream_id)
    return {"transcriptions": rows}


@router.get("/alerts")
async def list_alerts(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    stream_id: str = Query(None),
):
    rows = await get_alerts(config.db_path, limit, offset, stream_id=stream_id)
    return {"alerts": rows}


@router.get("/events")
async def list_events(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    status: str = Query(None),
    stream_id: str = Query(None),
):
    rows = await get_events(config.db_path, limit, offset, status, stream_id=stream_id)
    return {"events": rows}


@router.get("/events/map")
async def list_map_events(
    status: str = Query(None),
    hours: float = Query(None, gt=0),
    limit: int = Query(200, ge=1, le=500),
    stream_id: str = Query(None),
):
    since = None
    if hours is not None:
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    rows = await get_events_with_location(config.db_path, limit, status if status else None, since, stream_id=stream_id)
    return {"events": rows}


@router.get("/summaries/current")
async def current_summaries():
    return await get_latest_summaries(config.db_path)


@router.get("/summaries")
async def list_summaries(
    hours: float = Query(None, gt=0),
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0),
    stream_id: str = Query(None),
):
    rows = await get_summaries(config.db_path, hours, limit, offset, stream_id=stream_id)
    return {"summaries": rows}


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
async def stream_audio(stream_id: str = Query(None)):
    # Look up stream URL from DB
    url = None
    if stream_id:
        s = await get_stream(config.db_path, stream_id)
        if s:
            url = s["url"]
    if not url:
        # Fall back to first enabled stream
        streams = await get_streams(config.db_path, enabled_only=True)
        if streams:
            url = streams[0]["url"]
    if not url:
        raise HTTPException(status_code=503, detail="No stream URL configured")

    client = httpx.AsyncClient(timeout=None, follow_redirects=True)
    try:
        upstream = await client.send(
            client.build_request("GET", url),
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
    import pipeline_state
    if pipeline_state.restart_pipeline:
        await pipeline_state.restart_pipeline()
    return config.to_dict()


# --- Stream CRUD ---


@router.get("/streams")
async def list_streams_endpoint():
    streams = await get_streams(config.db_path)
    return {"streams": streams}


@router.post("/streams")
async def create_stream_endpoint(body: dict):
    import pipeline_state

    stream_id = body.get("id", "").strip().lower()
    name = body.get("name", "").strip()
    url = body.get("url", "").strip()

    if not stream_id or not name or not url:
        raise HTTPException(status_code=400, detail="id, name, and url are required")
    if not re.match(r'^[a-z0-9][a-z0-9_-]{0,29}$', stream_id):
        raise HTTPException(status_code=400, detail="id must be lowercase alphanumeric/hyphens/underscores, 1-30 chars")

    existing = await get_streams(config.db_path)
    if len(existing) >= 10:
        raise HTTPException(status_code=400, detail="Maximum 10 streams allowed")
    if any(s["id"] == stream_id for s in existing):
        raise HTTPException(status_code=409, detail="Stream ID already exists")

    color = body.get("color", "#00e89d")
    enabled = body.get("enabled", True)
    result = await create_stream(config.db_path, stream_id, name, url, color, enabled)

    if pipeline_state.restart_pipeline:
        await pipeline_state.restart_pipeline()
    return result


@router.put("/streams/{stream_id}")
async def update_stream_endpoint(stream_id: str, body: dict):
    import pipeline_state

    existing = await get_stream(config.db_path, stream_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Stream not found")

    kwargs = {}
    for key in ("name", "url", "color"):
        if key in body:
            kwargs[key] = body[key]
    if "enabled" in body:
        kwargs["enabled"] = 1 if body["enabled"] else 0

    result = await update_stream(config.db_path, stream_id, **kwargs)

    if pipeline_state.restart_pipeline:
        await pipeline_state.restart_pipeline()
    return result


@router.delete("/streams/{stream_id}")
async def delete_stream_endpoint(stream_id: str):
    import pipeline_state

    all_streams = await get_streams(config.db_path)
    if len(all_streams) <= 1:
        raise HTTPException(status_code=400, detail="Cannot delete the last stream")

    deleted = await delete_stream(config.db_path, stream_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Stream not found")

    if pipeline_state.restart_pipeline:
        await pipeline_state.restart_pipeline()
    return {"deleted": True}


@router.get("/models")
async def list_models():
    models = await fetch_models(config.openrouter_api_key)
    return {"models": models}


@router.get("/status")
async def get_status():
    import pipeline_state
    from websocket_manager import ws_manager

    counts = await get_counts(config.db_path)
    stream_states = pipeline_state.all_states()
    # Aggregate a top-level "running" flag for backward compat
    any_running = any(s.get("running") for s in stream_states.values())
    return {
        "pipeline": {"running": any_running, "streams": stream_states},
        "websocket_clients": ws_manager.connection_count,
        "counts": counts,
        "config": config.to_dict(),
    }


# --- Alert Feedback ---


@router.post("/alerts/{alert_id}/feedback")
async def submit_alert_feedback(alert_id: int, body: dict):
    from websocket_manager import ws_manager

    feedback_type = body.get("feedback_type")
    if feedback_type not in ("correct", "false_positive", "correction"):
        raise HTTPException(status_code=400, detail="feedback_type must be 'correct', 'false_positive', or 'correction'")

    result = await insert_alert_feedback(
        config.db_path, alert_id, feedback_type,
        corrected_summary=body.get("corrected_summary"),
        corrected_severity=body.get("corrected_severity"),
        corrected_category=body.get("corrected_category"),
        notes=body.get("notes"),
    )
    await ws_manager.broadcast({"type": "feedback", "data": result})
    return result


# --- Review Queue ---


@router.get("/review/queue")
async def list_review_queue(
    review_type: str = Query("all"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    items = await get_review_queue(config.db_path, review_type, limit, offset)
    return {"items": items}


@router.get("/review/stats")
async def review_stats():
    stats = await get_feedback_stats(config.db_path)
    return stats


@router.post("/transcriptions/{transcription_id}/correct")
async def correct_transcription(transcription_id: int, body: dict):
    corrected_text = body.get("corrected_text")
    if not corrected_text:
        raise HTTPException(status_code=400, detail="corrected_text is required")

    result = await submit_transcription_correction(config.db_path, transcription_id, corrected_text)
    if not result:
        raise HTTPException(status_code=404, detail="Transcription not found")

    # Suggest dictionary entries from the correction
    from text_corrector import learn_from_correction
    original = result.get("text", "")
    suggestions = learn_from_correction(original, corrected_text)

    return {"transcription": result, "dictionary_suggestions": suggestions}


@router.post("/transcriptions/{transcription_id}/confirm")
async def confirm_transcription_endpoint(transcription_id: int):
    result = await confirm_transcription(config.db_path, transcription_id)
    if not result:
        raise HTTPException(status_code=404, detail="Transcription not found")
    return result


# --- Regional Dictionary ---


@router.get("/dictionary")
async def list_dictionary(
    category: str = Query(None),
    active_only: bool = Query(True),
):
    entries = await get_dictionary_entries(config.db_path, category, active_only)
    return {"entries": entries}


@router.post("/dictionary")
async def create_dictionary_entry(body: dict):
    from text_corrector import invalidate_dictionary_cache

    term = body.get("term", "").strip()
    replacement = body.get("replacement", "").strip()
    if not term or not replacement:
        raise HTTPException(status_code=400, detail="term and replacement are required")

    category = body.get("category", "general")
    result = await upsert_dictionary_entry(config.db_path, term, replacement, category)
    invalidate_dictionary_cache()
    return result


@router.delete("/dictionary/{entry_id}")
async def remove_dictionary_entry(entry_id: int):
    from text_corrector import invalidate_dictionary_cache

    deleted = await delete_dictionary_entry(config.db_path, entry_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Entry not found")
    invalidate_dictionary_cache()
    return {"deleted": True}


# --- Training Data Export ---


@router.get("/export/training-data")
async def export_training_data(format: str = Query("jsonl")):
    import json as json_mod

    transcription_data = await get_training_data(config.db_path)
    alert_data = await get_alert_training_data(config.db_path)

    if format == "jsonl":
        lines = []
        for t in transcription_data:
            lines.append(json_mod.dumps({
                "type": "transcription",
                "id": t["id"],
                "audio_file": t.get("audio_file"),
                "original": t["text"],
                "corrected": t["corrected_text"],
                "confidence": t.get("confidence"),
            }))
        for a in alert_data:
            lines.append(json_mod.dumps({
                "type": "alert",
                "alert_id": a["alert_id"],
                "original_summary": a["summary"],
                "original_severity": a["severity"],
                "original_category": a["category"],
                "feedback_type": a["feedback_type"],
                "corrected_summary": a.get("corrected_summary"),
                "corrected_severity": a.get("corrected_severity"),
                "corrected_category": a.get("corrected_category"),
            }))
        content = "\n".join(lines) + "\n" if lines else ""
        return StreamingResponse(
            iter([content]),
            media_type="application/jsonl",
            headers={"Content-Disposition": "attachment; filename=training-data.jsonl"},
        )

    return {"transcriptions": transcription_data, "alerts": alert_data}


# --- Video Export ---


def _wrap_text(draw, text: str, font, max_width: int) -> list[str]:
    """Word-wrap text to fit within max_width pixels."""
    words = text.split()
    lines = []
    current_line = ""
    for word in words:
        test = f"{current_line} {word}".strip()
        bbox = draw.textbbox((0, 0), test, font=font)
        if bbox[2] - bbox[0] <= max_width:
            current_line = test
        else:
            if current_line:
                lines.append(current_line)
            current_line = word
    if current_line:
        lines.append(current_line)
    return lines or [""]


def _render_text_image(text: str, width: int, height: int) -> str:
    """Render overlay text onto a transparent PNG. Returns temp file path."""
    from PIL import Image, ImageDraw, ImageFont

    img = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # Find a font
    font_size = 36
    font = None
    font_paths = [
        "/System/Library/Fonts/Helvetica.ttc",
        "/System/Library/Fonts/SFNSMono.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/TTF/DejaVuSans.ttf",
    ]
    for fp in font_paths:
        if os.path.isfile(fp):
            try:
                font = ImageFont.truetype(fp, font_size)
                break
            except Exception:
                continue
    if font is None:
        font = ImageFont.load_default()

    padding_x = 40
    max_text_width = width - (padding_x * 2)
    lines = _wrap_text(draw, text, font, max_text_width)

    # Measure text block height
    line_height = font_size + 8
    text_block_height = len(lines) * line_height

    # Position in lower third
    box_padding_y = 24
    box_height = text_block_height + (box_padding_y * 2) + 4  # +4 for accent border
    box_top = height - box_height - 80  # 80px from bottom

    # Semi-transparent dark background box
    draw.rectangle(
        [(0, box_top), (width, box_top + box_height)],
        fill=(12, 18, 32, 200),
    )
    # Accent-colored top border
    draw.rectangle(
        [(0, box_top), (width, box_top + 4)],
        fill=(0, 232, 157, 255),
    )

    # Draw text lines
    y = box_top + 4 + box_padding_y
    for line in lines:
        draw.text((padding_x, y), line, font=font, fill=(220, 228, 240, 255))
        y += line_height

    fd, path = tempfile.mkstemp(suffix=".png")
    os.close(fd)
    img.save(path, "PNG")
    return path


def _prepare_background(image_bytes: bytes, width: int, height: int) -> str:
    """Resize/crop image to cover target size & darken it. Returns temp file path."""
    from PIL import Image, ImageEnhance, ImageFile
    import io

    ImageFile.LOAD_TRUNCATED_IMAGES = True
    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")

    # Cover-crop: scale so the image fully covers width x height, then center-crop
    src_ratio = img.width / img.height
    dst_ratio = width / height
    if src_ratio > dst_ratio:
        # Image is wider — scale by height, crop width
        new_h = height
        new_w = int(img.width * (height / img.height))
    else:
        # Image is taller — scale by width, crop height
        new_w = width
        new_h = int(img.height * (width / img.width))
    img = img.resize((new_w, new_h), Image.LANCZOS)
    left = (new_w - width) // 2
    top = (new_h - height) // 2
    img = img.crop((left, top, left + width, top + height))

    # Darken so waveform is visible
    img = ImageEnhance.Brightness(img).enhance(0.35)

    fd, path = tempfile.mkstemp(suffix=".png")
    os.close(fd)
    img.save(path, "PNG")
    return path


def _generate_video(audio_path: str, overlay_text: str | None, bg_path: str | None = None) -> str:
    """Run FFmpeg to generate an MP4 with waveform visualization. Returns temp file path."""
    fd, output_path = tempfile.mkstemp(suffix=".mp4")
    os.close(fd)

    text_img_path = None
    try:
        width, height = 1080, 1350
        raw_w, raw_h = 540, 250
        wave_h = 400
        pad_top = (height - wave_h) // 2

        def _esc(p):
            return p.replace("\\", "\\\\").replace(":", "\\:").replace("'", "\\'")

        # Build waveform with alpha: showwaves → split → alphamerge
        waves_base = (
            f"[0:a]volume=20dB,"
            f"showwaves=s={raw_w}x{raw_h}:mode=cline:rate=30"
            f":colors=0xff8c22:scale=cbrt:draw=full,"
            f"scale={width}:{wave_h}:flags=neighbor,"
            f"pad={width}:{height}:0:{pad_top}:color=black"
        )

        if bg_path:
            # Waveform with alpha, overlaid on background photo
            filter_complex = (
                f"{waves_base},"
                f"format=rgba,colorkey=color=black:similarity=0.08:blend=0.0"
                f"[keyed];"
                f"movie={_esc(bg_path)},scale={width}:{height}[bg];"
                f"[bg][keyed]overlay=0:0:format=rgb[composited];"
                f"[composited]format=yuv420p"
            )
        else:
            filter_complex = waves_base

        # Add text overlay if provided
        if overlay_text and overlay_text.strip():
            text_img_path = _render_text_image(overlay_text.strip(), width, height)
            filter_complex += (
                f"[base];"
                f"movie={_esc(text_img_path)}[text];"
                f"[base][text]overlay=0:0[out]"
            )
        else:
            filter_complex += "[out]"

        cmd = [
            "ffmpeg", "-y",
            "-i", audio_path,
            "-filter_complex", filter_complex,
            "-map", "[out]", "-map", "0:a",
            "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            "-pix_fmt", "yuv420p",
            "-movflags", "+faststart",
            "-shortest",
            output_path,
        ]

        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=120,
        )
        if result.returncode != 0:
            if os.path.isfile(output_path):
                os.unlink(output_path)
            raise RuntimeError(f"FFmpeg failed: {result.stderr[-500:]}")

        return output_path
    except Exception:
        if os.path.isfile(output_path):
            os.unlink(output_path)
        raise
    finally:
        if text_img_path and os.path.isfile(text_img_path):
            os.unlink(text_img_path)


@router.post("/video/{transcription_id}")
async def export_video(
    transcription_id: int,
    text: str = Form(""),
    background: UploadFile | None = File(None),
):
    row = await get_transcription(config.db_path, transcription_id)
    if not row:
        raise HTTPException(status_code=404, detail="Transcription not found")

    audio_file = row.get("audio_file")
    if not audio_file:
        raise HTTPException(status_code=404, detail="No audio file for this transcription")

    audio_path = os.path.join(config.audio_dir, audio_file)
    if not os.path.isfile(audio_path):
        raise HTTPException(status_code=404, detail="Audio file not found on disk")

    bg_path = None
    try:
        if background and background.filename:
            bg_bytes = await background.read()
            if len(bg_bytes) > 0:
                bg_path = await asyncio.to_thread(
                    _prepare_background, bg_bytes, 1080, 1350,
                )

        video_path = await asyncio.to_thread(
            _generate_video, audio_path, text, bg_path,
        )
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Video generation timed out")
    finally:
        if bg_path and os.path.isfile(bg_path):
            os.unlink(bg_path)

    def cleanup():
        if os.path.isfile(video_path):
            os.unlink(video_path)

    return FileResponse(
        video_path,
        media_type="video/mp4",
        filename=f"scanner_{transcription_id}.mp4",
        background=BackgroundTask(cleanup),
    )
