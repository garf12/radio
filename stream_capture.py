from __future__ import annotations

import asyncio
import io
import logging
import struct
import subprocess
import time
import wave
from typing import AsyncGenerator, Callable, Awaitable

import numpy as np

logger = logging.getLogger(__name__)

SAMPLE_RATE = 16000
BYTES_PER_SAMPLE = 2  # 16-bit PCM
SUB_CHUNK_DURATION_S = 0.5
SUB_CHUNK_BYTES = int(SAMPLE_RATE * BYTES_PER_SAMPLE * SUB_CHUNK_DURATION_S)  # 16000

# Type for the audio level callback: (rms, peak) -> None
AudioLevelCallback = Callable[[float, float], Awaitable[None] | None]


def _bytes_for_duration(seconds: int) -> int:
    return SAMPLE_RATE * BYTES_PER_SAMPLE * seconds


def _start_ffmpeg(stream_url: str) -> subprocess.Popen:
    cmd = [
        "ffmpeg",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "30",
        "-i", stream_url,
        "-f", "s16le",
        "-acodec", "pcm_s16le",
        "-ac", "1",
        "-ar", str(SAMPLE_RATE),
        "-loglevel", "error",
        "pipe:1",
    ]
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def audio_to_wav_bytes(raw_pcm: bytes) -> bytes:
    """Wrap raw 16-bit mono PCM data in a WAV header."""
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(BYTES_PER_SAMPLE)
        wf.setframerate(SAMPLE_RATE)
        wf.writeframes(raw_pcm)
    return buf.getvalue()


def _compute_levels(raw_pcm: bytes) -> tuple[float, float]:
    """Compute RMS and peak levels from raw PCM bytes. Returns (rms, peak) as 0.0-1.0."""
    samples = np.frombuffer(raw_pcm, dtype=np.int16).astype(np.float32) / 32768.0
    if len(samples) == 0:
        return 0.0, 0.0
    rms = float(np.sqrt(np.mean(samples ** 2)))
    peak = float(np.max(np.abs(samples)))
    return rms, peak


async def read_chunks(
    stream_url: str,
    chunk_duration_s: int = 30,
    stop_event: asyncio.Event | None = None,
    on_audio_level: AudioLevelCallback | None = None,
) -> AsyncGenerator[tuple[str, np.ndarray, bytes], None]:
    """Yield (iso_timestamp, numpy_float32_array, raw_pcm_bytes) chunks from the stream."""
    chunk_bytes = _bytes_for_duration(chunk_duration_s)
    backoff = 1

    while stop_event is None or not stop_event.is_set():
        if not stream_url:
            logger.warning("No stream URL configured, waiting...")
            await asyncio.sleep(5)
            continue

        logger.info("Starting FFmpeg for %s", stream_url)
        proc = await asyncio.to_thread(_start_ffmpeg, stream_url)
        backoff = 1  # reset on successful start

        try:
            while stop_event is None or not stop_event.is_set():
                # Read in 0.5s sub-chunks, accumulating into full buffer
                raw_parts: list[bytes] = []
                total_read = 0

                while total_read < chunk_bytes:
                    remaining = chunk_bytes - total_read
                    read_size = min(SUB_CHUNK_BYTES, remaining)
                    raw = await asyncio.to_thread(proc.stdout.read, read_size)
                    if not raw:
                        break

                    raw_parts.append(raw)
                    total_read += len(raw)

                    # Fire audio level callback for each sub-chunk
                    if on_audio_level and len(raw) > 0:
                        rms, peak = _compute_levels(raw)
                        result = on_audio_level(rms, peak)
                        if asyncio.iscoroutine(result):
                            asyncio.create_task(result)

                if total_read == 0:
                    logger.warning("FFmpeg stdout closed")
                    break

                full_raw = b"".join(raw_parts)
                timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                samples = np.frombuffer(full_raw, dtype=np.int16).astype(np.float32) / 32768.0
                backoff = 1
                yield (timestamp, samples, full_raw)

        except Exception:
            logger.exception("Error reading from FFmpeg")
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()

        if stop_event and stop_event.is_set():
            break

        logger.info("Reconnecting in %ds...", backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30)
