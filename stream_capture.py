from __future__ import annotations

import asyncio
import logging
import subprocess
import time
from typing import AsyncGenerator

import numpy as np

logger = logging.getLogger(__name__)

SAMPLE_RATE = 16000
BYTES_PER_SAMPLE = 2  # 16-bit PCM


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


async def read_chunks(
    stream_url: str,
    chunk_duration_s: int = 30,
    stop_event: asyncio.Event | None = None,
) -> AsyncGenerator[tuple[str, np.ndarray], None]:
    """Yield (iso_timestamp, numpy_float32_array) chunks from the stream."""
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
                raw = await asyncio.to_thread(proc.stdout.read, chunk_bytes)
                if not raw:
                    logger.warning("FFmpeg stdout closed")
                    break

                timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                samples = np.frombuffer(raw, dtype=np.int16).astype(np.float32) / 32768.0
                backoff = 1
                yield (timestamp, samples)

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
