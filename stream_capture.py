from __future__ import annotations

import asyncio
import enum
import io
import logging
import subprocess
import time
import wave
from collections import deque
from typing import AsyncGenerator, Callable, Awaitable

import numpy as np

logger = logging.getLogger(__name__)


def _safe_task(coro):
    """Create a task that logs exceptions instead of letting them go unhandled."""
    task = asyncio.create_task(coro)
    task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
    return task


SAMPLE_RATE = 16000
BYTES_PER_SAMPLE = 2  # 16-bit PCM
SUB_CHUNK_DURATION_S = 0.5
SUB_CHUNK_BYTES = int(SAMPLE_RATE * BYTES_PER_SAMPLE * SUB_CHUNK_DURATION_S)  # 16000

# Type for the audio level callback: (rms, peak) -> None
AudioLevelCallback = Callable[[float, float], Awaitable[None] | None]

# Type for VAD state callback: (state_name) -> None
VadStateCallback = Callable[[str], Awaitable[None] | None]


class VadState(enum.Enum):
    WAITING = "waiting"
    RECORDING = "recording"
    GRACE_PERIOD = "grace_period"


def _bytes_for_duration(seconds: float) -> int:
    return int(SAMPLE_RATE * BYTES_PER_SAMPLE * seconds)


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
    stop_event: asyncio.Event | None = None,
    on_audio_level: AudioLevelCallback | None = None,
    on_vad_state: VadStateCallback | None = None,
    vad_threshold: float = 0.01,
    vad_grace_period_s: float = 1.5,
    vad_pre_roll_s: float = 1.0,
    max_chunk_duration_s: float = 60.0,
    min_chunk_duration_s: float = 1.5,
) -> AsyncGenerator[tuple[str, np.ndarray, bytes], None]:
    """Yield (iso_timestamp, numpy_float32_array, raw_pcm_bytes) chunks from the stream.

    Uses energy-based VAD to detect speech boundaries instead of fixed-duration
    chunking.  Only yields audio containing actual radio transmissions.
    """
    from vad import EnergyVAD

    vad = EnergyVAD(threshold=vad_threshold)

    pre_roll_sub_chunks = max(1, int(vad_pre_roll_s / SUB_CHUNK_DURATION_S))
    grace_sub_chunks = max(1, int(vad_grace_period_s / SUB_CHUNK_DURATION_S))
    max_sub_chunks = int(max_chunk_duration_s / SUB_CHUNK_DURATION_S)
    min_sub_chunks = max(1, int(min_chunk_duration_s / SUB_CHUNK_DURATION_S))

    backoff = 1

    while stop_event is None or not stop_event.is_set():
        if not stream_url:
            logger.warning("No stream URL configured, waiting...")
            await asyncio.sleep(5)
            continue

        logger.info("Starting FFmpeg for %s", stream_url)
        proc = await asyncio.to_thread(_start_ffmpeg, stream_url)
        backoff = 1  # reset on successful start

        vad.reset()

        try:
            # State machine
            state = VadState.WAITING
            pre_roll: deque[bytes] = deque(maxlen=pre_roll_sub_chunks)
            recording_parts: list[bytes] = []
            recording_sub_chunks = 0
            grace_remaining = 0
            chunk_start_time: str | None = None

            async def _set_state(new_state: VadState):
                nonlocal state
                if state != new_state:
                    state = new_state
                    if on_vad_state:
                        result = on_vad_state(new_state.value)
                        if asyncio.iscoroutine(result):
                            _safe_task(result)

            await _set_state(VadState.WAITING)

            while stop_event is None or not stop_event.is_set():
                raw = await asyncio.to_thread(proc.stdout.read, SUB_CHUNK_BYTES)
                if not raw:
                    logger.warning("FFmpeg stdout closed")
                    break

                # Audio level callback (every sub-chunk regardless of VAD state)
                rms, peak = _compute_levels(raw)
                if on_audio_level and len(raw) > 0:
                    result = on_audio_level(rms, peak)
                    if asyncio.iscoroutine(result):
                        asyncio.create_task(result)

                # Energy-based speech detection (cheap, no thread needed)
                speech_detected = rms >= vad_threshold

                # --- State machine ---

                if state == VadState.WAITING:
                    pre_roll.append(raw)
                    if speech_detected:
                        logger.info(
                            "Speech onset detected (rms=%.4f, threshold=%.4f)",
                            rms, vad_threshold,
                        )
                        # Transition to RECORDING, include pre-roll
                        recording_parts = list(pre_roll)
                        recording_sub_chunks = len(recording_parts)
                        chunk_start_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                        pre_roll.clear()
                        await _set_state(VadState.RECORDING)

                elif state == VadState.RECORDING:
                    recording_parts.append(raw)
                    recording_sub_chunks += 1

                    # Max duration safety cap
                    if recording_sub_chunks >= max_sub_chunks:
                        logger.info(
                            "Max chunk duration reached (%ds), yielding",
                            int(recording_sub_chunks * SUB_CHUNK_DURATION_S),
                        )
                        yield _assemble_chunk(recording_parts, chunk_start_time)
                        recording_parts = []
                        recording_sub_chunks = 0
                        chunk_start_time = None
                        backoff = 1
                        await _set_state(VadState.WAITING)
                    elif not speech_detected:
                        # Enter grace period
                        grace_remaining = grace_sub_chunks
                        await _set_state(VadState.GRACE_PERIOD)

                elif state == VadState.GRACE_PERIOD:
                    recording_parts.append(raw)
                    recording_sub_chunks += 1

                    if speech_detected:
                        # Speech resumed — back to recording
                        await _set_state(VadState.RECORDING)
                    else:
                        grace_remaining -= 1
                        if grace_remaining <= 0:
                            # Grace expired — yield chunk if long enough
                            duration_s = recording_sub_chunks * SUB_CHUNK_DURATION_S
                            if recording_sub_chunks >= min_sub_chunks:
                                logger.info(
                                    "Grace expired, yielding chunk (%.1fs)",
                                    duration_s,
                                )
                                yield _assemble_chunk(recording_parts, chunk_start_time)
                                backoff = 1
                            else:
                                logger.debug(
                                    "Chunk too short (%.1fs < %.1fs), discarding",
                                    duration_s, min_chunk_duration_s,
                                )
                            recording_parts = []
                            recording_sub_chunks = 0
                            chunk_start_time = None
                            await _set_state(VadState.WAITING)

                    # Also check max duration during grace
                    if recording_sub_chunks >= max_sub_chunks:
                        logger.info(
                            "Max chunk duration reached during grace (%ds), yielding",
                            int(recording_sub_chunks * SUB_CHUNK_DURATION_S),
                        )
                        yield _assemble_chunk(recording_parts, chunk_start_time)
                        recording_parts = []
                        recording_sub_chunks = 0
                        chunk_start_time = None
                        backoff = 1
                        await _set_state(VadState.WAITING)

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


def _assemble_chunk(
    parts: list[bytes], timestamp: str | None
) -> tuple[str, np.ndarray, bytes]:
    """Join recorded sub-chunks into a single (timestamp, audio_array, raw_pcm) tuple."""
    full_raw = b"".join(parts)
    ts = timestamp or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    samples = np.frombuffer(full_raw, dtype=np.int16).astype(np.float32) / 32768.0
    return (ts, samples, full_raw)
