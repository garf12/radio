from __future__ import annotations

import asyncio
import logging

import numpy as np
from faster_whisper import WhisperModel

logger = logging.getLogger(__name__)

_model: WhisperModel | None = None
_current_model_size: str | None = None


def _load_model(model_size: str) -> WhisperModel:
    global _model, _current_model_size
    if _model is not None and _current_model_size == model_size:
        return _model
    logger.info("Loading Whisper model: %s", model_size)
    _model = WhisperModel(model_size, device="cpu", compute_type="int8")
    _current_model_size = model_size
    logger.info("Whisper model loaded: %s", model_size)
    return _model


def _transcribe(audio: np.ndarray, model_size: str) -> tuple[str, float]:
    model = _load_model(model_size)
    segments, info = model.transcribe(
        audio,
        beam_size=5,
        vad_filter=True,
        vad_parameters=dict(
            min_silence_duration_ms=500,
            speech_pad_ms=300,
        ),
    )
    texts = []
    for seg in segments:
        texts.append(seg.text.strip())
    text = " ".join(texts)
    return text, info.duration


async def transcribe_chunk(audio: np.ndarray, model_size: str = "base") -> tuple[str, float]:
    """Transcribe an audio chunk. Returns (text, duration_seconds)."""
    return await asyncio.to_thread(_transcribe, audio, model_size)
