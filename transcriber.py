from __future__ import annotations

import asyncio
import logging

import numpy as np

logger = logging.getLogger(__name__)

_MODEL_MAP = {
    "tiny": "mlx-community/whisper-tiny",
    "base": "mlx-community/whisper-base",
    "small": "mlx-community/whisper-small",
    "medium": "mlx-community/whisper-medium",
    "large-v3": "mlx-community/whisper-large-v3-mlx",
    "turbo": "mlx-community/whisper-turbo",
}


def _transcribe(audio: np.ndarray, model_size: str) -> tuple[str, float]:
    import mlx_whisper

    repo = _MODEL_MAP.get(model_size, _MODEL_MAP["base"])
    logger.info("Transcribing with model: %s (%s)", model_size, repo)

    result = mlx_whisper.transcribe(
        audio,
        path_or_hf_repo=repo,
        language="en",
        temperature=0.0,
        no_speech_threshold=0.6,
        hallucination_silence_threshold=2.0,
        condition_on_previous_text=False,
    )

    text = result.get("text", "").strip()
    segments = result.get("segments", [])
    duration = segments[-1]["end"] if segments else 0.0
    return text, duration


async def transcribe_chunk(audio: np.ndarray, model_size: str = "base") -> tuple[str, float]:
    """Transcribe an audio chunk. Returns (text, duration_seconds)."""
    return await asyncio.to_thread(_transcribe, audio, model_size)
