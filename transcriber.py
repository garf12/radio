from __future__ import annotations

import asyncio
import logging
import re

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

# Audio energy gate: RMS below this is considered silence/static.
# 16-bit PCM normalized to float32 — typical static is ~0.002-0.005
_SILENCE_RMS_THRESHOLD = 0.008

# Known Whisper hallucination phrases (lowercased, stripped).
# These appear when Whisper is fed silence, static, or low-energy audio.
_HALLUCINATION_PHRASES = {
    "thank you.",
    "thank you",
    "thanks.",
    "thanks",
    "thank you so much.",
    "thank you so much",
    "thanks for watching.",
    "thanks for watching",
    "thanks for listening.",
    "thanks for listening",
    "please subscribe.",
    "please subscribe",
    "like and subscribe.",
    "like and subscribe",
    "bye.",
    "bye",
    "goodbye.",
    "goodbye",
    "see you next time.",
    "see you next time",
    "you",
    "the end.",
    "the end",
    "...",
    "so",
    "okay.",
    "okay",
    "oh",
    "ah",
    "uh",
    "hmm",
    "huh",
    "yeah.",
    "yeah",
    "yes.",
    "yes",
    "no.",
    "no",
    "i'm sorry.",
    "i'm sorry",
    "subtitles by the amara.org community",
}

# Pattern: same short phrase repeated (with optional punctuation/spaces)
_REPETITION_RE = re.compile(r"^(.{2,30}?)[\s.,!?]*(?:\1[\s.,!?]*){2,}$", re.IGNORECASE)


def _is_hallucination(text: str, segments: list[dict]) -> bool:
    """Detect likely Whisper hallucinations."""
    cleaned = text.strip().lower()

    # Empty
    if not cleaned:
        return True

    # Exact match to known hallucination phrases
    if cleaned in _HALLUCINATION_PHRASES:
        logger.info("Filtered hallucination (exact match): %r", text)
        return True

    # Repetitive pattern: same short phrase repeated 3+ times
    if _REPETITION_RE.match(cleaned):
        logger.info("Filtered hallucination (repetition): %r", text)
        return True

    # Check if all segments have high no_speech_prob
    if segments:
        high_no_speech = sum(
            1 for s in segments if s.get("no_speech_prob", 0) > 0.5
        )
        if high_no_speech == len(segments):
            logger.info(
                "Filtered hallucination (all %d segments no_speech_prob > 0.5): %r",
                len(segments), text,
            )
            return True

    # Very short text with few real words is suspicious
    words = cleaned.split()
    if len(words) <= 3 and all(len(w) <= 4 for w in words):
        logger.info("Filtered hallucination (too short/simple): %r", text)
        return True

    return False


def _transcribe(audio: np.ndarray, model_size: str) -> tuple[str, float]:
    import mlx_whisper

    # Audio energy gate: skip transcription if the chunk is just static/silence
    rms = float(np.sqrt(np.mean(audio ** 2)))
    if rms < _SILENCE_RMS_THRESHOLD:
        logger.debug("Skipping transcription, audio RMS %.4f below threshold %.4f", rms, _SILENCE_RMS_THRESHOLD)
        return "", 0.0

    repo = _MODEL_MAP.get(model_size, _MODEL_MAP["base"])
    logger.info("Transcribing with model: %s (%s), audio RMS: %.4f", model_size, repo, rms)

    result = mlx_whisper.transcribe(
        audio,
        path_or_hf_repo=repo,
        language="en",
        temperature=0.0,
        no_speech_threshold=0.5,
        hallucination_silence_threshold=1.0,
        condition_on_previous_text=False,
        compression_ratio_threshold=2.0,
        logprob_threshold=-0.8,
    )

    segments = result.get("segments", [])

    # Filter out segments with high no_speech_prob individually
    good_segments = [s for s in segments if s.get("no_speech_prob", 0) <= 0.5]
    text = " ".join(s.get("text", "").strip() for s in good_segments).strip()
    duration = good_segments[-1]["end"] if good_segments else 0.0

    # Final hallucination check on the assembled text
    if _is_hallucination(text, good_segments):
        return "", 0.0

    return text, duration


async def transcribe_chunk(audio: np.ndarray, model_size: str = "base") -> tuple[str, float]:
    """Transcribe an audio chunk. Returns (text, duration_seconds)."""
    return await asyncio.to_thread(_transcribe, audio, model_size)
