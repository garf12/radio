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
_SILENCE_RMS_THRESHOLD = 0.005

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


def _compute_confidence(segments: list[dict]) -> tuple[float, float, float, float]:
    """Compute composite confidence score and raw metrics from segments.

    Returns (confidence, avg_logprob, avg_no_speech_prob, max_compression_ratio).
    """
    if not segments:
        return 0.0, -1.0, 1.0, 0.0

    avg_logprob = sum(s.get("avg_logprob", -1.0) for s in segments) / len(segments)
    avg_no_speech = sum(s.get("no_speech_prob", 0.0) for s in segments) / len(segments)
    max_compression = max((s.get("compression_ratio", 0.0) for s in segments), default=0.0)

    # Map logprob range [-1.0, 0.0] to [0.0, 1.0], penalized by speech uncertainty
    confidence = max(0.0, min(1.0, (avg_logprob + 1.0) * (1.0 - avg_no_speech)))

    return confidence, avg_logprob, avg_no_speech, max_compression


def _detect_flags(confidence: float, avg_no_speech: float, max_compression: float) -> list[str]:
    """Auto-detect quality issues from metrics."""
    flags = []
    if confidence < 0.3:
        flags.append("low_confidence")
    if max_compression > 2.0:
        flags.append("high_compression")
    if avg_no_speech > 0.4:
        flags.append("high_no_speech")
    return flags


def _transcribe(audio: np.ndarray, model_size: str) -> dict:
    import mlx_whisper

    empty_result = {
        "text": "", "duration": 0.0, "confidence": 0.0,
        "avg_logprob": -1.0, "avg_no_speech_prob": 1.0, "max_compression_ratio": 0.0,
        "flags": [], "segment_details": [],
    }

    # Audio energy gate: skip transcription if the chunk is just static/silence
    rms = float(np.sqrt(np.mean(audio ** 2)))
    if rms < _SILENCE_RMS_THRESHOLD:
        logger.debug("Skipping transcription, audio RMS %.4f below threshold %.4f", rms, _SILENCE_RMS_THRESHOLD)
        return empty_result

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
        return empty_result

    # Compute confidence metrics from good segments
    confidence, avg_logprob, avg_no_speech, max_compression = _compute_confidence(good_segments)
    flags = _detect_flags(confidence, avg_no_speech, max_compression)

    # Per-segment details for storage
    segment_details = [
        {
            "text": s.get("text", "").strip(),
            "start": s.get("start", 0.0),
            "end": s.get("end", 0.0),
            "avg_logprob": round(s.get("avg_logprob", -1.0), 4),
            "no_speech_prob": round(s.get("no_speech_prob", 0.0), 4),
            "compression_ratio": round(s.get("compression_ratio", 0.0), 4),
        }
        for s in good_segments
    ]

    if flags:
        logger.info("Transcription flags: %s (confidence=%.2f)", flags, confidence)

    return {
        "text": text,
        "duration": duration,
        "confidence": round(confidence, 4),
        "avg_logprob": round(avg_logprob, 4),
        "avg_no_speech_prob": round(avg_no_speech, 4),
        "max_compression_ratio": round(max_compression, 4),
        "flags": flags,
        "segment_details": segment_details,
    }


# Global lock: MLX/Metal GPU cannot handle concurrent transcriptions.
# Multiple streams must serialize through this lock to avoid GPU crashes.
_transcribe_lock = asyncio.Lock()


async def transcribe_chunk(audio: np.ndarray, model_size: str = "base") -> dict:
    """Transcribe an audio chunk. Returns dict with text, duration, confidence, flags, etc."""
    async with _transcribe_lock:
        return await asyncio.to_thread(_transcribe, audio, model_size)
