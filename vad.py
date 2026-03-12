"""Energy-based Voice Activity Detection for radio scanner audio.

Radio scanners have a clean on/off pattern: squelch opens (RMS jumps) and
closes (RMS drops).  A neural VAD like Silero doesn't work well on compressed,
noisy radio streams, so we use a simple RMS energy detector instead.

The detector computes the RMS of each sub-chunk and compares it against a
threshold.  This is lightweight (no model, no ONNX) and reliable for
squelch-gated radio audio.
"""

from __future__ import annotations

import logging

import numpy as np

logger = logging.getLogger(__name__)


class EnergyVAD:
    """RMS energy-based voice activity detector for radio audio."""

    def __init__(self, threshold: float = 0.01):
        """
        Args:
            threshold: RMS level (0.0–1.0) above which audio is considered
                       speech/active.  Typical radio static is ~0.002-0.005;
                       speech is ~0.01-0.05+.
        """
        self.threshold = threshold
        logger.info("Energy VAD initialized (threshold=%.4f)", threshold)

    def reset(self) -> None:
        """No-op for API compatibility — energy detector is stateless."""
        pass

    def detect(self, pcm_bytes: bytes) -> tuple[bool, float]:
        """Check whether a chunk of 16-bit PCM audio contains speech.

        Returns (is_speech, rms_level).
        """
        samples = np.frombuffer(pcm_bytes, dtype=np.int16).astype(np.float32) / 32768.0
        if len(samples) == 0:
            return False, 0.0
        rms = float(np.sqrt(np.mean(samples ** 2)))
        return rms >= self.threshold, rms
