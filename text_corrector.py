"""Regional dictionary application and auto-learning from corrections."""
from __future__ import annotations

import asyncio
import logging
import re
import time
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)

# Cache for dictionary entries
_dict_cache: list[dict] = []
_dict_cache_time: float = 0.0
_CACHE_TTL = 300  # 5 minutes


def _load_dictionary(db_path: str) -> list[dict]:
    """Load active dictionary entries from DB, with caching."""
    global _dict_cache, _dict_cache_time
    now = time.time()
    if _dict_cache and (now - _dict_cache_time) < _CACHE_TTL:
        return _dict_cache

    from database import _get_dictionary_entries
    _dict_cache = _get_dictionary_entries(db_path, active_only=True)
    _dict_cache_time = now
    return _dict_cache


def invalidate_dictionary_cache() -> None:
    """Force reload of dictionary cache on next use."""
    global _dict_cache_time
    _dict_cache_time = 0.0


def _apply_dictionary(db_path: str, text: str) -> tuple[str, list[dict]]:
    """Apply regional dictionary replacements to text.

    Returns (corrected_text, list of applied corrections).
    """
    entries = _load_dictionary(db_path)
    if not entries:
        return text, []

    applied = []
    corrected = text
    for entry in entries:
        term = entry["term"]
        replacement = entry["replacement"]
        # Case-insensitive whole-word replacement
        pattern = re.compile(r'\b' + re.escape(term) + r'\b', re.IGNORECASE)
        new_text = pattern.sub(replacement, corrected)
        if new_text != corrected:
            applied.append({"term": term, "replacement": replacement})
            corrected = new_text

    return corrected, applied


async def apply_regional_dictionary(db_path: str, text: str) -> tuple[str, list[dict]]:
    """Async wrapper for dictionary application."""
    return await asyncio.to_thread(_apply_dictionary, db_path, text)


def learn_from_correction(original: str, corrected: str) -> list[dict]:
    """Detect word-level differences that could become dictionary entries.

    Returns list of suggested entries: [{term, replacement}].
    Does NOT auto-add to dictionary — returns suggestions only.
    """
    orig_words = original.lower().split()
    corr_words = corrected.lower().split()

    matcher = SequenceMatcher(None, orig_words, corr_words)
    suggestions = []

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "replace":
            orig_phrase = " ".join(orig_words[i1:i2])
            corr_phrase = " ".join(corr_words[j1:j2])
            # Only suggest short replacements (1-3 words)
            if (i2 - i1) <= 3 and (j2 - j1) <= 3:
                suggestions.append({"term": orig_phrase, "replacement": corr_phrase})

    return suggestions
