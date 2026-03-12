"""Shared mutable pipeline state — per-stream.

This exists as a separate module to avoid the __main__ vs 'main' dual-import
problem: when main.py is run directly, it loads as __main__, but late imports
like ``from main import ...`` create a second copy of the module globals.
Keeping mutable state here ensures a single source of truth.
"""

from __future__ import annotations


def _new_stream_state() -> dict:
    return {
        "running": False,
        "error": None,
        "last_transcription": None,
        "last_chunk": None,
        "chunks_processed": 0,
        "silent_chunks": 0,
        "vad_state": "waiting",
    }


# Per-stream state registry: stream_id -> state dict
_streams: dict[str, dict] = {}


def get_state(stream_id: str) -> dict:
    """Return (or create) the state dict for a given stream."""
    if stream_id not in _streams:
        _streams[stream_id] = _new_stream_state()
    return _streams[stream_id]


def remove_state(stream_id: str) -> None:
    """Remove state for a stopped stream."""
    _streams.pop(stream_id, None)


def all_states() -> dict[str, dict]:
    """Return a snapshot of all stream states."""
    return {sid: dict(s) for sid, s in _streams.items()}


# Legacy compat: single-stream callers can still read `state`
state = _new_stream_state()

# Restart callbacks — set by main.py at startup
restart_pipeline = None   # restarts all / syncs streams
restart_stream = None     # restarts a single stream by id
