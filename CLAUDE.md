# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

TXKtoday Scanner Monitor — a real-time police radio scanner application that captures live audio streams (via FFmpeg), transcribes them (mlx-whisper on macOS), analyzes transcripts for alerts (OpenRouter LLM), geocodes locations (Google Maps), and presents everything through a web dashboard with WebSocket-driven live updates. Supports up to 10 simultaneous streams.

## Running the Application

```bash
# Setup
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Run (starts FastAPI on http://localhost:8000)
python main.py
```

Requires FFmpeg installed on the system. Configuration via `.env` file (see `.env.example`) — key variables: `STREAM_URL`, `OPENROUTER_API_KEY`, `WHISPER_MODEL`, `GOOGLE_MAPS_API_KEY`.

## Architecture

### Processing Pipeline (main.py)

The core is a `StreamManager` class that manages per-stream async pipeline tasks:

```
FFmpeg stream capture → Energy VAD filtering → audio chunks → mlx-whisper transcription → LLM analysis → 10-code contradiction check → geocoding → DB persistence → webhook
```

Each stage broadcasts results via WebSocket (`audio_level`, `transcription`, `alert`, `event`, `summary` message types). Periodic tasks generate 10-minute and hourly situational summaries.

### Multi-Stream Architecture

- `StreamManager` in main.py manages up to 10 concurrent pipeline tasks
- Each stream has independent state tracked via `pipeline_state.py` (running, error, chunks_processed, vad_state, etc.)
- Streams are CRUD-managed via `/api/streams` and stored in the `streams` DB table
- All transcriptions, alerts, and events are tagged with `stream_id`
- Health check auto-restarts dead pipeline tasks every minute

### Key Components

- **stream_capture.py** — FFmpeg subprocess yielding PCM audio chunks with energy-based VAD gating, auto-reconnect, and exponential backoff. VAD parameters (threshold, grace period, pre-roll, min/max duration) are configurable.
- **vad.py** — `EnergyVAD` class: lightweight RMS energy detector (threshold ~0.01; radio static ~0.002-0.005, speech ~0.01-0.05+). Stateless, suited for radio squelch patterns.
- **transcriber.py** — mlx-whisper wrapper with global async lock to prevent concurrent GPU usage. Includes hallucination filtering (68 known phrases, repetition detection, no_speech_prob gating, audio energy gate). Computes confidence metrics stored in DB.
- **analyzer.py** — OpenRouter LLM client analyzing a rolling window (~2 min) of transcripts. Outputs structured JSON alerts. Injects operator feedback context (false positive rates, correction patterns) into LLM system prompt. Also generates 10-min and hourly situational summaries.
- **scanner_vocab.py** — Police 10-code dictionary (63 codes) with category/severity mapping. `detect_analysis_contradictions()` cross-checks LLM output against detected 10-codes.
- **text_corrector.py** — Regional dictionary application (whole-word regex replacement) with 5-min TTL cache. `learn_from_correction()` suggests new dictionary entries from operator edits.
- **webhook.py** — Fire-and-forget async POST for alert notifications (e.g., n8n). Sends alert + event context, never raises.
- **geocoder.py** — Google Maps geocoding with address normalization and DB-backed cache
- **database.py** — SQLite with WAL mode; all operations via `asyncio.to_thread()`. 30+ async functions.
- **config.py** — Dataclass config loaded from env vars, persisted to DB `settings` table (18 keys), updatable via API. `Config.load_saved()` merges DB values on startup.
- **pipeline_state.py** — Per-stream mutable state registry with restart callbacks

### Database Tables

SQLite (`scanner.db`) with WAL mode:

- `transcriptions` — with `stream_id`, confidence metrics, flags, review status, `corrected_text`
- `alerts` — with `stream_id`, `event_id` FK
- `events` — with location, coordinates, `stream_id`; severity only escalates
- `streams` — id, name, url, enabled, color, sort_order
- `summaries` — 10-min and hourly, with `summary_type`, `key_themes`, `activity_level`
- `alert_feedback` — operator corrections: feedback_type (correct/false_positive/correction)
- `regional_dictionary` — term replacements with category and active flag
- `geocode_cache`, `settings`

### API Layer

- **routes/api.py** — REST endpoints under `/api/`:
  - Transcriptions, alerts, events, config, status, audio playback
  - Stream CRUD: `/api/streams`, `/api/streams/{stream_id}`
  - Dictionary CRUD: `/api/dictionary`
  - Feedback: `/api/alerts/{id}/feedback`, `/api/transcriptions/{id}/correct`, `/api/transcriptions/{id}/confirm`
  - Review queue: `/api/review/queue`, `/api/review/stats`
  - Training export: `/api/export/training-data` (JSONL)
  - Models: `/api/models` (fetches available OpenRouter models)
- **routes/ws.py** + **websocket_manager.py** — WebSocket at `/ws` broadcasting real-time updates; messages include `stream_id`

### Frontend

Vanilla JS (no framework) in `static/`:
- **index.html / app.js** — Main dashboard with tabs: Live, History, Map, Events. Multi-stream color coding.
- **review.html / review.js** — Review queue for transcription corrections and alert feedback
- **settings.html / settings.js** — Stream management, model selection, VAD parameters, regional dictionary, webhook config
- **style.css** — All styles

## Conventions

- All blocking I/O (DB queries, HTTP calls, file writes) uses `asyncio.to_thread()`
- DB queries return dicts via custom `row_factory`
- WebSocket messages follow `{"type": "...", "data": {...}}` format with `stream_id` where applicable
- Alert severities: `critical`, `high`, `medium`, `low`
- Alert categories: `shooting`, `pursuit`, `fire`, `accident`, `medical`, `missing_person`, `robbery`, `assault`, `drug_activity`, `hazmat`, `other`
- Config changes via PUT `/api/config` trigger pipeline restart
- Audio saved as WAV files in `audio/` directory
- Global transcription lock prevents concurrent GPU/model usage
- Stale events auto-resolve after `event_timeout_minutes` (default 45)
- `models/silero_vad.onnx` exists but is unused; EnergyVAD in `vad.py` is the active VAD

## No Test Suite

There are currently no automated tests in this project.
