# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

TXKtoday Scanner Monitor — a real-time police radio scanner application that captures live audio streams (via FFmpeg), transcribes them (Faster-Whisper), analyzes transcripts for alerts (OpenRouter LLM), geocodes locations (Google Maps), and presents everything through a web dashboard with WebSocket-driven live updates.

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

The core is an async pipeline loop in `run_pipeline()`:

```
FFmpeg stream capture → 30s audio chunks → Whisper transcription → LLM analysis → geocoding → DB persistence
```

Each stage broadcasts results via WebSocket (`audio_level`, `transcription`, `alert`, `event` message types).

### Key Components

- **stream_capture.py** — FFmpeg subprocess yielding PCM audio chunks with auto-reconnect and exponential backoff
- **transcriber.py** — Faster-Whisper wrapper with model caching, VAD filtering, int8 quantization on CPU
- **analyzer.py** — OpenRouter LLM client that analyzes a rolling window (~2 min) of transcripts; outputs structured JSON alerts with severity/category/location; tracks recent alerts for deduplication
- **geocoder.py** — Google Maps geocoding with address normalization and DB-backed cache
- **database.py** — SQLite with WAL mode; all operations wrapped in `asyncio.to_thread()` for non-blocking access
- **config.py** — Dataclass config loaded from env vars, persisted to DB `settings` table, updatable via API

### Database Tables

`transcriptions`, `alerts`, `events`, `geocode_cache`, `settings` — all in SQLite (`scanner.db`). Events link to multiple alerts; severity only escalates, never downgrades.

### API Layer

- **routes/api.py** — REST endpoints under `/api/` for transcriptions, alerts, events, config, status, audio playback
- **routes/ws.py** + **websocket_manager.py** — WebSocket at `/ws` broadcasting real-time updates to all connected clients

### Frontend

Single-page app in `static/` (vanilla JS, no framework). Tabs: Live, History, Map, Settings. WebSocket-driven for real-time data; Google Maps integration for event markers.

## Conventions

- All blocking I/O (DB queries, HTTP calls, file writes) uses `asyncio.to_thread()`
- DB queries return dicts via custom `row_factory`
- WebSocket messages follow `{"type": "...", "data": {...}}` format
- Alert severities: `critical`, `high`, `medium`, `low`
- Alert categories: `shooting`, `pursuit`, `fire`, `accident`, `medical`, `missing_person`, `robbery`, `assault`, `drug_activity`, `hazmat`, `other`
- Config changes via PUT `/api/config` trigger pipeline restart
- Audio saved as WAV files in `audio/` directory

## No Test Suite

There are currently no automated tests in this project.
