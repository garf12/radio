from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import datetime, timedelta, timezone


def _get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db(db_path: str) -> None:
    conn = _get_conn(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS transcriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            text TEXT NOT NULL,
            duration_s REAL
        );
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            status TEXT NOT NULL DEFAULT 'active',
            category TEXT NOT NULL,
            severity TEXT NOT NULL,
            title TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_events_status ON events(status);
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transcription_id INTEGER,
            timestamp TEXT NOT NULL,
            summary TEXT NOT NULL,
            severity TEXT NOT NULL,
            category TEXT NOT NULL,
            raw_context TEXT,
            model_used TEXT,
            FOREIGN KEY (transcription_id) REFERENCES transcriptions(id)
        );
        CREATE INDEX IF NOT EXISTS idx_transcriptions_ts ON transcriptions(timestamp);
        CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts(timestamp);
        CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts(severity);
    """)
    # Migration: add audio_file column if missing
    try:
        conn.execute("ALTER TABLE transcriptions ADD COLUMN audio_file TEXT")
    except sqlite3.OperationalError:
        pass  # column already exists
    # Migration: add event_id FK on alerts if missing
    try:
        conn.execute("ALTER TABLE alerts ADD COLUMN event_id INTEGER REFERENCES events(id)")
    except sqlite3.OperationalError:
        pass  # column already exists
    # Migration: add location columns on events
    for col_sql in [
        "ALTER TABLE events ADD COLUMN location_text TEXT",
        "ALTER TABLE events ADD COLUMN latitude REAL",
        "ALTER TABLE events ADD COLUMN longitude REAL",
    ]:
        try:
            conn.execute(col_sql)
        except sqlite3.OperationalError:
            pass
    # Geocode cache table
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS geocode_cache (
            address_key TEXT PRIMARY KEY,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            formatted_address TEXT,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS summaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            summary_text TEXT NOT NULL,
            period_start TEXT NOT NULL,
            period_end TEXT NOT NULL,
            transcription_count INTEGER NOT NULL DEFAULT 0,
            event_references TEXT,
            key_themes TEXT,
            activity_level TEXT DEFAULT 'moderate',
            model_used TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_summaries_ts ON summaries(timestamp);
    """)
    conn.close()


def _insert_transcription(db_path: str, text: str, duration_s: float, audio_file: str | None = None) -> dict:
    conn = _get_conn(db_path)
    ts = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        "INSERT INTO transcriptions (timestamp, text, duration_s, audio_file) VALUES (?, ?, ?, ?)",
        (ts, text, duration_s, audio_file),
    )
    row_id = cur.lastrowid
    conn.commit()
    conn.close()
    return {"id": row_id, "timestamp": ts, "text": text, "duration_s": duration_s, "audio_file": audio_file}


async def insert_transcription(db_path: str, text: str, duration_s: float, audio_file: str | None = None) -> dict:
    return await asyncio.to_thread(_insert_transcription, db_path, text, duration_s, audio_file)


def _insert_alert(
    db_path: str,
    transcription_id: int | None,
    summary: str,
    severity: str,
    category: str,
    raw_context: str,
    model_used: str,
) -> dict:
    conn = _get_conn(db_path)
    ts = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        "INSERT INTO alerts (transcription_id, timestamp, summary, severity, category, raw_context, model_used) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (transcription_id, ts, summary, severity, category, raw_context, model_used),
    )
    row_id = cur.lastrowid
    conn.commit()
    conn.close()
    return {
        "id": row_id,
        "transcription_id": transcription_id,
        "timestamp": ts,
        "summary": summary,
        "severity": severity,
        "category": category,
        "model_used": model_used,
    }


async def insert_alert(
    db_path: str,
    transcription_id: int | None,
    summary: str,
    severity: str,
    category: str,
    raw_context: str,
    model_used: str,
) -> dict:
    return await asyncio.to_thread(
        _insert_alert, db_path, transcription_id, summary, severity, category, raw_context, model_used
    )


def _get_transcriptions(db_path: str, limit: int = 50, offset: int = 0) -> list[dict]:
    conn = _get_conn(db_path)
    rows = conn.execute(
        "SELECT * FROM transcriptions ORDER BY id DESC LIMIT ? OFFSET ?",
        (limit, offset),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


async def get_transcriptions(db_path: str, limit: int = 50, offset: int = 0) -> list[dict]:
    return await asyncio.to_thread(_get_transcriptions, db_path, limit, offset)


def _get_transcription(db_path: str, transcription_id: int) -> dict | None:
    conn = _get_conn(db_path)
    row = conn.execute(
        "SELECT * FROM transcriptions WHERE id = ?", (transcription_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


async def get_transcription(db_path: str, transcription_id: int) -> dict | None:
    return await asyncio.to_thread(_get_transcription, db_path, transcription_id)


def _get_alerts(db_path: str, limit: int = 50, offset: int = 0) -> list[dict]:
    conn = _get_conn(db_path)
    rows = conn.execute(
        "SELECT * FROM alerts ORDER BY id DESC LIMIT ? OFFSET ?",
        (limit, offset),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


async def get_alerts(db_path: str, limit: int = 50, offset: int = 0) -> list[dict]:
    return await asyncio.to_thread(_get_alerts, db_path, limit, offset)


def _insert_event(
    db_path: str, title: str, category: str, severity: str,
    location_text: str | None = None, latitude: float | None = None, longitude: float | None = None,
) -> dict:
    conn = _get_conn(db_path)
    now = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        "INSERT INTO events (status, category, severity, title, created_at, updated_at, location_text, latitude, longitude) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("active", category, severity, title, now, now, location_text, latitude, longitude),
    )
    row_id = cur.lastrowid
    conn.commit()
    conn.close()
    return {
        "id": row_id, "status": "active", "category": category, "severity": severity,
        "title": title, "created_at": now, "updated_at": now,
        "location_text": location_text, "latitude": latitude, "longitude": longitude,
    }


async def insert_event(
    db_path: str, title: str, category: str, severity: str,
    location_text: str | None = None, latitude: float | None = None, longitude: float | None = None,
) -> dict:
    return await asyncio.to_thread(_insert_event, db_path, title, category, severity, location_text, latitude, longitude)


_SEVERITY_ORDER = {"low": 0, "medium": 1, "high": 2, "critical": 3}


def _update_event(
    db_path: str, event_id: int, severity: str | None = None, status: str | None = None,
    location_text: str | None = None, latitude: float | None = None, longitude: float | None = None,
) -> dict | None:
    conn = _get_conn(db_path)
    row = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
    if not row:
        conn.close()
        return None
    event = dict(row)
    now = datetime.now(timezone.utc).isoformat()
    new_severity = event["severity"]
    if severity and _SEVERITY_ORDER.get(severity, 0) > _SEVERITY_ORDER.get(event["severity"], 0):
        new_severity = severity
    new_status = status if status else event["status"]
    new_location_text = event.get("location_text")
    new_lat = event.get("latitude")
    new_lng = event.get("longitude")
    # Update location if provided and event doesn't already have coords (or location text differs)
    if location_text and latitude is not None and longitude is not None:
        if new_lat is None or new_location_text != location_text:
            new_location_text = location_text
            new_lat = latitude
            new_lng = longitude
    conn.execute(
        "UPDATE events SET severity = ?, status = ?, updated_at = ?, location_text = ?, latitude = ?, longitude = ? WHERE id = ?",
        (new_severity, new_status, now, new_location_text, new_lat, new_lng, event_id),
    )
    conn.commit()
    event.update(severity=new_severity, status=new_status, updated_at=now,
                 location_text=new_location_text, latitude=new_lat, longitude=new_lng)
    conn.close()
    return event


async def update_event(
    db_path: str, event_id: int, severity: str | None = None, status: str | None = None,
    location_text: str | None = None, latitude: float | None = None, longitude: float | None = None,
) -> dict | None:
    return await asyncio.to_thread(_update_event, db_path, event_id, severity, status, location_text, latitude, longitude)


def _link_alert_to_event(db_path: str, alert_id: int, event_id: int) -> None:
    conn = _get_conn(db_path)
    conn.execute("UPDATE alerts SET event_id = ? WHERE id = ?", (event_id, alert_id))
    conn.commit()
    conn.close()


async def link_alert_to_event(db_path: str, alert_id: int, event_id: int) -> None:
    return await asyncio.to_thread(_link_alert_to_event, db_path, alert_id, event_id)


def _get_active_events(db_path: str) -> list[dict]:
    conn = _get_conn(db_path)
    rows = conn.execute(
        "SELECT * FROM events WHERE status = 'active' ORDER BY updated_at DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


async def get_active_events(db_path: str) -> list[dict]:
    return await asyncio.to_thread(_get_active_events, db_path)


def _get_events(db_path: str, limit: int = 50, offset: int = 0, status: str | None = None) -> list[dict]:
    conn = _get_conn(db_path)
    where = ""
    params: list = []
    if status:
        where = "WHERE e.status = ? "
        params.append(status)
    params.extend([limit, offset])
    rows = conn.execute(
        "SELECT e.*, COUNT(a.id) AS alert_count FROM events e "
        "LEFT JOIN alerts a ON a.event_id = e.id "
        f"{where}GROUP BY e.id ORDER BY e.updated_at DESC LIMIT ? OFFSET ?",
        params,
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


async def get_events(db_path: str, limit: int = 50, offset: int = 0, status: str | None = None) -> list[dict]:
    return await asyncio.to_thread(_get_events, db_path, limit, offset, status)


def _get_event_with_alerts(db_path: str, event_id: int) -> dict | None:
    conn = _get_conn(db_path)
    row = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
    if not row:
        conn.close()
        return None
    event = dict(row)
    alerts = conn.execute(
        "SELECT * FROM alerts WHERE event_id = ? ORDER BY timestamp ASC",
        (event_id,),
    ).fetchall()
    conn.close()
    event["alerts"] = [dict(a) for a in alerts]
    return event


async def get_event_with_alerts(db_path: str, event_id: int) -> dict | None:
    return await asyncio.to_thread(_get_event_with_alerts, db_path, event_id)


def _get_counts(db_path: str) -> dict:
    conn = _get_conn(db_path)
    t_count = conn.execute("SELECT COUNT(*) FROM transcriptions").fetchone()[0]
    a_count = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
    e_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    e_active = conn.execute("SELECT COUNT(*) FROM events WHERE status = 'active'").fetchone()[0]
    try:
        s_count = conn.execute("SELECT COUNT(*) FROM summaries").fetchone()[0]
    except sqlite3.OperationalError:
        s_count = 0
    conn.close()
    return {"transcriptions": t_count, "alerts": a_count, "events": e_count, "events_active": e_active, "summaries": s_count}


async def get_counts(db_path: str) -> dict:
    return await asyncio.to_thread(_get_counts, db_path)


# --- Auto-resolve stale events & duplicate detection ---


def _auto_resolve_stale_events(db_path: str, timeout_minutes: int) -> list[dict]:
    conn = _get_conn(db_path)
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=timeout_minutes)).isoformat()
    rows = conn.execute(
        "SELECT * FROM events WHERE status = 'active' AND updated_at < ?", (cutoff,)
    ).fetchall()
    resolved = []
    now = datetime.now(timezone.utc).isoformat()
    for row in rows:
        conn.execute(
            "UPDATE events SET status = 'resolved', updated_at = ? WHERE id = ?",
            (now, row["id"]),
        )
        event = dict(row)
        event["status"] = "resolved"
        event["updated_at"] = now
        resolved.append(event)
    conn.commit()
    conn.close()
    return resolved


async def auto_resolve_stale_events(db_path: str, timeout_minutes: int) -> list[dict]:
    return await asyncio.to_thread(_auto_resolve_stale_events, db_path, timeout_minutes)


def _word_overlap_ratio(a: str, b: str) -> float:
    """Jaccard similarity of word sets from two strings."""
    words_a = set(a.lower().split())
    words_b = set(b.lower().split())
    if not words_a or not words_b:
        return 0.0
    return len(words_a & words_b) / len(words_a | words_b)


def _find_matching_event(db_path: str, category: str, location_text: str, alert_summary: str = "") -> dict | None:
    """Find an active event matching by category + location or title similarity.

    Matching strategy (checked in order):
    1. Same category + location word overlap >= 40%
    2. Same category + title vs alert_summary word overlap >= 40%
    3. Any category + location word overlap >= 60% (handles category mismatches)
    """
    conn = _get_conn(db_path)
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat()
    rows = conn.execute(
        "SELECT * FROM events WHERE status = 'active' AND updated_at >= ? ORDER BY updated_at DESC",
        (cutoff,),
    ).fetchall()
    conn.close()

    if not rows:
        return None

    best_match = None
    best_score = 0.0

    for row in rows:
        same_category = row["category"] == category

        # Check location similarity
        loc_score = 0.0
        if location_text and row["location_text"]:
            loc_score = _word_overlap_ratio(location_text, row["location_text"])

        # Check title vs summary similarity
        title_score = 0.0
        if alert_summary and row["title"]:
            title_score = _word_overlap_ratio(alert_summary, row["title"])

        # Strategy 1: same category + location overlap >= 40%
        if same_category and loc_score >= 0.4:
            score = loc_score + 1.0  # boost for same category
            if score > best_score:
                best_score = score
                best_match = row

        # Strategy 2: same category + title/summary overlap >= 40%
        if same_category and title_score >= 0.4:
            score = title_score + 1.0
            if score > best_score:
                best_score = score
                best_match = row

        # Strategy 3: cross-category location match >= 60%
        if loc_score >= 0.6:
            score = loc_score
            if score > best_score:
                best_score = score
                best_match = row

    if best_match:
        return dict(best_match)
    return None


async def find_matching_event(db_path: str, category: str, location_text: str, alert_summary: str = "") -> dict | None:
    return await asyncio.to_thread(_find_matching_event, db_path, category, location_text, alert_summary)


# --- Geocode cache & map queries ---


def _get_events_with_location(db_path: str, limit: int = 200, status: str | None = None, since: str | None = None) -> list[dict]:
    conn = _get_conn(db_path)
    conditions = ["latitude IS NOT NULL", "longitude IS NOT NULL"]
    params: list = []
    if status:
        conditions.append("status = ?")
        params.append(status)
    if since:
        conditions.append("updated_at >= ?")
        params.append(since)
    where = " AND ".join(conditions)
    params.append(limit)
    rows = conn.execute(
        f"SELECT * FROM events WHERE {where} ORDER BY updated_at DESC LIMIT ?",
        params,
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


async def get_events_with_location(db_path: str, limit: int = 200, status: str | None = None, since: str | None = None) -> list[dict]:
    return await asyncio.to_thread(_get_events_with_location, db_path, limit, status, since)


def _get_cached_geocode(db_path: str, address_key: str) -> tuple[float, float] | None:
    conn = _get_conn(db_path)
    row = conn.execute(
        "SELECT latitude, longitude FROM geocode_cache WHERE address_key = ?", (address_key,)
    ).fetchone()
    conn.close()
    if row:
        return (row["latitude"], row["longitude"])
    return None


async def get_cached_geocode(db_path: str, address_key: str) -> tuple[float, float] | None:
    return await asyncio.to_thread(_get_cached_geocode, db_path, address_key)


def _insert_geocode_cache(db_path: str, address_key: str, lat: float, lng: float, formatted_address: str | None) -> None:
    conn = _get_conn(db_path)
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO geocode_cache (address_key, latitude, longitude, formatted_address, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (address_key, lat, lng, formatted_address, now),
    )
    conn.commit()
    conn.close()


async def insert_geocode_cache(db_path: str, address_key: str, lat: float, lng: float, formatted_address: str | None) -> None:
    return await asyncio.to_thread(_insert_geocode_cache, db_path, address_key, lat, lng, formatted_address)


# --- Settings persistence ---


def save_settings(db_path: str, data: dict) -> None:
    conn = _get_conn(db_path)
    for key, value in data.items():
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (key, str(value)),
        )
    conn.commit()
    conn.close()


def load_settings(db_path: str) -> dict:
    conn = _get_conn(db_path)
    rows = conn.execute("SELECT key, value FROM settings").fetchall()
    conn.close()
    return {r["key"]: r["value"] for r in rows}


# --- Summaries ---


def _get_recent_transcriptions(db_path: str, minutes: int = 10) -> list[dict]:
    conn = _get_conn(db_path)
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
    rows = conn.execute(
        "SELECT * FROM transcriptions WHERE timestamp >= ? ORDER BY timestamp ASC",
        (cutoff,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


async def get_recent_transcriptions(db_path: str, minutes: int = 10) -> list[dict]:
    return await asyncio.to_thread(_get_recent_transcriptions, db_path, minutes)


def _insert_summary(
    db_path: str,
    summary_text: str,
    period_start: str,
    period_end: str,
    transcription_count: int,
    event_references: list | None,
    key_themes: list | None,
    activity_level: str,
    model_used: str | None,
) -> dict:
    conn = _get_conn(db_path)
    ts = datetime.now(timezone.utc).isoformat()
    event_refs_json = json.dumps(event_references) if event_references else None
    themes_json = json.dumps(key_themes) if key_themes else None
    cur = conn.execute(
        "INSERT INTO summaries (timestamp, summary_text, period_start, period_end, transcription_count, "
        "event_references, key_themes, activity_level, model_used) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (ts, summary_text, period_start, period_end, transcription_count,
         event_refs_json, themes_json, activity_level, model_used),
    )
    row_id = cur.lastrowid
    conn.commit()
    conn.close()
    return {
        "id": row_id,
        "timestamp": ts,
        "summary_text": summary_text,
        "period_start": period_start,
        "period_end": period_end,
        "transcription_count": transcription_count,
        "event_references": event_references or [],
        "key_themes": key_themes or [],
        "activity_level": activity_level,
        "model_used": model_used,
    }


async def insert_summary(
    db_path: str,
    summary_text: str,
    period_start: str,
    period_end: str,
    transcription_count: int,
    event_references: list | None,
    key_themes: list | None,
    activity_level: str,
    model_used: str | None,
) -> dict:
    return await asyncio.to_thread(
        _insert_summary, db_path, summary_text, period_start, period_end,
        transcription_count, event_references, key_themes, activity_level, model_used,
    )


def _get_summaries(db_path: str, hours: float | None = None, limit: int = 100, offset: int = 0) -> list[dict]:
    conn = _get_conn(db_path)
    if hours is not None:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        rows = conn.execute(
            "SELECT * FROM summaries WHERE timestamp >= ? ORDER BY timestamp DESC LIMIT ? OFFSET ?",
            (cutoff, limit, offset),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM summaries ORDER BY timestamp DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
    conn.close()
    results = []
    for r in rows:
        d = dict(r)
        # Parse JSON strings to lists
        try:
            d["event_references"] = json.loads(d["event_references"]) if d.get("event_references") else []
        except (json.JSONDecodeError, TypeError):
            d["event_references"] = []
        try:
            d["key_themes"] = json.loads(d["key_themes"]) if d.get("key_themes") else []
        except (json.JSONDecodeError, TypeError):
            d["key_themes"] = []
        results.append(d)
    return results


async def get_summaries(db_path: str, hours: float | None = None, limit: int = 100, offset: int = 0) -> list[dict]:
    return await asyncio.to_thread(_get_summaries, db_path, hours, limit, offset)
