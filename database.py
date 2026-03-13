from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


def _get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db(db_path: str) -> None:
    conn = _get_conn(db_path)
    try:
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
        # Migration: add summary_type column if missing
        try:
            conn.execute("ALTER TABLE summaries ADD COLUMN summary_type TEXT DEFAULT '10min'")
        except sqlite3.OperationalError:
            pass  # column already exists
        # Streams table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS streams (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                enabled INTEGER DEFAULT 1,
                color TEXT DEFAULT '#00e89d',
                sort_order INTEGER DEFAULT 0,
                created_at TEXT,
                updated_at TEXT
            )
        """)
        # Migration: add stream_id column to relevant tables
        for tbl in ("transcriptions", "alerts", "events", "summaries"):
            try:
                conn.execute(f"ALTER TABLE {tbl} ADD COLUMN stream_id TEXT")
            except sqlite3.OperationalError:
                pass
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{tbl}_stream_id ON {tbl}(stream_id)")
        # Migration: add confidence/review columns to transcriptions
        for col_sql in [
            "ALTER TABLE transcriptions ADD COLUMN confidence REAL",
            "ALTER TABLE transcriptions ADD COLUMN flags TEXT",
            "ALTER TABLE transcriptions ADD COLUMN segment_details TEXT",
            "ALTER TABLE transcriptions ADD COLUMN needs_review INTEGER DEFAULT 0",
            "ALTER TABLE transcriptions ADD COLUMN review_status TEXT",
            "ALTER TABLE transcriptions ADD COLUMN corrected_text TEXT",
            "ALTER TABLE transcriptions ADD COLUMN reviewed_at TEXT",
        ]:
            try:
                conn.execute(col_sql)
            except sqlite3.OperationalError:
                pass
        # Learning loop tables
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS alert_feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_id INTEGER NOT NULL,
                feedback_type TEXT NOT NULL,
                corrected_summary TEXT,
                corrected_severity TEXT,
                corrected_category TEXT,
                notes TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (alert_id) REFERENCES alerts(id)
            );
            CREATE TABLE IF NOT EXISTS regional_dictionary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                term TEXT NOT NULL,
                replacement TEXT NOT NULL,
                category TEXT DEFAULT 'general',
                frequency INTEGER DEFAULT 0,
                active INTEGER DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_regional_dictionary_term
                ON regional_dictionary(term);
            CREATE TABLE IF NOT EXISTS feedback_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                period_date TEXT UNIQUE NOT NULL,
                total_alerts INTEGER DEFAULT 0,
                true_positives INTEGER DEFAULT 0,
                false_positives INTEGER DEFAULT 0,
                corrections INTEGER DEFAULT 0,
                avg_confidence REAL,
                review_queue_size INTEGER DEFAULT 0
            );
        """)
    finally:
        conn.close()


def _seed_default_stream(db_path: str, stream_url: str) -> None:
    """If streams table is empty, seed a default stream and backfill.

    Checks the env-var seed URL first, then falls back to the legacy
    ``stream_url`` value stored in the ``settings`` table (from before
    multi-stream support was added).
    """
    conn = _get_conn(db_path)
    try:
        count = conn.execute("SELECT COUNT(*) FROM streams").fetchone()[0]
        if count > 0:
            return
        # Resolve URL: prefer env-var seed, fall back to legacy settings table
        url = stream_url
        if not url:
            row = conn.execute(
                "SELECT value FROM settings WHERE key = 'stream_url'"
            ).fetchone()
            if row:
                url = row[0]
        if not url:
            return
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO streams (id, name, url, enabled, color, sort_order, created_at, updated_at) "
            "VALUES (?, ?, ?, 1, '#00e89d', 0, ?, ?)",
            ("default", "Default Scanner", url, now, now),
        )
        for tbl in ("transcriptions", "alerts", "events", "summaries"):
            conn.execute(f"UPDATE {tbl} SET stream_id = 'default' WHERE stream_id IS NULL")
        conn.commit()
    finally:
        conn.close()


async def seed_default_stream(db_path: str, stream_url: str) -> None:
    return await asyncio.to_thread(_seed_default_stream, db_path, stream_url)


# --- Stream CRUD ---


def _get_streams(db_path: str, enabled_only: bool = False) -> list[dict]:
    conn = _get_conn(db_path)
    try:
        if enabled_only:
            rows = conn.execute("SELECT * FROM streams WHERE enabled = 1 ORDER BY sort_order, created_at").fetchall()
        else:
            rows = conn.execute("SELECT * FROM streams ORDER BY sort_order, created_at").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


async def get_streams(db_path: str, enabled_only: bool = False) -> list[dict]:
    return await asyncio.to_thread(_get_streams, db_path, enabled_only)


def _get_stream(db_path: str, stream_id: str) -> dict | None:
    conn = _get_conn(db_path)
    try:
        row = conn.execute("SELECT * FROM streams WHERE id = ?", (stream_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


async def get_stream(db_path: str, stream_id: str) -> dict | None:
    return await asyncio.to_thread(_get_stream, db_path, stream_id)


def _create_stream(db_path: str, stream_id: str, name: str, url: str, color: str = "#00e89d", enabled: bool = True) -> dict:
    conn = _get_conn(db_path)
    try:
        now = datetime.now(timezone.utc).isoformat()
        sort_order = conn.execute("SELECT COALESCE(MAX(sort_order), -1) + 1 FROM streams").fetchone()[0]
        conn.execute(
            "INSERT INTO streams (id, name, url, enabled, color, sort_order, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (stream_id, name, url, 1 if enabled else 0, color, sort_order, now, now),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM streams WHERE id = ?", (stream_id,)).fetchone()
        return dict(row)
    finally:
        conn.close()


async def create_stream(db_path: str, stream_id: str, name: str, url: str, color: str = "#00e89d", enabled: bool = True) -> dict:
    return await asyncio.to_thread(_create_stream, db_path, stream_id, name, url, color, enabled)


def _update_stream(db_path: str, stream_id: str, **kwargs) -> dict | None:
    conn = _get_conn(db_path)
    try:
        row = conn.execute("SELECT * FROM streams WHERE id = ?", (stream_id,)).fetchone()
        if not row:
            return None
        allowed = {"name", "url", "enabled", "color", "sort_order"}
        sets = []
        params = []
        for k, v in kwargs.items():
            if k in allowed:
                sets.append(f"{k} = ?")
                params.append(v)
        if not sets:
            return dict(row)
        now = datetime.now(timezone.utc).isoformat()
        sets.append("updated_at = ?")
        params.append(now)
        params.append(stream_id)
        conn.execute(f"UPDATE streams SET {', '.join(sets)} WHERE id = ?", params)
        conn.commit()
        row = conn.execute("SELECT * FROM streams WHERE id = ?", (stream_id,)).fetchone()
        return dict(row)
    finally:
        conn.close()


async def update_stream(db_path: str, stream_id: str, **kwargs) -> dict | None:
    return await asyncio.to_thread(_update_stream, db_path, stream_id, **kwargs)


def _delete_stream(db_path: str, stream_id: str) -> bool:
    conn = _get_conn(db_path)
    try:
        cur = conn.execute("DELETE FROM streams WHERE id = ?", (stream_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


async def delete_stream(db_path: str, stream_id: str) -> bool:
    return await asyncio.to_thread(_delete_stream, db_path, stream_id)


def _insert_transcription(
    db_path: str, text: str, duration_s: float, audio_file: str | None = None,
    confidence: float | None = None, flags: list | None = None, segment_details: list | None = None,
    stream_id: str | None = None,
) -> dict:
    conn = _get_conn(db_path)
    try:
        ts = datetime.now(timezone.utc).isoformat()
        flags_json = json.dumps(flags) if flags else None
        segments_json = json.dumps(segment_details) if segment_details else None
        needs_review = 1 if flags else 0
        review_status = "pending" if flags else None
        cur = conn.execute(
            "INSERT INTO transcriptions (timestamp, text, duration_s, audio_file, confidence, flags, segment_details, needs_review, review_status, stream_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (ts, text, duration_s, audio_file, confidence, flags_json, segments_json, needs_review, review_status, stream_id),
        )
        row_id = cur.lastrowid
        conn.commit()
        return {
            "id": row_id, "timestamp": ts, "text": text, "duration_s": duration_s,
            "audio_file": audio_file, "confidence": confidence,
            "flags": flags or [], "needs_review": needs_review,
            "stream_id": stream_id,
        }
    finally:
        conn.close()


async def insert_transcription(
    db_path: str, text: str, duration_s: float, audio_file: str | None = None,
    confidence: float | None = None, flags: list | None = None, segment_details: list | None = None,
    stream_id: str | None = None,
) -> dict:
    return await asyncio.to_thread(
        _insert_transcription, db_path, text, duration_s, audio_file, confidence, flags, segment_details, stream_id,
    )


def _insert_alert(
    db_path: str,
    transcription_id: int | None,
    summary: str,
    severity: str,
    category: str,
    raw_context: str,
    model_used: str,
    stream_id: str | None = None,
) -> dict:
    conn = _get_conn(db_path)
    try:
        ts = datetime.now(timezone.utc).isoformat()
        cur = conn.execute(
            "INSERT INTO alerts (transcription_id, timestamp, summary, severity, category, raw_context, model_used, stream_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (transcription_id, ts, summary, severity, category, raw_context, model_used, stream_id),
        )
        row_id = cur.lastrowid
        conn.commit()
        return {
            "id": row_id,
            "transcription_id": transcription_id,
            "timestamp": ts,
            "summary": summary,
            "severity": severity,
            "category": category,
            "model_used": model_used,
            "stream_id": stream_id,
        }
    finally:
        conn.close()


async def insert_alert(
    db_path: str,
    transcription_id: int | None,
    summary: str,
    severity: str,
    category: str,
    raw_context: str,
    model_used: str,
    stream_id: str | None = None,
) -> dict:
    return await asyncio.to_thread(
        _insert_alert, db_path, transcription_id, summary, severity, category, raw_context, model_used, stream_id,
    )


def _get_transcriptions(db_path: str, limit: int = 50, offset: int = 0, stream_id: str | None = None) -> list[dict]:
    conn = _get_conn(db_path)
    try:
        if stream_id:
            rows = conn.execute(
                "SELECT * FROM transcriptions WHERE stream_id = ? ORDER BY id DESC LIMIT ? OFFSET ?",
                (stream_id, limit, offset),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM transcriptions ORDER BY id DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


async def get_transcriptions(db_path: str, limit: int = 50, offset: int = 0, stream_id: str | None = None) -> list[dict]:
    return await asyncio.to_thread(_get_transcriptions, db_path, limit, offset, stream_id)


def _get_transcription(db_path: str, transcription_id: int) -> dict | None:
    conn = _get_conn(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM transcriptions WHERE id = ?", (transcription_id,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


async def get_transcription(db_path: str, transcription_id: int) -> dict | None:
    return await asyncio.to_thread(_get_transcription, db_path, transcription_id)


def _get_alerts(db_path: str, limit: int = 50, offset: int = 0, stream_id: str | None = None) -> list[dict]:
    conn = _get_conn(db_path)
    try:
        if stream_id:
            rows = conn.execute(
                "SELECT * FROM alerts WHERE stream_id = ? ORDER BY id DESC LIMIT ? OFFSET ?",
                (stream_id, limit, offset),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM alerts ORDER BY id DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


async def get_alerts(db_path: str, limit: int = 50, offset: int = 0, stream_id: str | None = None) -> list[dict]:
    return await asyncio.to_thread(_get_alerts, db_path, limit, offset, stream_id)


def _insert_event(
    db_path: str, title: str, category: str, severity: str,
    location_text: str | None = None, latitude: float | None = None, longitude: float | None = None,
    stream_id: str | None = None,
) -> dict:
    conn = _get_conn(db_path)
    try:
        now = datetime.now(timezone.utc).isoformat()
        cur = conn.execute(
            "INSERT INTO events (status, category, severity, title, created_at, updated_at, location_text, latitude, longitude, stream_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("active", category, severity, title, now, now, location_text, latitude, longitude, stream_id),
        )
        row_id = cur.lastrowid
        conn.commit()
        return {
            "id": row_id, "status": "active", "category": category, "severity": severity,
            "title": title, "created_at": now, "updated_at": now,
            "location_text": location_text, "latitude": latitude, "longitude": longitude,
            "stream_id": stream_id,
        }
    finally:
        conn.close()


async def insert_event(
    db_path: str, title: str, category: str, severity: str,
    location_text: str | None = None, latitude: float | None = None, longitude: float | None = None,
    stream_id: str | None = None,
) -> dict:
    return await asyncio.to_thread(_insert_event, db_path, title, category, severity, location_text, latitude, longitude, stream_id)


_SEVERITY_ORDER = {"low": 0, "medium": 1, "high": 2, "critical": 3}


def _update_event(
    db_path: str, event_id: int, severity: str | None = None, status: str | None = None,
    location_text: str | None = None, latitude: float | None = None, longitude: float | None = None,
) -> dict | None:
    conn = _get_conn(db_path)
    try:
        row = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
        if not row:
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
        return event
    finally:
        conn.close()


async def update_event(
    db_path: str, event_id: int, severity: str | None = None, status: str | None = None,
    location_text: str | None = None, latitude: float | None = None, longitude: float | None = None,
) -> dict | None:
    return await asyncio.to_thread(_update_event, db_path, event_id, severity, status, location_text, latitude, longitude)


def _link_alert_to_event(db_path: str, alert_id: int, event_id: int) -> None:
    conn = _get_conn(db_path)
    try:
        conn.execute("UPDATE alerts SET event_id = ? WHERE id = ?", (event_id, alert_id))
        conn.commit()
    finally:
        conn.close()


async def link_alert_to_event(db_path: str, alert_id: int, event_id: int) -> None:
    return await asyncio.to_thread(_link_alert_to_event, db_path, alert_id, event_id)


def _get_active_events(db_path: str, stream_id: str | None = None) -> list[dict]:
    conn = _get_conn(db_path)
    try:
        if stream_id:
            rows = conn.execute(
                "SELECT * FROM events WHERE status = 'active' AND stream_id = ? ORDER BY updated_at DESC",
                (stream_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM events WHERE status = 'active' ORDER BY updated_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


async def get_active_events(db_path: str, stream_id: str | None = None) -> list[dict]:
    return await asyncio.to_thread(_get_active_events, db_path, stream_id)


def _get_events(db_path: str, limit: int = 50, offset: int = 0, status: str | None = None, stream_id: str | None = None) -> list[dict]:
    conn = _get_conn(db_path)
    try:
        conditions: list[str] = []
        params: list = []
        if status:
            conditions.append("e.status = ?")
            params.append(status)
        if stream_id:
            conditions.append("e.stream_id = ?")
            params.append(stream_id)
        where = ("WHERE " + " AND ".join(conditions) + " ") if conditions else ""
        params.extend([limit, offset])
        rows = conn.execute(
            "SELECT e.*, COUNT(a.id) AS alert_count, "
            "(SELECT a2.transcription_id FROM alerts a2 "
            "JOIN transcriptions t ON t.id = a2.transcription_id "
            "WHERE a2.event_id = e.id AND t.audio_file IS NOT NULL "
            "ORDER BY a2.timestamp DESC LIMIT 1) AS audio_transcription_id "
            "FROM events e "
            "LEFT JOIN alerts a ON a.event_id = e.id "
            f"{where}GROUP BY e.id ORDER BY e.updated_at DESC LIMIT ? OFFSET ?",
            params,
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


async def get_events(db_path: str, limit: int = 50, offset: int = 0, status: str | None = None, stream_id: str | None = None) -> list[dict]:
    return await asyncio.to_thread(_get_events, db_path, limit, offset, status, stream_id)


def _get_event_with_alerts(db_path: str, event_id: int) -> dict | None:
    conn = _get_conn(db_path)
    try:
        row = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
        if not row:
            return None
        event = dict(row)
        alerts = conn.execute(
            "SELECT a.*, t.audio_file FROM alerts a "
            "LEFT JOIN transcriptions t ON t.id = a.transcription_id "
            "WHERE a.event_id = ? ORDER BY a.timestamp ASC",
            (event_id,),
        ).fetchall()
        event["alerts"] = [dict(a) for a in alerts]
        return event
    finally:
        conn.close()


async def get_event_with_alerts(db_path: str, event_id: int) -> dict | None:
    return await asyncio.to_thread(_get_event_with_alerts, db_path, event_id)


def _get_counts(db_path: str) -> dict:
    conn = _get_conn(db_path)
    try:
        t_count = conn.execute("SELECT COUNT(*) FROM transcriptions").fetchone()[0]
        a_count = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
        e_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        e_active = conn.execute("SELECT COUNT(*) FROM events WHERE status = 'active'").fetchone()[0]
        try:
            s_count = conn.execute("SELECT COUNT(*) FROM summaries").fetchone()[0]
        except sqlite3.OperationalError:
            s_count = 0
        return {"transcriptions": t_count, "alerts": a_count, "events": e_count, "events_active": e_active, "summaries": s_count}
    finally:
        conn.close()


async def get_counts(db_path: str) -> dict:
    return await asyncio.to_thread(_get_counts, db_path)


# --- Auto-resolve stale events & duplicate detection ---


def _auto_resolve_stale_events(db_path: str, timeout_minutes: int) -> list[dict]:
    conn = _get_conn(db_path)
    try:
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
        return resolved
    finally:
        conn.close()


async def auto_resolve_stale_events(db_path: str, timeout_minutes: int) -> list[dict]:
    return await asyncio.to_thread(_auto_resolve_stale_events, db_path, timeout_minutes)


def _word_overlap_ratio(a: str, b: str) -> float:
    """Jaccard similarity of word sets from two strings."""
    words_a = set(a.lower().split())
    words_b = set(b.lower().split())
    if not words_a or not words_b:
        return 0.0
    return len(words_a & words_b) / len(words_a | words_b)


def _find_matching_event(db_path: str, category: str, location_text: str, alert_summary: str = "", stream_id: str | None = None) -> dict | None:
    """Find an active event matching by category + location or title similarity.

    Matching strategy (checked in order):
    1. Same category + location word overlap >= 40%
    2. Same category + title vs alert_summary word overlap >= 40%
    3. Any category + location word overlap >= 60% (handles category mismatches)
    """
    conn = _get_conn(db_path)
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat()
        if stream_id:
            rows = conn.execute(
                "SELECT * FROM events WHERE status = 'active' AND updated_at >= ? AND stream_id = ? ORDER BY updated_at DESC",
                (cutoff, stream_id),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM events WHERE status = 'active' AND updated_at >= ? ORDER BY updated_at DESC",
                (cutoff,),
            ).fetchall()
    finally:
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


async def find_matching_event(db_path: str, category: str, location_text: str, alert_summary: str = "", stream_id: str | None = None) -> dict | None:
    return await asyncio.to_thread(_find_matching_event, db_path, category, location_text, alert_summary, stream_id)


# --- Geocode cache & map queries ---


def _get_events_with_location(db_path: str, limit: int = 200, status: str | None = None, since: str | None = None, stream_id: str | None = None) -> list[dict]:
    conn = _get_conn(db_path)
    try:
        conditions = ["latitude IS NOT NULL", "longitude IS NOT NULL"]
        params: list = []
        if status:
            conditions.append("status = ?")
            params.append(status)
        if since:
            conditions.append("updated_at >= ?")
            params.append(since)
        if stream_id:
            conditions.append("stream_id = ?")
            params.append(stream_id)
        where = " AND ".join(conditions)
        params.append(limit)
        rows = conn.execute(
            f"SELECT * FROM events WHERE {where} ORDER BY updated_at DESC LIMIT ?",
            params,
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


async def get_events_with_location(db_path: str, limit: int = 200, status: str | None = None, since: str | None = None, stream_id: str | None = None) -> list[dict]:
    return await asyncio.to_thread(_get_events_with_location, db_path, limit, status, since, stream_id)


def _get_cached_geocode(db_path: str, address_key: str) -> tuple[float, float] | None:
    conn = _get_conn(db_path)
    try:
        row = conn.execute(
            "SELECT latitude, longitude FROM geocode_cache WHERE address_key = ?", (address_key,)
        ).fetchone()
        if row:
            return (row["latitude"], row["longitude"])
        return None
    finally:
        conn.close()


async def get_cached_geocode(db_path: str, address_key: str) -> tuple[float, float] | None:
    return await asyncio.to_thread(_get_cached_geocode, db_path, address_key)


def _insert_geocode_cache(db_path: str, address_key: str, lat: float, lng: float, formatted_address: str | None) -> None:
    conn = _get_conn(db_path)
    try:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT OR REPLACE INTO geocode_cache (address_key, latitude, longitude, formatted_address, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (address_key, lat, lng, formatted_address, now),
        )
        conn.commit()
    finally:
        conn.close()


async def insert_geocode_cache(db_path: str, address_key: str, lat: float, lng: float, formatted_address: str | None) -> None:
    return await asyncio.to_thread(_insert_geocode_cache, db_path, address_key, lat, lng, formatted_address)


# --- Settings persistence ---


def save_settings(db_path: str, data: dict) -> None:
    conn = _get_conn(db_path)
    try:
        for key, value in data.items():
            conn.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                (key, str(value)),
            )
        conn.commit()
    finally:
        conn.close()


def load_settings(db_path: str) -> dict:
    conn = _get_conn(db_path)
    try:
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
        return {r["key"]: r["value"] for r in rows}
    finally:
        conn.close()


# --- Summaries ---


def _get_recent_transcriptions(db_path: str, minutes: int = 10, stream_id: str | None = None) -> list[dict]:
    conn = _get_conn(db_path)
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
        if stream_id:
            rows = conn.execute(
                "SELECT * FROM transcriptions WHERE timestamp >= ? AND stream_id = ? ORDER BY timestamp ASC",
                (cutoff, stream_id),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM transcriptions WHERE timestamp >= ? ORDER BY timestamp ASC",
                (cutoff,),
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


async def get_recent_transcriptions(db_path: str, minutes: int = 10, stream_id: str | None = None) -> list[dict]:
    return await asyncio.to_thread(_get_recent_transcriptions, db_path, minutes, stream_id)


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
    summary_type: str = "10min",
    stream_id: str | None = None,
) -> dict:
    conn = _get_conn(db_path)
    try:
        ts = datetime.now(timezone.utc).isoformat()
        event_refs_json = json.dumps(event_references) if event_references else None
        themes_json = json.dumps(key_themes) if key_themes else None
        cur = conn.execute(
            "INSERT INTO summaries (timestamp, summary_text, period_start, period_end, transcription_count, "
            "event_references, key_themes, activity_level, model_used, summary_type, stream_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (ts, summary_text, period_start, period_end, transcription_count,
             event_refs_json, themes_json, activity_level, model_used, summary_type, stream_id),
        )
        row_id = cur.lastrowid
        conn.commit()
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
            "summary_type": summary_type,
            "stream_id": stream_id,
        }
    finally:
        conn.close()


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
    summary_type: str = "10min",
    stream_id: str | None = None,
) -> dict:
    return await asyncio.to_thread(
        _insert_summary, db_path, summary_text, period_start, period_end,
        transcription_count, event_references, key_themes, activity_level, model_used,
        summary_type, stream_id,
    )


def _get_summaries(db_path: str, hours: float | None = None, limit: int = 100, offset: int = 0, stream_id: str | None = None) -> list[dict]:
    conn = _get_conn(db_path)
    try:
        conditions: list[str] = []
        params: list = []
        if hours is not None:
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
            conditions.append("timestamp >= ?")
            params.append(cutoff)
        if stream_id:
            conditions.append("stream_id = ?")
            params.append(stream_id)
        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        params.extend([limit, offset])
        rows = conn.execute(
            f"SELECT * FROM summaries{where} ORDER BY timestamp DESC LIMIT ? OFFSET ?",
            params,
        ).fetchall()
    finally:
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


async def get_summaries(db_path: str, hours: float | None = None, limit: int = 100, offset: int = 0, stream_id: str | None = None) -> list[dict]:
    return await asyncio.to_thread(_get_summaries, db_path, hours, limit, offset, stream_id)


def _parse_summary_json(d: dict) -> dict:
    """Parse JSON string fields in a summary row dict."""
    try:
        d["event_references"] = json.loads(d["event_references"]) if d.get("event_references") else []
    except (json.JSONDecodeError, TypeError):
        d["event_references"] = []
    try:
        d["key_themes"] = json.loads(d["key_themes"]) if d.get("key_themes") else []
    except (json.JSONDecodeError, TypeError):
        d["key_themes"] = []
    return d


def _get_latest_summaries(db_path: str) -> dict:
    """Return the most recent summary of each type ('10min' and 'hourly')."""
    conn = _get_conn(db_path)
    try:
        result = {"recent": None, "hourly": None}
        for summary_type, key in [("10min", "recent"), ("hourly", "hourly")]:
            row = conn.execute(
                "SELECT * FROM summaries WHERE summary_type = ? ORDER BY timestamp DESC LIMIT 1",
                (summary_type,),
            ).fetchone()
            if row:
                result[key] = _parse_summary_json(dict(row))
        return result
    finally:
        conn.close()


async def get_latest_summaries(db_path: str) -> dict:
    return await asyncio.to_thread(_get_latest_summaries, db_path)


# --- Review Queue & Feedback ---


def _get_review_queue(db_path: str, review_type: str = "all", limit: int = 50, offset: int = 0) -> list[dict]:
    conn = _get_conn(db_path)
    try:
        items = []
        if review_type in ("all", "transcriptions"):
            rows = conn.execute(
                "SELECT *, 'transcription' as item_type FROM transcriptions "
                "WHERE needs_review = 1 ORDER BY timestamp DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
            for r in rows:
                d = dict(r)
                try:
                    d["flags"] = json.loads(d["flags"]) if d.get("flags") else []
                except (json.JSONDecodeError, TypeError):
                    d["flags"] = []
                items.append(d)
        if review_type in ("all", "alerts"):
            rows = conn.execute(
                "SELECT a.*, 'alert' as item_type FROM alerts a "
                "LEFT JOIN alert_feedback f ON f.alert_id = a.id "
                "WHERE f.id IS NULL ORDER BY a.timestamp DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
            items.extend(dict(r) for r in rows)
        items.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        return items[:limit]
    finally:
        conn.close()


async def get_review_queue(db_path: str, review_type: str = "all", limit: int = 50, offset: int = 0) -> list[dict]:
    return await asyncio.to_thread(_get_review_queue, db_path, review_type, limit, offset)


def _submit_transcription_correction(db_path: str, transcription_id: int, corrected_text: str) -> dict | None:
    conn = _get_conn(db_path)
    try:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "UPDATE transcriptions SET corrected_text = ?, review_status = 'corrected', reviewed_at = ?, needs_review = 0 WHERE id = ?",
            (corrected_text, now, transcription_id),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM transcriptions WHERE id = ?", (transcription_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


async def submit_transcription_correction(db_path: str, transcription_id: int, corrected_text: str) -> dict | None:
    return await asyncio.to_thread(_submit_transcription_correction, db_path, transcription_id, corrected_text)


def _confirm_transcription(db_path: str, transcription_id: int) -> dict | None:
    conn = _get_conn(db_path)
    try:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "UPDATE transcriptions SET review_status = 'confirmed', reviewed_at = ?, needs_review = 0 WHERE id = ?",
            (now, transcription_id),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM transcriptions WHERE id = ?", (transcription_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


async def confirm_transcription(db_path: str, transcription_id: int) -> dict | None:
    return await asyncio.to_thread(_confirm_transcription, db_path, transcription_id)


def _insert_alert_feedback(
    db_path: str, alert_id: int, feedback_type: str,
    corrected_summary: str | None = None, corrected_severity: str | None = None,
    corrected_category: str | None = None, notes: str | None = None,
) -> dict:
    conn = _get_conn(db_path)
    try:
        ts = datetime.now(timezone.utc).isoformat()
        cur = conn.execute(
            "INSERT INTO alert_feedback (alert_id, feedback_type, corrected_summary, corrected_severity, corrected_category, notes, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (alert_id, feedback_type, corrected_summary, corrected_severity, corrected_category, notes, ts),
        )
        row_id = cur.lastrowid
        conn.commit()
        # Update daily stats
        date_str = ts[:10]
        _increment_feedback_stat(conn, date_str, feedback_type)
        return {
            "id": row_id, "alert_id": alert_id, "feedback_type": feedback_type,
            "corrected_summary": corrected_summary, "corrected_severity": corrected_severity,
            "corrected_category": corrected_category, "notes": notes, "created_at": ts,
        }
    finally:
        conn.close()


def _increment_feedback_stat(conn: sqlite3.Connection, date_str: str, feedback_type: str) -> None:
    """Increment daily feedback stats."""
    conn.execute(
        "INSERT INTO feedback_stats (period_date, total_alerts) VALUES (?, 0) "
        "ON CONFLICT(period_date) DO NOTHING",
        (date_str,),
    )
    if feedback_type == "correct":
        conn.execute(
            "UPDATE feedback_stats SET true_positives = true_positives + 1 WHERE period_date = ?",
            (date_str,),
        )
    elif feedback_type == "false_positive":
        conn.execute(
            "UPDATE feedback_stats SET false_positives = false_positives + 1 WHERE period_date = ?",
            (date_str,),
        )
    elif feedback_type == "correction":
        conn.execute(
            "UPDATE feedback_stats SET corrections = corrections + 1 WHERE period_date = ?",
            (date_str,),
        )
    conn.commit()


async def insert_alert_feedback(
    db_path: str, alert_id: int, feedback_type: str,
    corrected_summary: str | None = None, corrected_severity: str | None = None,
    corrected_category: str | None = None, notes: str | None = None,
) -> dict:
    return await asyncio.to_thread(
        _insert_alert_feedback, db_path, alert_id, feedback_type,
        corrected_summary, corrected_severity, corrected_category, notes,
    )


def _get_recent_false_positives(db_path: str, limit: int = 5) -> list[dict]:
    conn = _get_conn(db_path)
    try:
        rows = conn.execute(
            "SELECT f.*, a.summary, a.category, a.severity FROM alert_feedback f "
            "JOIN alerts a ON a.id = f.alert_id "
            "WHERE f.feedback_type = 'false_positive' "
            "ORDER BY f.created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


async def get_recent_false_positives(db_path: str, limit: int = 5) -> list[dict]:
    return await asyncio.to_thread(_get_recent_false_positives, db_path, limit)


def _get_correction_patterns(db_path: str, limit: int = 10) -> list[dict]:
    """Get frequently corrected category patterns."""
    conn = _get_conn(db_path)
    try:
        rows = conn.execute(
            "SELECT a.category as original_category, f.corrected_category, COUNT(*) as count "
            "FROM alert_feedback f JOIN alerts a ON a.id = f.alert_id "
            "WHERE f.feedback_type = 'correction' AND f.corrected_category IS NOT NULL "
            "AND f.corrected_category != a.category "
            "GROUP BY a.category, f.corrected_category ORDER BY count DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


async def get_correction_patterns(db_path: str, limit: int = 10) -> list[dict]:
    return await asyncio.to_thread(_get_correction_patterns, db_path, limit)


# --- Regional Dictionary ---


def _get_dictionary_entries(db_path: str, category: str | None = None, active_only: bool = True) -> list[dict]:
    conn = _get_conn(db_path)
    try:
        conditions = []
        params: list = []
        if active_only:
            conditions.append("active = 1")
        if category:
            conditions.append("category = ?")
            params.append(category)
        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        rows = conn.execute(
            f"SELECT * FROM regional_dictionary{where} ORDER BY term ASC",
            params,
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


async def get_dictionary_entries(db_path: str, category: str | None = None, active_only: bool = True) -> list[dict]:
    return await asyncio.to_thread(_get_dictionary_entries, db_path, category, active_only)


def _upsert_dictionary_entry(db_path: str, term: str, replacement: str, category: str = "general") -> dict:
    conn = _get_conn(db_path)
    try:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO regional_dictionary (term, replacement, category, frequency, active, created_at, updated_at) "
            "VALUES (?, ?, ?, 0, 1, ?, ?) "
            "ON CONFLICT(term) DO UPDATE SET replacement = excluded.replacement, category = excluded.category, "
            "updated_at = excluded.updated_at",
            (term.lower(), replacement, category, now, now),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM regional_dictionary WHERE term = ?", (term.lower(),)).fetchone()
        return dict(row)
    finally:
        conn.close()


async def upsert_dictionary_entry(db_path: str, term: str, replacement: str, category: str = "general") -> dict:
    return await asyncio.to_thread(_upsert_dictionary_entry, db_path, term, replacement, category)


def _delete_dictionary_entry(db_path: str, entry_id: int) -> bool:
    conn = _get_conn(db_path)
    try:
        cur = conn.execute("DELETE FROM regional_dictionary WHERE id = ?", (entry_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


async def delete_dictionary_entry(db_path: str, entry_id: int) -> bool:
    return await asyncio.to_thread(_delete_dictionary_entry, db_path, entry_id)


# --- Feedback Stats ---


def _get_feedback_stats(db_path: str) -> dict:
    conn = _get_conn(db_path)
    try:
        # Recent stats (last 7 days)
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
        rows = conn.execute(
            "SELECT * FROM feedback_stats WHERE period_date >= ? ORDER BY period_date DESC",
            (cutoff,),
        ).fetchall()
        total_tp = sum(r["true_positives"] for r in rows)
        total_fp = sum(r["false_positives"] for r in rows)
        total_corrections = sum(r["corrections"] for r in rows)
        total_feedback = total_tp + total_fp + total_corrections
        fp_rate = (total_fp / total_feedback * 100) if total_feedback > 0 else 0.0

        # Pending review count
        pending_transcriptions = conn.execute(
            "SELECT COUNT(*) FROM transcriptions WHERE needs_review = 1"
        ).fetchone()[0]
        pending_alerts = conn.execute(
            "SELECT COUNT(*) FROM alerts a LEFT JOIN alert_feedback f ON f.alert_id = a.id WHERE f.id IS NULL"
        ).fetchone()[0]

        # Dictionary entry count
        dict_count = conn.execute("SELECT COUNT(*) FROM regional_dictionary WHERE active = 1").fetchone()[0]

        return {
            "false_positive_rate": round(fp_rate, 1),
            "true_positives": total_tp,
            "false_positives": total_fp,
            "corrections": total_corrections,
            "total_feedback": total_feedback,
            "pending_transcriptions": pending_transcriptions,
            "pending_alerts": pending_alerts,
            "pending_total": pending_transcriptions + pending_alerts,
            "dictionary_entries": dict_count,
        }
    finally:
        conn.close()


async def get_feedback_stats(db_path: str) -> dict:
    return await asyncio.to_thread(_get_feedback_stats, db_path)


# --- Training Data Export ---


def _get_training_data(db_path: str) -> list[dict]:
    """Get corrected transcription pairs for training data export."""
    conn = _get_conn(db_path)
    try:
        rows = conn.execute(
            "SELECT id, text, corrected_text, audio_file, confidence, flags "
            "FROM transcriptions WHERE corrected_text IS NOT NULL "
            "ORDER BY reviewed_at DESC"
        ).fetchall()
    finally:
        conn.close()
    results = []
    for r in rows:
        d = dict(r)
        try:
            d["flags"] = json.loads(d["flags"]) if d.get("flags") else []
        except (json.JSONDecodeError, TypeError):
            d["flags"] = []
        results.append(d)
    return results


async def get_training_data(db_path: str) -> list[dict]:
    return await asyncio.to_thread(_get_training_data, db_path)


def _get_alert_training_data(db_path: str) -> list[dict]:
    """Get alert feedback pairs for training data export."""
    conn = _get_conn(db_path)
    try:
        rows = conn.execute(
            "SELECT a.id as alert_id, a.summary, a.severity, a.category, a.raw_context, "
            "f.feedback_type, f.corrected_summary, f.corrected_severity, f.corrected_category "
            "FROM alert_feedback f JOIN alerts a ON a.id = f.alert_id "
            "ORDER BY f.created_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


async def get_alert_training_data(db_path: str) -> list[dict]:
    return await asyncio.to_thread(_get_alert_training_data, db_path)


# --- Data Retention Cleanup ---


def _cleanup_old_data(db_path: str, retention_days: int = 30) -> dict:
    """Delete transcriptions, alerts, summaries, and resolved events older than retention_days.

    Returns counts of deleted rows per table.
    """
    conn = _get_conn(db_path)
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).isoformat()
        deleted = {}

        deleted["transcriptions"] = conn.execute(
            "DELETE FROM transcriptions WHERE timestamp < ?", (cutoff,)
        ).rowcount
        deleted["alerts"] = conn.execute(
            "DELETE FROM alerts WHERE timestamp < ?", (cutoff,)
        ).rowcount
        deleted["summaries"] = conn.execute(
            "DELETE FROM summaries WHERE timestamp < ?", (cutoff,)
        ).rowcount
        deleted["events"] = conn.execute(
            "DELETE FROM events WHERE status = 'resolved' AND updated_at < ?", (cutoff,)
        ).rowcount
        deleted["alert_feedback"] = conn.execute(
            "DELETE FROM alert_feedback WHERE created_at < ?", (cutoff,)
        ).rowcount
        deleted["geocode_cache"] = conn.execute(
            "DELETE FROM geocode_cache WHERE created_at < ?", (cutoff,)
        ).rowcount

        conn.commit()

        # Truncate the WAL file to reclaim disk space
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

        return deleted
    finally:
        conn.close()


async def cleanup_old_data(db_path: str, retention_days: int = 30) -> dict:
    return await asyncio.to_thread(_cleanup_old_data, db_path, retention_days)
