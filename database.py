from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timezone


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
    conn.close()


def _insert_transcription(db_path: str, text: str, duration_s: float) -> dict:
    conn = _get_conn(db_path)
    ts = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        "INSERT INTO transcriptions (timestamp, text, duration_s) VALUES (?, ?, ?)",
        (ts, text, duration_s),
    )
    row_id = cur.lastrowid
    conn.commit()
    conn.close()
    return {"id": row_id, "timestamp": ts, "text": text, "duration_s": duration_s}


async def insert_transcription(db_path: str, text: str, duration_s: float) -> dict:
    return await asyncio.to_thread(_insert_transcription, db_path, text, duration_s)


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


def _get_counts(db_path: str) -> dict:
    conn = _get_conn(db_path)
    t_count = conn.execute("SELECT COUNT(*) FROM transcriptions").fetchone()[0]
    a_count = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
    conn.close()
    return {"transcriptions": t_count, "alerts": a_count}


async def get_counts(db_path: str) -> dict:
    return await asyncio.to_thread(_get_counts, db_path)
