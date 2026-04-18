"""
Local SQLite store for daily OHLCV with incremental updates.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "ohlcv_store.db"


def _conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS ohlcv_daily (
            symbol TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            source TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL,
            PRIMARY KEY(symbol, date)
        )
        """
    )
    return c


def upsert_ohlcv(symbol: str, df: pd.DataFrame, source: str = "") -> int:
    if df is None or df.empty:
        return 0
    sym = (symbol or "").strip().upper()
    if not sym:
        return 0
    d = df.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    for c in ("open", "high", "low", "close", "volume"):
        d[c] = pd.to_numeric(d[c], errors="coerce")
    d = d.dropna(subset=["date", "open", "high", "low", "close", "volume"]).sort_values("date")
    if d.empty:
        return 0
    now = datetime.now(timezone.utc).isoformat()
    rows = [
        (
            sym,
            str(r["date"].strftime("%Y-%m-%d")),
            float(r["open"]),
            float(r["high"]),
            float(r["low"]),
            float(r["close"]),
            float(r["volume"]),
            str(source or ""),
            now,
        )
        for _, r in d.iterrows()
    ]
    with _conn() as c:
        c.executemany(
            """
            INSERT INTO ohlcv_daily(symbol, date, open, high, low, close, volume, source, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, date) DO UPDATE SET
                open=excluded.open, high=excluded.high, low=excluded.low,
                close=excluded.close, volume=excluded.volume,
                source=excluded.source, updated_at=excluded.updated_at
            """,
            rows,
        )
    return len(rows)


def load_recent_ohlcv(symbol: str, sessions: int = 80) -> pd.DataFrame:
    sym = (symbol or "").strip().upper()
    if not sym:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
    lim = max(50, int(sessions))
    with _conn() as c:
        rows = c.execute(
            """
            SELECT date, open, high, low, close, volume
            FROM ohlcv_daily
            WHERE symbol=?
            ORDER BY date DESC
            LIMIT ?
            """,
            (sym, lim),
        ).fetchall()
    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
    d = pd.DataFrame([dict(r) for r in rows]).sort_values("date").reset_index(drop=True)
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    return d.dropna(subset=["date"])


def last_ohlcv_date(symbol: str) -> str | None:
    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    with _conn() as c:
        r = c.execute("SELECT MAX(date) AS d FROM ohlcv_daily WHERE symbol=?", (sym,)).fetchone()
    d = str(r["d"] or "").strip() if r else ""
    return d or None

