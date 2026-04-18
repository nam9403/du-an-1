"""
Materialized universe cache using Parquet for fast reload.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
CACHE_PATH = ROOT / "data" / "universe_scan_cache.parquet"


def save_universe_scan_cache(df: pd.DataFrame) -> bool:
    if df is None or df.empty:
        return False
    out = df.copy()
    out["_cached_at_utc"] = datetime.now(timezone.utc).isoformat()
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        out.to_parquet(CACHE_PATH, index=False)
        return True
    except Exception:
        return False


def load_universe_scan_cache(max_age_minutes: int = 180) -> pd.DataFrame:
    if not CACHE_PATH.exists():
        return pd.DataFrame()
    try:
        df = pd.read_parquet(CACHE_PATH)
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return df
    if "_cached_at_utc" not in df.columns:
        return df
    try:
        ts = datetime.fromisoformat(str(df["_cached_at_utc"].iloc[0]).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except Exception:
        return pd.DataFrame()
    age_min = (datetime.now(timezone.utc) - ts).total_seconds() / 60.0
    if age_min > float(max_age_minutes):
        return pd.DataFrame()
    return df.drop(columns=["_cached_at_utc"], errors="ignore")

