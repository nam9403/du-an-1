import pandas as pd
from pathlib import Path

from core import ohlcv_store as store


def test_ohlcv_store_upsert_and_load(monkeypatch):
    p = Path("e:/Du an 1/data/ohlcv_store_test.db")
    monkeypatch.setattr(store, "DB_PATH", p)
    df = pd.DataFrame(
        [
            {"date": "2026-01-01", "open": 10, "high": 11, "low": 9, "close": 10.5, "volume": 1000},
            {"date": "2026-01-02", "open": 10.5, "high": 11.2, "low": 10.2, "close": 11.0, "volume": 1200},
        ]
    )
    n = store.upsert_ohlcv("FPT", df, source="test")
    assert n == 2
    out = store.load_recent_ohlcv("FPT", sessions=50)
    assert len(out) == 2
    assert store.last_ohlcv_date("FPT") == "2026-01-02"

