from __future__ import annotations

import pandas as pd
import pytest

from core.engine import OhlcvDataError, compute_technical_indicators, detect_market_phase_from_ohlcv


def _sample_ohlcv(n: int = 60) -> pd.DataFrame:
    rows = []
    base = 100.0
    for i in range(n):
        c = base + i * 0.2
        rows.append(
            {
                "open": c - 0.3,
                "high": c + 0.5,
                "low": c - 0.6,
                "close": c,
                "volume": 1_000_000 + i * 1000,
            }
        )
    return pd.DataFrame(rows)


def test_compute_technical_indicators() -> None:
    df = _sample_ohlcv()
    out = compute_technical_indicators(df)
    assert "ma20" in out.columns and "rsi14" in out.columns


def test_detect_phase_returns_struct() -> None:
    phase = detect_market_phase_from_ohlcv(_sample_ohlcv())
    assert phase.phase in ("accumulation", "breakout", "distribution", "neutral")
    assert phase.reason
    assert "close" in phase.metrics


def test_ohlcv_too_short() -> None:
    with pytest.raises(OhlcvDataError):
        compute_technical_indicators(_sample_ohlcv(10))
