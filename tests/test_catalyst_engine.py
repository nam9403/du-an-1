from __future__ import annotations

import numpy as np
import pandas as pd

from core.catalyst_engine import calculate_catalyst_score


def _ohlcv_with_spike() -> pd.DataFrame:
    n = 80
    close = np.linspace(100, 112, n)
    volume = np.full(n, 500_000.0)
    close[-1] = close[-2] * 1.03
    volume[-1] = volume[-20:].mean() * 2.5
    return pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=n, freq="B"),
            "open": close,
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
            "volume": volume,
        }
    )


def _ohlcv_weak() -> pd.DataFrame:
    n = 80
    close = np.linspace(120, 100, n)
    volume = np.full(n, 200_000.0)
    return pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=n, freq="B"),
            "open": close,
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
            "volume": volume,
        }
    )


def test_catalyst_high_score_case() -> None:
    out = calculate_catalyst_score(
        "FPT",
        _ohlcv_with_spike(),
        {"margin_of_safety_composite_pct": 25.0},
        news_text="FPT trúng thầu dự án lớn, chuẩn bị tăng vốn.",
    )
    assert out.symbol == "FPT"
    assert out.catalyst_score >= 70
    assert out.passed is True
    assert out.flow_signal in ("Dòng tiền vào mạnh", "Dòng tiền chưa bứt phá")
    assert out.news_signal == "Tin tức tích cực"
    assert 0 <= out.data_quality_pct <= 100


def test_catalyst_low_score_case() -> None:
    out = calculate_catalyst_score(
        "HPG",
        _ohlcv_weak(),
        {"margin_of_safety_composite_pct": 5.0},
        news_text="",
    )
    assert out.symbol == "HPG"
    assert out.catalyst_score < 70
    assert out.passed is False
    assert isinstance(out.reasons, list)

