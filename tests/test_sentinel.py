from __future__ import annotations

import pandas as pd

from core.sentinel import (
    _market_noise_case,
    ohlcv_last_session_change_pct,
)


def test_ohlcv_last_change() -> None:
    df = pd.DataFrame(
        {
            "close": [100.0, 97.0],
            "open": [99, 96],
            "high": [101, 98],
            "low": [98, 95],
            "volume": [1e6, 1e6],
        }
    )
    assert ohlcv_last_session_change_pct(df) == -3.0


def test_noise_case_a() -> None:
    r = _market_noise_case(-4.0, -2.0, True)
    assert r["case"] == "A_market_wide_discount"


def test_noise_case_b() -> None:
    r = _market_noise_case(-5.0, 0.5, False)
    assert r["case"] == "B_idiosyncratic"


def test_noise_neutral() -> None:
    r = _market_noise_case(-1.0, None, True)
    assert r["triggered"] is False
