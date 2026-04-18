from __future__ import annotations

import pandas as pd

from core import valuation_optimizer as vo


def _mock_ohlcv() -> pd.DataFrame:
    d = pd.date_range("2024-01-01", periods=120, freq="B")
    px = [100.0 + i * 0.2 for i in range(len(d))]
    return pd.DataFrame(
        {
            "date": d,
            "open": px,
            "high": [p + 0.5 for p in px],
            "low": [p - 0.5 for p in px],
            "close": px,
            "volume": [100_000.0] * len(d),
        }
    )


def test_backtest_legend_vs_legacy_for_symbol_smoke(monkeypatch) -> None:
    monkeypatch.setattr(vo, "fetch_long_ohlcv_for_backtest", lambda *_args, **_kwargs: (_mock_ohlcv(), "2y"))
    snap = {
        "symbol": "T",
        "price": 100.0,
        "eps": 12.0,
        "growth_rate_pct": 10.0,
        "book_value_per_share": 50.0,
        "debt_to_equity": 0.3,
        "current_ratio": 2.0,
        "roe": 18.0,
    }
    out = vo.backtest_legend_vs_legacy_for_symbol("T", snap)
    assert out.days >= 100
    assert isinstance(out.legend_return_pct, float)
    assert isinstance(out.legacy_return_pct, float)


def test_calibrate_legend_thresholds_returns_top_candidates(monkeypatch) -> None:
    monkeypatch.setattr(vo, "fetch_long_ohlcv_for_backtest", lambda *_args, **_kwargs: (_mock_ohlcv(), "2y"))
    snaps = {
        "AAA": {
            "symbol": "AAA",
            "price": 100.0,
            "eps": 9.0,
            "growth_rate_pct": 11.0,
            "book_value_per_share": 45.0,
            "debt_to_equity": 0.4,
            "current_ratio": 1.8,
            "roe": 17.0,
        },
        "BBB": {
            "symbol": "BBB",
            "price": 90.0,
            "eps": 8.0,
            "growth_rate_pct": 7.0,
            "book_value_per_share": 40.0,
            "debt_to_equity": 0.45,
            "current_ratio": 1.7,
            "roe": 16.0,
        },
    }
    out = vo.calibrate_legend_thresholds(snaps, symbols=["AAA", "BBB"])
    assert "best" in out
    assert isinstance(out.get("top10"), list)
    assert len(out["top10"]) >= 1

