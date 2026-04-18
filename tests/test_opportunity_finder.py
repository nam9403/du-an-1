from __future__ import annotations

import numpy as np
import pandas as pd

from core.opportunity_finder import evaluate_stock_opportunity_3m


def _mock_ohlcv(uptrend: bool = True) -> pd.DataFrame:
    n = 100
    base = np.linspace(100, 130, n) if uptrend else np.linspace(130, 100, n)
    return pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=n, freq="B"),
            "open": base,
            "high": base + 1.0,
            "low": base - 1.0,
            "close": base,
            "volume": np.full(n, 500_000.0),
        }
    )


def test_opportunity_finder_high_opportunity_case() -> None:
    ohlcv = _mock_ohlcv(uptrend=True)
    valuation = {"piotroski_score": 8, "margin_of_safety_composite_pct": 18.0}
    financials = {"debt_to_equity": 0.5, "revenue_growth_yoy": 12.0, "profit_growth_yoy": 14.0}
    out = evaluate_stock_opportunity_3m("FPT", ohlcv, valuation, financials)
    assert out.symbol == "FPT"
    assert out.score >= 60
    assert out.probability_up_3m_pct >= 60
    assert 0 <= out.bull_prob_pct <= 100
    assert 0 <= out.base_prob_pct <= 100
    assert 0 <= out.bear_prob_pct <= 100
    assert abs((out.bull_prob_pct + out.base_prob_pct + out.bear_prob_pct) - 100) < 0.5
    assert 0 <= out.walkforward_reliability_pct <= 100
    assert isinstance(out.return_3m_recent_pct, float)
    assert 0 <= out.technical_score <= 50
    assert 0 <= out.fundamental_score <= 50
    assert 0 <= out.data_quality_pct <= 100
    assert isinstance(out.recommendation_reason, str) and len(out.recommendation_reason) > 0
    assert out.status in ("CƠ HỘI CAO", "THEO DÕI")


def test_opportunity_finder_cautious_case() -> None:
    ohlcv = _mock_ohlcv(uptrend=False)
    valuation = {"piotroski_score": 3, "margin_of_safety_composite_pct": -5.0}
    financials = {"debt_to_equity": 1.5, "revenue_growth_yoy": -3.0, "profit_growth_yoy": -8.0}
    out = evaluate_stock_opportunity_3m("HPG", ohlcv, valuation, financials)
    assert out.status in ("THẬN TRỌNG", "THEO DÕI")
    assert out.score <= 70

