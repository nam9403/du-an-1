from __future__ import annotations

import numpy as np
import pandas as pd

from core.strategy_backtest import equity_yearly_returns_pct


def test_equity_yearly_returns_simple() -> None:
    d = pd.date_range("2023-06-01", "2024-06-01", freq="B")
    eq = np.linspace(100e6, 110e6, len(d))
    df = equity_yearly_returns_pct(list(d), eq)
    assert not df.empty
    assert "Năm" in df.columns
