from __future__ import annotations

import numpy as np
import pandas as pd

from core.strategy_backtest import run_phase_signal_backtest


def test_run_phase_signal_backtest_smoke() -> None:
    n = 120
    rng = np.random.default_rng(42)
    price = 100 + np.cumsum(rng.normal(0, 0.5, size=n))
    df = pd.DataFrame(
        {
            "date": pd.date_range("2023-01-01", periods=n, freq="B"),
            "open": price,
            "high": price + 0.5,
            "low": price - 0.5,
            "close": price,
            "volume": rng.uniform(1e5, 5e5, size=n),
        }
    )
    bench = pd.Series(df["close"].values, index=df["date"].dt.normalize())
    res = run_phase_signal_backtest(df, bench, initial_cash=100_000_000.0, warmup=60)
    assert len(res.equity_app) == len(res.equity_bench) == len(res.equity_buyhold_stock) == len(res.dates)
    assert res.max_drawdown_pct >= 0
