"""Fallback Matplotlib khi Kaleido không chạy."""
from __future__ import annotations

from unittest.mock import patch

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from core.chart_export import allocation_png_for_pdf, candlestick_png_for_pdf


def test_candlestick_png_matplotlib_fallback() -> None:
    n = 80
    df = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=n, freq="B"),
            "open": np.linspace(100, 110, n),
            "high": np.linspace(101, 111, n),
            "low": np.linspace(99, 109, n),
            "close": np.linspace(100.5, 110.5, n),
            "volume": np.full(n, 1e6),
        }
    )
    val = {"composite_target_price": 105.0}
    with patch.object(go.Figure, "write_image", side_effect=RuntimeError("no kaleido")):
        png = candlestick_png_for_pdf(df, val)
    assert png[:8] == b"\x89PNG\r\n\x1a\n"


def test_allocation_png_matplotlib_fallback() -> None:
    with patch.object(go.Figure, "write_image", side_effect=RuntimeError("no kaleido")):
        png = allocation_png_for_pdf(20_000_000, 80_000_000)
    assert png[:8] == b"\x89PNG\r\n\x1a\n"
