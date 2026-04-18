"""Xuất biểu đồ Plotly sang PNG (dùng cho PDF)."""

from __future__ import annotations

from io import BytesIO
from typing import Any

import plotly.graph_objects as go


def plotly_figure_to_png_bytes(fig: go.Figure, *, width: int = 1100, height: int = 640, scale: int = 1) -> bytes:
    """Ưu tiên Kaleido; lỗi môi trường (Chrome headless) thì ném ra để tầng trên dùng fallback."""
    buf = BytesIO()
    fig.write_image(buf, format="png", width=width, height=height, scale=scale, engine="kaleido")
    buf.seek(0)
    return buf.read()


def _matplotlib_close_intrinsic_png(ohlcv: Any, valuation: dict[str, Any]) -> bytes:
    """Fallback không cần Kaleido: đường giá đóng + đường định giá."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import pandas as pd

    df = ohlcv.copy() if hasattr(ohlcv, "copy") else ohlcv
    df = pd.DataFrame(df)
    if df.empty or "close" not in df.columns:
        fig, ax = plt.subplots(figsize=(8, 3), dpi=120)
        ax.text(0.5, 0.5, "Không có dữ liệu OHLCV", ha="center", va="center")
        ax.set_axis_off()
        buf = BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        return buf.read()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    fig, ax = plt.subplots(figsize=(11, 5.5), dpi=120)
    ax.plot(df["date"], df["close"], color="#1f77b4", linewidth=1.2, label="Giá đóng cửa")
    intrinsic = float(valuation.get("composite_target_price") or valuation.get("intrinsic_value_graham") or 0)
    if intrinsic > 0:
        ax.axhline(intrinsic, color="#7f7f7f", linestyle="--", linewidth=1.5, label="Giá trị nội tại (ước)")
    ax.set_title("Giá & định giá tham chiếu (fallback Matplotlib)")
    ax.legend(loc="best", fontsize=8)
    ax.grid(True, alpha=0.25)
    fig.autofmt_xdate()
    buf = BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def _matplotlib_pie_png(allocated_vnd: float, remaining_vnd: float) -> bytes:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    a = max(float(allocated_vnd), 0.0)
    b = max(float(remaining_vnd), 0.0)
    if a + b <= 0:
        a, b = 1.0, 0.0
    fig, ax = plt.subplots(figsize=(6, 4), dpi=120)
    ax.pie(
        [a, b],
        labels=["Mã phân tích", "Vốn còn lại"],
        autopct="%1.1f%%",
        startangle=90,
    )
    ax.set_title("Phân bổ vốn (fallback Matplotlib)")
    buf = BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def candlestick_png_for_pdf(ohlcv: Any, valuation: dict[str, Any]) -> bytes:
    """Plotly+Kaleido nếu được; không thì Matplotlib."""
    fig = build_candlestick_valuation_figure(ohlcv, valuation)
    try:
        return plotly_figure_to_png_bytes(fig)
    except Exception:
        if ohlcv is None or (hasattr(ohlcv, "empty") and ohlcv.empty):
            return _matplotlib_close_intrinsic_png(
                __import__("pandas").DataFrame({"date": [], "close": []}), valuation
            )
        return _matplotlib_close_intrinsic_png(ohlcv, valuation)


def allocation_png_for_pdf(allocated_vnd: float, remaining_vnd: float) -> bytes:
    fig = build_allocation_pie_figure(allocated_vnd, remaining_vnd)
    try:
        return plotly_figure_to_png_bytes(fig, width=900, height=520)
    except Exception:
        return _matplotlib_pie_png(allocated_vnd, remaining_vnd)


def build_candlestick_valuation_figure(ohlcv: Any, valuation: dict[str, Any]) -> go.Figure:
    """Giống render_candlestick_with_intrinsic trong app — dùng cho PDF."""
    import pandas as pd

    if ohlcv is None or (hasattr(ohlcv, "empty") and ohlcv.empty):
        fig = go.Figure()
        fig.add_annotation(text="Không có dữ liệu OHLCV", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        fig.update_layout(height=400, margin={"l": 40, "r": 20, "t": 30, "b": 40})
        return fig
    df = ohlcv
    fig = go.Figure()
    fig.add_trace(
        go.Candlestick(
            x=df["date"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            name="Giá",
        )
    )
    intrinsic = float(valuation.get("composite_target_price") or valuation.get("intrinsic_value_graham") or 0)
    if intrinsic > 0:
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=[intrinsic] * len(df),
                mode="lines",
                name="Giá trị nội tại (tham chiếu)",
                line={"dash": "dot", "width": 2},
            )
        )
    fig.update_layout(
        title="Giá & định giá tham chiếu",
        height=460,
        xaxis_rangeslider_visible=False,
        margin={"l": 10, "r": 10, "t": 40, "b": 10},
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def build_allocation_pie_figure(allocated_vnd: float, remaining_vnd: float) -> go.Figure:
    fig = go.Figure(
        data=[
            go.Pie(
                labels=["Phân bổ mã phân tích", "Vốn còn lại"],
                values=[max(allocated_vnd, 0), max(remaining_vnd, 0)],
                hole=0.45,
            )
        ]
    )
    fig.update_layout(
        title="Phân bổ vốn (ước lượng)",
        height=320,
        margin={"l": 10, "r": 10, "t": 40, "b": 10},
    )
    return fig
