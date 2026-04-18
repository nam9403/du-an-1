"""Tiện ích định hướng đầu tư giá trị: xu hướng giá gần đây (OHLCV)."""

from __future__ import annotations

import pandas as pd


def approx_trading_days_for_months(months: float = 3.0) -> int:
    """~21 phiên/tháng giao dịch (ước lượng)."""
    return max(15, int(round(months * 21.0)))


def price_change_pct_since_sessions(ohlcv: pd.DataFrame, sessions: int = 63) -> float | None:
    """
    % thay đổi giá đóng cửa: phiên cách đây `sessions` phiên -> phiên mới nhất.
    Trả None nếu không đủ dữ liệu.
    """
    if ohlcv is None or ohlcv.empty or "close" not in ohlcv.columns:
        return None
    close = pd.to_numeric(ohlcv["close"], errors="coerce").dropna()
    if len(close) < sessions + 1:
        return None
    a = float(close.iloc[-(sessions + 1)])
    b = float(close.iloc[-1])
    if a <= 0:
        return None
    return (b / a - 1.0) * 100.0


def label_price_trend_vi(pct: float | None) -> str:
    """Nhãn xu hướng ~3 tháng (tham chiếu) cho UI pilot."""
    if pct is None:
        return "Chưa đủ OHLCV"
    if pct >= 15.0:
        return "Tăng mạnh"
    if pct >= 5.0:
        return "Tăng"
    if pct >= -5.0:
        return "Đi ngang / tích lũy"
    if pct >= -15.0:
        return "Giảm"
    return "Giảm mạnh"


def pilot_trend_from_ohlcv(ohlcv: pd.DataFrame, months: float = 3.0) -> tuple[float | None, str]:
    """(pct_change, nhãn tiếng Việt)."""
    n = approx_trading_days_for_months(months)
    pct = price_change_pct_since_sessions(ohlcv, sessions=n)
    return pct, label_price_trend_vi(pct)
