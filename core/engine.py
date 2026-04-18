"""Data engine (technical) + Trend & Flow phase detection."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

import pandas as pd

from core.valuation import value_investing_summary
from scrapers.portal import fetch_financial_indicators, fetch_latest_news, fetch_ohlcv_history


class EngineError(Exception):
    """Base error for technical data engine."""


class OhlcvDataError(EngineError):
    """Raised when OHLCV data is missing or invalid."""


@dataclass(frozen=True)
class MarketPhaseResult:
    """Structured output for market phase detection."""

    phase: str
    reason: str
    metrics: dict[str, float]


def _validate_ohlcv(df: pd.DataFrame, min_rows: int = 50) -> pd.DataFrame:
    """Validate and normalize OHLCV data."""
    if df is None or df.empty:
        raise OhlcvDataError("OHLCV trống.")

    required = {"open", "high", "low", "close", "volume"}
    miss = [c for c in required if c not in df.columns]
    if miss:
        raise OhlcvDataError(f"Thiếu cột OHLCV bắt buộc: {', '.join(miss)}")

    out = df.copy()
    for col in ("open", "high", "low", "close", "volume"):
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=["open", "high", "low", "close", "volume"])

    if len(out) < min_rows:
        raise OhlcvDataError(f"Không đủ dữ liệu: cần >= {min_rows} phiên, hiện có {len(out)}.")
    return out


def _compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """Compute RSI using simple rolling average gains/losses."""
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.rolling(period, min_periods=period).mean()
    avg_loss = loss.rolling(period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    # Keep numeric dtype explicit to avoid future pandas downcasting warning.
    return pd.to_numeric(rsi, errors="coerce").fillna(50.0).astype(float)


def compute_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add technical indicators from OHLCV:
    - MA20, MA50
    - RSI14
    - vol_ma20
    - vol_std20
    """
    out = _validate_ohlcv(df, min_rows=50)
    out["ma20"] = out["close"].rolling(20, min_periods=20).mean()
    out["ma50"] = out["close"].rolling(50, min_periods=50).mean()
    out["rsi14"] = _compute_rsi(out["close"], period=14)
    out["vol_ma20"] = out["volume"].rolling(20, min_periods=20).mean()
    out["vol_std20"] = out["volume"].rolling(20, min_periods=20).std()
    return out


def detect_market_phase_from_ohlcv(df: pd.DataFrame) -> MarketPhaseResult:
    """
    Detect market phase:
    - Accumulation: 10-session range < 5% and volume < average 20 sessions.
    - Breakout: daily close change > 3% and volume > 1.5x average 20 sessions.
    - Distribution/Weakness: daily close change < 0 and volume > 1.5x average 20 sessions.
    """
    out = compute_technical_indicators(df)
    last = out.iloc[-1]
    last10 = out.iloc[-10:]

    high_10 = float(last10["high"].max())
    low_10 = float(last10["low"].min())
    range_pct_10 = ((high_10 - low_10) / low_10 * 100.0) if low_10 > 0 else 0.0

    prev_close = float(out.iloc[-2]["close"])
    close = float(last["close"])
    day_change_pct = ((close - prev_close) / prev_close * 100.0) if prev_close > 0 else 0.0

    vol = float(last["volume"])
    vol_ma20 = float(last["vol_ma20"]) if pd.notna(last["vol_ma20"]) else 0.0
    vol_multiple = (vol / vol_ma20) if vol_ma20 > 0 else 0.0

    accumulation = (range_pct_10 < 5.0) and (vol < vol_ma20 if vol_ma20 > 0 else False)
    breakout = (day_change_pct > 3.0) and (vol_multiple > 1.5)
    distribution = (day_change_pct < 0.0) and (vol_multiple > 1.5)

    if breakout:
        phase = "breakout"
        reason = "Giá tăng >3% và khối lượng >1.5 lần trung bình 20 phiên."
    elif distribution:
        phase = "distribution"
        reason = "Giá giảm kèm khối lượng lớn, tín hiệu suy yếu/phân phối."
    elif accumulation:
        phase = "accumulation"
        reason = "Giá đi ngang biên hẹp (<5%) và thanh khoản thấp hơn trung bình."
    else:
        phase = "neutral"
        reason = "Chưa xuất hiện tín hiệu tích lũy/bùng nổ/phân phối rõ."

    return MarketPhaseResult(
        phase=phase,
        reason=reason,
        metrics={
            "close": close,
            "day_change_pct": day_change_pct,
            "range_pct_10": range_pct_10,
            "volume": vol,
            "vol_ma20": vol_ma20,
            "vol_multiple": vol_multiple,
            "ma20": float(last["ma20"]) if pd.notna(last["ma20"]) else 0.0,
            "ma50": float(last["ma50"]) if pd.notna(last["ma50"]) else 0.0,
            "rsi14": float(last["rsi14"]) if pd.notna(last["rsi14"]) else 50.0,
            "vol_std20": float(last["vol_std20"]) if pd.notna(last["vol_std20"]) else 0.0,
        },
    )


def detect_market_phase(
    ticker: str,
    ohlcv_provider: Callable[[str], pd.DataFrame],
) -> MarketPhaseResult:
    """
    Wrapper using a provider function to fetch OHLCV then detect phase.

    `ohlcv_provider` should return a DataFrame with columns:
    open, high, low, close, volume (>= 50 rows).
    """
    if not ticker or not ticker.strip():
        raise OhlcvDataError("Ticker rỗng.")
    try:
        raw = ohlcv_provider(ticker.strip().upper())
    except Exception as e:  # pragma: no cover - provider-specific failures
        raise OhlcvDataError(f"Lỗi lấy OHLCV cho {ticker}: {e}") from e
    return detect_market_phase_from_ohlcv(raw)


def build_investment_context(
    ticker: str,
    snapshot: dict,
    *,
    sessions: int = 80,
    news_limit: int = 10,
    fast_mode: bool = False,
) -> dict:
    """
    Aggregate 3-layer data for UI/AI:
    - technical: OHLCV + indicators + market phase
    - financial: ratio indicators
    - valuation: dynamic industry valuation summary
    - news: latest headlines
    """
    sym = (ticker or "").strip().upper()
    if not sym:
        raise OhlcvDataError("Ticker rỗng.")

    valuation = value_investing_summary(snapshot)
    with ThreadPoolExecutor(max_workers=3) as ex:
        fut_ohlcv = ex.submit(fetch_ohlcv_history, sym, sessions)
        fut_fin = ex.submit(fetch_financial_indicators, sym, fast_mode=fast_mode)
        fut_news = ex.submit(fetch_latest_news, sym, news_limit)
        ohlcv = fut_ohlcv.result()
        try:
            financials = fut_fin.result()
        except Exception:
            financials = {}
        try:
            news = fut_news.result()
        except Exception:
            news = []

    technical = compute_technical_indicators(ohlcv)
    phase = detect_market_phase_from_ohlcv(ohlcv)

    return {
        "ticker": sym,
        "technical": {
            "ohlcv": technical,
            "latest": technical.iloc[-1].to_dict() if not technical.empty else {},
            "market_phase": asdict(phase),
        },
        "financials": financials,
        "valuation": valuation,
        "news": news,
    }


def backtest_action_strategy(ohlcv: pd.DataFrame, horizon_days: int = 10) -> dict:
    """
    Backtest BUY/HOLD/AVOID action rules on historical OHLCV.
    Rule proxy:
    - BUY when close > ma20 and ma20 > ma50 and RSI in [45,70]
    - AVOID when close < ma20 and RSI < 45
    - HOLD otherwise
    Metric:
    - forward return after `horizon_days`
    - win rate for BUY signals
    """
    d = compute_technical_indicators(ohlcv).copy()
    if len(d) < max(60, horizon_days + 25):
        return {"samples": 0, "buy_signals": 0, "buy_win_rate_pct": 0.0, "buy_avg_return_pct": 0.0}

    d["fwd_ret_pct"] = d["close"].shift(-horizon_days) / d["close"] - 1.0

    def _signal(row: pd.Series) -> str:
        c = float(row.get("close") or 0)
        ma20 = float(row.get("ma20") or 0)
        ma50 = float(row.get("ma50") or 0)
        rsi = float(row.get("rsi14") or 50)
        if c > ma20 > ma50 and 45 <= rsi <= 70:
            return "BUY"
        if c < ma20 and rsi < 45:
            return "AVOID"
        return "HOLD"

    d["signal"] = d.apply(_signal, axis=1)
    valid = d.dropna(subset=["fwd_ret_pct"])
    if valid.empty:
        return {"samples": 0, "buy_signals": 0, "buy_win_rate_pct": 0.0, "buy_avg_return_pct": 0.0}

    buy = valid[valid["signal"] == "BUY"]
    if buy.empty:
        return {
            "samples": int(len(valid)),
            "buy_signals": 0,
            "buy_win_rate_pct": 0.0,
            "buy_avg_return_pct": 0.0,
            "horizon_days": int(horizon_days),
        }
    win_rate = (buy["fwd_ret_pct"] > 0).mean() * 100.0
    avg_ret = buy["fwd_ret_pct"].mean() * 100.0
    return {
        "samples": int(len(valid)),
        "buy_signals": int(len(buy)),
        "buy_win_rate_pct": float(round(win_rate, 2)),
        "buy_avg_return_pct": float(round(avg_ret, 2)),
        "horizon_days": int(horizon_days),
    }
