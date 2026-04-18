"""
Catalyst Engine: detect short-term breakout catalysts.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class CatalystResult:
    symbol: str
    catalyst_score: float
    passed: bool
    flow_signal: str
    news_signal: str
    rsi_signal: str
    trend_signal: str
    confidence_pct: float
    data_quality_pct: float
    reasons: list[str]


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def _calc_rsi(close: pd.Series, period: int = 14) -> float | None:
    c = pd.to_numeric(close, errors="coerce").dropna()
    if len(c) < period + 5:
        return None
    delta = c.diff()
    up = delta.clip(lower=0.0)
    down = -delta.clip(upper=0.0)
    avg_up = up.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_down = down.ewm(alpha=1.0 / period, adjust=False).mean()
    rs = avg_up / avg_down.replace(0, pd.NA)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    try:
        return float(rsi.iloc[-1])
    except Exception:
        return None


def _news_positive_hits(news_text: str) -> tuple[int, list[str]]:
    txt = str(news_text or "").lower()
    keys = ["trúng thầu", "co tuc", "cổ tức", "tang von", "tăng vốn", "la nina", "ký hợp đồng", "mo rong"]
    hits = [k for k in keys if k in txt]
    return len(hits), hits[:3]


def calculate_catalyst_score(
    symbol: str,
    ohlcv: pd.DataFrame,
    valuation: dict[str, Any],
    news_text: str = "",
) -> CatalystResult:
    score = 0.0
    reasons: list[str] = []
    flow_signal = "Không rõ"
    news_signal = "Không rõ"
    rsi_signal = "Không rõ"
    trend_signal = "Không rõ"

    d = ohlcv.copy() if ohlcv is not None else pd.DataFrame()
    if not d.empty:
        d["close"] = pd.to_numeric(d.get("close"), errors="coerce")
        d["volume"] = pd.to_numeric(d.get("volume"), errors="coerce")
        d = d.dropna(subset=["close", "volume"])
    mos = _safe_float(valuation.get("margin_of_safety_composite_pct"), 0.0)

    if mos > 20.0:
        score += 25.0
        reasons.append("MoS > 20% (định giá hấp dẫn).")

    if len(d) >= 21:
        c_now = float(d["close"].iloc[-1])
        c_prev = float(d["close"].iloc[-2])
        v_now = float(d["volume"].iloc[-1])
        v_avg20 = float(d["volume"].tail(20).mean())
        price_change = ((c_now / c_prev) - 1.0) * 100.0 if c_prev > 0 else 0.0
        vol_spike = v_now > 2.0 * v_avg20 and price_change > 2.0
        if vol_spike:
            score += 20.0
            flow_signal = "Dòng tiền vào mạnh"
            reasons.append("Volume phiên >2x Vol20 và giá tăng >2%.")
        else:
            flow_signal = "Dòng tiền chưa bứt phá"

        ma20 = float(d["close"].tail(20).mean())
        if c_now > ma20:
            score += 10.0
            trend_signal = "Giá trên MA20"
            reasons.append("Giá nằm trên MA20 (xác nhận xu hướng).")
        else:
            trend_signal = "Giá dưới MA20"

        rsi = _calc_rsi(d["close"], 14)
        if rsi is not None and rsi < 35.0 and price_change > 0:
            score += 20.0
            rsi_signal = f"RSI hồi phục từ vùng thấp ({rsi:.1f})"
            reasons.append("RSI ở vùng thấp và đang hồi phục.")
        elif rsi is not None and rsi < 30.0:
            score += 10.0
            rsi_signal = f"RSI quá bán ({rsi:.1f})"
            reasons.append("RSI quá bán, cần xác nhận thêm.")
        elif rsi is not None:
            rsi_signal = f"RSI trung tính ({rsi:.1f})"
    else:
        flow_signal = "Thiếu dữ liệu volume"
        trend_signal = "Thiếu dữ liệu xu hướng"
        rsi_signal = "Thiếu dữ liệu RSI"

    hit_count, hit_words = _news_positive_hits(news_text)
    if hit_count > 0:
        score += 25.0
        news_signal = "Tin tức tích cực"
        reasons.append(f"Từ khóa tích cực: {', '.join(hit_words)}.")
    else:
        news_signal = "Không có tín hiệu tin tích cực rõ"

    score = max(0.0, min(100.0, score))
    passed = score >= 70.0

    quality = 0.0
    quality += 45.0 if len(d) >= 60 else 0.0
    quality += 20.0 if valuation else 0.0
    quality += 35.0 if bool(news_text and str(news_text).strip()) else 0.0
    quality = max(0.0, min(100.0, quality))

    confidence = 35.0 + score * 0.5 + (10.0 if passed else 0.0)
    if quality < 50.0:
        confidence -= 15.0
    confidence = max(10.0, min(95.0, confidence))

    return CatalystResult(
        symbol=symbol.upper(),
        catalyst_score=round(score, 2),
        passed=passed,
        flow_signal=flow_signal,
        news_signal=news_signal,
        rsi_signal=rsi_signal,
        trend_signal=trend_signal,
        confidence_pct=round(confidence, 2),
        data_quality_pct=round(quality, 2),
        reasons=reasons[:5],
    )

