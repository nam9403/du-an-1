"""
Opportunity finder (3-month horizon) for VN stocks.

Rule-based probabilistic scoring:
- Technical accumulation/momentum
- Fundamental quality
- Valuation margin of safety
- Liquidity safety
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class OpportunityResult:
    symbol: str
    score: float
    probability_up_3m_pct: float
    expected_return_3m_pct: float
    confidence_pct: float
    bull_prob_pct: float
    base_prob_pct: float
    bear_prob_pct: float
    historical_hit_rate_3m_pct: float
    walkforward_reliability_pct: float
    return_3m_recent_pct: float
    alpha_3m_vs_benchmark_pct: float | None
    avg_volume_20: float
    technical_score: float
    fundamental_score: float
    data_quality_pct: float
    recommendation_reason: str
    thesis: str
    risks: str
    invalidation: str
    status: str


def _pct(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a / b - 1.0) * 100.0


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def _technical_score(ohlcv: pd.DataFrame) -> tuple[float, list[str], list[str], float]:
    if ohlcv is None or ohlcv.empty or len(ohlcv) < 70:
        return 0.0, [], ["Thiếu dữ liệu kỹ thuật >= 70 phiên."], 0.0

    d = ohlcv.copy()
    d["close"] = pd.to_numeric(d["close"], errors="coerce")
    d["volume"] = pd.to_numeric(d["volume"], errors="coerce")
    d = d.dropna(subset=["close", "volume"])
    if len(d) < 70:
        return 0.0, [], ["Dữ liệu kỹ thuật không đủ sau làm sạch."], 0.0

    close = d["close"]
    vol = d["volume"]
    c = float(close.iloc[-1])
    c_20 = float(close.iloc[-21])
    c_63 = float(close.iloc[-64])
    ma20 = float(close.tail(20).mean())
    ma50 = float(close.tail(50).mean())
    vol20 = float(vol.tail(20).mean())
    vol5 = float(vol.tail(5).mean())

    mom_1m = _pct(c, c_20)
    mom_3m = _pct(c, c_63)
    vol_ratio = vol5 / vol20 if vol20 > 0 else 0.0

    score = 0.0
    reasons: list[str] = []
    risks: list[str] = []
    confidence = 0.0

    if c > ma20 > ma50:
        score += 28
        confidence += 20
        reasons.append("Giá duy trì trên MA20 và MA50 (xu hướng tích cực).")
    elif c > ma20:
        score += 14
        confidence += 10
        reasons.append("Giá nằm trên MA20 (đà ngắn hạn ổn).")
    else:
        risks.append("Giá dưới MA20, xung lực ngắn hạn còn yếu.")

    if -2.0 <= mom_1m <= 12.0 and mom_3m > 0:
        score += 22
        confidence += 16
        reasons.append("Động lượng 1-3 tháng lành mạnh, giống pha tích lũy đi lên.")
    elif mom_3m > 0:
        score += 10
        confidence += 8
    else:
        risks.append("Động lượng 3 tháng âm.")

    if 0.75 <= vol_ratio <= 1.35:
        score += 12
        confidence += 8
        reasons.append("Thanh khoản ổn định, chưa có tín hiệu phân phối mạnh.")
    elif vol_ratio > 1.8 and mom_1m < 0:
        risks.append("Khối lượng cao kèm giá yếu, rủi ro phân phối.")

    expected = max(-8.0, min(25.0, 0.35 * mom_3m + 0.18 * mom_1m))
    return score, reasons, risks, expected


def _historical_hit_rate_3m(ohlcv: pd.DataFrame, threshold_pct: float = 8.0) -> float:
    if ohlcv is None or ohlcv.empty or len(ohlcv) < 130:
        return 0.0
    d = ohlcv.copy()
    d["close"] = pd.to_numeric(d["close"], errors="coerce")
    d = d.dropna(subset=["close"])
    if len(d) < 130:
        return 0.0
    close = d["close"]
    horizon = 63
    wins = 0
    total = 0
    for i in range(60, len(close) - horizon):
        p0 = float(close.iloc[i])
        p1 = float(close.iloc[i + horizon])
        if p0 <= 0:
            continue
        ret = (p1 / p0 - 1.0) * 100.0
        wins += 1 if ret >= threshold_pct else 0
        total += 1
    if total <= 0:
        return 0.0
    return round(wins / total * 100.0, 2)


def _walkforward_reliability_3m(ohlcv: pd.DataFrame, threshold_pct: float = 8.0) -> float:
    """
    Reliability proxy from rolling window consistency.
    Higher when recent/mid/old hit-rates are all reasonably stable.
    """
    if ohlcv is None or ohlcv.empty or len(ohlcv) < 180:
        return 0.0
    d = ohlcv.copy()
    d["close"] = pd.to_numeric(d["close"], errors="coerce")
    d = d.dropna(subset=["close"])
    if len(d) < 180:
        return 0.0
    n = len(d)
    seg = max(60, n // 3)
    segments = [
        d.iloc[max(0, n - seg) : n],
        d.iloc[max(0, n - 2 * seg) : max(0, n - seg)],
        d.iloc[max(0, n - 3 * seg) : max(0, n - 2 * seg)],
    ]
    hits = [_historical_hit_rate_3m(s, threshold_pct=threshold_pct) for s in segments if len(s) >= 130]
    if not hits:
        return 0.0
    avg = sum(hits) / len(hits)
    var = sum((x - avg) ** 2 for x in hits) / len(hits)
    stability_penalty = min(25.0, (var ** 0.5) * 0.8)
    score = max(0.0, min(100.0, avg - stability_penalty))
    return round(score, 2)


def _recent_return_3m_pct(ohlcv: pd.DataFrame) -> float:
    if ohlcv is None or ohlcv.empty or len(ohlcv) < 64:
        return 0.0
    d = ohlcv.copy()
    d["close"] = pd.to_numeric(d["close"], errors="coerce")
    d = d.dropna(subset=["close"])
    if len(d) < 64:
        return 0.0
    c0 = float(d["close"].iloc[-64])
    c1 = float(d["close"].iloc[-1])
    return round(_pct(c1, c0), 2)


def _fundamental_score(valuation: dict[str, Any], financials: dict[str, Any]) -> tuple[float, list[str], list[str]]:
    score = 0.0
    reasons: list[str] = []
    risks: list[str] = []

    f_score = int(_safe_float(valuation.get("piotroski_score"), 0))
    mos = _safe_float(valuation.get("margin_of_safety_composite_pct"), 0.0)
    de = financials.get("debt_to_equity")
    rev = financials.get("revenue_growth_yoy")
    profit = financials.get("profit_growth_yoy")

    if f_score >= 7:
        score += 18
        reasons.append("F-Score cao, chất lượng tài chính tốt.")
    elif f_score >= 5:
        score += 10
    else:
        risks.append("F-Score thấp, nền tảng tài chính chưa mạnh.")

    if mos >= 15:
        score += 20
        reasons.append("Biên an toàn cao theo định giá tổng hợp.")
    elif mos >= 5:
        score += 10
    else:
        risks.append("Biên an toàn mỏng hoặc âm.")

    try:
        de_v = float(de) if de is not None else None
    except (TypeError, ValueError):
        de_v = None
    if de_v is not None and de_v <= 0.7:
        score += 8
    elif de_v is not None and de_v > 1.2:
        risks.append("Đòn bẩy nợ cao.")

    try:
        rev_v = float(rev) if rev is not None else None
    except (TypeError, ValueError):
        rev_v = None
    try:
        profit_v = float(profit) if profit is not None else None
    except (TypeError, ValueError):
        profit_v = None

    if rev_v is not None and profit_v is not None and rev_v > 8 and profit_v > 8:
        score += 12
        reasons.append("Doanh thu và lợi nhuận tăng trưởng đồng thuận.")
    elif rev_v is not None and rev_v < 0:
        risks.append("Doanh thu suy giảm YoY.")

    return score, reasons, risks


def evaluate_stock_opportunity_3m(
    symbol: str,
    ohlcv: pd.DataFrame,
    valuation: dict[str, Any],
    financials: dict[str, Any],
    benchmark_return_3m_pct: float | None = None,
) -> OpportunityResult:
    t_score, t_reasons, t_risks, exp_tech = _technical_score(ohlcv)
    f_score, f_reasons, f_risks = _fundamental_score(valuation, financials)

    total = max(0.0, min(100.0, t_score + f_score))
    prob = max(5.0, min(95.0, 20.0 + total * 0.65))
    confidence = max(10.0, min(95.0, 35.0 + len(t_reasons) * 8.0 + len(f_reasons) * 6.0))
    exp_ret = max(-10.0, min(30.0, exp_tech + (f_score - 20.0) * 0.18))
    hit_rate = _historical_hit_rate_3m(ohlcv, threshold_pct=8.0)
    reliability = _walkforward_reliability_3m(ohlcv, threshold_pct=8.0)
    ret_3m_recent = _recent_return_3m_pct(ohlcv)
    prob = max(5.0, min(95.0, prob * 0.75 + hit_rate * 0.25))
    confidence = max(10.0, min(95.0, confidence * 0.55 + hit_rate * 0.25 + reliability * 0.20))

    bull = max(3.0, min(85.0, prob * 0.7))
    bear = max(5.0, min(80.0, (100.0 - prob) * 0.65))
    base = max(5.0, 100.0 - bull - bear)
    norm = bull + base + bear
    if norm > 0:
        bull, base, bear = bull * 100.0 / norm, base * 100.0 / norm, bear * 100.0 / norm

    avg_vol20 = 0.0
    if ohlcv is not None and not ohlcv.empty and "volume" in ohlcv.columns:
        v = pd.to_numeric(ohlcv["volume"], errors="coerce").dropna()
        if len(v) >= 20:
            avg_vol20 = float(v.tail(20).mean())
    alpha_3m = None
    if benchmark_return_3m_pct is not None:
        try:
            alpha_3m = round(ret_3m_recent - float(benchmark_return_3m_pct), 2)
        except (TypeError, ValueError):
            alpha_3m = None

    reasons = t_reasons + f_reasons
    risks = t_risks + f_risks
    thesis = " ; ".join(reasons[:4]) if reasons else "Dữ liệu hiện tại chưa đủ rõ để khẳng định cơ hội 3 tháng."
    risk_text = " ; ".join(risks[:3]) if risks else "Rủi ro chính chưa nổi bật ở bộ lọc hiện tại."

    status = "THEO DÕI"
    if prob >= 70 and total >= 65 and exp_ret >= 8:
        status = "CƠ HỘI CAO"
    elif prob < 45 or total < 40:
        status = "THẬN TRỌNG"

    invalidation = "Thesis bị vô hiệu nếu giá thủng MA50 kèm khối lượng tăng mạnh hoặc biên an toàn chuyển âm sâu."
    # Data quality guard: prevent overconfident output on thin/partial inputs.
    quality = 0.0
    quality += 35.0 if ohlcv is not None and not ohlcv.empty and len(ohlcv) >= 130 else 0.0
    quality += 20.0 if hit_rate >= 0 else 0.0
    quality += 20.0 if reliability >= 0 else 0.0
    quality += 15.0 if valuation and any(valuation.get(k) is not None for k in ("price", "piotroski_score", "margin_of_safety_composite_pct")) else 0.0
    quality += 10.0 if financials and any(financials.get(k) is not None for k in ("debt_to_equity", "revenue_growth_yoy")) else 0.0
    quality = max(0.0, min(100.0, quality))

    reco_reason = "Điểm tổng hợp đạt ngưỡng theo profile."
    if status == "CƠ HỘI CAO":
        reco_reason = "Xác suất tăng + điểm cơ hội + độ tin cậy đều cao, phù hợp giải ngân có kỷ luật."
    elif status == "THẬN TRỌNG":
        reco_reason = "Điểm/xác suất thấp hoặc rủi ro nổi trội, ưu tiên phòng thủ."
    if quality < 55:
        status = "THẬN TRỌNG"
        reco_reason = "Chất lượng dữ liệu chưa đủ dày để kết luận mạnh, ưu tiên theo dõi thêm."

    return OpportunityResult(
        symbol=symbol.upper(),
        score=round(total, 2),
        probability_up_3m_pct=round(prob, 2),
        expected_return_3m_pct=round(exp_ret, 2),
        confidence_pct=round(confidence, 2),
        bull_prob_pct=round(bull, 2),
        base_prob_pct=round(base, 2),
        bear_prob_pct=round(bear, 2),
        historical_hit_rate_3m_pct=round(hit_rate, 2),
        walkforward_reliability_pct=round(reliability, 2),
        return_3m_recent_pct=round(ret_3m_recent, 2),
        alpha_3m_vs_benchmark_pct=alpha_3m,
        avg_volume_20=round(avg_vol20, 2),
        technical_score=round(max(0.0, min(50.0, t_score)), 2),
        fundamental_score=round(max(0.0, min(50.0, f_score)), 2),
        data_quality_pct=round(quality, 2),
        recommendation_reason=reco_reason,
        thesis=thesis,
        risks=risk_text,
        invalidation=invalidation,
        status=status,
    )

