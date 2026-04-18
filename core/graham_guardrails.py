"""
Guardrails theo phong cách Benjamin Graham cho thị trường Việt Nam.
"""

from __future__ import annotations

from typing import Any


def _to_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def evaluate_graham_7_criteria(snapshot: dict[str, Any], valuation: dict[str, Any]) -> dict[str, Any]:
    """
    Chấm bộ lọc 7 tiêu chí Graham theo dữ liệu sẵn có.
    Unknown được tính là không đạt để giữ thiên hướng thận trọng.
    """
    market_cap_bn = _to_float(snapshot.get("market_cap_bn_vnd") or snapshot.get("market_cap_bn"))
    de = _to_float(snapshot.get("debt_to_equity"))
    debt_asset = _to_float(snapshot.get("debt_to_asset"))
    current_ratio = _to_float(snapshot.get("current_ratio"))
    dividend_years = _to_float(snapshot.get("dividend_paid_years"))
    eps_growth_10y = _to_float(snapshot.get("eps_growth_10y_pct"))
    eps_positive_10y = _to_float(snapshot.get("eps_positive_years_10y"))
    pe = _to_float(snapshot.get("pe"))
    pb = _to_float(snapshot.get("pb"))
    if pe is None:
        price = _to_float(valuation.get("price"))
        eps = _to_float(valuation.get("eps_for_graham")) or _to_float(valuation.get("eps"))
        if price and eps and eps > 0:
            pe = price / eps
    if pb is None:
        price = _to_float(valuation.get("price"))
        bv = _to_float(valuation.get("book_value_per_share"))
        if price and bv and bv > 0:
            pb = price / bv

    checks = [
        {
            "id": "scale",
            "label_vi": "Quy mô doanh nghiệp đủ lớn",
            "passed": bool(market_cap_bn is not None and market_cap_bn >= 10_000),
        },
        {
            "id": "balance_sheet",
            "label_vi": "Đòn bẩy tài chính lành mạnh",
            "passed": bool((de is not None and de <= 1.2) or (debt_asset is not None and debt_asset <= 0.55)),
        },
        {
            "id": "liquidity",
            "label_vi": "Thanh khoản ngắn hạn an toàn",
            "passed": bool(current_ratio is not None and current_ratio >= 1.5),
        },
        {
            "id": "earnings_stability",
            "label_vi": "Lợi nhuận ổn định dài hạn",
            "passed": bool(eps_positive_10y is not None and eps_positive_10y >= 8),
        },
        {
            "id": "dividend_record",
            "label_vi": "Lịch sử chi trả cổ tức bền",
            "passed": bool(dividend_years is not None and dividend_years >= 7),
        },
        {
            "id": "growth_10y",
            "label_vi": "Tăng trưởng EPS 10 năm dương",
            "passed": bool(eps_growth_10y is not None and eps_growth_10y >= 25),
        },
        {
            "id": "valuation_moderate",
            "label_vi": "Định giá không quá đắt (P/E, P/B)",
            "passed": bool((pe is not None and pe <= 20) and (pb is None or pb <= 2.5)),
        },
    ]
    passed = sum(1 for c in checks if c["passed"])
    return {
        "passed": passed,
        "total": len(checks),
        "pass_rate_pct": round(passed / len(checks) * 100.0, 1),
        "checks": checks,
    }


def mr_market_sentiment_index(
    *,
    stock_day_change_pct: float | None,
    index_day_change_pct: float | None,
    structural_news_count: int = 0,
) -> dict[str, Any]:
    """
    Chỉ số Mr. Market 0-100 (sợ hãi -> hưng phấn).
    """
    score = 50.0
    if stock_day_change_pct is not None:
        score += max(-20.0, min(20.0, stock_day_change_pct * 3.0))
    if index_day_change_pct is not None:
        score += max(-10.0, min(10.0, index_day_change_pct * 4.0))
    score -= min(20.0, max(0.0, structural_news_count) * 7.0)
    score = max(0.0, min(100.0, score))

    mood = "Trung tính"
    if score <= 33:
        mood = "Sợ hãi cao"
    elif score >= 67:
        mood = "Hưng phấn cao"
    return {"score": round(score, 1), "mood_vi": mood}


def watcher_governance_debt_flags(snapshot: dict[str, Any]) -> dict[str, Any]:
    """
    Watcher theo dõi biến động quản trị và cấu trúc nợ.
    """
    de = _to_float(snapshot.get("debt_to_equity"))
    de_prev = _to_float(snapshot.get("debt_to_equity_prior"))
    insider_net_sell_pct = _to_float(snapshot.get("insider_net_sell_pct"))
    flags: list[str] = []
    if de is not None and de_prev is not None and (de - de_prev) >= 0.25:
        flags.append("Nợ/vốn chủ tăng nhanh so với kỳ trước.")
    if de is not None and de >= 1.8:
        flags.append("Đòn bẩy tài chính đang cao.")
    if insider_net_sell_pct is not None and insider_net_sell_pct >= 0.5:
        flags.append("Ban lãnh đạo có xu hướng bán ròng đáng kể.")
    return {"flag_count": len(flags), "flags_vi": flags}

