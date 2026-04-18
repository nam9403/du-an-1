"""
Sentinel Engine — cảnh báo rủi ro đa tầng: nhiễu thị trường vs VN-Index, tin tức (tạm/cấu trúc), 7 Whys rút gọn.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from core.valuation import value_investing_summary
from scrapers.financial_data import fetch_financial_snapshot
from scrapers.portal import PortalDataError, fetch_financial_indicators, fetch_latest_news, fetch_ohlcv_history

# Ngưỡng (có thể chỉnh qua env)
DROP_TRIGGER_PCT = float(os.environ.get("SENTINEL_DROP_TRIGGER_PCT", "-3"))
INDEX_STRESS_PCT = float(os.environ.get("SENTINEL_INDEX_STRESS_PCT", "-1.2"))  # thị trường giảm rõ
INDEX_CALM_PCT = float(os.environ.get("SENTINEL_INDEX_CALM_PCT", "-0.4"))  # thị trường gần ngang/tăng

_INDEX_CANDIDATES = ("VNINDEX", "VN30", "E1VFVN30")


def ohlcv_last_session_change_pct(df: pd.DataFrame | None) -> float | None:
    """% thay đổi giữa 2 phiên đóng cửa gần nhất."""
    if df is None or df.empty or len(df) < 2:
        return None
    try:
        prev = float(df.iloc[-2]["close"])
        last = float(df.iloc[-1]["close"])
    except (TypeError, ValueError, KeyError):
        return None
    if prev <= 0:
        return None
    return (last - prev) / prev * 100.0


def _fetch_benchmark_change_pct() -> tuple[float | None, str | None, str]:
    """
    Lấy % thay đổi phiên gần nhất của chỉ số tham chiếu (VN-Index / VN30 / proxy ETF).
    Trả (pct, symbol_used, note).
    """
    errors: list[str] = []
    for sym in _INDEX_CANDIDATES:
        try:
            df = fetch_ohlcv_history(sym, sessions=15)
            chg = ohlcv_last_session_change_pct(df)
            if chg is not None:
                src = str(df.attrs.get("source", ""))
                return chg, sym, f"Nguồn OHLCV: {src or 'live'}"
        except PortalDataError as e:
            errors.append(f"{sym}:{e}")
            continue
    return None, None, "Không lấy được chỉ số tham chiếu: " + "; ".join(errors[:3])


def _fundamentals_still_strong(valuation: dict[str, Any], financials: dict[str, Any]) -> bool:
    fs = int(valuation.get("piotroski_score") or 0)
    mos = float(valuation.get("margin_of_safety_composite_pct") or -999)
    de = financials.get("debt_to_equity")
    try:
        de_f = float(de) if de is not None else None
    except (TypeError, ValueError):
        de_f = None
    de_ok = de_f is None or de_f < 1.35
    return fs >= 5 and mos >= -8.0 and de_ok


def _market_noise_case(
    stock_chg: float | None,
    index_chg: float | None,
    fundamentals_ok: bool,
) -> dict[str, Any]:
    """Phân loại A/B/neutral theo tương quan với chỉ số."""
    if stock_chg is None:
        return {
            "triggered": False,
            "case": "no_data",
            "label_vi": "Chưa đủ dữ liệu giá để lọc nhiễu.",
            "detail_vi": "Cần ít nhất 2 phiên OHLCV cho mã.",
        }

    triggered = stock_chg <= DROP_TRIGGER_PCT
    if not triggered:
        return {
            "triggered": False,
            "case": "neutral",
            "label_vi": "Biến động trong ngưỡng theo dõi",
            "detail_vi": f"Phiên gần nhất: {stock_chg:+.2f}% (ngưỡng cảnh báo sâu: ≤ {DROP_TRIGGER_PCT}%).",
        }

    if index_chg is None:
        return {
            "triggered": True,
            "case": "unknown_index",
            "label_vi": "Giảm sâu — chưa đối chiếu được chỉ số",
            "detail_vi": f"Cổ phiếu {stock_chg:+.2f}% nhưng không có dữ liệu VN-Index/VN30. Xem tin tức & BCTC trước khi quyết định.",
        }

    # Case A: thị trường chung stress, DN vẫn tốt → cơ hội chiết khấu
    if index_chg <= INDEX_STRESS_PCT and fundamentals_ok:
        return {
            "triggered": True,
            "case": "A_market_wide_discount",
            "label_vi": "Cơ hội vàng — Thị trường đang chiết khấu vô lý?",
            "detail_vi": (
                f"Giá mã giảm {stock_chg:+.2f}% trong khi chỉ số tham chiếu cũng giảm mạnh ({index_chg:+.2f}%). "
                f"Chỉ số tài chính vẫn tương đối tốt — có thể là nhiễu vĩ mô, không phải hỏng nội tại."
            ),
        }

    if index_chg <= INDEX_STRESS_PCT and not fundamentals_ok:
        return {
            "triggered": True,
            "case": "A_market_wide_weak_fundamentals",
            "label_vi": "Thị trường xấu — nội tại cần xem lại",
            "detail_vi": (
                f"Cả thị trường ({index_chg:+.2f}%) và tín hiệu BCTC/F-Score chưa đủ mạnh. "
                f"Thận trọng, ưu tiên quản trị rủi ro."
            ),
        }

    # Case B: mã tụt mạnh, chỉ số nhẹ → rủi ro đặc thù
    if index_chg >= INDEX_CALM_PCT:
        return {
            "triggered": True,
            "case": "B_idiosyncratic",
            "label_vi": "Rủi ro nội tại — Cần kiểm tra sâu",
            "detail_vi": (
                f"Mã giảm {stock_chg:+.2f}% trong khi chỉ số tham chiếu gần ngang/tích cực ({index_chg:+.2f}%). "
                f"Dễ là tin đặc thù ngành/DN — đọc tin & BCTC."
            ),
        }

    return {
        "triggered": True,
        "case": "mixed",
        "label_vi": "Giảm đồng thuận một phần với thị trường",
        "detail_vi": (
            f"Mã {stock_chg:+.2f}%, chỉ số {index_chg:+.2f}% — không rõ ràng A/B; kết hợp tin tức & định giá."
        ),
    }


_TEMPORARY_KW = re.compile(
    r"thuế\s*quan|chiến\s*tranh|vĩ\s*mô|covid|lãi\s*suất|tăng\s*giá|"
    r"thị\s*trường\s*chung|ngắn\s*hạn|tin\s*đồn|biến\s*động\s*ngắn|fed|ecb",
    re.I,
)
_STRUCTURAL_KW = re.compile(
    r"gian\s*lận|kiểm\s*toán|kiện|phá\s*sản|mất\s*thị\s*phần|"
    r"thoái\s*vốn|trọng\s*tài|điều\s*tra|thanh\s*tra|lừa\s*đảo|"
    r"thay\s*đổi\s*mô\s*hình|sụt\s*giảm\s*cốt\s*lõi",
    re.I,
)


def _fallback_news_classification(headlines: list[dict[str, str]]) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    structural_count = 0
    for h in headlines[:8]:
        t = (h.get("title") or "")[:500]
        if _STRUCTURAL_KW.search(t):
            cls = "structural"
            structural_count += 1
            reason = "Từ khóa gợi ý rủi ro cấu trúc/dài hạn (heuristic)."
        elif _TEMPORARY_KW.search(t):
            cls = "temporary"
            reason = "Từ khóa gợi ý sóng ngắn/vĩ mô (heuristic)."
        else:
            cls = "unclear"
            reason = "Chưa phân loại rõ — cần đọc chi tiết."
        items.append({"title": t, "class": cls, "reason_vi": reason})

    if structural_count >= 2:
        impact = "yes"
        advice = "Nhiều tin mang tính cấu trúc — ưu tiên đọc BCTC & công bố chính thống trước khi mua thêm."
    elif structural_count == 0:
        impact = "no"
        advice = "Chưa thấy cụm tin mang tính phá vỡ nền tảng; biến động giá có thể do tâm lý/ngắn hạn."
    else:
        impact = "unclear"
        advice = "Có ít nhất một tin đáng theo dõi sâu; không vội bán tháo chỉ vì một dòng tiêu đề."

    return {
        "items": items,
        "three_year_cashflow_impact": impact,
        "advice_vi": advice,
        "llm_used": False,
    }


def _llm_classify_news(headlines: list[dict[str, str]], ticker: str) -> dict[str, Any] | None:
    if not headlines:
        return None
    payload = {
        "ticker": ticker,
        "headlines": [{"title": h.get("title", ""), "source": h.get("source", "")} for h in headlines[:6]],
    }
    system = (
        "Bạn là chuyên gia đầu tư giá trị Việt Nam. Phân loại tin: "
        "'temporary' (ngắn hạn, vĩ mô, tin đồn) vs 'structural' (gian lận, mất thị phần cốt lõi, "
        "thay đổi mô hình xấu không thể đảo ngắn). "
        "Trả lời CHỈ JSON: "
        '{"items":[{"title_snippet":"","class":"temporary|structural|unclear","reason_vi":""}],'
        '"three_year_cashflow_impact":"yes|no|unclear",'
        '"advice_vi":"Một đoạn ngắn: tin này có làm suy giảm dòng tiền 3-5 năm không? Khuyến nghị giữ/mua thêm hay thận trọng."}'
    )
    user = json.dumps(payload, ensure_ascii=False)
    messages = [{"role": "system", "content": system}, {"role": "user", "content": user}]

    try:
        if os.environ.get("GROQ_API_KEY"):
            from core.ai_logic import _call_groq

            text = _call_groq(messages)
        elif os.environ.get("OPENAI_API_KEY"):
            from core.ai_logic import _call_openai

            text = _call_openai(messages)
        elif os.environ.get("GEMINI_API_KEY"):
            from core.ai_logic import _call_gemini

            text = _call_gemini(messages)
        else:
            return None
    except Exception:
        return None

    text = (text or "").strip()
    m = re.search(r"\{[\s\S]*\}\s*$", text)
    if not m:
        return None
    try:
        obj = json.loads(m.group(0))
    except ValueError:
        return None
    if not isinstance(obj, dict):
        return None
    obj["llm_used"] = True
    return obj


def _build_whys_deep_dive(
    ticker: str,
    stock_chg: float | None,
    index_chg: float | None,
    index_sym: str | None,
    noise: dict[str, Any],
    valuation: dict[str, Any],
    news_block: dict[str, Any],
) -> dict[str, Any]:
    price = float(valuation.get("price") or 0)
    comp = float(valuation.get("composite_target_price") or 0)
    mos = valuation.get("margin_of_safety_composite_pct")
    mos_s = f"{float(mos):.1f}%" if mos is not None else "N/A"

    idx_txt = f"{index_sym or '—'} ({index_chg:+.2f}% phiên gần nhất)" if index_chg is not None else "chưa có chỉ số"

    w1 = (
        f"Tại sao giá biến động? Phiên gần nhất: {stock_chg:+.2f}%."
        if stock_chg is not None
        else "Chưa đủ dữ liệu giá 2 phiên để giải thích biến động."
    )
    w2 = f"Tin tức & tâm lý: {news_block.get('advice_vi', 'Xem mục phân tích tin bên dưới.')[:400]}"
    w3 = f"So với thị trường: chỉ số tham chiếu {idx_txt}."
    w4 = f"Định giá: giá {price:,.0f} vs mục tiêu tổng hợp ~{comp:,.0f} (MOS tổng hợp {mos_s})."

    calm = noise.get("detail_vi", "")
    if noise.get("case") == "A_market_wide_discount":
        calm = (
            "Đừng hoảng loạn. Giá có thể giảm theo sóng thị trường hoặc tin ngắn hạn, "
            "trong khi nền tảng BCTC vẫn được điểm tốt — đây có thể là vùng tích lũy khi biên an toàn mở rộng. "
            "Luôn xác minh BCTC và khẩu vị rủi ro của bạn."
        )
    elif noise.get("case") == "B_idiosyncratic":
        calm = (
            "Ưu tiên làm rõ tin đặc thù cho mã: đọc công bố, hỏi 'tin này có làm hỏng mô hình kinh doanh 3-5 năm không?'. "
            "Tránh bán tháo chỉ vì một nhịp đỏ nếu chưa có bằng chứng cấu trúc."
        )

    steps = [
        {"why": 1, "question": "Tại sao giá giảm/tăng mạnh phiên gần nhất?", "answer_vi": w1},
        {"why": 2, "question": "Tin tức là tạm thời hay cấu trúc?", "answer_vi": w2},
        {"why": 3, "question": "Thị trường chung đang làm gì?", "answer_vi": w3},
        {"why": 4, "question": "Giá so với giá trị nội tại?", "answer_vi": w4},
    ]
    return {"steps": steps, "calm_conclusion_vi": calm}


def _strategic_table(valuation: dict[str, Any]) -> list[dict[str, Any]]:
    price = float(valuation.get("price") or 0)
    graham = float(valuation.get("intrinsic_value_graham") or 0)
    comp = float(valuation.get("composite_target_price") or 0)
    mos_g = valuation.get("margin_of_safety_pct")
    mos_c = valuation.get("margin_of_safety_composite_pct")
    rows = [
        {"Chỉ tiêu": "Giá thị trường (tham chiếu)", "Giá trị (VND)": f"{price:,.0f}"},
        {"Chỉ tiêu": "Giá trị nội tại (Graham)", "Giá trị (VND)": f"{graham:,.0f}" if graham > 0 else "—"},
        {"Chỉ tiêu": "Giá mục tiêu tổng hợp", "Giá trị (VND)": f"{comp:,.0f}" if comp > 0 else "—"},
        {"Chỉ tiêu": "MOS Graham (%)", "Giá trị (VND)": f"{float(mos_g):.1f}%" if mos_g is not None else "—"},
        {"Chỉ tiêu": "MOS tổng hợp (%)", "Giá trị (VND)": f"{float(mos_c):.1f}%" if mos_c is not None else "—"},
    ]
    return rows


def check_security_alerts(
    ticker: str,
    snapshot: dict[str, Any] | None = None,
    *,
    enable_llm: bool = True,
    news_limit: int = 8,
) -> dict[str, Any]:
    """
    Phân tích đa tầng cho một mã: nhiễu thị trường vs chỉ số, tin tức, 7 Whys rút gọn, UI chiến lược.
    """
    sym = (ticker or "").strip().upper()
    if not sym:
        raise ValueError("Ticker rỗng.")

    if snapshot is None:
        snapshot = fetch_financial_snapshot(sym)
    if snapshot is None:
        raise ValueError(f"Không có snapshot cho {sym}.")

    valuation = value_investing_summary(snapshot)

    financials: dict[str, Any] = {}
    try:
        financials = fetch_financial_indicators(sym, fast_mode=False)
    except PortalDataError:
        financials = {}

    fundamentals_ok = _fundamentals_still_strong(valuation, financials)

    stock_chg: float | None = None
    try:
        ohlcv = fetch_ohlcv_history(sym, sessions=15)
        stock_chg = ohlcv_last_session_change_pct(ohlcv)
    except PortalDataError:
        pass

    index_chg, index_sym, index_note = _fetch_benchmark_change_pct()
    noise = _market_noise_case(stock_chg, index_chg, fundamentals_ok)

    try:
        news = fetch_latest_news(sym, limit=news_limit)
    except PortalDataError:
        news = []

    news_intel = _fallback_news_classification(news)
    if enable_llm and news:
        llm_res = _llm_classify_news(news, sym)
        if isinstance(llm_res, dict) and llm_res.get("items"):
            news_intel = llm_res
            fb = _fallback_news_classification(news)
            for k in ("three_year_cashflow_impact", "advice_vi"):
                if not news_intel.get(k):
                    news_intel[k] = fb.get(k)
            news_intel["llm_used"] = True
        else:
            news_intel.setdefault("llm_used", False)
    else:
        news_intel.setdefault("llm_used", False)

    whys = _build_whys_deep_dive(sym, stock_chg, index_chg, index_sym, noise, valuation, news_intel)

    show_anchor = (
        noise.get("case") == "A_market_wide_discount"
        or (
            noise.get("triggered")
            and fundamentals_ok
            and float(valuation.get("margin_of_safety_composite_pct") or -999) >= 5
        )
    )

    return {
        "ticker": sym,
        "as_of_utc": datetime.now(timezone.utc).isoformat(),
        "stock_day_change_pct": stock_chg,
        "index_day_change_pct": index_chg,
        "index_symbol_used": index_sym,
        "index_fetch_note": index_note,
        "noise_filter": noise,
        "fundamentals_strong": fundamentals_ok,
        "news_intelligence": news_intel,
        "whys_deep_dive": whys,
        "strategic_ui": {
            "show_value_anchor": bool(show_anchor),
            "value_anchor_title": "🛡️ Điểm tựa giá trị",
            "value_anchor_subtitle_vi": (
                "Giá đang phản ứng mạnh nhưng chân giá trị (định giá + BCTC) còn vững — tránh quyết định cảm tính."
                if show_anchor
                else ""
            ),
            "price_vs_value_rows": _strategic_table(valuation),
        },
    }
