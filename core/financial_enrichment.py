"""
Gộp chuỗi BCTC (API) vào snapshot: tăng trưởng, piotroski bổ sung, meta minh bạch.
"""

from __future__ import annotations

import os
from typing import Any

from scrapers.financial_statements import compute_trend_metrics, fetch_vndirect_financial_statement_rows


def attach_financial_statements_to_snapshot(snapshot: dict[str, Any], symbol: str) -> None:
    """
    In-place: thêm financial_report_series, trend metrics, và cập nhật growth_rate_pct nếu có dữ liệu.
    Tắt: II_FETCH_FINANCIAL_STATEMENTS=0
    """
    if os.environ.get("II_FETCH_FINANCIAL_STATEMENTS", "1").strip().lower() in ("0", "false", "no"):
        return

    sym = (symbol or snapshot.get("symbol") or "").strip().upper()
    if not sym:
        return

    rows, src = fetch_vndirect_financial_statement_rows(sym, size=12)
    if not rows:
        snapshot["financial_report_fetch"] = {"ok": False, "source": "vndirect_finfo", "detail": src}
        return

    trends = compute_trend_metrics(rows)
    snapshot["financial_report_series"] = rows[:8]
    snapshot["financial_report_trends"] = trends
    snapshot["financial_report_fetch"] = {
        "ok": True,
        "source": "vndirect_finfo",
        "endpoint_hint": src,
        "disclaimer": "Số liệu tóm tắt từ API công khai, không thay thế BCTC niêm yết đầy đủ.",
    }

    ry = trends.get("revenue_yoy_pct")
    py = trends.get("profit_yoy_pct")
    if ry is not None:
        blended = float(ry)
        if py is not None:
            blended = 0.65 * float(ry) + 0.35 * float(py)
        old_g = float(snapshot.get("growth_rate_pct") or 0)
        if old_g == 0 or snapshot.get("source", "").startswith("mock"):
            snapshot["growth_rate_pct"] = round(blended, 2)
            snapshot["growth_rate_pct_source"] = "financial_report_yoy_blend"
        else:
            snapshot["growth_rate_pct_alternate_from_bctc"] = round(blended, 2)

    # Bổ sung piotroski từ 2 kỳ gần nhất (LNST, doanh thu tuyệt đối) nếu thiếu
    pio = snapshot.get("piotroski")
    if not isinstance(pio, dict):
        pio = {}
    if len(rows) >= 2 and pio.get("net_income") is None:
        a, b = rows[0], rows[1]
        ni0 = a.get("net_income")
        ni1 = b.get("net_income")
        r0, r1 = a.get("revenue"), b.get("revenue")
        if ni0 is not None:
            pio["net_income"] = float(ni0)
        if ni1 is not None:
            pio["net_income_prior"] = float(ni1)
        if r0 is not None:
            pio["revenue"] = float(r0)
        if r1 is not None:
            pio["revenue_prior"] = float(r1)
        ta0 = a.get("total_assets")
        ta1 = b.get("total_assets")
        if ta0 is not None:
            pio.setdefault("total_assets", float(ta0))
        if ta1 is not None:
            pio.setdefault("total_assets_prior", float(ta1))
        snapshot["piotroski"] = pio
        snapshot["piotroski_partial_from"] = "financial_report_api"
