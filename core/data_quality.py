"""
Điểm tin cậy dữ liệu (1–5) và metadata nguồn cho snapshot tài chính.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

_PIOTROSKI_VALUATION_KEYS = frozenset(
    {
        "net_income",
        "net_income_prior",
        "operating_cash_flow",
        "total_assets",
        "total_assets_prior",
        "revenue",
        "revenue_prior",
        "current_assets",
        "current_liabilities",
    }
)


def compute_bctc_readiness(snapshot: dict[str, Any]) -> dict[str, Any]:
    """
    Đánh giá mức đủ dữ liệu để áp dụng Graham / định giá tổng hợp một cách có ý nghĩa.
    Không thay thế đọc BCTC niêm yết; chỉ là checklist trong app.
    """
    checks: list[dict[str, Any]] = []
    score = 0

    src = str(snapshot.get("source") or "")
    fr = snapshot.get("financial_report_fetch") or {}
    if isinstance(fr, dict) and fr.get("ok"):
        score += 12
        checks.append({"name": "Chuỗi BCTC (API nhiều kỳ)", "ok": True, "detail": "vndirect_finfo"})

    overlay = snapshot.get("live_fundamentals_overlay") or {}
    if overlay.get("ok"):
        score += 15
        checks.append(
            {
                "name": "Gộp chỉ số live",
                "ok": True,
                "detail": str(overlay.get("live_source") or "live"),
            }
        )
    elif "live_overlay" in src or "live_overlay" in str(snapshot.get("source_detail", "")):
        score += 10
        checks.append({"name": "Gộp chỉ số live", "ok": True, "detail": src})
    elif src == "mock_json":
        checks.append(
            {
                "name": "Nguồn cơ bản",
                "ok": False,
                "detail": "Chỉ mock cục bộ — nên bật II_MERGE_LIVE_FUNDAMENTALS=1 hoặc II_SKIP_MOCK=1",
            }
        )
    elif src in ("http_api",) or any(x in src for x in ("vietstock", "vndirect", "cafef", "cache:")):
        score += 25
        checks.append({"name": "Nguồn cơ bản", "ok": True, "detail": src[:80]})
    else:
        score += 10
        checks.append({"name": "Nguồn cơ bản", "ok": True, "detail": src or "unknown"})

    eps = float(snapshot.get("eps") or 0)
    if eps > 0:
        score += 20
        checks.append({"name": "EPS dương", "ok": True, "detail": f"{eps:,.0f}"})
    else:
        checks.append({"name": "EPS dương", "ok": False, "detail": "Thiếu hoặc ≤ 0 — Graham hạn chế"})

    if snapshot.get("sector_pe_5y_avg") is not None and float(snapshot.get("sector_pe_5y_avg") or 0) > 0:
        score += 10
        checks.append({"name": "P/E ngành (chuẩn)", "ok": True, "detail": "Có trong snapshot"})
    else:
        checks.append({"name": "P/E ngành (chuẩn)", "ok": False, "detail": "Đang dùng mặc định nội bộ"})

    bv = float(snapshot.get("book_value_per_share") or 0)
    if bv > 0:
        score += 15
        checks.append({"name": "BVPS", "ok": True, "detail": f"{bv:,.0f}"})
    else:
        checks.append({"name": "BVPS", "ok": False, "detail": "Thiếu — P/B kém tin cậy"})

    g = snapshot.get("growth_rate_pct")
    ef = snapshot.get("eps_forward")
    if (g is not None and float(g) != 0.0) or (ef is not None and float(ef or 0) > 0):
        score += 5
        checks.append({"name": "Tăng trưởng / EPS forward", "ok": True, "detail": "Có"})
    else:
        checks.append({"name": "Tăng trưởng / EPS forward", "ok": False, "detail": "Đang 0 hoặc thiếu"})

    pio = snapshot.get("piotroski") if isinstance(snapshot.get("piotroski"), dict) else {}
    hit = sum(1 for k in _PIOTROSKI_VALUATION_KEYS if pio.get(k) is not None)
    if hit >= 6:
        score += 15
        checks.append({"name": "Khối BCTC (Piotroski)", "ok": True, "detail": f"{hit} trường chính"})
    elif hit >= 3:
        score += 8
        checks.append({"name": "Khối BCTC (Piotroski)", "ok": True, "detail": f"Chỉ {hit} trường — nên bổ sung"})
    else:
        checks.append({"name": "Khối BCTC (Piotroski)", "ok": False, "detail": "Quá thiếu cho F-Score đầy đủ"})

    ps = str(snapshot.get("price_source") or "")
    if ps and ps != "unknown":
        score += 5
        checks.append({"name": "Giá thị trường", "ok": True, "detail": ps[:60]})
    else:
        checks.append({"name": "Giá thị trường", "ok": False, "detail": "Nguồn giá không rõ"})

    score = max(0, min(100, score))
    if score >= 80:
        tier = "A"
    elif score >= 60:
        tier = "B"
    elif score >= 40:
        tier = "C"
    else:
        tier = "D"

    hints: list[str] = []
    if src == "mock_json" and not overlay.get("ok"):
        hints.append("Đặt II_MERGE_LIVE_FUNDAMENTALS=1 (mặc định) để tự gộp EPS/BVPS từ Vietstock/VNDirect khi có mạng.")
    if snapshot.get("sector_pe_5y_avg") is None:
        hints.append("Cập nhật sector_pe_5y_avg trong snapshot/API nếu có P/E ngành 5 năm.")
    if hit < 6:
        hints.append("Ưu tiên nguồn có đủ chỉ tiêu BCTC (LNST, TS, vốn…) cho F-Score và chất lượng định giá.")

    return {
        "score_0_100": score,
        "tier": tier,
        "graham_reliable": bool(score >= 55 and eps > 0),
        "checks": checks,
        "action_hints": hints,
    }


def compute_snapshot_confidence(snapshot: dict[str, Any]) -> tuple[int, list[str]]:
    """
    1 = rất thấp, 5 = tốt cho mục đích minh học/screening.
    Không thay thế kiểm toán BCTC niêm yết.
    """
    reasons: list[str] = []
    score = 3
    src = str(snapshot.get("source") or "")
    ps = str(snapshot.get("price_source") or "")

    if src == "mock_json":
        score -= 1
        reasons.append("BCTC/EPS từ file mẫu cục bộ")
    elif "live_overlay" in src or src == "mock_json+live_overlay":
        score += 1
        reasons.append("Đã gộp chỉ số cơ bản từ nguồn live lên snapshot mẫu")
    elif src in ("http_api",) or "vietstock" in src or "vndirect" in src or "cafef" in src:
        score += 1
        reasons.append("Chỉ số từ nguồn scrape/API")

    if ps and ("portal" in ps.lower() or "quote:" in ps.lower()):
        score += 1
        reasons.append("Giá cập nhật từ thị trường (OHLCV/quote)")
    elif not ps or ps == "unknown":
        score -= 1
        reasons.append("Nguồn giá không rõ hoặc thiếu")

    pio = snapshot.get("piotroski")
    if isinstance(pio, dict) and len(pio) >= 4:
        score += 1
        reasons.append("Khối Piotroski/BCTC đủ trường")

    score = max(1, min(5, score))
    return score, reasons


def enrich_snapshot_quality_fields(row: dict[str, Any]) -> None:
    """Gắn điểm tin cậy + provenance (idempotent)."""
    if row.get("_dq_enriched"):
        return
    row["_dq_enriched"] = True
    row["snapshot_fetched_at_utc"] = datetime.now(timezone.utc).isoformat()
    conf, reasons = compute_snapshot_confidence(row)
    row["data_confidence_score"] = conf
    row["data_confidence_reasons"] = reasons
    bctc = compute_bctc_readiness(row)
    row["bctc_readiness"] = bctc
    row["data_provenance"] = {
        "snapshot_fetched_at_utc": row["snapshot_fetched_at_utc"],
        "fundamentals_source": str(row.get("source") or "unknown"),
        "price_source": str(row.get("price_source") or "unknown"),
        "confidence_1_to_5": conf,
        "confidence_reasons": reasons,
        "bctc_tier": bctc.get("tier"),
        "bctc_score_0_100": bctc.get("score_0_100"),
        "graham_reliable_per_checklist": bctc.get("graham_reliable"),
    }


def build_data_provenance(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Dùng khi cần object provenance mà snapshot đã có đủ trường."""
    conf, reasons = compute_snapshot_confidence(snapshot)
    return {
        "snapshot_fetched_at_utc": str(snapshot.get("snapshot_fetched_at_utc") or ""),
        "fundamentals_source": str(snapshot.get("source") or "unknown"),
        "price_source": str(snapshot.get("price_source") or "unknown"),
        "confidence_1_to_5": conf,
        "confidence_reasons": reasons,
    }
