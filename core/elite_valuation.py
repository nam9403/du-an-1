"""
Nâng cấp định giá cấp cao: kịch bản Bear/Base/Bull, dải intrinsic, DCF OCF đơn giản,
so sánh peer cùng cluster, audit fingerprint, điểm chất lượng mô hình (thang tối đa 9.5).
"""

from __future__ import annotations

import hashlib
import json
import os
from statistics import median
from typing import Any


def _clip_g(g: float) -> float:
    return max(0.0, min(float(g), 45.0))


def _clip_y(y: float) -> float:
    return max(2.0, min(float(y), 16.0))


def _env_float(name: str, default: str) -> float:
    try:
        return float(os.environ.get(name, default).strip().replace(",", "."))
    except (TypeError, ValueError):
        return float(default)


def merge_elite_valuation(snapshot: dict[str, Any], base: dict[str, Any]) -> dict[str, Any]:
    """
    Gắn thêm các trường elite vào dict kết quả value_investing_summary (đã tính xong base).
    Không gọi đệ quy include_extensions.
    """
    from core.valuation import value_investing_summary

    base_g = float(base.get("growth_rate_pct") or 0)
    base_y = float(base.get("bond_yield_pct_used") or snapshot.get("bond_yield_pct") or 4.4)

    bear_g = _clip_g(base_g + _env_float("II_SCENARIO_BEAR_G_DELTA", "-3"))
    bull_g = _clip_g(base_g + _env_float("II_SCENARIO_BULL_G_DELTA", "4"))
    bear_y = _clip_y(base_y + _env_float("II_SCENARIO_BEAR_Y_DELTA", "0.8"))
    bull_y = _clip_y(base_y + _env_float("II_SCENARIO_BULL_Y_DELTA", "-0.4"))

    base_row = {
        "id": "base",
        "label_vi": "Cơ sở",
        "growth_rate_pct": base_g,
        "bond_yield_pct": base_y,
        "intrinsic_graham": float(base.get("intrinsic_value_graham") or 0),
        "composite_target_price": float(base.get("composite_target_price") or 0),
        "margin_of_safety_composite_pct": base.get("margin_of_safety_composite_pct"),
    }
    scenario_rows: list[dict[str, Any]] = []

    for sid, lbl, gg, yy in (
        ("bear", "Bi quan", bear_g, bear_y),
        ("bull", "Lạc quan", bull_g, bull_y),
    ):
        s2 = {**snapshot, "growth_rate_pct": gg, "bond_yield_pct": yy}
        v = value_investing_summary(s2, include_extensions=False)
        scenario_rows.append(
            {
                "id": sid,
                "label_vi": lbl,
                "growth_rate_pct": gg,
                "bond_yield_pct": yy,
                "intrinsic_graham": float(v.get("intrinsic_value_graham") or 0),
                "composite_target_price": float(v.get("composite_target_price") or 0),
                "margin_of_safety_composite_pct": v.get("margin_of_safety_composite_pct"),
            }
        )

    ordered = [scenario_rows[0], base_row, scenario_rows[1]]

    composites = [float(x.get("composite_target_price") or 0) for x in ordered if float(x.get("composite_target_price") or 0) > 0]
    band: dict[str, Any] = {}
    if len(composites) >= 2:
        lo, hi = min(composites), max(composites)
        mid = float(base.get("composite_target_price") or 0)
        spread_pct = ((hi - lo) / lo * 100.0) if lo > 0 else None
        band = {
            "composite_low": lo,
            "composite_mid": mid,
            "composite_high": hi,
            "composite_spread_pct": spread_pct,
            "note_vi": "Dải từ kịch bi quan ↔ lạc quan (g và Y thay đổi); không phải khoảng tin cậy thống kê.",
        }

    dcf_block = _simple_ocf_dcf_per_share(snapshot, base)
    if os.environ.get("II_SKIP_PEER_FETCH", "").strip().lower() in ("1", "true", "yes", "on"):
        peer_block = {
            "ok": False,
            "skipped": True,
            "reason_vi": "Peer tắt (II_SKIP_PEER_FETCH) — bật trong môi trường thật để so sánh ngành.",
            "rows": [],
        }
    else:
        peer_block = _peer_relative_analysis(snapshot, base)
    audit = _audit_fingerprint(snapshot, base)
    score, detail = _excellence_score(snapshot, base, peer_block, band)

    base["scenario_valuation"] = ordered
    base["intrinsic_band"] = band
    base["simple_dcf"] = dcf_block
    base["peer_relative"] = peer_block
    base["valuation_audit"] = audit
    base["valuation_excellence_score"] = score
    base["valuation_excellence_detail_vi"] = detail
    base["valuation_disclaimer_vi"] = VALUATION_DISCLAIMER_VI
    return base


VALUATION_DISCLAIMER_VI = (
    "Công cụ hỗ trợ phân tích định tính & định lượng; không phải tư vấn đầu tư hay dự báo giá. "
    "Kịch bản, DCF đơn giản và so sánh peer dựa trên giả định và dữ liệu công khai — "
    "cần đối chiếu BCTC niêm yết và phán đoán độc lập trước khi quyết định."
)


def _simple_ocf_dcf_per_share(snapshot: dict[str, Any], base_val: dict[str, Any]) -> dict[str, Any]:
    pio = snapshot.get("piotroski") if isinstance(snapshot.get("piotroski"), dict) else {}
    ocf = float(pio.get("operating_cash_flow") or 0)
    sh = float(pio.get("shares_outstanding") or 0)
    capex_ratio = _env_float("II_DCF_CAPEX_RATIO", "0.25")
    erp = _env_float("II_DCF_EQUITY_RISK_PREMIUM", "5.5") / 100.0
    g_pct = float(base_val.get("growth_rate_pct") or 5)
    g = min(max(g_pct / 100.0, 0.02), 0.12)

    if ocf <= 0 or sh <= 0:
        return {
            "ok": False,
            "reason_vi": "Thiếu operating_cash_flow hoặc shares_outstanding trong khối piotroski — không tính DCF OCF.",
        }

    fcf_company = ocf * (1.0 - min(max(capex_ratio, 0.0), 0.6))
    fcf_ps = fcf_company / sh
    y = float(base_val.get("bond_yield_pct_used") or 5) / 100.0
    wacc = min(max(y + erp, 0.07), 0.22)
    g_adj = min(g, wacc - 0.015)
    if wacc <= g_adj:
        g_adj = wacc - 0.01
    tv = fcf_ps * (1.0 + g_adj) / (wacc - g_adj) if (wacc - g_adj) > 0 else 0.0
    price = float(base_val.get("price") or 0)
    mos = ((tv - price) / tv * 100.0) if tv > 0 and price > 0 else None
    return {
        "ok": True,
        "fcf_per_share_approx": round(fcf_ps, 4),
        "wacc_approx_pct": round(wacc * 100, 2),
        "terminal_growth_pct": round(g_adj * 100, 2),
        "intrinsic_per_share_ocf_gordon": round(tv, 2),
        "margin_of_safety_pct": round(mos, 2) if mos is not None else None,
        "note_vi": (
            "FCF ≈ OCP × (1 − tỷ lệ capex ước); WACC ≈ Y + premium — mô hình 1 tầng Gordon, "
            "chỉ mang tính minh họa khi có đủ piotroski."
        ),
    }


def _peer_relative_analysis(snapshot: dict[str, Any], base_val: dict[str, Any]) -> dict[str, Any]:
    sym = str(snapshot.get("symbol") or "").strip().upper()
    if not sym:
        return {"ok": False, "rows": [], "reason_vi": "Thiếu mã"}

    try:
        from scrapers.financial_data import peer_symbols_same_cluster, snapshot_for_peer_compare
    except ImportError:
        return {"ok": False, "rows": [], "reason_vi": "Không import được financial_data"}

    peers = peer_symbols_same_cluster(sym, limit=int(os.environ.get("II_PEER_LIMIT", "7")))
    pr = float(base_val.get("price") or 0)
    eps = float(base_val.get("eps_for_graham") or base_val.get("eps") or 0)
    ours_pe = (pr / eps) if pr > 0 and eps > 0 else None
    ours_bv = float(base_val.get("book_value_per_share") or 0)
    ours_pb = (pr / ours_bv) if pr > 0 and ours_bv > 0 else None

    rows: list[dict[str, Any]] = []
    for p in peers[1 : 1 + 6]:
        sn = snapshot_for_peer_compare(p)
        if not sn:
            continue
        px = float(sn.get("price") or 0)
        ep = float(sn.get("eps") or 0)
        pe = (px / ep) if px > 0 and ep > 0 else None
        bv = float(sn.get("book_value_per_share") or 0)
        pb = (px / bv) if px > 0 and bv > 0 else None
        rows.append({"symbol": p, "pe": round(pe, 2) if pe is not None else None, "pb": round(pb, 2) if pb is not None else None})

    pes = [r["pe"] for r in rows if r.get("pe") is not None and r["pe"] and r["pe"] > 0]
    pbs = [r["pb"] for r in rows if r.get("pb") is not None and r["pb"] and r["pb"] > 0]
    med_pe = float(median(pes)) if len(pes) >= 1 else None
    med_pb = float(median(pbs)) if len(pbs) >= 1 else None

    pe_note = ""
    if ours_pe is not None and med_pe is not None and med_pe > 0:
        rel = (ours_pe / med_pe - 1.0) * 100.0
        pe_note = f"P/E mã này ~{rel:+.0f}% so với trung vị peer cùng cluster (ước)."

    return {
        "ok": len(rows) >= 1,
        "peer_sample_size": len(rows),
        "symbol_pe": round(ours_pe, 2) if ours_pe is not None else None,
        "symbol_pb": round(ours_pb, 2) if ours_pb is not None else None,
        "peer_median_pe": round(med_pe, 2) if med_pe is not None else None,
        "peer_median_pb": round(med_pb, 2) if med_pb is not None else None,
        "pe_vs_peer_note_vi": pe_note or None,
        "rows": rows,
    }


def _audit_fingerprint(snapshot: dict[str, Any], base_val: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "symbol": str(snapshot.get("symbol") or ""),
        "eps": snapshot.get("eps"),
        "eps_ttm": snapshot.get("eps_ttm"),
        "eps_for_graham": base_val.get("eps_for_graham"),
        "price": base_val.get("price"),
        "growth_rate_pct": base_val.get("growth_rate_pct"),
        "bond_yield_pct_used": base_val.get("bond_yield_pct_used"),
        "composite_target_price": base_val.get("composite_target_price"),
        "source": snapshot.get("source"),
    }
    raw = json.dumps(payload, sort_keys=True, default=str)
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return {
        "input_hash_sha256": h,
        "input_preview_keys": list(payload.keys()),
    }


def _excellence_score(
    snapshot: dict[str, Any],
    base_val: dict[str, Any],
    peer: dict[str, Any],
    band: dict[str, Any],
) -> tuple[float, list[str]]:
    """Thang 0–9.5: minh bạch tiêu chí, không bao hàm 'hoàn hảo tuyệt đối'."""
    score = 6.0
    reasons: list[str] = []

    snote = str(base_val.get("sector_pe_5y_note") or "")
    if "cung cấp" in snote and "sector_pe_5y_avg" in snote:
        score += 0.45
        reasons.append("Có P/E ngành (sector_pe_5y_avg) trong snapshot")

    if snapshot.get("eps_ttm"):
        score += 0.25
        reasons.append("Có eps_ttm")

    if str(base_val.get("eps_basis_key") or "") == "ttm":
        score += 0.15
        reasons.append("EPS cho Graham từ TTM")

    br = snapshot.get("bctc_readiness") if isinstance(snapshot.get("bctc_readiness"), dict) else {}
    tier = str(br.get("tier") or "")
    if tier in ("A", "B"):
        score += 0.45
        reasons.append(f"BCTC readiness tier {tier}")

    fr = snapshot.get("financial_report_fetch") if isinstance(snapshot.get("financial_report_fetch"), dict) else {}
    if fr.get("ok"):
        score += 0.3
        reasons.append("Chuỗi BCTC API tải được")

    if peer.get("ok") and int(peer.get("peer_sample_size") or 0) >= 2:
        score += 0.35
        reasons.append("So sánh peer ≥2 mã")

    spread = band.get("composite_spread_pct") if isinstance(band, dict) else None
    if spread is not None and float(spread) < 45.0:
        score += 0.15
        reasons.append("Dải kịch bản composite không quá rộng (ước)")

    if base_val.get("fair_pb_note") and "cung cấp" in str(base_val.get("fair_pb_note")):
        score += 0.2
        reasons.append("Có P/B tham chiếu từ dữ liệu")

    score = min(9.5, round(score, 2))
    return score, reasons

