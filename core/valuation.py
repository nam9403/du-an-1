"""
Lõi định giá: Benjamin Graham, P/E Forward (EPS dự phóng × P/E ngành 5y),
P/B (BVPS × P/B tham chiếu — nhấn mạnh nhóm ngân hàng), tổng hợp trọng số,
Piotroski F-Score, Margin of Safety.
"""

from __future__ import annotations

import json
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
_STOCK_META_PATH = _ROOT / "data" / "stock_metadata.json"

# Mặc định khi snapshot không có P/E ngành 5 năm / P/B tham chiếu (nên thay bằng số liệu thật trong JSON/API)
_DEFAULT_SECTOR_PE_5Y_NON_BANK = 14.0
_DEFAULT_SECTOR_PE_5Y_BANK = 10.0
_DEFAULT_FAIR_PB_BANK = 1.25
_DEFAULT_FAIR_PB_NON_BANK = 2.25

_BANK_SYMBOLS = frozenset(
    {
        "VCB",
        "BID",
        "CTG",
        "TCB",
        "VPB",
        "MBB",
        "ACB",
        "STB",
        "HDB",
        "TPB",
        "VIB",
        "MSB",
        "SSB",
        "NAB",
        "OCB",
        "SHB",
        "EIB",
        "PGB",
        "BAB",
        "NVB",
        "ABB",
    }
)


def benjamin_graham_value(
    eps: float,
    growth_rate_pct: float,
    bond_yield_pct: float = 4.4,
    base_multiplier: float = 8.5,
) -> float:
    """
    Giá trị nội tại theo Graham (dạng điều chỉnh lợi suất):
    V = EPS × (base_multiplier + 2 × g) × (4.4 / Y)

    - g: tốc độ tăng trưởng dự kiến (%/năm), ví dụ 7 cho 7%.
    - Y: lợi suất trái phiếu chuẩn (AAA), mặc định 4.4% để hệ số = 1.
    """
    if eps <= 0:
        return 0.0
    g = max(growth_rate_pct, 0.0)
    y = bond_yield_pct if bond_yield_pct > 0 else 4.4
    factor = 4.4 / y
    return float(eps * (base_multiplier + 2.0 * g) * factor)


def margin_of_safety_pct(intrinsic_value: float, market_price: float) -> float | None:
    """
    Biên an toàn = (V - P) / V × 100. Trả None nếu V <= 0.
    """
    if intrinsic_value <= 0:
        return None
    return float((intrinsic_value - market_price) / intrinsic_value * 100.0)


def _safe_div(num: float, den: float) -> float:
    return num / den if den else 0.0


def piotroski_f_score(metrics: dict[str, Any]) -> tuple[int, list[str]]:
    """
    F-Score 0–9 từ một dict kỳ hiện tại & kỳ trước (số tuyệt đối, không cần /cổ).

    Khóa mong đợi (tên có thể map từ báo cáo):
    net_income, net_income_prior, operating_cash_flow, operating_cash_flow_prior,
    total_assets, total_assets_prior, long_term_debt, long_term_debt_prior,
    current_assets, current_liabilities, current_assets_prior, current_liabilities_prior,
    shares_outstanding, shares_outstanding_prior,
    revenue, revenue_prior, cogs, cogs_prior
    """
    reasons: list[str] = []

    ni = float(metrics.get("net_income") or 0)
    ni_p = float(metrics.get("net_income_prior") or 0)
    ocf = float(metrics.get("operating_cash_flow") or 0)
    ocf_p = float(metrics.get("operating_cash_flow_prior") or 0)
    ta = float(metrics.get("total_assets") or 0)
    ta_p = float(metrics.get("total_assets_prior") or 0)
    ltd = float(metrics.get("long_term_debt") or 0)
    ltd_p = float(metrics.get("long_term_debt_prior") or 0)
    ca = float(metrics.get("current_assets") or 0)
    cl = float(metrics.get("current_liabilities") or 0)
    ca_p = float(metrics.get("current_assets_prior") or 0)
    cl_p = float(metrics.get("current_liabilities_prior") or 0)
    sh = float(metrics.get("shares_outstanding") or 0)
    sh_p = float(metrics.get("shares_outstanding_prior") or 0)
    rev = float(metrics.get("revenue") or 0)
    rev_p = float(metrics.get("revenue_prior") or 0)
    cogs = float(metrics.get("cogs") or 0)
    cogs_p = float(metrics.get("cogs_prior") or 0)

    roa = _safe_div(ni, ta)
    roa_p = _safe_div(ni_p, ta_p)
    avg_assets = (ta + ta_p) / 2.0 if ta_p else ta
    avg_assets_p = ta_p
    ltd_ratio = _safe_div(ltd, avg_assets) if avg_assets else 0.0
    ltd_ratio_p = _safe_div(ltd_p, avg_assets_p) if avg_assets_p else 0.0
    cr = _safe_div(ca, cl)
    cr_p = _safe_div(ca_p, cl_p)
    gm = _safe_div(rev - cogs, rev) if rev else 0.0
    gm_p = _safe_div(rev_p - cogs_p, rev_p) if rev_p else 0.0
    # MVP: vòng quay tài sản so sánh năm n / n-1 (cùng cách định nghĩa mẫu số)
    ato = _safe_div(rev, avg_assets) if avg_assets else _safe_div(rev, ta)
    ato_p = _safe_div(rev_p, ta_p) if ta_p else 0.0

    score = 0

    if ni > 0:
        score += 1
        reasons.append("LNST dương (+1)")
    if ocf > 0:
        score += 1
        reasons.append("Dòng tiền HĐKD dương (+1)")
    if roa > roa_p:
        score += 1
        reasons.append("ROA cải thiện (+1)")
    if ocf > ni:
        score += 1
        reasons.append("OCF > LNST (chất lượng lợi nhuận) (+1)")
    if ltd_ratio < ltd_ratio_p:
        score += 1
        reasons.append("Tỷ lệ nợ dài hạn trên tài sản giảm (+1)")
    if cr > cr_p:
        score += 1
        reasons.append("Hệ số thanh toán hiện hành cải thiện (+1)")
    if sh <= sh_p:
        score += 1
        reasons.append("Không pha loãng (CP lưu hành không tăng) (+1)")
    if gm > gm_p:
        score += 1
        reasons.append("Biên lợi nhuận gộp cải thiện (+1)")
    if ato > ato_p:
        score += 1
        reasons.append("Vòng quay tài sản cải thiện (+1)")

    return score, reasons


def _bank_quality_score(metrics: dict[str, Any]) -> tuple[int, list[str]]:
    """
    Quality score for financial institutions (scaled to 0-9).
    Uses available proxy metrics when full bank-specific ratios are unavailable.
    """
    reasons: list[str] = []
    ni = float(metrics.get("net_income") or 0)
    ni_p = float(metrics.get("net_income_prior") or 0)
    ocf = float(metrics.get("operating_cash_flow") or 0)
    rev = float(metrics.get("revenue") or 0)
    rev_p = float(metrics.get("revenue_prior") or 0)
    sh = float(metrics.get("shares_outstanding") or 0)
    sh_p = float(metrics.get("shares_outstanding_prior") or 0)

    raw = 0
    if ni > 0:
        raw += 1
        reasons.append("LNST dương (+1)")
    if ni_p > 0 and ni > ni_p:
        raw += 1
        reasons.append("LNST tăng so với kỳ trước (+1)")
    if ocf > 0:
        raw += 1
        reasons.append("Dòng tiền HĐKD dương (+1)")
    if ni > 0 and ocf >= 0.8 * ni:
        raw += 1
        reasons.append("OCF bám sát LNST (chất lượng lợi nhuận) (+1)")
    if sh <= sh_p and sh_p > 0:
        raw += 1
        reasons.append("Không pha loãng cổ phần (+1)")
    if rev > rev_p and rev_p > 0:
        raw += 1
        reasons.append("Thu nhập/DT hoạt động tăng (+1)")

    # Scale 6-point raw score to 9-point convention for UI consistency.
    scaled = int(round(raw * 9.0 / 6.0))
    scaled = max(0, min(9, scaled))
    return scaled, reasons


def forward_pe_target_price(eps_forward: float, sector_pe_5y_avg: float) -> float:
    """
    Giá mục tiêu theo P/E Forward: EPS (dự phóng) × P/E trung bình ngành ~5 năm.
    """
    if eps_forward <= 0 or sector_pe_5y_avg <= 0:
        return 0.0
    return float(eps_forward * sector_pe_5y_avg)


def book_value_target_price(book_value_per_share: float, fair_pb_multiple: float) -> float:
    """
    Giá mục tiêu theo sổ sách: BVPS × hệ số P/B tham chiếu (thường dùng cho cổ phiếu ngân hàng).
    """
    if book_value_per_share <= 0 or fair_pb_multiple <= 0:
        return 0.0
    return float(book_value_per_share * fair_pb_multiple)


def get_industry_weights(industry_name: str) -> dict[str, float]:
    """
    Trọng số định giá theo ngành (P/E Forward, P/B, Graham).
    Khóa trả về: 'PE', 'PB', 'Graham' — tổng = 1.0.

    Phân nhóm (theo chuỗi mô tả ngành tiếng Việt/Anh từ metadata hoặc scraper):
    - Tài chính (NH, BH, CK): PE 0.2, PB 0.7, Graham 0.1
    - Bất động sản: 0.3 / 0.5 / 0.2
    - Công nghệ / Viễn thông: 0.75 / 0.1 / 0.15
    - Tiêu dùng / Bán lẻ / Vận tải: 0.6 / 0.1 / 0.3
    - Năng lượng / Dầu khí / Sản xuất: 0.5 / 0.3 / 0.2
    - Phòng thủ (Y tế, Điện nước): 0.4 / 0.1 / 0.5
    - Mặc định: 0.4 / 0.3 / 0.3
    """
    cluster, _ = _classify_industry_cluster(industry_name or "")
    pe, pb, gr = _INDUSTRY_WEIGHT_TABLE[cluster]
    return {"PE": pe, "PB": pb, "Graham": gr}


def weights_for_industry_cluster(cluster_id: str) -> dict[str, float]:
    """Trọng số theo mã nhóm cố định (dùng khi đã biết cluster từ metadata)."""
    cid = (cluster_id or "default").strip().lower()
    if cid not in _INDUSTRY_WEIGHT_TABLE:
        cid = "default"
    pe, pb, gr = _INDUSTRY_WEIGHT_TABLE[cid]
    return {"PE": pe, "PB": pb, "Graham": gr}


def _industry_cluster_label_vi(cluster: str) -> str:
    return _INDUSTRY_CLUSTER_LABELS_VI.get(cluster, _INDUSTRY_CLUSTER_LABELS_VI["default"])


# (PE, PB, Graham) — đúng thứ tự công thức: Final = PE_price*PE + PB_price*PB + Graham_price*Graham
_INDUSTRY_WEIGHT_TABLE: dict[str, tuple[float, float, float]] = {
    "financial": (0.2, 0.7, 0.1),
    "real_estate": (0.3, 0.5, 0.2),
    "tech_telecom": (0.75, 0.1, 0.15),
    "consumer_retail_transport": (0.6, 0.1, 0.3),
    "energy_manufacturing": (0.5, 0.3, 0.2),
    "defensive": (0.4, 0.1, 0.5),
    "default": (0.4, 0.3, 0.3),
}

_INDUSTRY_CLUSTER_LABELS_VI = {
    "financial": "Nhóm Tài chính (Ngân hàng, Bảo hiểm, Chứng khoán)",
    "real_estate": "Nhóm Bất động sản",
    "tech_telecom": "Nhóm Công nghệ / Viễn thông",
    "consumer_retail_transport": "Nhóm Tiêu dùng / Bán lẻ / Vận tải",
    "energy_manufacturing": "Nhóm Năng lượng / Dầu khí / Sản xuất",
    "defensive": "Nhóm phòng thủ (Y tế, Điện — nước)",
    "default": "Mặc định (đa ngành)",
}

_INDUSTRY_SUBTYPE_LABELS_VI = {
    "bank": "Ngân hàng",
    "securities": "Chứng khoán",
    "insurance": "Bảo hiểm",
    "real_estate_residential": "Bất động sản dân dụng",
    "real_estate_kcn": "Bất động sản khu công nghiệp",
    "consumer_staples": "Tiêu dùng thiết yếu",
    "consumer_retail": "Bán lẻ tiêu dùng",
    "oil_gas": "Dầu khí",
    "steel_materials": "Thép & vật liệu cơ bản",
    "pharma_healthcare": "Dược & y tế",
    "technology_services": "Công nghệ dịch vụ",
    "other": "Khác",
}

# (PE, PB, Graham) cho phân nhóm sâu; chỉ áp dụng khi nhận diện được subtype.
_INDUSTRY_SUBTYPE_WEIGHT_TABLE: dict[str, tuple[float, float, float]] = {
    "bank": (0.15, 0.75, 0.10),
    "securities": (0.45, 0.45, 0.10),
    "insurance": (0.30, 0.55, 0.15),
    "real_estate_residential": (0.35, 0.50, 0.15),
    "real_estate_kcn": (0.45, 0.35, 0.20),
    "consumer_staples": (0.50, 0.20, 0.30),
    "consumer_retail": (0.60, 0.15, 0.25),
    "oil_gas": (0.45, 0.35, 0.20),
    "steel_materials": (0.40, 0.40, 0.20),
    "pharma_healthcare": (0.40, 0.20, 0.40),
    "technology_services": (0.75, 0.10, 0.15),
}


def _classify_industry_cluster(industry_text: str) -> tuple[str, str]:
    """
    Trả (cluster_id, chuỗi gốc đã chuẩn hóa để hiển thị).
    Thứ tự ưu tiên: tài chính → BĐS → công nghệ → tiêu dùng → năng lượng/sản xuất → phòng thủ.
    """
    raw = (industry_text or "").strip()
    t = raw.lower()
    t = re.sub(r"\s+", " ", t)

    def hit(keys: tuple[str, ...]) -> bool:
        return any(k in t for k in keys)

    # Tài chính (ưu tiên cao nếu có từ khóa CK/BH/NH)
    if hit(
        (
            "ngân hàng",
            "bank",
            "bao hiem",
            "bảo hiểm",
            "insurance",
            "chứng khoán",
            "chung khoan",
            "securities",
            "broker",
            "tài chính",
            "tai chinh",
            "finance",
            "đầu tư",
            "dau tu",
        )
    ):
        return "financial", raw or "Tài chính"

    if hit(("bất động sản", "bat dong san", "real estate", "bđs", "khu công nghiệp", "kcn")):
        return "real_estate", raw or "Bất động sản"

    if hit(
        (
            "công nghệ",
            "cong nghe",
            "technology",
            "software",
            "phần mềm",
            "phan mem",
            "viễn thông",
            "vien thong",
            "telecom",
            "it ",
            "cntt",
            "máy tính",
            "may tinh",
        )
    ):
        return "tech_telecom", raw or "Công nghệ / Viễn thông"

    if hit(
        (
            "tiêu dùng",
            "tieu dung",
            "bán lẻ",
            "ban le",
            "retail",
            "vận tải",
            "van tai",
            "logistics",
            "hàng không",
            "hang khong",
            "du lịch",
            "du lich",
        )
    ):
        return "consumer_retail_transport", raw or "Tiêu dùng / Bán lẻ / Vận tải"

    if hit(
        (
            "y tế",
            "y te",
            "health",
            "dược",
            "duoc",
            "pharma",
            "điện nước",
            "dien nuoc",
            "utility",
            "utilities",
            "nước sạch",
            "nuoc sach",
            "điện lực",
            "dien luc",
        )
    ):
        return "defensive", raw or "Y tế / Tiện ích"

    if hit(
        (
            "năng lượng",
            "nang luong",
            "dầu khí",
            "dau khi",
            "oil",
            "gas",
            "san xuat",
            "sản xuất",
            "manufacturing",
            "hóa chất",
            "hoa chat",
            "thép",
            "thep",
            "khai khoáng",
            "khai khoang",
        )
    ):
        return "energy_manufacturing", raw or "Năng lượng / Sản xuất"

    return "default", raw or "Chưa phân loại ngành"


def _classify_industry_subtype(industry_text: str, cluster_id: str) -> str:
    t = (industry_text or "").strip().lower()
    if cluster_id == "financial":
        if any(k in t for k in ("ngân hàng", "ngan hang", "bank")):
            return "bank"
        if any(k in t for k in ("chứng khoán", "chung khoan", "securities", "broker", "môi giới", "moi gioi")):
            return "securities"
        if any(k in t for k in ("bảo hiểm", "bao hiem", "insurance", "phi nhân thọ", "nhân thọ", "tai nan")):
            return "insurance"
    if cluster_id == "real_estate":
        if any(
            k in t
            for k in (
                "khu công nghiệp",
                "khu cong nghiep",
                "kcn",
                "industrial park",
                "bất động sản công nghiệp",
                "bat dong san cong nghiep",
                "hạ tầng khu công nghiệp",
                "ha tang khu cong nghiep",
            )
        ):
            return "real_estate_kcn"
        return "real_estate_residential"
    if cluster_id == "consumer_retail_transport":
        if any(k in t for k in ("bán lẻ", "ban le", "retail", "thế giới di động", "the gioi di dong")):
            return "consumer_retail"
        return "consumer_staples"
    if cluster_id == "energy_manufacturing":
        if any(k in t for k in ("dầu khí", "dau khi", "oil", "gas")):
            return "oil_gas"
        if any(k in t for k in ("thép", "thep", "steel", "vật liệu", "vat lieu")):
            return "steel_materials"
        return "other"
    if cluster_id == "defensive":
        if any(k in t for k in ("dược", "duoc", "pharma", "y tế", "y te", "health")):
            return "pharma_healthcare"
        return "other"
    if cluster_id == "tech_telecom":
        if any(k in t for k in ("công nghệ", "cong nghe", "technology", "software", "it", "dịch vụ công nghệ")):
            return "technology_services"
        return "other"
    return "other"


@lru_cache(maxsize=1)
def _load_stock_metadata_file() -> dict[str, Any]:
    if not _STOCK_META_PATH.exists():
        return {}
    try:
        with open(_STOCK_META_PATH, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _metadata_for_symbol(symbol: str) -> dict[str, Any]:
    meta = _load_stock_metadata_file()
    entry = meta.get(symbol.strip().upper())
    return entry if isinstance(entry, dict) else {}


def resolve_industry_for_snapshot(snapshot: dict[str, Any]) -> tuple[str, str]:
    """
    Trả (industry_display_vi, cluster_id).
    Ưu tiên industry_cluster trong snapshot/metadata; sau đó chuỗi ngành; cuối cùng phân loại từ text.
    """
    sym = str(snapshot.get("symbol") or "").strip().upper()
    meta = _metadata_for_symbol(sym)

    cluster_override = snapshot.get("industry_cluster") or meta.get("industry_cluster")
    if isinstance(cluster_override, str):
        cid = cluster_override.strip().lower().replace("-", "_")
        if cid in _INDUSTRY_WEIGHT_TABLE:
            disp = str(
                snapshot.get("industry")
                or snapshot.get("industry_name")
                or meta.get("industry_vi")
                or meta.get("industry")
                or _industry_cluster_label_vi(cid)
            )
            return disp, cid

    text = (
        snapshot.get("industry")
        or snapshot.get("industry_name")
        or snapshot.get("sector_name")
        or snapshot.get("icb_industry")
        or meta.get("industry_vi")
        or meta.get("industry")
        or ""
    )
    text = str(text).strip()
    cluster, hint = _classify_industry_cluster(text)
    display = text if text else hint
    return display, cluster


def weighted_composite_target_price(
    graham_value: float,
    forward_pe_value: float,
    pb_value: float,
    weight_graham: float,
    weight_forward_pe: float,
    weight_pb: float,
) -> tuple[float, dict[str, Any]]:
    """
    Trung bình trọng số; chỉ tính các phương pháp có giá trị > 0, chuẩn hóa lại trọng số.
    Trả về (giá tổng hợp, meta: weights_effective, included).
    """
    candidates: list[tuple[str, float, float]] = []
    if graham_value > 0:
        candidates.append(("graham", graham_value, weight_graham))
    if forward_pe_value > 0:
        candidates.append(("forward_pe", forward_pe_value, weight_forward_pe))
    if pb_value > 0:
        candidates.append(("pb", pb_value, weight_pb))

    if not candidates:
        return 0.0, {"included": [], "weights_effective": {}}

    w_sum = sum(c[2] for c in candidates)
    if w_sum <= 0:
        return 0.0, {"included": [], "weights_effective": {}}

    total = sum(val * w / w_sum for _, val, w in candidates)
    eff_w = {name: w / w_sum for name, _, w in candidates}
    return float(total), {
        "included": [c[0] for c in candidates],
        "weights_effective": eff_w,
    }


def _is_bank_sector(snapshot: dict[str, Any]) -> bool:
    if snapshot.get("is_bank") is True:
        return True
    sec = str(snapshot.get("sector") or snapshot.get("sector_key") or "").lower()
    if "bank" in sec or "ngân hàng" in sec or sec in ("nh", "banks"):
        return True
    sym = str(snapshot.get("symbol") or "").strip().upper()
    return sym in _BANK_SYMBOLS


def _resolve_eps_forward(snapshot: dict[str, Any]) -> tuple[float | None, str]:
    raw = snapshot.get("eps_forward")
    if raw is not None:
        v = float(raw)
        if v > 0:
            return v, "eps_forward (dữ liệu cung cấp)"
    eps = float(snapshot.get("eps") or 0)
    g = float(snapshot.get("growth_rate_pct") or 0)
    if eps > 0:
        return eps * (1.0 + max(g, 0.0) / 100.0), "EPS × (1 + tăng trưởng%) — ước EPS năm tới"
    return None, ""


def _resolve_sector_pe_5y(snapshot: dict[str, Any]) -> tuple[float, str]:
    raw = snapshot.get("sector_pe_5y_avg")
    if raw is not None:
        v = float(raw)
        if v > 0:
            return v, "sector_pe_5y_avg (P/E TB ngành 5 năm — cung cấp)"
    if _is_bank_sector(snapshot):
        return _DEFAULT_SECTOR_PE_5Y_BANK, f"P/E ngành mặc định NH (~{_DEFAULT_SECTOR_PE_5Y_BANK})"
    return _DEFAULT_SECTOR_PE_5Y_NON_BANK, (
        f"P/E ngành mặc định phi-NH (~{_DEFAULT_SECTOR_PE_5Y_NON_BANK}) — nên cập nhật trong dữ liệu"
    )


def _resolve_fair_pb_multiple(snapshot: dict[str, Any]) -> tuple[float, str]:
    for key in ("sector_pb_5y_avg", "target_pb_multiple", "fair_pb_multiple"):
        raw = snapshot.get(key)
        if raw is not None:
            v = float(raw)
            if v > 0:
                return v, f"{key} (P/B tham chiếu — cung cấp)"
    if _is_bank_sector(snapshot):
        return _DEFAULT_FAIR_PB_BANK, f"P/B tham chiếu mặc định NH (~{_DEFAULT_FAIR_PB_BANK})"
    return _DEFAULT_FAIR_PB_NON_BANK, (
        f"P/B tham chiếu mặc định phi-NH (~{_DEFAULT_FAIR_PB_NON_BANK}) — BV ít ý nghĩa với nhiều DN tăng trưởng"
    )


def _composite_weights_from_industry(
    snapshot: dict[str, Any],
) -> tuple[float, float, float, dict[str, float], str, str, str]:
    """
    Trả (w_graham, w_pe, w_pb, weights_template dict PE/PB/Graham, industry_display, cluster_id).
    Cho phép ghi đè bằng weight_graham, weight_forward_pe, weight_pb nếu cả ba đều có.
    """
    industry_display, cluster_id = resolve_industry_for_snapshot(snapshot)
    sym = str(snapshot.get("symbol") or "").strip().upper()
    meta = _metadata_for_symbol(sym)
    subtype_override = (
        str(snapshot.get("industry_subtype") or meta.get("industry_subtype") or "")
        .strip()
        .lower()
    )
    if subtype_override in _INDUSTRY_SUBTYPE_LABELS_VI:
        subtype_id = subtype_override
    else:
        subtype_id = _classify_industry_subtype(industry_display, cluster_id)
    w_tpl = weights_for_industry_cluster(cluster_id)
    if subtype_id in _INDUSTRY_SUBTYPE_WEIGHT_TABLE:
        pe, pb, gr = _INDUSTRY_SUBTYPE_WEIGHT_TABLE[subtype_id]
        w_tpl = {"PE": pe, "PB": pb, "Graham": gr}

    g_raw = snapshot.get("weight_graham")
    pe_raw = snapshot.get("weight_forward_pe")
    pb_raw = snapshot.get("weight_pb")
    if g_raw is not None and pe_raw is not None and pb_raw is not None:
        return float(g_raw), float(pe_raw), float(pb_raw), w_tpl, industry_display, cluster_id, subtype_id

    return (
        w_tpl["Graham"],
        w_tpl["PE"],
        w_tpl["PB"],
        w_tpl,
        industry_display,
        cluster_id,
        subtype_id,
    )


def _pick_positive_float(snapshot: dict[str, Any], keys: tuple[str, ...]) -> float:
    for k in keys:
        raw = snapshot.get(k)
        if raw is None:
            continue
        try:
            v = float(raw)
        except (TypeError, ValueError):
            continue
        if v > 0:
            return v
    return 0.0


def _safe_ratio(num: float, den: float) -> float:
    return float(num / den) if den else 0.0


def _resolve_hybrid_inputs(snapshot: dict[str, Any]) -> tuple[float, float, float, list[str]]:
    """
    Resolve D/E, CR, ROE with fallbacks from piotroski block.
    Returns (de, cr, roe_pct, notes).
    """
    notes: list[str] = []
    pio = snapshot.get("piotroski") if isinstance(snapshot.get("piotroski"), dict) else {}

    de = _pick_positive_float(snapshot, ("debt_to_equity", "d_e", "de"))
    if de <= 0 and pio:
        ltd = float(pio.get("long_term_debt") or 0)
        ta = float(pio.get("total_assets") or 0)
        equity_proxy = max(ta - ltd, 0.0)
        de = _safe_ratio(ltd, equity_proxy)
        if de > 0:
            notes.append("Suy luận D/E từ piotroski (nợ dài hạn / vốn chủ ước tính).")

    cr = _pick_positive_float(snapshot, ("current_ratio", "cr"))
    if cr <= 0 and pio:
        ca = float(pio.get("current_assets") or 0)
        cl = float(pio.get("current_liabilities") or 0)
        cr = _safe_ratio(ca, cl)
        if cr > 0:
            notes.append("Suy luận CR từ piotroski (tài sản NH / nợ NH).")

    roe = _pick_positive_float(snapshot, ("roe_5y_avg", "roe", "roe_avg_5y"))
    if roe <= 0 and pio:
        ni = float(pio.get("net_income") or 0)
        ta = float(pio.get("total_assets") or 0)
        ltd = float(pio.get("long_term_debt") or 0)
        equity_proxy = max(ta - ltd, 0.0)
        roe = _safe_ratio(ni, equity_proxy) * 100.0
        if roe > 0:
            notes.append("Suy luận ROE từ piotroski (LNST / vốn chủ ước tính).")

    return de, cr, roe, notes


def _legend_thresholds(snapshot: dict[str, Any]) -> tuple[str, dict[str, float]]:
    """
    Risk profile for decision thresholds.
    Profiles:
    - defensive: stricter buy requirements
    - balanced: default
    - aggressive: allows earlier accumulation
    """
    profile = str(snapshot.get("legend_profile") or os.environ.get("II_LEGEND_PROFILE", "balanced")).strip().lower()
    if profile not in ("defensive", "balanced", "aggressive"):
        profile = "balanced"
    table = {
        "defensive": {"strong_buy_mos_min": 25.0, "max_peg_for_buy": 1.20, "watch_buy_mos_min": 8.0},
        "balanced": {"strong_buy_mos_min": 18.0, "max_peg_for_buy": 1.35, "watch_buy_mos_min": 8.0},
        "aggressive": {"strong_buy_mos_min": 12.0, "max_peg_for_buy": 1.50, "watch_buy_mos_min": 5.0},
    }
    out = dict(table[profile])

    mos_ovr = snapshot.get("legend_strong_buy_mos_min")
    if mos_ovr is None:
        mos_ovr = os.environ.get("II_LEGEND_STRONG_BUY_MOS_MIN")
    peg_ovr = snapshot.get("legend_max_peg_for_buy")
    if peg_ovr is None:
        peg_ovr = os.environ.get("II_LEGEND_MAX_PEG_FOR_BUY")
    watch_ovr = snapshot.get("legend_watch_buy_mos_min")
    if watch_ovr is None:
        watch_ovr = os.environ.get("II_LEGEND_WATCH_BUY_MOS_MIN")
    try:
        if mos_ovr is not None:
            out["strong_buy_mos_min"] = max(0.0, min(60.0, float(mos_ovr)))
    except (TypeError, ValueError):
        pass
    try:
        if peg_ovr is not None:
            out["max_peg_for_buy"] = max(0.5, min(3.0, float(peg_ovr)))
    except (TypeError, ValueError):
        pass
    try:
        if watch_ovr is not None:
            out["watch_buy_mos_min"] = max(-20.0, min(40.0, float(watch_ovr)))
    except (TypeError, ValueError):
        pass
    return profile, out


def value_investing_summary(snapshot: dict[str, Any], include_extensions: bool = True) -> dict[str, Any]:
    """Định giá theo Hybrid Legend Model + giữ tương thích key cũ."""
    price = float(snapshot.get("price") or 0)
    eps_reported = float(snapshot.get("eps") or 0)
    eps_ttm = _pick_positive_float(snapshot, ("eps_ttm",))
    eps_for_graham = eps_ttm if eps_ttm > 0 else max(eps_reported, 0.0)
    eps_basis_key = "ttm" if eps_ttm > 0 else "reported"
    g = float(snapshot.get("growth_rate_pct") or 0)
    bond = float(snapshot.get("bond_yield_pct") or 4.4)
    bv = _pick_positive_float(snapshot, ("book_value_per_share", "bvps"))

    de, cr, roe, fallback_notes = _resolve_hybrid_inputs(snapshot)
    has_graham_inputs = de > 0 and cr > 0 and eps_for_graham > 0 and bv > 0

    # Step 1 - Graham survival + Graham Number
    graham_pass = bool(has_graham_inputs and de <= 0.5 and cr >= 1.5)
    graham_number = float((22.5 * eps_for_graham * bv) ** 0.5) if eps_for_graham > 0 and bv > 0 else 0.0

    # Step 2 - Buffett quality / moat proxy
    buffett_pass = bool(roe > 0 and de > 0 and roe >= 15 and de <= 0.5)

    # Step 3 - Lynch PEG/GARP
    pe = (price / eps_for_graham) if price > 0 and eps_for_graham > 0 else 0.0
    peg = (pe / g) if pe > 0 and g > 0 else 0.0
    if peg <= 0:
        peg_label = "Không xác định"
    elif peg < 1:
        peg_label = "Cực kỳ hấp dẫn"
    elif peg <= 1.5:
        peg_label = "Hợp lý"
    else:
        peg_label = "Quá đắt"
    lynch_pass = bool(peg > 0 and peg <= 1.5)

    # Step 4 - Hybrid intrinsic value
    eps_fwd, eps_fwd_note = _resolve_eps_forward(snapshot)
    sector_pe, sector_pe_note = _resolve_sector_pe_5y(snapshot)
    intrinsic = graham_number
    if g > 5 and roe > 15 and graham_number > 0 and peg > 0:
        # Growth adjustment: blend Graham anchor with forward P/E target.
        growth_target = forward_pe_target_price(eps_fwd, sector_pe) if eps_fwd else 0.0
        peg_factor = max(0.75, min(1.4, 1.2 / peg))
        growth_adj = growth_target * peg_factor if growth_target > 0 else 0.0
        if growth_adj > 0:
            # Quality growth businesses should not be anchored too heavily to Graham number.
            blend_growth = 0.55 if peg <= 1.5 else 0.4
            intrinsic = graham_number * (1.0 - blend_growth) + growth_adj * blend_growth
        else:
            intrinsic = graham_number * peg_factor
    elif g > 5 and roe > 15 and eps_for_graham > 0:
        intrinsic = benjamin_graham_value(eps_for_graham, g, bond_yield_pct=bond)
    intrinsic = float(intrinsic)

    mos = margin_of_safety_pct(graham_number, price)
    mos_composite = margin_of_safety_pct(intrinsic, price)
    safe_buy_price = intrinsic * 0.7 if intrinsic > 0 else 0.0

    profile, thresholds = _legend_thresholds(snapshot)
    if not has_graham_inputs:
        decision = "Theo dõi"
    elif not graham_pass:
        decision = "Bỏ qua"
    elif (
        buffett_pass
        and lynch_pass
        and (mos_composite is not None and mos_composite >= thresholds["strong_buy_mos_min"])
        and (peg > 0 and peg <= thresholds["max_peg_for_buy"])
    ):
        decision = "Mua mạnh"
    elif mos_composite is not None and mos_composite < -10 and not lynch_pass:
        decision = "Bỏ qua"
    else:
        decision = "Theo dõi"

    # Backward-compatible blocks
    fair_pb, fair_pb_note = _resolve_fair_pb_multiple(snapshot)
    tgt_pe = forward_pe_target_price(eps_fwd, sector_pe) if eps_fwd is not None and eps_fwd > 0 else 0.0
    tgt_pb = book_value_target_price(bv, fair_pb) if bv > 0 else 0.0
    w_g, w_pe, w_pb, w_tpl, industry_display, cluster_id, subtype_id = _composite_weights_from_industry(snapshot)
    comp_meta = {"included": ["hybrid_legend"], "weights_effective": {"hybrid_legend": 1.0}}

    cluster_label = _industry_cluster_label_vi(cluster_id)
    subtype_label = _INDUSTRY_SUBTYPE_LABELS_VI.get(subtype_id, _INDUSTRY_SUBTYPE_LABELS_VI["other"])
    valuation_transparency_line = (
        f"Hybrid Legend: Graham {'Đạt' if graham_pass else 'Trượt'}, Buffett {'Đạt' if buffett_pass else 'Trượt'}, "
        f"Lynch {'Đạt' if lynch_pass else 'Trượt'} · PEG={peg:.2f} ({peg_label}) · Ngành: {industry_display}."
    )

    raw_pio = snapshot.get("piotroski")
    pio_block = raw_pio if isinstance(raw_pio, dict) else {}
    if pio_block:
        f_score, f_reasons = _bank_quality_score(pio_block) if cluster_id == "financial" else piotroski_f_score(pio_block)
    else:
        f_score, f_reasons = (0, [])

    if not has_graham_inputs:
        value_trap_warning = "Thiếu dữ liệu đầu vào cho bộ lọc Graham/Buffett; cần xác minh D/E, CR, ROE trước khi giải ngân."
    elif not graham_pass:
        value_trap_warning = "Đòn bẩy cao hoặc thanh khoản yếu (D/E > 0.5 hoặc CR < 1.5) là rủi ro lớn nhất."
    elif roe < 15:
        value_trap_warning = "ROE chưa đủ mạnh, doanh nghiệp có thể rẻ nhưng thiếu chất lượng sinh lời bền vững."
    elif peg > 1.5:
        value_trap_warning = "Tăng trưởng không đủ bù định giá (PEG cao), dễ mua phải cổ phiếu đắt."
    else:
        value_trap_warning = "Không thấy bẫy giá trị lớn theo bộ lọc hiện tại."

    advice = (
        f"Kết luận: {decision}. "
        f"Giá mua an toàn <= {safe_buy_price:,.0f} {snapshot.get('currency', 'VND')}. "
        f"Pass/Fail: Graham={'Đạt' if graham_pass else 'Trượt'}, Buffett={'Đạt' if buffett_pass else 'Trượt'}, "
        f"Lynch={'Đạt' if lynch_pass else 'Trượt'}. "
        f"Cảnh báo: {value_trap_warning}"
    )

    result = {
        "symbol": snapshot.get("symbol", ""),
        "name": snapshot.get("name", ""),
        "piotroski_block": dict(pio_block),
        "price": price,
        "currency": snapshot.get("currency", "VND"),
        "eps": eps_reported,
        "eps_for_graham": eps_for_graham,
        "eps_basis_key": eps_basis_key,
        "growth_rate_pct_source": str(snapshot.get("growth_rate_pct_source") or "unspecified"),
        "growth_rate_pct_source_label_vi": str(snapshot.get("growth_rate_pct_source_label_vi") or "Không chỉ định"),
        "bond_yield_pct_used": bond,
        "growth_rate_pct": g,
        "book_value_per_share": bv,
        "debt_to_equity": de,
        "current_ratio": cr,
        "roe_5y_avg": roe,
        "intrinsic_value_graham": graham_number,
        "margin_of_safety_pct": mos,
        "target_price_forward_pe": tgt_pe,
        "target_price_pb_fair": tgt_pb,
        "composite_target_price": intrinsic,
        "margin_of_safety_composite_pct": mos_composite,
        "composite_weights_base": {"graham": w_g, "forward_pe": w_pe, "pb": w_pb},
        "industry_weights_template": dict(w_tpl),
        "industry_display_vi": industry_display,
        "industry_cluster_id": cluster_id,
        "industry_cluster_label_vi": cluster_label,
        "industry_subtype_id": subtype_id,
        "industry_subtype_label_vi": subtype_label,
        "valuation_transparency_line": valuation_transparency_line,
        "composite_meta": comp_meta,
        "eps_forward_used": eps_fwd,
        "eps_forward_note": eps_fwd_note,
        "sector_pe_5y_used": sector_pe,
        "sector_pe_5y_note": sector_pe_note,
        "fair_pb_multiple_used": fair_pb,
        "fair_pb_note": fair_pb_note,
        "is_bank_sector": _is_bank_sector(snapshot),
        "piotroski_score": f_score,
        "piotroski_detail": f_reasons,
        "advice": advice,
        "instant_conclusion": decision,
        "safe_buy_price": safe_buy_price,
        "legend_scorecard": {
            "graham_pass": graham_pass,
            "buffett_pass": buffett_pass,
            "lynch_pass": lynch_pass,
        },
        "legend_data_ready": {
            "has_graham_inputs": has_graham_inputs,
            "de_available": de > 0,
            "cr_available": cr > 0,
            "roe_available": roe > 0,
        },
        "legend_fallback_notes": fallback_notes,
        "legend_profile": profile,
        "legend_thresholds": dict(thresholds),
        "graham_number": graham_number,
        "pe_current": pe,
        "peg": peg if peg > 0 else None,
        "peg_label": peg_label,
        "value_trap_warning": value_trap_warning,
        "data_trust": snapshot.get("data_trust") if isinstance(snapshot.get("data_trust"), dict) else {},
        "data_source": snapshot.get("source", "unknown"),
    }

    if include_extensions:
        try:
            from core.elite_valuation import merge_elite_valuation

            return merge_elite_valuation(snapshot, result)
        except Exception:
            return result
    return result
