"""Hằng số UI và nhãn — tách khỏi app.py để dễ bảo trì."""

from __future__ import annotations

from pathlib import Path

WATCHLIST_DEFAULT = ["VNM", "FPT", "HPG"]
PROFILE_OPTIONS = {
    "Defensive Investor (Phòng vệ)": "defensive",
    "Enterprising Investor (Năng động)": "enterprising",
    "Mạo hiểm/Lướt sóng": "aggressive_trading",
    # Legacy aliases giữ tương thích dữ liệu cũ/session cũ.
    "An toàn & Cổ tức (legacy)": "safe_dividend",
    "Tăng trưởng (legacy)": "growth",
}

SUBTYPE_LABEL_TO_ID = {
    "Ngân hàng": "bank",
    "Chứng khoán": "securities",
    "Bảo hiểm": "insurance",
    "BĐS dân dụng": "real_estate_residential",
    "BĐS khu công nghiệp": "real_estate_kcn",
    "Tiêu dùng thiết yếu": "consumer_staples",
    "Bán lẻ tiêu dùng": "consumer_retail",
    "Dầu khí": "oil_gas",
    "Thép & vật liệu": "steel_materials",
    "Dược & y tế": "pharma_healthcare",
    "Công nghệ dịch vụ": "technology_services",
    "Khác": "other",
}

ACTION_LABELS_VI = {
    "BUY": "MUA",
    "HOLD": "GIỮ",
    "WATCH": "THEO DÕI",
    "AVOID": "TRÁNH MUA",
    "SELL": "BÁN/GIẢM TỶ TRỌNG",
}

PHASE_LABELS_VI = {
    "accumulation": "TÍCH LŨY",
    "breakout": "BỨT PHÁ",
    "distribution": "PHÂN PHỐI",
    "neutral": "TRUNG TÍNH",
}

LEGAL_DISCLAIMER_VI = """
**Không phải tư vấn đầu tư.** Công cụ chỉ hỗ trợ phân tích và học tập. Mọi quyết định mua/bán là trách nhiệm của bạn.

**Dữ liệu:** Một phần chỉ số có thể lấy từ file mẫu, scraper hoặc API — luôn đối chiếu BCTC, công bố niêm yết và giá khớp lệnh thực tế.

**Mô hình:** Định giá (Graham, P/E, P/B), pha thị trường và văn bản AI là **giả định có giới hạn**, không đảm bảo lợi nhuận hay độ chính xác tuyệt đối.
"""


def get_app_version(default: str = "0.0.0-dev") -> str:
    root = Path(__file__).resolve().parent
    version_path = root / "VERSION"
    try:
        value = version_path.read_text(encoding="utf-8").strip()
    except OSError:
        return default
    return value or default


def action_vi(x: str | None) -> str:
    key = str(x or "").strip().upper()
    return ACTION_LABELS_VI.get(key, key or "N/A")


def phase_vi(x: str | None) -> str:
    key = str(x or "").strip().lower()
    return PHASE_LABELS_VI.get(key, key.upper() if key else "N/A")
