"""
Cấu hình tập trung (biến môi trường). Dùng cho định giá và hiển thị mặc định.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True)
class AppSettings:
    """Cài đặt đọc một lần từ môi trường."""

    default_bond_yield_pct: float
    merge_live_fundamentals: bool
    skip_mock_snapshot: bool


def _parse_yield(raw: str | None) -> float:
    if not raw or not str(raw).strip():
        return 4.4
    try:
        y = float(str(raw).strip().replace(",", "."))
    except ValueError:
        return 4.4
    return max(0.5, min(y, 25.0))


def _env_flag(name: str, default: bool = False) -> bool:
    v = os.environ.get(name, "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "on")


@lru_cache
def get_settings() -> AppSettings:
    """
    II_DEFAULT_BOND_YIELD_PCT hoặc BOND_YIELD_PCT: lợi suất trái phiếu chuẩn (%/năm) cho công thức Graham.
    Mặc định 4.4 (tương thích Graham classic factor 4.4/Y).

    II_MERGE_LIVE_FUNDAMENTALS: 1 (mặc định) — với mã có trong mock_financials.json, vẫn thử gộp EPS/BVPS…
    từ scrape live để định giá sát thực tế hơn.

    II_SKIP_MOCK: 1 — bỏ qua hoàn toàn mock; chỉ dùng scrape/API (thất bại nếu không có mạng).

    II_FETCH_FINANCIAL_STATEMENTS: 1 — gọi API BCTC nhiều kỳ (VNDirect Finfo) và gộp vào snapshot.
    Trong test mặc định tắt (conftest). II_FSTATEMENT_TIMEOUT: giây chờ (mặc định 20).

    II_SKIP_PEER_FETCH: 1 — không gọi snapshot peer (test nhanh). Production: unset hoặc 0.

    Kịch bản Bear/Bull (elite): II_SCENARIO_BEAR_G_DELTA, II_SCENARIO_BULL_G_DELTA (mặc định -3 / +4),
    II_SCENARIO_BEAR_Y_DELTA, II_SCENARIO_BULL_Y_DELTA (± lợi suất %). DCF: II_DCF_CAPEX_RATIO,
    II_DCF_EQUITY_RISK_PREMIUM, II_PEER_LIMIT.

    Cache snapshot đĩa: II_SNAPSHOT_DISK_CACHE (1=bật), II_SNAPSHOT_CACHE_TTL_SEC (mặc định 1800).
    II_READ_STALE_DISK=1 — đọc snapshot trên đĩa kể cả khi quá TTL (bồn “cạn” vẫn dùng được) trước mock/scrape.
    Listing VN: II_VN_LISTING_API (1=gọi Finfo lấy danh sách mã). Extra: data/vn_universe_extra.txt (mỗi dòng một mã).
    Refresh nền: scripts/snapshot_disk_refresh.py, snapshot_disk_refresh_loop.py, start_data_tank.bat (II_REFRESH_INTERVAL_SEC).

    UI tự làm mới khi đĩa đổi: II_UI_DISK_POLL_SEC (giây, mặc định 45) — fragment soi cached_at, không cần F5.
    """
    raw = os.environ.get("II_DEFAULT_BOND_YIELD_PCT") or os.environ.get("BOND_YIELD_PCT")
    return AppSettings(
        default_bond_yield_pct=_parse_yield(raw),
        merge_live_fundamentals=_env_flag("II_MERGE_LIVE_FUNDAMENTALS", True),
        skip_mock_snapshot=_env_flag("II_SKIP_MOCK", False),
    )


@lru_cache
def http_timeout_seconds() -> float:
    """
    Timeout HTTP mặc định (requests). II_HTTP_TIMEOUT hoặc VALUE_INVESTOR_PORTAL_TIMEOUT.
    Giới hạn 3–60 giây.
    """
    raw = os.environ.get("II_HTTP_TIMEOUT") or os.environ.get("VALUE_INVESTOR_PORTAL_TIMEOUT", "12")
    try:
        t = float(str(raw).strip().replace(",", "."))
    except ValueError:
        t = 12.0
    return max(3.0, min(60.0, t))
