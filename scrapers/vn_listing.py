"""
Danh sách mã giao dịch (VN) — ưu tiên API công khai, dự phòng metadata dự án.
Kết quả được cache file để không gọi API mỗi lần chạy.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from scrapers.finance_scraper import VNDIRECT_HEADERS

_ROOT = Path(__file__).resolve().parent.parent
_LISTING_CACHE_PATH = _ROOT / "data" / "vn_listing_symbols.json"
_EXTRA_PATH = _ROOT / "data" / "vn_universe_extra.txt"
_META_PATH = _ROOT / "data" / "stock_metadata.json"


def _http_timeout() -> float:
    try:
        from core.config import http_timeout_seconds

        return http_timeout_seconds()
    except Exception:
        return 18.0


def _load_metadata_symbols() -> list[str]:
    if not _META_PATH.exists():
        return []
    try:
        with open(_META_PATH, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return []
        return sorted({str(k).strip().upper() for k in data if str(k).strip()})
    except (OSError, json.JSONDecodeError):
        return []


def _load_extra_txt() -> list[str]:
    if not _EXTRA_PATH.exists():
        return []
    out: list[str] = []
    try:
        text = _EXTRA_PATH.read_text(encoding="utf-8")
        for line in text.splitlines():
            sym = line.strip().upper().split("#")[0].strip()
            if sym and sym.isalnum():
                out.append(sym)
    except OSError:
        return []
    return out


def fetch_vndirect_stock_codes(*, size: int = 4000) -> tuple[list[str], str]:
    """
    Thử lấy danh sách mã từ Finfo v4/stocks. Trả (codes, detail).
    """
    url = f"https://finfo-api.vndirect.com.vn/v4/stocks?size={size}&sort=code:asc"
    try:
        r = requests.get(url, headers=VNDIRECT_HEADERS, timeout=_http_timeout())
        if r.status_code != 200:
            return [], f"HTTP{r.status_code}"
        js = r.json()
        rows = js.get("data") if isinstance(js, dict) else None
        if not isinstance(rows, list):
            return [], "no_data"
        codes: list[str] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            c = row.get("code") or row.get("ticker") or row.get("symbol")
            if c:
                s = str(c).strip().upper()
                if 2 <= len(s) <= 6 and s.replace("&", "").isalnum():
                    codes.append(s)
        return sorted(set(codes)), "ok"
    except requests.RequestException as e:
        return [], str(e)


def load_or_refresh_listing_cache(*, max_age_hours: float = 24.0) -> tuple[list[str], str]:
    """
    Đọc cache file nếu còn mới; nếu không thì gọi API và ghi cache.
    """
    now = datetime.now(timezone.utc)
    if _LISTING_CACHE_PATH.exists():
        try:
            with open(_LISTING_CACHE_PATH, encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                ts = raw.get("fetched_at")
                codes = raw.get("codes")
                if isinstance(codes, list) and ts:
                    try:
                        fetched = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                        age_h = (now - fetched).total_seconds() / 3600.0
                        if age_h < max_age_hours:
                            return [str(x).upper() for x in codes if x], "disk_fresh"
                    except ValueError:
                        pass
        except (OSError, json.JSONDecodeError):
            pass

    codes, detail = fetch_vndirect_stock_codes()
    if not codes:
        merged = sorted(set(_load_metadata_symbols()) | set(_load_extra_txt()))
        return merged, f"api_fail:{detail}"

    payload = {
        "fetched_at": now.isoformat(),
        "source": "vndirect_finfo_v4_stocks",
        "detail": detail,
        "codes": codes,
    }
    _LISTING_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(_LISTING_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError:
        pass
    return codes, detail


def list_tradable_vn_symbols(*, use_api: bool = True) -> list[str]:
    """
    Hợp nhất: API listing (nếu bật) + stock_metadata.json + vn_universe_extra.txt.
    """
    api_codes: list[str] = []
    note = ""
    if use_api and os.environ.get("II_VN_LISTING_API", "1").strip().lower() not in ("0", "false", "no"):
        api_codes, note = load_or_refresh_listing_cache()
    meta = _load_metadata_symbols()
    extra = _load_extra_txt()
    merged = sorted(set(api_codes) | set(meta) | set(extra))
    return merged


def listing_cache_info() -> dict[str, Any]:
    if not _LISTING_CACHE_PATH.exists():
        return {"path": str(_LISTING_CACHE_PATH), "exists": False}
    try:
        with open(_LISTING_CACHE_PATH, encoding="utf-8") as f:
            raw = json.load(f)
        return {
            "path": str(_LISTING_CACHE_PATH),
            "exists": True,
            "fetched_at": raw.get("fetched_at") if isinstance(raw, dict) else None,
            "count": len(raw.get("codes") or []) if isinstance(raw, dict) else 0,
        }
    except (OSError, json.JSONDecodeError):
        return {"path": str(_LISTING_CACHE_PATH), "exists": True, "error": True}
