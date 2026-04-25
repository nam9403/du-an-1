"""
Lấy snapshot tài chính: ưu tiên JSON mẫu; có thể mở rộng requests tới nguồn mở.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd
import requests  # type: ignore

from core.snapshot_disk_cache import get_cached_snapshot, get_disk_snapshot_any_age, put_snapshot
from core.valuation import value_investing_summary

# Đường dẫn tới mock JSON (cùng cây dự án)
_ROOT = Path(__file__).resolve().parent.parent
_MOCK_PATH = _ROOT / "data" / "mock_financials.json"
_META_PATH = _ROOT / "data" / "stock_metadata.json"


def _prefer_live_snapshot() -> bool:
    """
    Policy:
    - prod/preprod/staging: ưu tiên nguồn live trước mock.
    - dev/local: giữ hành vi ưu tiên mock để chạy nhanh/offline.
    Có thể override bằng II_SNAPSHOT_PREFER_LIVE=1/0.
    """
    force = str(os.environ.get("II_SNAPSHOT_PREFER_LIVE", "") or "").strip().lower()
    if force in ("1", "true", "yes", "on"):
        return True
    if force in ("0", "false", "no", "off"):
        return False
    env_name = str(os.environ.get("II_ENV", "dev") or "").strip().lower()
    return env_name in ("prod", "production", "preprod", "staging", "stage")


def _snapshot_attach_live_enabled() -> bool:
    """
    Mặc định tắt live attach để ưu tiên tốc độ mở app.
    Bật lại bằng II_SNAPSHOT_ATTACH_LIVE=1 khi cần ép lấy giá live ngay.
    """
    v = os.environ.get("II_SNAPSHOT_ATTACH_LIVE", "0").strip().lower()
    return v in ("1", "true", "yes", "on")


def _align_price_with_ohlcv_enabled() -> bool:
    """
    Khi bật (II_ALIGN_PRICE_WITH_OHLCV=1): đồng bộ giá định giá với giá đóng cửa OHLCV (cùng nguồn biểu đồ).
    Mặc định tắt để tránh gọi thêm mạng; script run_app.* bật giúp người dùng khi cần khớp chart.
    Luôn có sửa chọn phiên mới nhất từ VNDirect quote (finance_scraper) khi không bật align.
    """
    v = os.environ.get("II_ALIGN_PRICE_WITH_OHLCV", "0").strip().lower()
    return v in ("1", "true", "yes", "on")


def _load_mock_json() -> dict[str, Any]:
    if not _MOCK_PATH.exists():
        return {}
    with open(_MOCK_PATH, encoding="utf-8") as f:
        raw = json.load(f)
    return raw if isinstance(raw, dict) else {}


def _load_stock_metadata() -> dict[str, Any]:
    if not _META_PATH.exists():
        return {}
    try:
        with open(_META_PATH, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _merge_stock_metadata(row: dict[str, Any], sym: str) -> None:
    """Bổ sung industry / industry_cluster từ stock_metadata.json nếu thiếu."""
    meta_all = _load_stock_metadata()
    block = meta_all.get(sym.upper())
    if not isinstance(block, dict):
        return
    if not row.get("industry") and not row.get("industry_name"):
        iv = block.get("industry_vi") or block.get("industry")
        if iv:
            row["industry"] = str(iv)
    if not row.get("industry_cluster") and block.get("industry_cluster"):
        row["industry_cluster"] = str(block["industry_cluster"]).strip().lower()
    if not row.get("industry_subtype") and block.get("industry_subtype"):
        row["industry_subtype"] = str(block["industry_subtype"]).strip().lower()


def _resolve_snapshot_market_price(row: dict[str, Any], sym: str) -> None:
    """
    Chuẩn hóa giá cho MOS / P/E / P/B:
    - Ưu tiên giá đóng cửa phiên gần nhất trên chuỗi OHLCV (cùng nguồn biểu đồ).
    - Nếu II_SNAPSHOT_ATTACH_LIVE=1 và OHLCV không lấy được: fallback quote tươi (bỏ cache).
    """
    align = _align_price_with_ohlcv_enabled()
    attach = _snapshot_attach_live_enabled()
    if not (align or attach):
        return

    quote_px = float(row.get("price") or 0)

    try:
        from scrapers.portal import fetch_ohlcv_history

        ohlcv = fetch_ohlcv_history(sym, sessions=80)
        if not ohlcv.empty:
            close = float(ohlcv.iloc[-1].get("close") or 0)
            if close > 0:
                row["price"] = close
                src = str(ohlcv.attrs.get("source", "live"))
                row["price_source"] = f"ohlcv_session_close:{src}"
                last_d = ohlcv.iloc[-1].get("date")
                if last_d is not None:
                    ts = pd.Timestamp(last_d)
                    if pd.notna(ts):
                        row["price_as_of"] = str(ts.date())
                if quote_px > 0 and abs(close - quote_px) / quote_px > 0.08:
                    row["price_crosscheck_note_vi"] = (
                        f"Giá quote ({quote_px:,.0f}) lệch >8% so với đóng cửa OHLCV ({close:,.0f}); "
                        "đã dùng OHLCV cho định giá."
                    )
                return
    except Exception:
        pass

    if not attach:
        return

    try:
        from scrapers.finance_scraper import ScraperError, get_stock_data

        q = get_stock_data(sym, use_cache=False)
        px = float(q.get("price") or 0)
        if px > 0:
            row["price"] = px
            row["price_source"] = f"quote_fresh:{q.get('source', 'unknown')}"
    except (ValueError, ScraperError):
        pass


def fetch_financial_snapshot(symbol: str, *, bypass_cache: bool = False) -> dict[str, Any] | None:
    """
    Trả về dict chứa giá, EPS, tăng trưởng, book value, block piotroski, v.v.
    symbol: mã không phân biệt hoa thường (VNM, fpt).
    """
    sym = symbol.strip().upper()
    stale_disk: dict[str, Any] | None = None
    prefer_live = _prefer_live_snapshot()
    if not bypass_cache:
        hot = get_cached_snapshot(sym)
        if isinstance(hot, dict):
            hot_source = str(hot.get("source") or "").strip().lower()
            # In preprod/prod, don't keep serving mock-only hot cache forever.
            if not (prefer_live and "mock_json" in hot_source):
                return hot
        if os.environ.get("II_READ_STALE_DISK", "1").strip().lower() in ("1", "true", "yes", "on"):
            stale_disk = get_disk_snapshot_any_age(sym)
            if isinstance(stale_disk, dict):
                stale_source = str(stale_disk.get("source") or "").strip().lower()
                if prefer_live and "mock_json" in stale_source:
                    stale_disk = None

    data = _load_mock_json()
    mock_row: dict[str, Any] | None = None
    if sym in data:
        mock_row = dict(data[sym])
        mock_row["source"] = "mock_json"

    if not prefer_live and mock_row is not None:
        _resolve_snapshot_market_price(mock_row, sym)
        _merge_stock_metadata(mock_row, sym)
        put_snapshot(sym, mock_row)
        return mock_row

    try:
        from scrapers.finance_scraper import ScraperError, get_stock_data

        scraped = get_stock_data(sym, use_cache=not prefer_live)
        if stale_disk is not None and stale_disk.get("_disk_cache", {}).get("stale"):
            ttl = stale_disk.get("_disk_cache", {}).get("ttl_sec")
            scraped["snapshot_fundamentals_note_vi"] = (
                f"TTL cache cơ bản đã cũ ({ttl}s); đã làm mới giá live và giữ chế độ thận trọng."
            )
            scraped.setdefault("symbol", sym)
        _resolve_snapshot_market_price(scraped, sym)
        _merge_stock_metadata(scraped, sym)
        put_snapshot(sym, scraped)
        return scraped
    except ScraperError:
        if mock_row is not None:
            _resolve_snapshot_market_price(mock_row, sym)
            _merge_stock_metadata(mock_row, sym)
            put_snapshot(sym, mock_row)
            return mock_row
        if stale_disk is not None:
            return stale_disk

    # Stub: gọi API công khai (ví dụ Stooq / FMP)
    env_url = os.environ.get("VALUE_INVESTOR_FINANCIAL_API_URL")
    if env_url:
        try:
            r = requests.get(f"{env_url.rstrip('/')}/{sym}", timeout=10)
            r.raise_for_status()
            payload = r.json()
            if not isinstance(payload, dict):
                payload = {}
            payload["source"] = "http_api"
            _merge_stock_metadata(payload, sym)
            put_snapshot(sym, payload)
            return payload
        except (requests.RequestException, ValueError):
            pass

    if mock_row is not None:
        _resolve_snapshot_market_price(mock_row, sym)
        _merge_stock_metadata(mock_row, sym)
        put_snapshot(sym, mock_row)
        return mock_row
    if stale_disk is not None:
        return stale_disk
    return None


def peer_symbols_same_cluster(symbol: str, limit: int = 8) -> list[str]:
    """
    Các mã trong `stock_metadata.json` cùng `industry_cluster` với `symbol`.
    Luôn đặt `symbol` đầu danh sách. Nếu không có cluster → chỉ `[symbol]`.
    """
    sym_u = symbol.strip().upper()
    meta_all = _load_stock_metadata()
    me = meta_all.get(sym_u)
    cluster = None
    if isinstance(me, dict):
        cluster = me.get("industry_cluster")
    if not cluster:
        return [sym_u]
    peers: list[str] = []
    for k, v in sorted(meta_all.items(), key=lambda x: str(x[0]).upper()):
        if not isinstance(v, dict) or v.get("industry_cluster") != cluster:
            continue
        peers.append(str(k).strip().upper())
    ordered = [sym_u] + [p for p in peers if p != sym_u]
    return ordered[:limit]


def build_peer_comparison_dataframe(symbols: list[str]) -> tuple[pd.DataFrame, list[str]]:
    """
    Tra cứu từng mã, tính định giá; trả (bảng, danh sách mã không tải được).
    """
    rows: list[dict[str, Any]] = []
    failed: list[str] = []
    for sym in symbols:
        sym_u = sym.strip().upper()
        snap = fetch_financial_snapshot(sym_u)
        if snap is None:
            failed.append(sym_u)
            continue
        s = value_investing_summary(snap)
        price = float(s.get("price") or 0)
        eps = float(s.get("eps") or 0)
        bv = float(s.get("book_value_per_share") or 0)
        pe = (price / eps) if eps > 0 else None
        pb = (price / bv) if bv > 0 else None
        mos_g = s.get("margin_of_safety_pct")
        mos_c = s.get("margin_of_safety_composite_pct")
        rows.append(
            {
                "Mã": s.get("symbol", sym_u),
                "Tên": str(s.get("name") or "")[:40],
                "Giá": round(price, 1),
                "P/E": round(pe, 2) if pe is not None else None,
                "P/B": round(pb, 2) if pb is not None else None,
                "Graham": round(float(s.get("intrinsic_value_graham") or 0), 1),
                "Mục tiêu TH": round(float(s.get("composite_target_price") or 0), 1),
                "MOS G%": round(float(mos_g), 2) if mos_g is not None else None,
                "MOS TH%": round(float(mos_c), 2) if mos_c is not None else None,
                "F-Score": int(s.get("piotroski_score") or 0),
                "Nguồn": str(s.get("data_source", "")),
            }
        )
    return pd.DataFrame(rows), failed


def financials_to_dataframe(records: list[dict[str, Any]]) -> pd.DataFrame:
    """Tiện ích: nhiều mã -> một bảng (dùng sau cho so sánh ngành)."""
    return pd.DataFrame(records)


def list_universe_symbols(limit: int | None = None) -> list[str]:
    """Danh sách mã từ stock_metadata.json (dùng cho quét cơ hội)."""
    meta_all = _load_stock_metadata()
    syms = [str(k).strip().upper() for k in meta_all.keys() if str(k).strip()]
    syms = sorted(set(syms))
    if limit is not None and limit > 0:
        return syms[:limit]
    return syms


def universe_subtype_map() -> dict[str, str]:
    """
    Map symbol -> industry_subtype from stock_metadata.json.
    Missing subtype defaults to 'other'.
    """
    meta_all = _load_stock_metadata()
    out: dict[str, str] = {}
    for k, v in meta_all.items():
        sym = str(k).strip().upper()
        if not sym or not isinstance(v, dict):
            continue
        out[sym] = str(v.get("industry_subtype") or "other").strip().lower()
    return out
