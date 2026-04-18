"""
Bảng giá watchlist: làm mới định kỳ (không dùng cache scrape dài) + fallback snapshot.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from scrapers.financial_data import fetch_financial_snapshot
from scrapers.finance_scraper import ScraperError, get_stock_data

_MAX = max(5, min(40, int(os.environ.get("WATCHLIST_LIVE_MAX_SYMBOLS", "25"))))


def _trim(symbols: list[str]) -> list[str]:
    out: list[str] = []
    for s in symbols:
        u = (s or "").strip().upper()
        if u and u not in out:
            out.append(u)
    return out[:_MAX]


def watchlist_signature(symbols: list[str]) -> str:
    """Khóa ổn định để reset session khi đổi watchlist."""
    return "|".join(_trim(symbols))


def normalize_watchlist_symbols(symbols: list[str]) -> list[str]:
    return _trim(symbols)


@st.cache_data(ttl=45, show_spinner=False)
def watchlist_live_prices_cached(symbols: tuple[str, ...]) -> tuple[pd.DataFrame, str]:
    """Giá live — cache ngắn để rerun Streamlit không gọi mạng lặp lại liên tục."""
    return build_watchlist_price_dataframe(list(symbols), live=True)


def build_watchlist_price_dataframe(symbols: list[str], *, live: bool) -> tuple[pd.DataFrame, str]:
    """
    live=True: ưu tiên quote mới (get_stock_data use_cache=False), fallback snapshot.
    live=False: chỉ snapshot/mock + giá portal (nhẹ hơn, phù hợp khi tắt auto-refresh).

    Gợi ý UX (stale-while-revalidate): gọi live=False để hiển thị ngay từ cache đĩa,
    sau đó (fragment / lượt sau) gọi live=True để đồng bộ giá sàn.
    """
    syms = _trim(symbols)
    rows: list[dict] = []
    for sym in syms:
        price: float | None = None
        src = "unknown"
        note = ""

        if live:
            try:
                d = get_stock_data(sym, use_cache=False)
                p = float(d.get("price") or 0)
                if p > 0:
                    price = p
                    src = str(d.get("source") or "")
                    note = "Live"
            except ScraperError as e:
                note = str(e)[:100]

        if price is None or price <= 0:
            snap = fetch_financial_snapshot(sym)
            if snap is None:
                rows.append(
                    {
                        "Mã": sym,
                        "Giá hiện tại": None,
                        "Nguồn giá": src,
                        "Trạng thái": "Chưa có dữ liệu" if not note else note,
                    }
                )
                continue
            price = float(snap.get("price") or 0)
            src = str(snap.get("price_source") or snap.get("source") or "snapshot")
            if price > 0:
                note = note or "Snapshot"
            elif not note:
                note = "Chưa có giá"

        if price is None or price <= 0:
            rows.append(
                {
                    "Mã": sym,
                    "Giá hiện tại": None,
                    "Nguồn giá": src,
                    "Trạng thái": note or "Chưa có giá",
                }
            )
        else:
            rows.append(
                {
                    "Mã": sym,
                    "Giá hiện tại": round(float(price), 2),
                    "Nguồn giá": src,
                    "Trạng thái": note or "Sẵn sàng",
                }
            )

    df = pd.DataFrame(rows)
    if not df.empty:
        ok = df["Giá hiện tại"].notna() & (df["Giá hiện tại"] > 0)
        df = df.assign(__ready=ok.astype(int)).sort_values(["__ready", "Mã"], ascending=[False, True]).drop(columns=["__ready"])
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return df, ts
