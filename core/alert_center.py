"""
Trung tâm cảnh báo: gắn nhãn 🔴 Xả hàng / 🟠 Tin xấu / 🟡 Thủng nền từ watchlist.
"""

from __future__ import annotations

import os
from typing import Any

import pandas as pd

from core.engine import compute_technical_indicators, detect_market_phase_from_ohlcv
from core.sentinel import _STRUCTURAL_KW, ohlcv_last_session_change_pct
from core.valuation import value_investing_summary
from scrapers.financial_data import fetch_financial_snapshot
from scrapers.portal import PortalDataError, fetch_latest_news, fetch_ohlcv_history

_MAX_SYMBOLS = max(5, min(24, int(os.environ.get("ALERT_CENTER_MAX_SYMBOLS", "16"))))

BADGE_DUMP = ("dump", "🔴 Xả hàng")
BADGE_NEWS = ("bad_news", "🟠 Tin xấu")
BADGE_SUPPORT = ("support_broken", "🟡 Thủng nền")


def _dedupe_badges(rows: list[tuple[str, str]]) -> list[tuple[str, str]]:
    seen: set[str] = set()
    out: list[tuple[str, str]] = []
    for bid, lab in rows:
        if bid in seen:
            continue
        seen.add(bid)
        out.append((bid, lab))
    return out


def scan_symbol_danger_flags(sym: str) -> dict[str, Any] | None:
    """
    Trả về dict {symbol, badges, detail} nếu có ít nhất một nhãn; ngược lại None.
    """
    sym_u = (sym or "").strip().upper()
    if not sym_u:
        return None

    badge_rows: list[tuple[str, str]] = []
    notes: list[str] = []

    # --- Kỹ thuật: pha + MA50 ---
    try:
        df = fetch_ohlcv_history(sym_u, sessions=65)
        phase = detect_market_phase_from_ohlcv(df)
        ti = compute_technical_indicators(df)
        last = ti.iloc[-1]
        close = float(last["close"])
        ma50 = float(last["ma50"]) if pd.notna(last.get("ma50")) else 0.0
        vm = float(phase.metrics.get("vol_multiple") or 0)
        day_chg = ohlcv_last_session_change_pct(df) or 0.0

        if phase.phase == "distribution" and vm >= 1.2:
            badge_rows.append(BADGE_DUMP)
            notes.append(f"Pha phân phối + thanh khoản cao (x{vm:.2f})")

        if ma50 > 0 and close < ma50 * 0.996 and day_chg <= -1.2:
            badge_rows.append(BADGE_SUPPORT)
            notes.append(f"Giá dưới MA50 và phiên yếu ({day_chg:+.2f}%)")

        if phase.phase == "distribution" and day_chg <= -3.0 and vm >= 1.0:
            if "dump" not in [x[0] for x in badge_rows]:
                badge_rows.append(BADGE_DUMP)
                notes.append("Giảm mạnh trong pha phân phối")
    except PortalDataError:
        pass
    except Exception:
        pass

    # --- Cơ bản: F-Score + MOS ---
    try:
        snap = fetch_financial_snapshot(sym_u)
        if snap:
            val = value_investing_summary(snap)
            fs = int(val.get("piotroski_score") or 0)
            mos = float(val.get("margin_of_safety_composite_pct") or -999)
            if fs <= 3 and mos < -20:
                badge_rows.append(BADGE_DUMP)
                notes.append(f"F-Score thấp ({fs}) và MOS âm sâu ({mos:.0f}%)")
    except Exception:
        pass

    # --- Tin tức ---
    try:
        news = fetch_latest_news(sym_u, limit=6)
        for n in news:
            t = (n.get("title") or "")[:800]
            if _STRUCTURAL_KW.search(t):
                badge_rows.append(BADGE_NEWS)
                notes.append("Có tin gợi ý rủi ro cấu trúc (heuristic)")
                break
    except PortalDataError:
        pass
    except Exception:
        pass

    badge_rows = _dedupe_badges(badge_rows)
    if not badge_rows:
        return None

    return {
        "symbol": sym_u,
        "badges": [{"id": b[0], "label": b[1]} for b in badge_rows],
        "detail": " · ".join(notes[:4])[:400],
    }


def scan_watchlist_danger_alerts(
    symbols: list[str],
    *,
    holding_symbols: list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Quét từng mã trong giới hạn; chỉ trả các mã có cảnh báo.
    `holding_symbols`: mã đang có trong danh mục — gắn `in_portfolio` và ưu tiên quét trước
    (thứ tự `symbols` nên là: danh mục trước, watchlist sau).
    """
    hold_set = {(s or "").strip().upper() for s in (holding_symbols or []) if s}
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in symbols:
        s = (raw or "").strip().upper()
        if not s or s in seen:
            continue
        seen.add(s)
        if len(out) >= _MAX_SYMBOLS:
            break
        row = scan_symbol_danger_flags(s)
        if row:
            row["in_portfolio"] = s in hold_set
            out.append(row)
    # Ưu tiên: nhiều nhãn hơn; sau đó mã đang giữ
    out.sort(
        key=lambda r: (len(r.get("badges") or []), r.get("in_portfolio", False)),
        reverse=True,
    )
    return out
