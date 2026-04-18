"""
Background jobs for production-like operation.
"""

from __future__ import annotations

from typing import Any

from core.ai_logic import generate_strategic_report
from core.product_layer import process_notification_queue
from scrapers.financial_data import fetch_financial_snapshot, list_universe_symbols
from scrapers.portal import fetch_financial_indicators


def prefetch_symbols(
    symbols: list[str],
    profile: str = "growth",
    capital: float = 100_000_000.0,
    *,
    warm_financial_live: bool = False,
) -> dict[str, Any]:
    ok = 0
    fail = 0
    for sym in symbols:
        s = (sym or "").strip().upper()
        if not s:
            continue
        snap = fetch_financial_snapshot(s)
        if snap is None:
            fail += 1
            continue
        try:
            # Default to cache-first in scheduler to avoid UI stalls.
            fetch_financial_indicators(s, fast_mode=not warm_financial_live)
        except Exception:
            pass
        try:
            generate_strategic_report(
                s,
                snap,
                profile=profile,
                total_capital_vnd=float(capital),
                sessions=60,
                news_limit=5,
                enable_llm=False,
                fast_mode=True,
            )
            ok += 1
        except Exception:
            fail += 1
    return {"ok": ok, "fail": fail, "total": ok + fail}


def run_background_cycle(
    *,
    universe_limit: int = 30,
    watchlist: list[str] | None = None,
    queue_jobs: int = 30,
    warm_financial_live: bool = False,
) -> dict[str, Any]:
    watch = [x.strip().upper() for x in (watchlist or ["FPT", "HPG", "VNM"]) if x.strip()]
    uni = list_universe_symbols(limit=max(0, int(universe_limit)))
    merged: list[str] = []
    for s in watch + uni:
        if s not in merged:
            merged.append(s)
    prefetch_stat = prefetch_symbols(
        merged,
        profile="growth",
        capital=100_000_000.0,
        warm_financial_live=warm_financial_live,
    )
    queue_stat = process_notification_queue(max_jobs=max(1, int(queue_jobs)))
    return {"prefetch": prefetch_stat, "queue": queue_stat}
