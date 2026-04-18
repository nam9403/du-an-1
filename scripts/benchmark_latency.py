"""
Measure core latency (no Streamlit). Run: python scripts/benchmark_latency.py
Optional: set II_SNAPSHOT_ATTACH_LIVE=0 for disk/mock path without OHLCV (faster).
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

SYMS = ("VNM", "FPT", "HPG")


def _fmt(sec: float) -> str:
    if sec >= 1:
        return f"{sec:.2f}s"
    return f"{sec * 1000.0:.0f}ms"


def main() -> None:
    from scrapers.financial_data import fetch_financial_snapshot, snapshot_for_peer_compare
    from core.snapshot_disk_cache import disk_cache_enabled, get_disk_snapshot_any_age
    from core.valuation import value_investing_summary

    attach = os.environ.get("II_SNAPSHOT_ATTACH_LIVE", "1")
    print("=== Investment Intelligence latency benchmark ===\n")
    print(f"disk_cache_enabled: {disk_cache_enabled()}")
    print(f"II_SNAPSHOT_ATTACH_LIVE: {attach}")
    print(f"CWD: {_ROOT}\n")

    for sym in SYMS:
        t0 = time.perf_counter()
        raw = get_disk_snapshot_any_age(sym)
        dt = time.perf_counter() - t0
        hit = isinstance(raw, dict) and bool(raw)
        ok = bool(
            hit
            and (
                raw.get("price")
                or (raw.get("eps") is not None if isinstance(raw, dict) else False)
            )
        )
        print(f"get_disk_snapshot_any_age({sym}): {_fmt(dt)}  hit={hit} usable={ok}")

    sym = SYMS[0]
    t0 = time.perf_counter()
    snap_full = fetch_financial_snapshot(sym)
    dt = time.perf_counter() - t0
    print(f"\nfetch_financial_snapshot({sym}) [full]: {_fmt(dt)}")

    t0 = time.perf_counter()
    snapshot_for_peer_compare("MWG")
    dt = time.perf_counter() - t0
    print(f"snapshot_for_peer_compare(MWG): {_fmt(dt)}")

    if snap_full:
        t0 = time.perf_counter()
        value_investing_summary(snap_full, include_extensions=False)
        dt = time.perf_counter() - t0
        print(f"value_investing_summary (no elite): {_fmt(dt)}")

        t0 = time.perf_counter()
        value_investing_summary(snap_full, include_extensions=True)
        dt = time.perf_counter() - t0
        print(f"value_investing_summary (elite): {_fmt(dt)}")

    try:
        from scrapers.portal import fetch_ohlcv_history

        sessions = max(50, min(120, int(os.environ.get("II_OHLCV_BULK_SESSIONS", "68"))))
        t0 = time.perf_counter()
        fetch_ohlcv_history(sym, sessions=sessions)
        dt = time.perf_counter() - t0
        print(f"\nfetch_ohlcv_history({sym}, sessions={sessions}): {_fmt(dt)}")
    except Exception as e:
        print(f"\nfetch_ohlcv_history failed: {e!r}")

    try:
        from core.ai_logic import generate_strategic_report

        if snap_full:
            t0 = time.perf_counter()
            generate_strategic_report(
                sym,
                snap_full,
                profile="growth",
                total_capital_vnd=100_000_000.0,
                sessions=60,
                news_limit=5,
                enable_llm=False,
                fast_mode=True,
            )
            dt = time.perf_counter() - t0
            print(f"\ngenerate_strategic_report (quick, no LLM): {_fmt(dt)}")
    except Exception as e:
        print(f"\ngenerate_strategic_report failed: {e!r}")

    print("\n--- Notes ---")
    print("Disk read + peer row without live attach: usually < 5s.")
    print("If fetch_financial_snapshot is slow: portal/network; try II_SNAPSHOT_ATTACH_LIVE=0 with warm disk.")
    print("Full analysis + news may still exceed 5s depending on network.")


if __name__ == "__main__":
    main()
