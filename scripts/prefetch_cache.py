"""
Prefetch cache for faster user experience.

Usage examples:
  python scripts/prefetch_cache.py --watchlist FPT,HPG,VNM
  python scripts/prefetch_cache.py --universe-limit 30
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from core.ai_logic import generate_strategic_report
from scrapers.financial_data import fetch_financial_snapshot, list_universe_symbols
from scrapers.portal import fetch_financial_indicators


def _parse_symbols(raw: str) -> list[str]:
    if not raw.strip():
        return []
    out: list[str] = []
    for x in raw.replace(";", ",").split(","):
        sym = x.strip().upper()
        if sym:
            out.append(sym)
    return out


def _prefetch_one(sym: str) -> tuple[bool, float, str]:
    t0 = time.time()
    snap = fetch_financial_snapshot(sym)
    if snap is None:
        return False, time.time() - t0, "snapshot_none"
    try:
        fetch_financial_indicators(sym, fast_mode=False)
    except Exception:
        pass
    try:
        generate_strategic_report(
            sym,
            snap,
            profile="growth",
            total_capital_vnd=100_000_000.0,
            sessions=60,
            news_limit=5,
            enable_llm=False,
            fast_mode=True,
        )
    except Exception as e:
        return False, time.time() - t0, f"report_error:{type(e).__name__}"
    return True, time.time() - t0, "ok"


def main() -> int:
    parser = argparse.ArgumentParser(description="Warm up snapshot/report caches.")
    parser.add_argument("--watchlist", default="FPT,HPG,VNM", help="Comma-separated symbols.")
    parser.add_argument("--universe-limit", type=int, default=0, help="If >0, append first N universe symbols.")
    args = parser.parse_args()

    syms = _parse_symbols(args.watchlist)
    if args.universe_limit and args.universe_limit > 0:
        for s in list_universe_symbols(limit=args.universe_limit):
            if s not in syms:
                syms.append(s)

    if not syms:
        print("No symbols to prefetch.")
        return 1

    ok_n = 0
    total_t = 0.0
    print(f"Prefetching {len(syms)} symbols...")
    for i, s in enumerate(syms, start=1):
        ok, elapsed, status = _prefetch_one(s)
        total_t += elapsed
        if ok:
            ok_n += 1
        print(f"[{i:02d}/{len(syms):02d}] {s} -> {status} ({elapsed:.2f}s)")

    avg_t = total_t / max(len(syms), 1)
    print(f"Done. success={ok_n}/{len(syms)} avg_time={avg_t:.2f}s")
    return 0 if ok_n > 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
