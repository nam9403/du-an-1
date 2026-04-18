"""
Làm mới cache snapshot trên đĩa cho danh sách mã (dùng cron hoặc server 24/7).

  python scripts/snapshot_disk_refresh.py --all
  python scripts/snapshot_disk_refresh.py --symbols FPT,VNM
  python scripts/snapshot_disk_refresh.py --max 200

Biến môi trường: II_SNAPSHOT_REFRESH_SLEEP (giây giữa mỗi mã, mặc định 0.35).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scrapers.financial_data import fetch_financial_snapshot
from scrapers.vn_listing import list_tradable_vn_symbols


def _sleep_between() -> float:
    try:
        return max(0.0, float(os.environ.get("II_SNAPSHOT_REFRESH_SLEEP", "0.35")))
    except ValueError:
        return 0.35


def main() -> int:
    p = argparse.ArgumentParser(description="Refresh snapshot disk cache for symbols.")
    p.add_argument("--all", action="store_true", help="Use full tradable list (API + metadata + extra).")
    p.add_argument("--max", type=int, default=0, help="Cap number of symbols (0 = no cap).")
    p.add_argument("--symbols", default="", help="Comma-separated; overrides --all when non-empty.")
    args = p.parse_args()

    if args.symbols.strip():
        syms = [x.strip().upper() for x in args.symbols.replace(";", ",").split(",") if x.strip()]
    elif args.all:
        syms = list_tradable_vn_symbols()
    else:
        from scrapers.financial_data import list_universe_symbols

        syms = list_universe_symbols(limit=None)

    if args.max and args.max > 0:
        syms = syms[: args.max]

    if not syms:
        print("No symbols.")
        return 1

    delay = _sleep_between()
    ok = fail = 0
    t0 = time.time()
    print(f"Refreshing {len(syms)} symbols (bypass disk read, rewrite cache)...", flush=True)
    for i, s in enumerate(syms, start=1):
        snap = fetch_financial_snapshot(s, bypass_cache=True)
        if snap:
            ok += 1
        else:
            fail += 1
        if i % 50 == 0 or i == len(syms) or i <= 3:
            print(f"  ... {i}/{len(syms)} ok={ok} fail={fail} (last={s})", flush=True)
        if delay and i < len(syms):
            time.sleep(delay)
    elapsed = time.time() - t0
    print(f"Done in {elapsed:.1f}s - success={ok} fail={fail}", flush=True)
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
