"""
Run one cycle of background jobs:
- prefetch cache for watchlist + universe
- process notification retry queue
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from core.background_jobs import run_background_cycle


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--watchlist", default="FPT,HPG,VNM")
    parser.add_argument("--universe-limit", type=int, default=30)
    parser.add_argument("--queue-jobs", type=int, default=30)
    args = parser.parse_args()

    watch = [x.strip().upper() for x in args.watchlist.replace(";", ",").split(",") if x.strip()]
    out = run_background_cycle(
        universe_limit=max(0, int(args.universe_limit)),
        watchlist=watch,
        queue_jobs=max(1, int(args.queue_jobs)),
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
