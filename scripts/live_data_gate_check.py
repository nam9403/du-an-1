#!/usr/bin/env python3
"""
Evaluate live data ratio for snapshot sources.

Usage:
  python scripts/live_data_gate_check.py
  python scripts/live_data_gate_check.py --tickers FPT,VNM,HPG,VCB --min-live-ratio 0.25
  python scripts/live_data_gate_check.py --require-min-live
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scrapers.financial_data import fetch_financial_snapshot  # noqa: E402


def classify_source(source: str) -> str:
    src = str(source or "").strip().lower()
    if not src:
        return "unknown"
    if "mock" in src:
        return "mock"
    if "cache" in src:
        return "cache"
    return "live"


def evaluate_live_data(
    tickers: list[str],
    fetch_fn: Callable[[str], dict[str, Any] | None] = fetch_financial_snapshot,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    counts = {"live": 0, "cache": 0, "mock": 0, "unknown": 0, "none": 0}
    for sym in tickers:
        snap = fetch_fn(sym)
        if snap is None:
            counts["none"] += 1
            rows.append({"symbol": sym, "status": "none", "source": None, "price": None})
            continue
        src = str(snap.get("source") or "")
        cls = classify_source(src)
        counts[cls] = int(counts.get(cls, 0)) + 1
        rows.append(
            {
                "symbol": sym,
                "status": "ok",
                "source": src,
                "source_class": cls,
                "price": snap.get("price"),
            }
        )
    total = len(tickers)
    live_ratio = (counts["live"] / total) if total > 0 else 0.0
    available_ratio = ((total - counts["none"]) / total) if total > 0 else 0.0
    return {
        "tickers": tickers,
        "rows": rows,
        "counts": counts,
        "total": total,
        "live_ratio": round(live_ratio, 4),
        "available_ratio": round(available_ratio, 4),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", default="FPT,VNM,HPG,VCB", help="Comma-separated symbols to probe.")
    ap.add_argument("--min-live-ratio", type=float, default=0.25, help="Minimum live ratio for GO decision.")
    ap.add_argument("--min-available-ratio", type=float, default=1.0, help="Minimum non-None snapshot ratio.")
    ap.add_argument("--require-min-live", action="store_true", help="Return non-zero if live ratio below threshold.")
    args = ap.parse_args()

    tickers = [x.strip().upper() for x in str(args.tickers).split(",") if x.strip()]
    result = evaluate_live_data(tickers=tickers)
    min_live = max(0.0, min(1.0, float(args.min_live_ratio)))
    min_available = max(0.0, min(1.0, float(args.min_available_ratio)))

    reasons: list[str] = []
    decision = "GO"
    if float(result["available_ratio"]) < min_available:
        decision = "HOLD"
        reasons.append("Snapshot availability below threshold.")
    if float(result["live_ratio"]) < min_live:
        decision = "HOLD"
        reasons.append("Live source ratio below threshold.")

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "decision": decision,
        "thresholds": {"min_live_ratio": min_live, "min_available_ratio": min_available},
        "reasons": reasons,
        **result,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if bool(args.require_min_live) and decision != "GO":
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
