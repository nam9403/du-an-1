"""
Calibrate Hybrid Legend thresholds against historical prices.

Usage:
  python scripts/optimize_legend_valuation.py
  python scripts/optimize_legend_valuation.py --limit 12 --range 2y
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_disk_snapshots() -> dict[str, dict[str, Any]]:
    p = ROOT / "data" / "snapshot_disk_cache.json"
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    syms = payload.get("symbols") if isinstance(payload.get("symbols"), dict) else {}
    out: dict[str, dict[str, Any]] = {}
    for sym, block in syms.items():
        if not isinstance(block, dict):
            continue
        snap = block.get("snapshot")
        if isinstance(snap, dict):
            out[str(sym).strip().upper()] = dict(snap)
    return out


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=12)
    parser.add_argument("--range", type=str, default="2y")
    args = parser.parse_args()

    from core.valuation_optimizer import calibrate_legend_thresholds

    snapshots = _load_disk_snapshots()
    symbols = sorted(snapshots.keys())[: max(1, int(args.limit))]
    if not symbols:
        print("Không có snapshot trong cache đĩa để hiệu chỉnh.")
        return

    print(f"Hiệu chỉnh trên {len(symbols)} mã, yahoo_range={args.range} ...")
    out = calibrate_legend_thresholds(snapshots, symbols=symbols, range_preference=args.range)
    best = out.get("best") or {}
    print("\n=== Best config ===")
    print(json.dumps(best, ensure_ascii=False, indent=2))
    print("\n=== Top 5 ===")
    for i, row in enumerate((out.get("top10") or [])[:5], start=1):
        print(f"{i}. {json.dumps(row, ensure_ascii=False)}")

    target = ROOT / "data" / "legend_calibration_report.json"
    target.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nĐã lưu báo cáo: {target}")


if __name__ == "__main__":
    main()

