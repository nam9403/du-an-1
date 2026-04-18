"""
Batch calibration for Hybrid Legend thresholds.

Use smaller symbol batches to avoid long Yahoo stalls.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
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


def _chunked(items: list[str], batch_size: int) -> list[list[str]]:
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=22)
    parser.add_argument("--batch-size", type=int, default=6)
    parser.add_argument("--range", type=str, default="2y")
    args = parser.parse_args()

    from core.valuation_optimizer import calibrate_legend_thresholds

    snapshots = _load_disk_snapshots()
    symbols = sorted(snapshots.keys())[: max(1, int(args.limit))]
    if not symbols:
        print("Không có snapshot trong cache đĩa để hiệu chỉnh batch.")
        return

    batch_size = max(2, int(args.batch_size))
    batches = _chunked(symbols, batch_size)
    print(f"Batch calibration: symbols={len(symbols)}, batches={len(batches)}, range={args.range}")

    # Reduced search grid for batch throughput.
    grid_profiles = ("defensive", "balanced", "aggressive")
    grid_mos = (8.0, 10.0, 14.0, 18.0)
    grid_peg = (1.1, 1.25, 1.4)
    grid_watch = (0.0, 5.0, 8.0)

    reports: list[dict[str, Any]] = []
    for i, batch in enumerate(batches, start=1):
        print(f"- Batch {i}/{len(batches)}: {len(batch)} mã")
        rep = calibrate_legend_thresholds(
            snapshots,
            symbols=batch,
            range_preference=args.range,
            grid_profiles=grid_profiles,
            grid_mos=grid_mos,
            grid_peg=grid_peg,
            grid_watch_mos=grid_watch,
        )
        best = rep.get("best") or {}
        reports.append(
            {
                "batch_index": i,
                "symbols": batch,
                "best": best,
                "symbols_with_price_history": rep.get("symbols_with_price_history", 0),
            }
        )
        print(
            "  best:",
            json.dumps(
                {
                    "profile": best.get("profile"),
                    "strong_buy_mos_min": best.get("strong_buy_mos_min"),
                    "max_peg_for_buy": best.get("max_peg_for_buy"),
                    "watch_buy_mos_min": best.get("watch_buy_mos_min"),
                    "objective": best.get("objective"),
                },
                ensure_ascii=False,
            ),
        )

    # Consensus selection across batch winners.
    keys = []
    for r in reports:
        b = r.get("best") or {}
        keys.append(
            (
                str(b.get("profile") or "balanced"),
                float(b.get("strong_buy_mos_min") or 18.0),
                float(b.get("max_peg_for_buy") or 1.35),
                float(b.get("watch_buy_mos_min") or 8.0),
            )
        )
    freq = Counter(keys)
    winner, winner_count = freq.most_common(1)[0]
    winner_profile, winner_mos, winner_peg, winner_watch = winner

    avg_obj = 0.0
    n_obj = 0
    for r in reports:
        b = r.get("best") or {}
        try:
            avg_obj += float(b.get("objective") or 0.0)
            n_obj += 1
        except (TypeError, ValueError):
            pass
    avg_obj = avg_obj / n_obj if n_obj else 0.0

    out = {
        "summary": {
            "symbols_total": len(symbols),
            "batches": len(batches),
            "batch_size": batch_size,
            "range": args.range,
            "consensus_count": winner_count,
            "consensus_ratio": round(winner_count / max(1, len(reports)), 4),
            "mean_batch_objective": avg_obj,
        },
        "consensus": {
            "profile": winner_profile,
            "strong_buy_mos_min": winner_mos,
            "max_peg_for_buy": winner_peg,
            "watch_buy_mos_min": winner_watch,
        },
        "batch_reports": reports,
    }

    target = ROOT / "data" / "legend_batch_calibration_report.json"
    target.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n=== Consensus config ===")
    print(json.dumps(out["consensus"], ensure_ascii=False, indent=2))
    print("\n=== Summary ===")
    print(json.dumps(out["summary"], ensure_ascii=False, indent=2))
    print(f"\nĐã lưu báo cáo: {target}")


if __name__ == "__main__":
    main()

