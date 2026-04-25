#!/usr/bin/env python3
"""
Build a mini timing dashboard (p50/p95) from JSONL metrics.

Usage:
  python scripts/build_timing_dashboard.py
  python scripts/build_timing_dashboard.py --in data/reports/logs/timing_metrics.jsonl --min-samples 3
"""
from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    vals = sorted(float(v) for v in values)
    if len(vals) == 1:
        return vals[0]
    idx = (len(vals) - 1) * q
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return vals[lo]
    frac = idx - lo
    return vals[lo] + (vals[hi] - vals[lo]) * frac


def load_events(path: Path) -> list[dict]:
    out: list[dict] = []
    if not path.exists():
        return out
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        raw = line.strip()
        if not raw:
            continue
        try:
            item = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(item, dict):
            continue
        metric = str(item.get("metric") or "").strip()
        duration_ms = item.get("duration_ms")
        if not metric:
            continue
        try:
            duration = float(duration_ms)
        except (TypeError, ValueError):
            continue
        item["duration_ms"] = duration
        item["metric"] = metric
        out.append(item)
    return out


def build_summary(events: list[dict], min_samples: int = 1) -> list[dict]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for e in events:
        grouped[str(e["metric"])].append(float(e["duration_ms"]))
    rows: list[dict] = []
    for metric, durations in grouped.items():
        if len(durations) < max(1, int(min_samples)):
            continue
        row = {
            "metric": metric,
            "samples": len(durations),
            "p50_ms": round(_percentile(durations, 0.50), 2),
            "p95_ms": round(_percentile(durations, 0.95), 2),
            "avg_ms": round(sum(durations) / len(durations), 2),
            "max_ms": round(max(durations), 2),
        }
        rows.append(row)
    rows.sort(key=lambda x: (float(x["p95_ms"]), float(x["avg_ms"])), reverse=True)
    return rows


def build_markdown(summary: list[dict], source: Path, total_events: int, min_samples: int) -> str:
    now = datetime.now(timezone.utc).isoformat()
    lines = [
        "# Timing Dashboard Mini",
        "",
        f"- Generated at (UTC): `{now}`",
        f"- Source: `{source.as_posix()}`",
        f"- Total parsed timing events: `{total_events}`",
        f"- Minimum samples per metric: `{min_samples}`",
        "",
    ]
    if not summary:
        lines.extend(
            [
                "No timing metrics found yet.",
                "",
                "Run app flows (scanner, queue, admin KPI) and generate again.",
            ]
        )
        return "\n".join(lines) + "\n"
    lines.extend(
        [
            "## p50/p95 by flow",
            "",
            "| Flow | Samples | p50 (ms) | p95 (ms) | Avg (ms) | Max (ms) |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in summary:
        lines.append(
            f"| `{row['metric']}` | {row['samples']} | {row['p50_ms']:.2f} | {row['p95_ms']:.2f} | {row['avg_ms']:.2f} | {row['max_ms']:.2f} |"
        )
    lines.extend(
        [
            "",
            "## Optimization targets",
            "",
            f"- Slowest p95 flow: `{summary[0]['metric']}` ({summary[0]['p95_ms']:.2f} ms)",
            "- Prioritize reducing p95 first, then avg.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default=str(ROOT / "data" / "reports" / "logs" / "timing_metrics.jsonl"))
    ap.add_argument("--out-json", default=str(ROOT / "artifacts" / "performance" / "timing_dashboard.json"))
    ap.add_argument("--out-md", default=str(ROOT / "artifacts" / "performance" / "timing_dashboard.md"))
    ap.add_argument("--min-samples", type=int, default=1)
    args = ap.parse_args()

    in_path = Path(args.inp)
    events = load_events(in_path)
    summary = build_summary(events, min_samples=max(1, int(args.min_samples)))

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": in_path.as_posix(),
        "total_events": len(events),
        "min_samples": int(args.min_samples),
        "summary": summary,
    }
    out_json = Path(args.out_json)
    out_md = Path(args.out_md)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    out_md.write_text(build_markdown(summary, in_path, len(events), int(args.min_samples)), encoding="utf-8")
    print(f"[OK] Wrote {out_json}")
    print(f"[OK] Wrote {out_md}")
    if summary:
        top = summary[0]
        print(f"[INFO] Slowest p95: {top['metric']} ({top['p95_ms']:.2f} ms)")
    else:
        print("[INFO] No timing events found yet.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
