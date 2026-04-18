"""
Report industry_subtype coverage quality in stock_metadata.json.

Usage:
    python scripts/industry_subtype_coverage_report.py
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
META_PATH = ROOT / "data" / "stock_metadata.json"


def main() -> None:
    if not META_PATH.exists():
        raise SystemExit(f"Missing metadata file: {META_PATH}")

    with open(META_PATH, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise SystemExit("Invalid metadata format")

    total = 0
    other = 0
    by_cluster: dict[str, dict[str, int]] = {}
    other_symbols: list[str] = []

    for sym, block in sorted(data.items()):
        if not isinstance(block, dict):
            continue
        total += 1
        cluster = str(block.get("industry_cluster") or "unknown").strip().lower()
        subtype = str(block.get("industry_subtype") or "missing").strip().lower()
        by_cluster.setdefault(cluster, {})
        by_cluster[cluster][subtype] = by_cluster[cluster].get(subtype, 0) + 1
        if subtype in ("other", "missing", ""):
            other += 1
            other_symbols.append(sym)

    covered = total - other
    coverage_pct = (covered / total * 100.0) if total else 0.0

    print(f"Total symbols: {total}")
    print(f"Subtype covered (not other/missing): {covered}")
    print(f"Coverage: {coverage_pct:.1f}%")
    print("")
    print("Distribution by cluster:")
    for cluster in sorted(by_cluster.keys()):
        print(f"- {cluster}:")
        sub = by_cluster[cluster]
        for st in sorted(sub.keys()):
            print(f"    {st}: {sub[st]}")
    print("")
    print("Symbols still other/missing:")
    print(", ".join(other_symbols) if other_symbols else "(none)")


if __name__ == "__main__":
    main()
