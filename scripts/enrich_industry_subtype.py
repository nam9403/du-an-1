"""
Enrich stock metadata with `industry_subtype` for all symbols.

Usage:
    python scripts/enrich_industry_subtype.py
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
META_PATH = ROOT / "data" / "stock_metadata.json"


def _norm(text: str) -> str:
    t = (text or "").strip().lower()
    t = re.sub(r"\s+", " ", t)
    return t


def infer_subtype(industry_text: str, cluster_id: str) -> str:
    t = _norm(industry_text)
    c = _norm(cluster_id).replace("-", "_")

    if c == "financial":
        if any(k in t for k in ("ngân hàng", "ngan hang", "bank")):
            return "bank"
        if any(k in t for k in ("chứng khoán", "chung khoan", "securities", "broker", "môi giới", "moi gioi")):
            return "securities"
        if any(k in t for k in ("bảo hiểm", "bao hiem", "insurance", "phi nhân thọ", "nhân thọ", "tai nan")):
            return "insurance"
        return "other"

    if c == "real_estate":
        if any(
            k in t
            for k in (
                "khu công nghiệp",
                "khu cong nghiep",
                "kcn",
                "industrial park",
                "bat dong san cong nghiep",
                "bất động sản công nghiệp",
                "hạ tầng khu công nghiệp",
                "ha tang khu cong nghiep",
            )
        ):
            return "real_estate_kcn"
        return "real_estate_residential"

    if c == "consumer_retail_transport":
        if any(k in t for k in ("bán lẻ", "ban le", "retail", "thế giới di động", "the gioi di dong")):
            return "consumer_retail"
        return "consumer_staples"

    if c == "energy_manufacturing":
        if any(k in t for k in ("dầu khí", "dau khi", "oil", "gas")):
            return "oil_gas"
        if any(k in t for k in ("thép", "thep", "steel", "vật liệu", "vat lieu")):
            return "steel_materials"
        return "other"

    if c == "defensive":
        if any(k in t for k in ("dược", "duoc", "pharma", "y tế", "y te", "health")):
            return "pharma_healthcare"
        return "other"

    if c == "tech_telecom":
        if any(k in t for k in ("công nghệ", "cong nghe", "technology", "software", "it", "dịch vụ công nghệ")):
            return "technology_services"
        return "other"

    return "other"


def main() -> None:
    if not META_PATH.exists():
        raise SystemExit(f"Missing metadata file: {META_PATH}")

    with open(META_PATH, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise SystemExit("Invalid metadata format: root must be object")

    updated = 0
    by_subtype: dict[str, int] = {}
    for sym, block in data.items():
        if not isinstance(block, dict):
            continue
        cluster = str(block.get("industry_cluster") or "").strip().lower()
        text = str(block.get("industry_vi") or block.get("industry") or "")
        inferred = infer_subtype(text, cluster)
        old = str(block.get("industry_subtype") or "").strip().lower()
        if old != inferred:
            block["industry_subtype"] = inferred
            updated += 1
        by_subtype[inferred] = by_subtype.get(inferred, 0) + 1

    with open(META_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"Updated symbols: {updated}")
    print("Subtype distribution:")
    for k in sorted(by_subtype.keys()):
        print(f"- {k}: {by_subtype[k]}")


if __name__ == "__main__":
    main()
