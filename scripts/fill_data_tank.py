"""
Đổ đầy bồn dữ liệu (data/snapshot_disk_cache.json) cho mọi mã niêm yết đang biết.

1) Làm mới danh sách mã từ API (nếu mạng tới được VNDirect) + metadata + extra.
2) Gọi fetch_financial_snapshot(..., bypass_cache=True) cho từng mã — ghi đĩa.

Chạy (PowerShell):
  cd "e:\\Du an 1"
  $env:PYTHONUNBUFFERED=1
  python scripts/fill_data_tank.py

Tuỳ chọn:
  --max 500     Giới hạn số mã (mặc định: không giới hạn)
  --skip-listing  Không gọi API listing (chỉ dùng metadata đã có)
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def main() -> int:
    p = argparse.ArgumentParser(description="Fill snapshot disk cache (data tank).")
    p.add_argument("--max", type=int, default=0, help="Cap symbols (0 = all).")
    p.add_argument("--skip-listing", action="store_true", help="Do not refresh VNDirect listing first.")
    args = p.parse_args()

    from scrapers.financial_data import fetch_financial_snapshot
    from scrapers.vn_listing import list_tradable_vn_symbols, load_or_refresh_listing_cache

    if not args.skip_listing:
        print("Refreshing tradable listing (API + disk cache)...", flush=True)
        codes, note = load_or_refresh_listing_cache(max_age_hours=0.0)
        print(f"  listing: {len(codes)} codes — {note}", flush=True)

    syms = list_tradable_vn_symbols()
    if args.max and args.max > 0:
        syms = syms[: args.max]
    if not syms:
        print("No symbols — kiểm tra data/stock_metadata.json và mạng tới finfo-api.vndirect.com.vn", flush=True)
        return 1

    delay = max(0.0, float(os.environ.get("II_SNAPSHOT_REFRESH_SLEEP", "0.35")))
    print(f"Filling tank for {len(syms)} symbols (delay {delay}s between calls)...", flush=True)

    ok = fail = 0
    t0 = time.time()
    for i, s in enumerate(syms, start=1):
        snap = fetch_financial_snapshot(s, bypass_cache=True)
        if snap:
            ok += 1
        else:
            fail += 1
        if i <= 5 or i % 25 == 0 or i == len(syms):
            print(f"  {i}/{len(syms)} ok={ok} fail={fail} last={s}", flush=True)
        if delay and i < len(syms):
            time.sleep(delay)

    elapsed = time.time() - t0
    print(f"Done in {elapsed:.1f}s - success={ok} fail={fail}", flush=True)
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
