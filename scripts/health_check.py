#!/usr/bin/env python3
"""
Kiểm tra sức khỏe bản build: pytest + probe tích hợp.

Cách “triệt để” về mặt vận hành không phải là hết lỗi API ngoài,
mà là: (1) luôn biết build có xanh không, (2) tách probe offline vs có mạng,
(3) log/monitor production riêng (Sentry, v.v.).

Usage:
  python scripts/health_check.py              # pytest + probe OFFLINE (CI an toàn)
  python scripts/health_check.py --full        # pytest + probe đầy đủ (cần mạng, trước release)
  python scripts/health_check.py --streamlit   # thêm: kiểm tra Streamlit khởi động (sau các bước trên)
  python scripts/health_check.py --skip-pytest # bỏ qua pytest (khi đã chạy trước đó)

Exit code 0 = pass, khác 0 = fail.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--full",
        action="store_true",
        help="Chạy probe có gọi Yahoo/OHLCV/alert (cần mạng ổn định).",
    )
    ap.add_argument(
        "--streamlit",
        action="store_true",
        help="Sau khi pass: chạy scripts/verify_streamlit_startup.py (HTTP 200 từ app).",
    )
    ap.add_argument(
        "--skip-pytest",
        action="store_true",
        help="Bỏ qua bước pytest nếu đã chạy từ pipeline bên ngoài.",
    )
    args = ap.parse_args()

    env = os.environ.copy()
    if not args.full:
        env["II_HEALTH_SKIP_NETWORK"] = "1"

    if not args.skip_pytest:
        r1 = subprocess.run(
            [sys.executable, "-m", "pytest", str(ROOT / "tests"), "-q"],
            cwd=str(ROOT),
            env=env,
        )
        if r1.returncode != 0:
            return r1.returncode

    r2 = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "integration_probe.py")],
        cwd=str(ROOT),
        env=env,
    )
    if r2.returncode != 0:
        return r2.returncode

    if args.streamlit:
        r3 = subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "verify_streamlit_startup.py")],
            cwd=str(ROOT),
            env=env,
        )
        return r3.returncode

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
