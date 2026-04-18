"""
Chạy liên tục: làm mới cache snapshot mỗi N giây (mặc định 1800 = 30 phút).

  python scripts/snapshot_disk_refresh_loop.py --all --max 400
  set II_REFRESH_INTERVAL_SEC=1800

Dùng trên server 24/7 hoặc systemd timer / Windows Task Scheduler.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    interval = float(os.environ.get("II_REFRESH_INTERVAL_SEC", "1800"))
    extra = sys.argv[1:]
    if not extra:
        extra = ["--all", "--max", os.environ.get("II_REFRESH_MAX_SYMBOLS", "500")]
    script = ROOT / "scripts" / "snapshot_disk_refresh.py"
    while True:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Running refresh...", flush=True)
        r = subprocess.run([sys.executable, str(script), *extra], cwd=str(ROOT))
        print(f"Exit code {r.returncode}. Next run in {interval}s.", flush=True)
        time.sleep(max(60.0, interval))


if __name__ == "__main__":
    main()
