#!/usr/bin/env python3
"""
Kiểm tra Streamlit khởi động được (mô phỏng bước “mở app” sau probe).

Chạy từ thư mục gốc dự án:
  python scripts/verify_streamlit_startup.py

Thoát 0 = HTTP 200 từ server local trong giới hạn thời gian; khác 0 = lỗi.
"""
from __future__ import annotations

import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TIMEOUT_SEC = 45


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = int(s.getsockname()[1])
    s.close()
    return port


def main() -> int:
    port = _free_port()
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(ROOT / "app.py"),
        "--server.headless",
        "true",
        f"--server.port={port}",
        "--browser.gatherUsageStats",
        "false",
    ]
    print(f"Starting Streamlit on 127.0.0.1:{port} ...")
    proc = subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + TIMEOUT_SEC
    try:
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                print(f"ERROR: Streamlit exited early (code {proc.returncode}).")
                return 2
            try:
                req = urllib.request.Request(f"http://127.0.0.1:{port}/")
                with urllib.request.urlopen(req, timeout=2) as r:
                    if r.status == 200:
                        print("OK: HTTP 200, app is up.")
                        return 0
            except (urllib.error.URLError, TimeoutError, OSError):
                time.sleep(0.5)
        print(f"ERROR: no HTTP 200 within {TIMEOUT_SEC}s.")
        return 1
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    raise SystemExit(main())
