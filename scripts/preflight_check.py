#!/usr/bin/env python3
"""
Preflight environment checks before running release pipeline.

Exit code:
  0 -> preflight passed
  1 -> hard failure (missing required baseline)
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _print(title: str, ok: bool, message: str) -> None:
    status = "OK" if ok else "WARN"
    print(f"[{status}] {title}: {message}")


def main() -> int:
    hard_fail = False

    py_ok = sys.version_info >= (3, 11)
    _print("Python", py_ok, f"{sys.version.split()[0]} (require >= 3.11)")
    if not py_ok:
        hard_fail = True

    version_path = ROOT / "VERSION"
    version_ok = version_path.exists() and bool(version_path.read_text(encoding="utf-8").strip())
    _print("VERSION", version_ok, "VERSION file exists and not empty")
    if not version_ok:
        hard_fail = True

    required_files = [
        ROOT / "README.md",
        ROOT / "CHANGELOG.md",
        ROOT / "SECURITY.md",
        ROOT / "RELEASE_CHECKLIST.md",
    ]
    for f in required_files:
        ok = f.exists()
        _print("File", ok, str(f.relative_to(ROOT)))
        if not ok:
            hard_fail = True

    has_provider_key = any(
        bool((os.environ.get(name) or "").strip())
        for name in ("GROQ_API_KEYS", "OPENAI_API_KEYS", "GEMINI_API_KEYS")
    )
    _print(
        "LLM keys",
        has_provider_key,
        "At least one of GROQ_API_KEYS/OPENAI_API_KEYS/GEMINI_API_KEYS is set",
    )

    has_app_secret = bool((os.environ.get("II_APP_SECRET_KEY") or "").strip())
    _print("App secret", has_app_secret, "II_APP_SECRET_KEY is set (recommended for production)")

    if hard_fail:
        print("\nPreflight failed. Fix hard failures before release.")
        return 1
    print("\nPreflight passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

