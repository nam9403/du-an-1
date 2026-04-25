#!/usr/bin/env python3
"""
Run pre-release validation and print release guidance.

Usage:
  python scripts/release_prep.py
  python scripts/release_prep.py --full
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _run(cmd: list[str]) -> int:
    print(f"> {' '.join(cmd)}")
    r = subprocess.run(cmd, cwd=str(ROOT))
    return int(r.returncode)


def _read_version(default: str = "0.0.0-dev") -> str:
    p = ROOT / "VERSION"
    try:
        v = p.read_text(encoding="utf-8").strip()
    except OSError:
        return default
    return v or default


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true", help="Include network-aware health check.")
    args = ap.parse_args()

    steps = [
        [sys.executable, str(ROOT / "scripts" / "preflight_check.py")],
        [sys.executable, str(ROOT / "scripts" / "sanitize_runtime_artifacts.py")],
        [sys.executable, str(ROOT / "scripts" / "check_secrets.py")],
        [sys.executable, str(ROOT / "scripts" / "build_timing_dashboard.py"), "--min-samples", "1"],
        [sys.executable, "-m", "pytest", "-q"],
        [sys.executable, str(ROOT / "scripts" / "health_check.py"), "--skip-pytest"],
    ]
    if args.full:
        steps.append([sys.executable, str(ROOT / "scripts" / "health_check.py"), "--full", "--skip-pytest"])

    for cmd in steps:
        rc = _run(cmd)
        if rc != 0:
            return rc

    rc_notes = _run([sys.executable, str(ROOT / "scripts" / "generate_release_notes.py"), "--version", version := _read_version()])
    if rc_notes != 0:
        return rc_notes

    print("\nRelease prep passed.")
    print(f"Suggested tag: v{version}")
    print("Next steps:")
    print("  1) Review CHANGELOG.md")
    print("  2) Review dist/release-notes.md")
    print(f"  3) Create tag: git tag v{version}")
    print("  4) Push tag:   git push origin --tags")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

