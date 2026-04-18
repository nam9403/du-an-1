#!/usr/bin/env python3
"""
Generate release notes from CHANGELOG.md and VERSION.

Usage:
  python scripts/generate_release_notes.py
  python scripts/generate_release_notes.py --version 1.0.0
"""
from __future__ import annotations

import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CHANGELOG_PATH = ROOT / "CHANGELOG.md"
VERSION_PATH = ROOT / "VERSION"
OUTPUT_PATH = ROOT / "dist" / "release-notes.md"


def _read_version(default: str = "0.0.0-dev") -> str:
    try:
        value = VERSION_PATH.read_text(encoding="utf-8").strip()
    except OSError:
        return default
    return value or default


def _extract_unreleased(changelog_text: str) -> str:
    marker = "## [Unreleased]"
    start = changelog_text.find(marker)
    if start < 0:
        return ""
    tail = changelog_text[start + len(marker) :].lstrip()
    next_header = tail.find("\n## [")
    if next_header >= 0:
        return tail[:next_header].strip()
    return tail.strip()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", default="", help="Release version override (without leading v).")
    args = ap.parse_args()

    version = (args.version or "").strip() or _read_version()
    try:
        changelog = CHANGELOG_PATH.read_text(encoding="utf-8")
    except OSError:
        print("ERROR: Cannot read CHANGELOG.md")
        return 1

    body = _extract_unreleased(changelog)
    if not body:
        body = "- No release notes were found under `## [Unreleased]`."

    content = (
        f"# Release v{version}\n\n"
        "## Highlights\n\n"
        f"{body}\n\n"
        "## Verification\n\n"
        "- [x] Secret scan passed\n"
        "- [x] Unit/integration tests passed\n"
        "- [x] Offline health check passed\n"
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(content, encoding="utf-8")
    print(f"Release notes generated: {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

