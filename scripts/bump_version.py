#!/usr/bin/env python3
"""
Bump semantic version in VERSION and prepare CHANGELOG Unreleased block.

Usage:
  python scripts/bump_version.py patch
  python scripts/bump_version.py minor
  python scripts/bump_version.py major
"""
from __future__ import annotations

import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VERSION_PATH = ROOT / "VERSION"
CHANGELOG_PATH = ROOT / "CHANGELOG.md"


def _parse(v: str) -> tuple[int, int, int]:
    parts = v.strip().split(".")
    if len(parts) != 3:
        raise ValueError(f"Invalid semantic version: {v!r}")
    return int(parts[0]), int(parts[1]), int(parts[2])


def _bump(version: str, level: str) -> str:
    major, minor, patch = _parse(version)
    if level == "major":
        major += 1
        minor = 0
        patch = 0
    elif level == "minor":
        minor += 1
        patch = 0
    else:
        patch += 1
    return f"{major}.{minor}.{patch}"


def _ensure_unreleased(changelog_text: str) -> str:
    marker = "## [Unreleased]"
    if marker in changelog_text:
        return changelog_text
    skeleton = (
        "## [Unreleased]\n\n"
        "### Added\n- \n\n"
        "### Changed\n- \n\n"
        "### Security\n- \n\n"
    )
    if "# Changelog" in changelog_text:
        return changelog_text.replace("# Changelog\n\n", "# Changelog\n\n" + skeleton)
    return "# Changelog\n\n" + skeleton + changelog_text


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("level", choices=["major", "minor", "patch"])
    args = ap.parse_args()

    current = VERSION_PATH.read_text(encoding="utf-8").strip()
    nxt = _bump(current, args.level)
    VERSION_PATH.write_text(nxt + "\n", encoding="utf-8")

    changelog = CHANGELOG_PATH.read_text(encoding="utf-8")
    updated = _ensure_unreleased(changelog)
    CHANGELOG_PATH.write_text(updated, encoding="utf-8")

    print(f"Bumped version: {current} -> {nxt}")
    print("Next: update CHANGELOG.md in [Unreleased], then run scripts/release_prep.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

