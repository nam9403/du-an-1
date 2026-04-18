#!/usr/bin/env python3
"""
Simple secret scanner for local runs and CI gates.

Exit code:
  0 -> no suspicious secrets found
  1 -> suspicious secrets detected
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

IGNORE_DIRS = {
    ".git",
    ".venv",
    "__pycache__",
    ".pytest_cache",
    ".pytest_tmp",
    "dist",
    "build",
    "htmlcov",
}

IGNORE_FILES = {
    "CHANGELOG.md",  # may mention secret handling terms in plain text
}

TEXT_EXTS = {
    ".py",
    ".md",
    ".txt",
    ".json",
    ".yml",
    ".yaml",
    ".ini",
    ".cfg",
    ".toml",
    ".bat",
    ".ps1",
    ".sh",
    ".env",
    ".example",
}

PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("OpenAI key", re.compile(r"\bsk-[A-Za-z0-9]{20,}\b")),
    ("Groq key", re.compile(r"\bgsk_[A-Za-z0-9]{20,}\b")),
    ("Gemini key", re.compile(r"\bAIza[0-9A-Za-z\-_]{20,}\b")),
    ("Generic API key assignment", re.compile(r"(?i)\b(api[_-]?key|token|secret)\b\s*[:=]\s*[\"'][^\"']{12,}[\"']")),
]

ALLOWLIST_SUBSTRINGS = (
    "sk-xxx",
    "sk_key_",
    "gsk_key_",
    "AIza_key_",
    "your_fernet_key_here",
    "example.com",
)


def _is_text_candidate(path: Path) -> bool:
    if path.name in IGNORE_FILES:
        return False
    if path.suffix.lower() in TEXT_EXTS:
        return True
    return path.name.startswith(".env")


def _iter_files() -> list[Path]:
    out: list[Path] = []
    for p in ROOT.rglob("*"):
        if not p.is_file():
            continue
        if any(part in IGNORE_DIRS for part in p.parts):
            continue
        if not _is_text_candidate(p):
            continue
        out.append(p)
    return out


def main() -> int:
    findings: list[str] = []
    for file_path in _iter_files():
        rel = file_path.relative_to(ROOT)
        try:
            content = file_path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for idx, line in enumerate(content.splitlines(), start=1):
            if any(s in line for s in ALLOWLIST_SUBSTRINGS):
                continue
            for label, rx in PATTERNS:
                if rx.search(line):
                    findings.append(f"{rel}:{idx} [{label}] {line.strip()[:180]}")
                    break
    if findings:
        print("Potential secrets detected:")
        for item in findings:
            print(f" - {item}")
        print("\nFailing build. Remove or rotate exposed credentials.")
        return 1
    print("Secret scan passed: no suspicious credentials found.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

