#!/usr/bin/env python3
"""
One-command production readiness gate.

Default pipeline:
  1) preflight_check --prod
  2) architecture_gate --strict
  3) release_readiness_summary --strict
  4) check_secrets
  5) scale_gate_check
  6) slo_gate_check
  7) pip-audit (requirements.txt)
  8) health_check --full --skip-pytest
  9) live_data_gate_check

Usage:
  python scripts/prod_readiness_check.py
  python scripts/prod_readiness_check.py --env dev
  python scripts/prod_readiness_check.py --env preprod
  python scripts/prod_readiness_check.py --skip-audit
  python scripts/prod_readiness_check.py --require-go
  python scripts/prod_readiness_check.py --dry-run
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _run(cmd: list[str], *, env: dict[str, str], dry_run: bool = False) -> int:
    print(f"> {' '.join(cmd)}")
    if dry_run:
        return 0
    res = subprocess.run(cmd, cwd=str(ROOT), env=env)
    return int(res.returncode)


def _resolve_env(name: str) -> str:
    value = str(name or "").strip().lower()
    if value in ("prod", "production"):
        return "prod"
    if value in ("preprod", "staging", "stage"):
        return "preprod"
    if value in ("dev", "development", "local"):
        return "dev"
    return "dev"


def _default_live_ratio(env_name: str) -> float:
    if env_name in ("prod", "preprod"):
        return 0.25
    return 0.0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--env",
        default=str(os.environ.get("II_ENV") or "dev"),
        help="Runtime environment: dev/preprod/prod (staging maps to preprod).",
    )
    ap.add_argument("--skip-audit", action="store_true", help="Skip pip-audit vulnerability scan.")
    ap.add_argument("--skip-health", action="store_true", help="Skip health_check step.")
    ap.add_argument("--require-go", action="store_true", help="Strict mode: block unless scale/slo gates return GO.")
    ap.add_argument("--require-live-data", action="store_true", help="Block if live data gate does not return GO.")
    ap.add_argument("--min-live-ratio", type=float, default=None, help="Minimum live snapshot source ratio.")
    ap.add_argument("--min-cx-samples", type=int, default=0, help="Minimum CX samples required by scale/slo gates.")
    ap.add_argument("--dry-run", action="store_true", help="Print steps only, do not execute.")
    args = ap.parse_args()

    env_name = _resolve_env(str(args.env))
    min_live_ratio = (
        float(args.min_live_ratio)
        if args.min_live_ratio is not None
        else _default_live_ratio(env_name)
    )
    require_live_data = bool(args.require_live_data or env_name in ("preprod", "prod"))

    env = os.environ.copy()
    env.setdefault("II_LOG_JSON", "1")
    env.setdefault("II_REQUIRE_APP_SECRET_KEY", "1")
    env["II_ENV"] = env_name

    scale_cmd = [sys.executable, str(ROOT / "scripts" / "scale_gate_check.py")]
    slo_cmd = [sys.executable, str(ROOT / "scripts" / "slo_gate_check.py")]
    min_samples = max(0, int(args.min_cx_samples))
    scale_cmd.extend(["--min-cx-samples", str(min_samples)])
    slo_cmd.extend(["--min-cx-samples", str(min_samples)])
    if bool(args.require_go):
        scale_cmd.append("--require-go")
        slo_cmd.append("--require-go")

    steps: list[list[str]] = [
        [sys.executable, str(ROOT / "scripts" / "preflight_check.py"), "--prod"],
        [sys.executable, str(ROOT / "scripts" / "architecture_gate.py"), "--strict"],
        [sys.executable, str(ROOT / "scripts" / "release_readiness_summary.py"), "--strict"],
        [sys.executable, str(ROOT / "scripts" / "sanitize_runtime_artifacts.py")],
        [sys.executable, str(ROOT / "scripts" / "check_secrets.py")],
        [sys.executable, str(ROOT / "scripts" / "stabilize_runtime_reliability.py")],
        scale_cmd,
        slo_cmd,
    ]

    if not args.skip_audit:
        pip_audit_bin = shutil.which("pip-audit")
        if pip_audit_bin:
            steps.append([pip_audit_bin, "-r", str(ROOT / "requirements.txt"), "--progress-spinner", "off"])
        else:
            if bool(args.dry_run):
                print("[WARN] pip-audit is not installed. Dry-run will skip actual vulnerability audit execution.")
                steps.append(["pip-audit", "-r", str(ROOT / "requirements.txt"), "--progress-spinner", "off"])
            else:
                print("[WARN] pip-audit is not installed. Install with: python -m pip install pip-audit")
                return 2

    if not args.skip_health:
        steps.append([sys.executable, str(ROOT / "scripts" / "health_check.py"), "--full", "--skip-pytest"])
    live_gate_cmd = [
        sys.executable,
        str(ROOT / "scripts" / "live_data_gate_check.py"),
        "--min-live-ratio",
        str(float(min_live_ratio)),
    ]
    if require_live_data:
        live_gate_cmd.append("--require-min-live")
    print(
        f"[INFO] Live data policy: env={env_name} min_live_ratio={min_live_ratio:.2f} require_min_live={require_live_data}"
    )
    steps.append(live_gate_cmd)

    for cmd in steps:
        rc = _run(cmd, env=env, dry_run=bool(args.dry_run))
        if rc != 0:
            print(f"[FAIL] Command failed with exit code {rc}")
            return rc

    print("[OK] Production readiness checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
