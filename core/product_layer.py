"""
Retention + Monetization support layer (SQLite backend).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import smtplib
import sqlite3
import base64
import secrets
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

import requests
from cryptography.fernet import Fernet, InvalidToken
from scrapers.financial_data import fetch_financial_snapshot

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "app_state.db"
LEGACY_STATE_PATH = ROOT / "data" / "user_state.json"
SECRETS_PATH = ROOT / "data" / "secrets_store.json"
APP_SECRET_PATH = ROOT / "data" / ".app_secret.key"

PIN_HASH_PREFIX = "pbkdf2_sha256"
PIN_HASH_ITERATIONS = 210_000

PLAN_FEATURES = {
    "free": {"scan_limit": 20, "llm_live": False, "alerts": 3, "analysis_per_day": 30},
    "pro": {"scan_limit": 80, "llm_live": True, "alerts": 20, "analysis_per_day": 200},
    "expert": {"scan_limit": 200, "llm_live": True, "alerts": 100, "analysis_per_day": 1000},
}


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    return c


def _init_db() -> None:
    with _conn() as c:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                plan_id TEXT NOT NULL DEFAULT 'free',
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS holdings (
                user_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                quantity REAL NOT NULL,
                avg_cost REAL NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (user_id, symbol)
            );
            CREATE TABLE IF NOT EXISTS alerts (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                type TEXT NOT NULL,
                threshold REAL NOT NULL,
                note TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS usage_daily (
                user_id TEXT NOT NULL,
                usage_date TEXT NOT NULL,
                feature TEXT NOT NULL,
                count INTEGER NOT NULL,
                PRIMARY KEY (user_id, usage_date, feature)
            );
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                ts TEXT NOT NULL,
                event TEXT NOT NULL,
                meta_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE TABLE IF NOT EXISTS notification_sent (
                dedup_key TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                sent_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS notification_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending',
                last_error TEXT NOT NULL DEFAULT '',
                next_retry_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS auth_users (
                user_id TEXT PRIMARY KEY,
                pin_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_price REAL NOT NULL,
                stop_loss REAL NOT NULL,
                take_profit REAL NOT NULL,
                thesis TEXT NOT NULL DEFAULT '',
                horizon_days INTEGER NOT NULL DEFAULT 30,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                decision_id INTEGER,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL DEFAULT 'LONG',
                quantity REAL NOT NULL,
                entry_price REAL NOT NULL,
                entry_fee REAL NOT NULL DEFAULT 0,
                entry_note TEXT NOT NULL DEFAULT '',
                opened_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'OPEN',
                exit_price REAL,
                exit_fee REAL,
                exit_note TEXT,
                closed_at TEXT
            );
            CREATE TABLE IF NOT EXISTS forecast_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                created_at TEXT NOT NULL,
                horizon_days INTEGER NOT NULL,
                base_price REAL NOT NULL,
                expected_price REAL NOT NULL,
                expected_return_pct REAL NOT NULL,
                realized_price REAL,
                realized_return_pct REAL,
                abs_error_pct REAL,
                direction_hit INTEGER,
                status TEXT NOT NULL DEFAULT 'pending'
            );
            CREATE TABLE IF NOT EXISTS premium_trials (
                user_id TEXT PRIMARY KEY,
                trial_start_at TEXT,
                trial_end_at TEXT,
                trial_consumed INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );
            """
        )
        # Lightweight schema migration for existing installations.
        cols = [str(r["name"]) for r in c.execute("PRAGMA table_info(trades)").fetchall()]
        if "decision_id" not in cols:
            c.execute("ALTER TABLE trades ADD COLUMN decision_id INTEGER")


_init_db()


def get_plan_features(plan_id: str) -> dict[str, Any]:
    return dict(PLAN_FEATURES.get(plan_id, PLAN_FEATURES["free"]))


def set_user_plan(user_id: str, plan_id: str) -> None:
    pid = str(plan_id or "free").strip().lower()
    if pid not in PLAN_FEATURES:
        pid = "free"
    with _conn() as c:
        c.execute(
            """
            INSERT INTO users(user_id, plan_id, updated_at) VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET plan_id=excluded.plan_id, updated_at=excluded.updated_at
            """,
            (user_id, pid, _utcnow_iso()),
        )


def get_user_plan(user_id: str, default_plan: str = "free") -> str:
    with _conn() as c:
        r = c.execute("SELECT plan_id FROM users WHERE user_id=?", (user_id,)).fetchone()
    pid = str((r["plan_id"] if r else default_plan) or default_plan).strip().lower()
    return pid if pid in PLAN_FEATURES else "free"


def get_trial_state(user_id: str) -> dict[str, Any]:
    uid = (user_id or "").strip()
    if not uid:
        return {"trial_consumed": 0, "trial_active": False}
    with _conn() as c:
        r = c.execute(
            "SELECT trial_start_at, trial_end_at, trial_consumed FROM premium_trials WHERE user_id=?",
            (uid,),
        ).fetchone()
    if r is None:
        return {"trial_consumed": 0, "trial_active": False}
    end_at = _parse_ts(str(r["trial_end_at"] or ""))
    active = bool(end_at and datetime.now(timezone.utc) <= end_at)
    return {
        "trial_start_at": str(r["trial_start_at"] or ""),
        "trial_end_at": str(r["trial_end_at"] or ""),
        "trial_consumed": int(r["trial_consumed"] or 0),
        "trial_active": active,
    }


def trial_is_active(user_id: str) -> bool:
    st = get_trial_state(user_id)
    return bool(st.get("trial_active"))


def start_premium_trial_7d(user_id: str) -> tuple[bool, str]:
    uid = (user_id or "").strip()
    if not uid:
        return False, "User rỗng."
    state = get_trial_state(uid)
    if int(state.get("trial_consumed") or 0) >= 1:
        return False, "Đã dùng trial trước đó."
    now = datetime.now(timezone.utc)
    end_at = now + timedelta(days=7)
    with _conn() as c:
        c.execute(
            """
            INSERT INTO premium_trials(user_id, trial_start_at, trial_end_at, trial_consumed, updated_at)
            VALUES (?, ?, ?, 1, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                trial_start_at=excluded.trial_start_at,
                trial_end_at=excluded.trial_end_at,
                trial_consumed=1,
                updated_at=excluded.updated_at
            """,
            (uid, now.isoformat(), end_at.isoformat(), _utcnow_iso()),
        )
    return True, "Đã kích hoạt trial 7 ngày."


def premium_features_unlocked(user_id: str, plan_id: str) -> bool:
    pid = str(plan_id or "free").strip().lower()
    if pid in ("pro", "expert"):
        return True
    return trial_is_active(user_id)


def _usage_key(feature: str) -> str:
    return f"{datetime.now(timezone.utc).date().isoformat()}:{feature}"


def _pin_hash(pin: str) -> str:
    raw = str(pin or "").strip()
    salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", raw.encode("utf-8"), salt, PIN_HASH_ITERATIONS)
    return (
        f"{PIN_HASH_PREFIX}${PIN_HASH_ITERATIONS}$"
        f"{base64.urlsafe_b64encode(salt).decode('ascii')}$"
        f"{base64.urlsafe_b64encode(dk).decode('ascii')}"
    )


def _pin_hash_legacy(pin: str) -> str:
    raw = str(pin or "").strip()
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _verify_pin_hash(stored_hash: str, pin: str) -> bool:
    val = str(stored_hash or "").strip()
    raw = str(pin or "").strip()
    if not val or not raw:
        return False
    if val.startswith(f"{PIN_HASH_PREFIX}$"):
        try:
            _, iter_text, salt_b64, digest_b64 = val.split("$", 3)
            rounds = max(120_000, int(iter_text))
            salt = base64.urlsafe_b64decode(salt_b64.encode("ascii"))
            expected = base64.urlsafe_b64decode(digest_b64.encode("ascii"))
        except (TypeError, ValueError):
            return False
        current = hashlib.pbkdf2_hmac("sha256", raw.encode("utf-8"), salt, rounds)
        return hmac.compare_digest(current, expected)
    return hmac.compare_digest(val, _pin_hash_legacy(raw))


def register_user_pin(user_id: str, pin: str) -> tuple[bool, str]:
    uid = (user_id or "").strip()
    if not uid:
        return False, "Mã khách hàng rỗng."
    p = str(pin or "").strip()
    if len(p) < 6:
        return False, "PIN tối thiểu 6 ký tự."
    now = _utcnow_iso()
    with _conn() as c:
        exists = c.execute("SELECT 1 FROM auth_users WHERE user_id=?", (uid,)).fetchone() is not None
        if exists:
            return False, "Tài khoản đã tồn tại."
        c.execute(
            "INSERT INTO auth_users(user_id, pin_hash, created_at, updated_at) VALUES (?, ?, ?, ?)",
            (uid, _pin_hash(p), now, now),
        )
    return True, "Đăng ký thành công."


def verify_user_pin(user_id: str, pin: str) -> bool:
    uid = (user_id or "").strip()
    if not uid:
        return False
    with _conn() as c:
        r = c.execute("SELECT pin_hash FROM auth_users WHERE user_id=?", (uid,)).fetchone()
    if r is None:
        return False
    ok = _verify_pin_hash(str(r["pin_hash"] or ""), str(pin or "").strip())
    if not ok:
        return False
    # Upgrade legacy sha256 hash on successful login.
    current_hash = str(r["pin_hash"] or "")
    if current_hash and not current_hash.startswith(f"{PIN_HASH_PREFIX}$"):
        upsert_user_pin(uid, str(pin or "").strip())
    return True


def upsert_user_pin(user_id: str, pin: str) -> tuple[bool, str]:
    uid = (user_id or "").strip()
    p = str(pin or "").strip()
    if not uid:
        return False, "Mã khách hàng rỗng."
    if len(p) < 6:
        return False, "PIN tối thiểu 6 ký tự."
    now = _utcnow_iso()
    with _conn() as c:
        c.execute(
            """
            INSERT INTO auth_users(user_id, pin_hash, created_at, updated_at) VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET pin_hash=excluded.pin_hash, updated_at=excluded.updated_at
            """,
            (uid, _pin_hash(p), now, now),
        )
    return True, "Đã cập nhật PIN."


def has_auth_user(user_id: str) -> bool:
    uid = (user_id or "").strip()
    if not uid:
        return False
    with _conn() as c:
        return c.execute("SELECT 1 FROM auth_users WHERE user_id=?", (uid,)).fetchone() is not None


def _secret_key(user_id: str) -> bytes:
    seed = f"{user_id}|value-investor-local".encode("utf-8")
    return hashlib.sha256(seed).digest()


def _xor_crypt(data: bytes, key: bytes) -> bytes:
    return bytes(b ^ key[i % len(key)] for i, b in enumerate(data))


def _get_fernet() -> Fernet:
    env_key = str(os.environ.get("II_APP_SECRET_KEY") or "").strip()
    if env_key:
        return Fernet(env_key.encode("ascii"))
    if APP_SECRET_PATH.exists():
        key = APP_SECRET_PATH.read_text(encoding="utf-8").strip()
        if key:
            return Fernet(key.encode("ascii"))
    APP_SECRET_PATH.parent.mkdir(parents=True, exist_ok=True)
    key = Fernet.generate_key().decode("ascii")
    APP_SECRET_PATH.write_text(key, encoding="utf-8")
    return Fernet(key.encode("ascii"))


def _load_secrets() -> dict[str, Any]:
    if not SECRETS_PATH.exists():
        return {}
    try:
        with open(SECRETS_PATH, encoding="utf-8") as f:
            raw = json.load(f)
        return raw if isinstance(raw, dict) else {}
    except (OSError, ValueError):
        return {}


def _save_secrets(data: dict[str, Any]) -> None:
    SECRETS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(SECRETS_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def save_secret(user_id: str, name: str, value: str) -> None:
    uid = (user_id or "").strip()
    nm = (name or "").strip()
    if not uid or not nm:
        return
    raw = (value or "").encode("utf-8")
    token = _get_fernet().encrypt(raw).decode("ascii")
    payload = _load_secrets()
    user_block = payload.get(uid) if isinstance(payload.get(uid), dict) else {}
    user_block[nm] = f"v2:{token}"
    payload[uid] = user_block
    _save_secrets(payload)


def load_secret(user_id: str, name: str, default: str = "") -> str:
    uid = (user_id or "").strip()
    nm = (name or "").strip()
    payload = _load_secrets()
    block = payload.get(uid) if isinstance(payload.get(uid), dict) else {}
    b64 = str(block.get(nm) or "")
    if not b64:
        return default
    try:
        if b64.startswith("v2:"):
            raw = _get_fernet().decrypt(b64[3:].encode("ascii"))
            return raw.decode("utf-8")
        enc = base64.b64decode(b64.encode("ascii"))
        dec = _xor_crypt(enc, _secret_key(uid))
        value = dec.decode("utf-8")
        if value:
            # Transparently migrate legacy secret to v2 format.
            save_secret(uid, nm, value)
        return value or default
    except (InvalidToken, ValueError, OSError):
        return default


def record_usage(user_id: str, feature: str, count: int = 1) -> None:
    if count <= 0:
        return
    d = datetime.now(timezone.utc).date().isoformat()
    with _conn() as c:
        c.execute(
            """
            INSERT INTO usage_daily(user_id, usage_date, feature, count) VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id, usage_date, feature) DO UPDATE SET count = usage_daily.count + excluded.count
            """,
            (user_id, d, feature, int(count)),
        )


def get_usage_today(user_id: str, feature: str) -> int:
    d = datetime.now(timezone.utc).date().isoformat()
    with _conn() as c:
        r = c.execute(
            "SELECT count FROM usage_daily WHERE user_id=? AND usage_date=? AND feature=?",
            (user_id, d, feature),
        ).fetchone()
    return int(r["count"]) if r else 0


def can_use_feature(user_id: str, feature: str, requested: int = 1, plan_id: str | None = None) -> tuple[bool, str]:
    pid = (plan_id or get_user_plan(user_id)).strip().lower()
    plan = get_plan_features(pid)
    if feature == "analysis":
        used = get_usage_today(user_id, "analysis")
        limit = int(plan.get("analysis_per_day") or 0)
        if used + requested > limit:
            return False, f"Vượt giới hạn phân tích/ngày của gói ({used}/{limit})."
    if feature == "scan":
        limit = int(plan.get("scan_limit") or 0)
        if requested > limit:
            return False, f"Gói hiện tại chỉ cho quét tối đa {limit} mã."
    if feature == "alert":
        current = len(list_alerts(user_id))
        limit = int(plan.get("alerts") or 0)
        if current + requested > limit:
            return False, f"Đã chạm giới hạn cảnh báo ({current}/{limit})."
    return True, "ok"


def log_event(user_id: str, event_type: str, meta: dict[str, Any] | None = None) -> None:
    with _conn() as c:
        c.execute(
            "INSERT INTO events(user_id, ts, event, meta_json) VALUES (?, ?, ?, ?)",
            (user_id, _utcnow_iso(), str(event_type or "unknown"), json.dumps(meta or {}, ensure_ascii=False)),
        )
        # Keep bounded
        c.execute(
            """
            DELETE FROM events
            WHERE id IN (
                SELECT id FROM events WHERE user_id=?
                ORDER BY id DESC LIMIT -1 OFFSET 5000
            )
            """,
            (user_id,),
        )


def record_forecast_snapshot(user_id: str, symbol: str, report: dict[str, Any]) -> None:
    fc = (report or {}).get("probabilistic_forecast") or {}
    val = (report or {}).get("valuation") or {}
    base_price = float(val.get("price") or 0.0)
    expected_price = float(fc.get("expected_price") or 0.0)
    expected_return = float(fc.get("expected_return_pct") or 0.0)
    horizon_days = int(fc.get("horizon_days") or 0)
    sym = str(symbol or "").strip().upper()
    if not sym or base_price <= 0 or expected_price <= 0 or horizon_days <= 0:
        return
    with _conn() as c:
        c.execute(
            """
            INSERT INTO forecast_records(
                user_id, symbol, created_at, horizon_days, base_price, expected_price, expected_return_pct, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
            """,
            (str(user_id or "default_user"), sym, _utcnow_iso(), horizon_days, base_price, expected_price, expected_return),
        )
        c.execute(
            """
            DELETE FROM forecast_records
            WHERE id IN (
                SELECT id FROM forecast_records
                WHERE user_id=? AND symbol=?
                ORDER BY id DESC LIMIT -1 OFFSET 1000
            )
            """,
            (str(user_id or "default_user"), sym),
        )


def _refresh_matured_forecasts(user_id: str, symbol: str) -> None:
    uid = str(user_id or "default_user")
    sym = str(symbol or "").strip().upper()
    if not sym:
        return
    now = datetime.now(timezone.utc)
    with _conn() as c:
        rows = c.execute(
            """
            SELECT id, created_at, horizon_days, base_price, expected_price
            FROM forecast_records
            WHERE user_id=? AND symbol=? AND status='pending'
            ORDER BY id ASC LIMIT 80
            """,
            (uid, sym),
        ).fetchall()
    if not rows:
        return
    snap = fetch_financial_snapshot(sym) or {}
    current_price = float(snap.get("price") or 0.0)
    if current_price <= 0:
        return
    with _conn() as c:
        for r in rows:
            created_at = _parse_ts(str(r["created_at"]))
            if created_at is None:
                continue
            horizon_days = int(r["horizon_days"] or 0)
            if horizon_days <= 0:
                continue
            due_at = created_at + timedelta(days=horizon_days)
            if now < due_at:
                continue
            base_price = float(r["base_price"] or 0.0)
            expected_price = float(r["expected_price"] or 0.0)
            if base_price <= 0 or expected_price <= 0:
                continue
            realized_ret = (current_price / base_price - 1.0) * 100.0
            expected_ret = (expected_price / base_price - 1.0) * 100.0
            abs_error = abs(realized_ret - expected_ret)
            direction_hit = 1 if (realized_ret >= 0 and expected_ret >= 0) or (realized_ret < 0 and expected_ret < 0) else 0
            c.execute(
                """
                UPDATE forecast_records
                SET realized_price=?, realized_return_pct=?, abs_error_pct=?, direction_hit=?, status='resolved'
                WHERE id=?
                """,
                (current_price, realized_ret, abs_error, direction_hit, int(r["id"])),
            )


def get_forecast_accuracy_dashboard(user_id: str, symbol: str, lookback_days: int = 540) -> dict[str, Any]:
    uid = str(user_id or "default_user")
    sym = str(symbol or "").strip().upper()
    if not sym:
        return {"records": 0, "resolved": 0, "hit_rate_pct": None, "mape_pct": None, "bias_pct": None, "rows": []}
    _refresh_matured_forecasts(uid, sym)
    since = (datetime.now(timezone.utc) - timedelta(days=max(30, int(lookback_days)))).isoformat()
    with _conn() as c:
        rows = c.execute(
            """
            SELECT created_at, horizon_days, base_price, expected_price, expected_return_pct,
                   realized_price, realized_return_pct, abs_error_pct, direction_hit, status
            FROM forecast_records
            WHERE user_id=? AND symbol=? AND created_at>=?
            ORDER BY created_at DESC LIMIT 300
            """,
            (uid, sym, since),
        ).fetchall()
    items = [dict(r) for r in rows]
    resolved = [x for x in items if str(x.get("status")) == "resolved"]
    hit_rate = None
    mape = None
    bias = None
    if resolved:
        hit_vals = [int(x.get("direction_hit") or 0) for x in resolved]
        err_vals = [float(x.get("abs_error_pct") or 0.0) for x in resolved]
        bias_vals = [
            float(x.get("realized_return_pct") or 0.0) - float(x.get("expected_return_pct") or 0.0) for x in resolved
        ]
        hit_rate = round(sum(hit_vals) / len(hit_vals) * 100.0, 2)
        mape = round(sum(err_vals) / len(err_vals), 2)
        bias = round(sum(bias_vals) / len(bias_vals), 2)
    return {
        "records": len(items),
        "resolved": len(resolved),
        "hit_rate_pct": hit_rate,
        "mape_pct": mape,
        "bias_pct": bias,
        "rows": items[:80],
    }


def get_forecast_benchmark_by_horizon(
    user_id: str,
    symbol: str,
    horizons: tuple[int, ...] = (30, 60, 90),
    lookback_days: int = 720,
) -> list[dict[str, Any]]:
    uid = str(user_id or "default_user")
    sym = str(symbol or "").strip().upper()
    if not sym:
        return []
    _refresh_matured_forecasts(uid, sym)
    since = (datetime.now(timezone.utc) - timedelta(days=max(60, int(lookback_days)))).isoformat()
    with _conn() as c:
        rows = c.execute(
            """
            SELECT horizon_days, expected_return_pct, realized_return_pct, abs_error_pct, direction_hit, status
            FROM forecast_records
            WHERE user_id=? AND symbol=? AND created_at>=?
            ORDER BY created_at DESC LIMIT 1000
            """,
            (uid, sym, since),
        ).fetchall()
    items = [dict(r) for r in rows if str(r["status"]) == "resolved"]
    out: list[dict[str, Any]] = []
    for h in horizons:
        grp = [x for x in items if int(x.get("horizon_days") or 0) == int(h)]
        if not grp:
            out.append(
                {
                    "horizon_days": int(h),
                    "samples": 0,
                    "hit_rate_pct": None,
                    "mape_pct": None,
                    "expected_return_pct_avg": None,
                    "realized_return_pct_avg": None,
                    "alpha_pct": None,
                    "beat_expected_pct": None,
                }
            )
            continue
        n = len(grp)
        hit = sum(int(x.get("direction_hit") or 0) for x in grp) / n * 100.0
        mape = sum(float(x.get("abs_error_pct") or 0.0) for x in grp) / n
        exp_avg = sum(float(x.get("expected_return_pct") or 0.0) for x in grp) / n
        real_avg = sum(float(x.get("realized_return_pct") or 0.0) for x in grp) / n
        alpha = real_avg - exp_avg
        beat = sum(1 for x in grp if float(x.get("realized_return_pct") or 0.0) > float(x.get("expected_return_pct") or 0.0)) / n * 100.0
        out.append(
            {
                "horizon_days": int(h),
                "samples": int(n),
                "hit_rate_pct": round(hit, 2),
                "mape_pct": round(mape, 2),
                "expected_return_pct_avg": round(exp_avg, 2),
                "realized_return_pct_avg": round(real_avg, 2),
                "alpha_pct": round(alpha, 2),
                "beat_expected_pct": round(beat, 2),
            }
        )
    return out


def get_forecast_leaderboard(user_id: str, limit: int = 20, lookback_days: int = 720) -> list[dict[str, Any]]:
    uid = str(user_id or "default_user")
    since = (datetime.now(timezone.utc) - timedelta(days=max(60, int(lookback_days)))).isoformat()
    with _conn() as c:
        rows = c.execute(
            """
            SELECT symbol, expected_return_pct, realized_return_pct, abs_error_pct, direction_hit
            FROM forecast_records
            WHERE user_id=? AND status='resolved' AND created_at>=?
            ORDER BY created_at DESC LIMIT 3000
            """,
            (uid, since),
        ).fetchall()
    items = [dict(r) for r in rows]
    if not items:
        return []
    grouped: dict[str, list[dict[str, Any]]] = {}
    for x in items:
        sym = str(x.get("symbol") or "").upper()
        if not sym:
            continue
        grouped.setdefault(sym, []).append(x)
    out: list[dict[str, Any]] = []
    for sym, grp in grouped.items():
        n = len(grp)
        if n <= 0:
            continue
        hit = sum(int(v.get("direction_hit") or 0) for v in grp) / n * 100.0
        mape = sum(float(v.get("abs_error_pct") or 0.0) for v in grp) / n
        exp_avg = sum(float(v.get("expected_return_pct") or 0.0) for v in grp) / n
        real_avg = sum(float(v.get("realized_return_pct") or 0.0) for v in grp) / n
        alpha = real_avg - exp_avg
        # Composite score: reward direction hit and alpha, penalize error.
        score = (0.55 * hit) + (0.25 * max(-20.0, min(20.0, alpha + 10.0))) - (0.6 * mape) + min(8.0, n * 0.4)
        out.append(
            {
                "symbol": sym,
                "samples": int(n),
                "hit_rate_pct": round(hit, 2),
                "mape_pct": round(mape, 2),
                "alpha_pct": round(alpha, 2),
                "score": round(score, 2),
            }
        )
    out.sort(key=lambda x: (x["score"], x["samples"]), reverse=True)
    return out[: max(5, min(int(limit), 100))]


def get_forecast_group_benchmark(
    user_id: str,
    symbols: list[str],
    lookback_days: int = 720,
) -> dict[str, Any]:
    uid = str(user_id or "default_user")
    syms = [str(s or "").strip().upper() for s in symbols if str(s or "").strip()]
    syms = sorted(set(syms))[:80]
    if not syms:
        return {"samples": 0, "hit_rate_pct": None, "mape_pct": None, "alpha_pct": None}
    since = (datetime.now(timezone.utc) - timedelta(days=max(60, int(lookback_days)))).isoformat()
    placeholders = ",".join(["?"] * len(syms))
    params: list[Any] = [uid, since, *syms]
    with _conn() as c:
        rows = c.execute(
            f"""
            SELECT expected_return_pct, realized_return_pct, abs_error_pct, direction_hit
            FROM forecast_records
            WHERE user_id=? AND status='resolved' AND created_at>=? AND symbol IN ({placeholders})
            ORDER BY created_at DESC LIMIT 4000
            """,
            tuple(params),
        ).fetchall()
    items = [dict(r) for r in rows]
    if not items:
        return {"samples": 0, "hit_rate_pct": None, "mape_pct": None, "alpha_pct": None}
    n = len(items)
    hit = sum(int(x.get("direction_hit") or 0) for x in items) / n * 100.0
    mape = sum(float(x.get("abs_error_pct") or 0.0) for x in items) / n
    exp_avg = sum(float(x.get("expected_return_pct") or 0.0) for x in items) / n
    real_avg = sum(float(x.get("realized_return_pct") or 0.0) for x in items) / n
    return {
        "samples": int(n),
        "hit_rate_pct": round(hit, 2),
        "mape_pct": round(mape, 2),
        "alpha_pct": round(real_avg - exp_avg, 2),
    }


def get_forecast_drift_signal(user_id: str, symbol: str, recent_n: int = 20, baseline_n: int = 60) -> dict[str, Any]:
    uid = str(user_id or "default_user")
    sym = str(symbol or "").strip().upper()
    if not sym:
        return {"status": "unknown", "message": "Thiếu mã cổ phiếu.", "delta_hit_pct": None, "delta_mape_pct": None}
    with _conn() as c:
        rows = c.execute(
            """
            SELECT direction_hit, abs_error_pct
            FROM forecast_records
            WHERE user_id=? AND symbol=? AND status='resolved'
            ORDER BY created_at DESC LIMIT 400
            """,
            (uid, sym),
        ).fetchall()
    items = [dict(r) for r in rows]
    if len(items) < max(12, recent_n + 6):
        return {
            "status": "insufficient_data",
            "message": "Chưa đủ dữ liệu resolved để phát hiện drift.",
            "delta_hit_pct": None,
            "delta_mape_pct": None,
        }
    recent = items[:recent_n]
    baseline = items[recent_n : recent_n + baseline_n]
    if not baseline:
        return {"status": "insufficient_data", "message": "Thiếu baseline để so sánh drift.", "delta_hit_pct": None, "delta_mape_pct": None}
    hit_recent = sum(int(x.get("direction_hit") or 0) for x in recent) / len(recent) * 100.0
    hit_base = sum(int(x.get("direction_hit") or 0) for x in baseline) / len(baseline) * 100.0
    mape_recent = sum(float(x.get("abs_error_pct") or 0.0) for x in recent) / len(recent)
    mape_base = sum(float(x.get("abs_error_pct") or 0.0) for x in baseline) / len(baseline)
    d_hit = hit_recent - hit_base
    d_mape = mape_recent - mape_base
    status = "stable"
    msg = "Hiệu năng dự báo ổn định."
    # Adaptive thresholds: stricter when enough samples, looser when sparse.
    sample_scale = min(1.0, len(items) / 120.0)
    hit_drop_th = 6.0 + (1.0 - sample_scale) * 4.0   # 6 -> 10
    mape_rise_th = 2.0 + (1.0 - sample_scale) * 2.0  # 2 -> 4
    hit_up_th = 5.0 + (1.0 - sample_scale) * 2.0     # 5 -> 7
    mape_down_th = 1.5 + (1.0 - sample_scale) * 1.5  # 1.5 -> 3

    if d_hit <= -hit_drop_th or d_mape >= mape_rise_th:
        status = "drift_down"
        msg = "Có dấu hiệu drift xấu: hit-rate giảm hoặc sai số tăng."
    elif d_hit >= hit_up_th and d_mape <= -mape_down_th:
        status = "improving"
        msg = "Mô hình đang cải thiện so với baseline."
    return {
        "status": status,
        "message": msg,
        "delta_hit_pct": round(d_hit, 2),
        "delta_mape_pct": round(d_mape, 2),
        "recent_hit_pct": round(hit_recent, 2),
        "recent_mape_pct": round(mape_recent, 2),
        "baseline_hit_pct": round(hit_base, 2),
        "baseline_mape_pct": round(mape_base, 2),
        "hit_drop_threshold_pct": round(hit_drop_th, 2),
        "mape_rise_threshold_pct": round(mape_rise_th, 2),
    }


def get_forecast_portfolio_dashboard(user_id: str, lookback_days: int = 720) -> dict[str, Any]:
    uid = str(user_id or "default_user")
    since = (datetime.now(timezone.utc) - timedelta(days=max(60, int(lookback_days)))).isoformat()
    with _conn() as c:
        rows = c.execute(
            """
            SELECT created_at, expected_return_pct, realized_return_pct, abs_error_pct, direction_hit
            FROM forecast_records
            WHERE user_id=? AND status='resolved' AND created_at>=?
            ORDER BY created_at DESC LIMIT 5000
            """,
            (uid, since),
        ).fetchall()
    items = [dict(r) for r in rows]
    if not items:
        return {"weekly": {}, "monthly": {}, "overall": {"samples": 0}}

    def _agg(window_days: int) -> dict[str, Any]:
        since_dt = datetime.now(timezone.utc) - timedelta(days=window_days)
        group = [x for x in items if (_parse_ts(str(x.get("created_at") or "")) or datetime.min.replace(tzinfo=timezone.utc)) >= since_dt]
        if not group:
            return {"samples": 0, "hit_rate_pct": None, "mape_pct": None, "alpha_pct": None}
        n = len(group)
        hit = sum(int(x.get("direction_hit") or 0) for x in group) / n * 100.0
        mape = sum(float(x.get("abs_error_pct") or 0.0) for x in group) / n
        exp_avg = sum(float(x.get("expected_return_pct") or 0.0) for x in group) / n
        real_avg = sum(float(x.get("realized_return_pct") or 0.0) for x in group) / n
        return {
            "samples": int(n),
            "hit_rate_pct": round(hit, 2),
            "mape_pct": round(mape, 2),
            "alpha_pct": round(real_avg - exp_avg, 2),
        }

    return {
        "weekly": _agg(7),
        "monthly": _agg(30),
        "overall": _agg(3650),
    }


def get_forecast_drift_streak(user_id: str, symbol: str, checks: int = 3) -> dict[str, Any]:
    uid = str(user_id or "default_user")
    sym = str(symbol or "").strip().upper()
    if not sym:
        return {"drift_down_streak": 0, "checks": checks}
    recent_window = [14, 20, 28, 36, 44]
    recent_window = recent_window[: max(1, min(checks, len(recent_window)))]
    streak = 0
    for n in recent_window:
        sig = get_forecast_drift_signal(uid, sym, recent_n=n, baseline_n=max(40, n * 2))
        if str(sig.get("status")) == "drift_down":
            streak += 1
        else:
            break
    return {"drift_down_streak": streak, "checks": len(recent_window)}


def export_forecast_health_report(user_id: str) -> str:
    uid = str(user_id or "default_user")
    report_dir = ROOT / "data" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "user_id": uid,
        "generated_at_utc": _utcnow_iso(),
        "portfolio_dashboard": get_forecast_portfolio_dashboard(uid),
        "leaderboard_top10": get_forecast_leaderboard(uid, limit=10),
    }
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out = report_dir / f"forecast_health_{uid}_{ts}.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return str(out)


def get_kpi_dashboard(user_id: str, days: int = 30) -> dict[str, Any]:
    since = (datetime.now(timezone.utc) - timedelta(days=max(1, int(days)))).isoformat()
    counts: dict[str, int] = {}
    with _conn() as c:
        ev_rows = c.execute(
            "SELECT event, COUNT(*) AS n FROM events WHERE user_id=? AND ts>=? GROUP BY event",
            (user_id, since),
        ).fetchall()
        for r in ev_rows:
            counts[str(r["event"])] = int(r["n"])
        alerts_total = int(c.execute("SELECT COUNT(*) AS n FROM alerts WHERE user_id=?", (user_id,)).fetchone()["n"])
        holdings_total = int(c.execute("SELECT COUNT(*) AS n FROM holdings WHERE user_id=?", (user_id,)).fetchone()["n"])
        events_total = int(
            c.execute("SELECT COUNT(*) AS n FROM events WHERE user_id=? AND ts>=?", (user_id, since)).fetchone()["n"]
        )
    return {
        "events_total": events_total,
        "by_event": counts,
        "alerts_total": alerts_total,
        "holdings_total": holdings_total,
        "plan_id": get_user_plan(user_id),
    }


def _parse_ts(s: str) -> datetime | None:
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None


def get_cohort_kpi(user_id: str) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    since7 = (now - timedelta(days=7)).isoformat()
    since30 = (now - timedelta(days=30)).isoformat()
    with _conn() as c:
        ev7 = c.execute("SELECT event, ts FROM events WHERE user_id=? AND ts>=?", (user_id, since7)).fetchall()
        ev30 = c.execute("SELECT event, ts FROM events WHERE user_id=? AND ts>=?", (user_id, since30)).fetchall()

    def _count(rows: list[sqlite3.Row], name: str) -> int:
        return sum(1 for r in rows if str(r["event"]) == name)

    active_days_7 = len({str(r["ts"])[:10] for r in ev7 if r["ts"]})
    active_days_30 = len({str(r["ts"])[:10] for r in ev30 if r["ts"]})

    return {
        "active_days_7": active_days_7,
        "active_days_30": active_days_30,
        "analysis_7": _count(ev7, "analysis_completed"),
        "analysis_30": _count(ev30, "analysis_completed"),
        "alerts_created_7": _count(ev7, "alert_created"),
        "alerts_created_30": _count(ev30, "alert_created"),
        "alerts_dispatched_7": _count(ev7, "alert_dispatched"),
        "alerts_dispatched_30": _count(ev30, "alert_dispatched"),
        "upgrade_intent_7": _count(ev7, "upgrade_intent"),
        "upgrade_intent_30": _count(ev30, "upgrade_intent"),
        "upgrade_success_7": _count(ev7, "upgrade_success"),
        "upgrade_success_30": _count(ev30, "upgrade_success"),
    }


def add_holding(user_id: str, symbol: str, quantity: float, avg_cost: float) -> None:
    if quantity <= 0 or avg_cost <= 0:
        return
    sym = symbol.strip().upper()
    with _conn() as c:
        r = c.execute("SELECT quantity, avg_cost FROM holdings WHERE user_id=? AND symbol=?", (user_id, sym)).fetchone()
        if r is None:
            c.execute(
                "INSERT INTO holdings(user_id, symbol, quantity, avg_cost, updated_at) VALUES (?, ?, ?, ?, ?)",
                (user_id, sym, float(quantity), float(avg_cost), _utcnow_iso()),
            )
        else:
            q_old = float(r["quantity"] or 0)
            c_old = float(r["avg_cost"] or 0)
            q_new = q_old + float(quantity)
            avg_new = ((q_old * c_old + float(quantity) * float(avg_cost)) / q_new) if q_new > 0 else c_old
            c.execute(
                "UPDATE holdings SET quantity=?, avg_cost=?, updated_at=? WHERE user_id=? AND symbol=?",
                (q_new, avg_new, _utcnow_iso(), user_id, sym),
            )


def list_holdings(user_id: str) -> list[dict[str, Any]]:
    with _conn() as c:
        rows = c.execute(
            "SELECT symbol, quantity, avg_cost FROM holdings WHERE user_id=? ORDER BY symbol ASC",
            (user_id,),
        ).fetchall()
    return [{"symbol": str(r["symbol"]), "quantity": float(r["quantity"]), "avg_cost": float(r["avg_cost"])} for r in rows]


def add_alert(user_id: str, symbol: str, alert_type: str, threshold: float, note: str = "") -> None:
    if threshold <= 0:
        return
    alert_id = hashlib.md5(f"{symbol}|{alert_type}|{threshold}|{datetime.now(timezone.utc).isoformat()}".encode()).hexdigest()[:12]
    with _conn() as c:
        c.execute(
            """
            INSERT INTO alerts(id, user_id, symbol, type, threshold, note, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (alert_id, user_id, symbol.strip().upper(), alert_type, float(threshold), note.strip(), _utcnow_iso()),
        )


def list_alerts(user_id: str) -> list[dict[str, Any]]:
    with _conn() as c:
        rows = c.execute(
            """
            SELECT id, symbol, type, threshold, note, created_at
            FROM alerts WHERE user_id=? ORDER BY created_at DESC
            """,
            (user_id,),
        ).fetchall()
    return [
        {
            "id": str(r["id"]),
            "symbol": str(r["symbol"]),
            "type": str(r["type"]),
            "threshold": float(r["threshold"]),
            "note": str(r["note"] or ""),
            "created_at": str(r["created_at"]),
        }
        for r in rows
    ]


def portfolio_snapshot(user_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for h in list_holdings(user_id):
        sym = str(h.get("symbol") or "").upper()
        qty = float(h.get("quantity") or 0)
        avg_cost = float(h.get("avg_cost") or 0)
        if not sym or qty <= 0:
            continue
        snap = fetch_financial_snapshot(sym) or {}
        px = float(snap.get("price") or 0)
        market_value = px * qty
        cost_value = avg_cost * qty
        pnl = market_value - cost_value
        pnl_pct = (pnl / cost_value * 100.0) if cost_value > 0 else 0.0
        rows.append(
            {
                "Mã": sym,
                "SL": int(round(qty)),
                "Giá vốn": round(avg_cost, 2),
                "Giá hiện tại": round(px, 2),
                "Giá trị": round(market_value, 0),
                "Lãi/Lỗ": round(pnl, 0),
                "Lãi/Lỗ %": round(pnl_pct, 2),
            }
        )
    return rows


def evaluate_alerts(user_id: str) -> list[dict[str, Any]]:
    fired: list[dict[str, Any]] = []
    alerts = list_alerts(user_id)
    for a in alerts:
        sym = str(a.get("symbol") or "").upper()
        tp = str(a.get("type") or "price_above")
        th = float(a.get("threshold") or 0)
        if not sym or th <= 0:
            continue
        snap = fetch_financial_snapshot(sym) or {}
        px = float(snap.get("price") or 0)
        if px <= 0:
            continue
        trigger = False
        if tp == "price_above" and px >= th:
            trigger = True
        if tp == "price_below" and px <= th:
            trigger = True
        if trigger:
            fired.append(
                {
                    "alert_id": str(a.get("id") or ""),
                    "Mã": sym,
                    "Loại": tp,
                    "Ngưỡng": th,
                    "Giá hiện tại": px,
                    "Ghi chú": str(a.get("note") or ""),
                }
            )
    return fired


def dispatch_alert_notifications(
    user_id: str,
    fired_alerts: list[dict[str, Any]],
    telegram_bot_token: str = "",
    telegram_chat_id: str = "",
) -> dict[str, int]:
    sent = 0
    skipped = 0
    failed = 0
    token = (telegram_bot_token or "").strip()
    chat_id = (telegram_chat_id or "").strip()
    if not token or not chat_id:
        return {"sent": 0, "skipped": len(fired_alerts), "failed": 0}

    for a in fired_alerts:
        raw_key = f"{a.get('alert_id')}|{a.get('Mã')}|{a.get('Loại')}|{a.get('Ngưỡng')}|{a.get('Giá hiện tại')}"
        key = hashlib.md5(raw_key.encode()).hexdigest()
        with _conn() as c:
            exists = c.execute("SELECT 1 FROM notification_sent WHERE dedup_key=?", (key,)).fetchone() is not None
        if exists:
            skipped += 1
            continue
        text = (
            f"🔔 Alert kích hoạt\n"
            f"Mã: {a.get('Mã')}\n"
            f"Loại: {a.get('Loại')}\n"
            f"Ngưỡng: {a.get('Ngưỡng')}\n"
            f"Giá hiện tại: {a.get('Giá hiện tại')}\n"
            f"Ghi chú: {a.get('Ghi chú')}"
        )
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        try:
            r = requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=10)
            if r.status_code == 200:
                sent += 1
                with _conn() as c:
                    c.execute(
                        "INSERT OR IGNORE INTO notification_sent(dedup_key, user_id, sent_at) VALUES (?, ?, ?)",
                        (key, user_id, _utcnow_iso()),
                    )
            else:
                failed += 1
        except requests.RequestException:
            failed += 1
    return {"sent": sent, "skipped": skipped, "failed": failed}


def dispatch_external_notifications(
    fired_alerts: list[dict[str, Any]],
    webhook_url: str = "",
    email_to: str = "",
    *,
    smtp_host: str = "",
    smtp_port: int = 587,
    smtp_user: str = "",
    smtp_password: str = "",
    smtp_from: str = "",
) -> dict[str, int]:
    sent = 0
    failed = 0
    if not fired_alerts:
        return {"sent": 0, "failed": 0}

    hook = (webhook_url or "").strip()
    if hook:
        try:
            r = requests.post(hook, json={"alerts": fired_alerts}, timeout=10)
            if r.status_code < 300:
                sent += 1
            else:
                failed += 1
        except requests.RequestException:
            failed += 1

    if email_to and smtp_host and smtp_user and smtp_password:
        try:
            body = "\n\n".join(
                [
                    (
                        f"Mã: {a.get('Mã')}\n"
                        f"Loại: {a.get('Loại')}\n"
                        f"Ngưỡng: {a.get('Ngưỡng')}\n"
                        f"Giá hiện tại: {a.get('Giá hiện tại')}\n"
                        f"Ghi chú: {a.get('Ghi chú')}"
                    )
                    for a in fired_alerts
                ]
            )
            msg = MIMEText(body, _charset="utf-8")
            msg["Subject"] = "Stock Alert Triggered"
            msg["From"] = smtp_from or smtp_user
            msg["To"] = email_to
            with smtplib.SMTP(smtp_host, int(smtp_port), timeout=10) as server:
                server.starttls()
                server.login(smtp_user, smtp_password)
                server.sendmail(msg["From"], [email_to], msg.as_string())
            sent += 1
        except Exception:
            failed += 1
    return {"sent": sent, "failed": failed}


def dispatch_text_notifications(
    text: str,
    telegram_bot_token: str = "",
    telegram_chat_id: str = "",
    webhook_url: str = "",
    email_to: str = "",
    *,
    smtp_host: str = "",
    smtp_port: int = 587,
    smtp_user: str = "",
    smtp_password: str = "",
    smtp_from: str = "",
) -> dict[str, int]:
    sent = 0
    failed = 0
    msg = (text or "").strip()
    if not msg:
        return {"sent": 0, "failed": 0}

    token = (telegram_bot_token or "").strip()
    chat_id = (telegram_chat_id or "").strip()
    if token and chat_id:
        try:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            r = requests.post(url, json={"chat_id": chat_id, "text": msg}, timeout=10)
            if r.status_code == 200:
                sent += 1
            else:
                failed += 1
        except requests.RequestException:
            failed += 1

    hook = (webhook_url or "").strip()
    if hook:
        try:
            r = requests.post(hook, json={"message": msg}, timeout=10)
            if r.status_code < 300:
                sent += 1
            else:
                failed += 1
        except requests.RequestException:
            failed += 1

    if email_to and smtp_host and smtp_user and smtp_password:
        try:
            m = MIMEText(msg, _charset="utf-8")
            m["Subject"] = "Daily Playbook"
            m["From"] = smtp_from or smtp_user
            m["To"] = email_to
            with smtplib.SMTP(smtp_host, int(smtp_port), timeout=10) as server:
                server.starttls()
                server.login(smtp_user, smtp_password)
                server.sendmail(m["From"], [email_to], m.as_string())
            sent += 1
        except Exception:
            failed += 1
    return {"sent": sent, "failed": failed}


def enqueue_notification(user_id: str, kind: str, payload: dict[str, Any], delay_seconds: int = 60) -> None:
    now = datetime.now(timezone.utc)
    next_retry = now + timedelta(seconds=max(1, int(delay_seconds)))
    with _conn() as c:
        c.execute(
            """
            INSERT INTO notification_queue(user_id, kind, payload_json, attempts, status, last_error, next_retry_at, created_at, updated_at)
            VALUES (?, ?, ?, 0, 'pending', '', ?, ?, ?)
            """,
            (user_id, kind, json.dumps(payload, ensure_ascii=False), next_retry.isoformat(), now.isoformat(), now.isoformat()),
        )


def process_notification_queue(max_jobs: int = 20) -> dict[str, int]:
    now = datetime.now(timezone.utc).isoformat()
    sent = 0
    failed = 0
    retried = 0
    with _conn() as c:
        jobs = c.execute(
            """
            SELECT id, user_id, kind, payload_json, attempts
            FROM notification_queue
            WHERE status='pending' AND next_retry_at<=?
            ORDER BY id ASC LIMIT ?
            """,
            (now, int(max_jobs)),
        ).fetchall()
    for j in jobs:
        jid = int(j["id"])
        kind = str(j["kind"] or "")
        attempts = int(j["attempts"] or 0)
        try:
            payload = json.loads(str(j["payload_json"] or "{}"))
        except ValueError:
            payload = {}

        ok = False
        if kind == "text":
            stat = dispatch_text_notifications(
                payload.get("text", ""),
                telegram_bot_token=payload.get("telegram_bot_token", ""),
                telegram_chat_id=payload.get("telegram_chat_id", ""),
                webhook_url=payload.get("webhook_url", ""),
                email_to=payload.get("email_to", ""),
                smtp_host=payload.get("smtp_host", ""),
                smtp_port=int(payload.get("smtp_port", 587) or 587),
                smtp_user=payload.get("smtp_user", ""),
                smtp_password=payload.get("smtp_password", ""),
                smtp_from=payload.get("smtp_from", ""),
            )
            ok = int(stat.get("sent") or 0) > 0 and int(stat.get("failed") or 0) == 0

        elif kind == "alerts":
            stat = dispatch_alert_notifications(
                str(j["user_id"]),
                payload.get("fired_alerts", []),
                telegram_bot_token=payload.get("telegram_bot_token", ""),
                telegram_chat_id=payload.get("telegram_chat_id", ""),
            )
            ok = int(stat.get("sent") or 0) > 0 and int(stat.get("failed") or 0) == 0

        if ok:
            with _conn() as c:
                c.execute(
                    "UPDATE notification_queue SET status='done', updated_at=? WHERE id=?",
                    (_utcnow_iso(), jid),
                )
            sent += 1
        else:
            attempts += 1
            if attempts >= 5:
                with _conn() as c:
                    c.execute(
                        "UPDATE notification_queue SET status='failed', attempts=?, last_error=?, updated_at=? WHERE id=?",
                        (attempts, "max_retries", _utcnow_iso(), jid),
                    )
                failed += 1
            else:
                next_retry = datetime.now(timezone.utc) + timedelta(seconds=60 * attempts)
                with _conn() as c:
                    c.execute(
                        """
                        UPDATE notification_queue
                        SET attempts=?, last_error=?, next_retry_at=?, updated_at=?
                        WHERE id=?
                        """,
                        (attempts, "retry_scheduled", next_retry.isoformat(), _utcnow_iso(), jid),
                    )
                retried += 1
    return {"sent": sent, "failed": failed, "retried": retried, "processed": len(jobs)}


def get_admin_kpi(days: int = 30) -> dict[str, Any]:
    since = (datetime.now(timezone.utc) - timedelta(days=max(1, int(days)))).isoformat()
    with _conn() as c:
        users = int(c.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"])
        alerts = int(c.execute("SELECT COUNT(*) AS n FROM alerts").fetchone()["n"])
        holdings = int(c.execute("SELECT COUNT(*) AS n FROM holdings").fetchone()["n"])
        decisions = int(c.execute("SELECT COUNT(*) AS n FROM decisions").fetchone()["n"])
        trades = int(c.execute("SELECT COUNT(*) AS n FROM trades").fetchone()["n"])
        events = int(c.execute("SELECT COUNT(*) AS n FROM events WHERE ts>=?", (since,)).fetchone()["n"])
        active_users = int(c.execute("SELECT COUNT(DISTINCT user_id) AS n FROM events WHERE ts>=?", (since,)).fetchone()["n"])
        plan_rows = c.execute("SELECT plan_id, COUNT(*) AS n FROM users GROUP BY plan_id").fetchall()
        ev_rows = c.execute("SELECT event, COUNT(*) AS n FROM events WHERE ts>=? GROUP BY event", (since,)).fetchall()
        q_rows = c.execute(
            "SELECT status, COUNT(*) AS n FROM notification_queue GROUP BY status ORDER BY status ASC"
        ).fetchall()
    return {
        "users_total": users,
        "active_users_period": active_users,
        "events_period": events,
        "alerts_total": alerts,
        "holdings_total": holdings,
        "decisions_total": decisions,
        "trades_total": trades,
        "plans": [{"plan_id": str(r["plan_id"]), "count": int(r["n"])} for r in plan_rows],
        "events_by_type": [{"event": str(r["event"]), "count": int(r["n"])} for r in ev_rows],
        "notification_queue": [{"status": str(r["status"]), "count": int(r["n"])} for r in q_rows],
    }


def open_trade(
    user_id: str,
    symbol: str,
    quantity: float,
    entry_price: float,
    *,
    decision_id: int | None = None,
    side: str = "LONG",
    entry_fee: float = 0.0,
    entry_note: str = "",
) -> tuple[bool, str]:
    sym = (symbol or "").strip().upper()
    sd = (side or "LONG").strip().upper()
    if sd not in ("LONG",):
        sd = "LONG"
    qty = float(quantity or 0)
    ep = float(entry_price or 0)
    fee = max(0.0, float(entry_fee or 0))
    if not sym:
        return False, "Mã giao dịch rỗng."
    if qty <= 0 or ep <= 0:
        return False, "Số lượng và giá mở phải > 0."
    did = int(decision_id or 0)
    if did <= 0:
        did = None
    if did is not None:
        with _conn() as c:
            d = c.execute(
                "SELECT id FROM decisions WHERE id=? AND user_id=?",
                (did, user_id),
            ).fetchone()
        if d is None:
            return False, "decision_id không tồn tại cho user này."
    with _conn() as c:
        c.execute(
            """
            INSERT INTO trades(user_id, decision_id, symbol, side, quantity, entry_price, entry_fee, entry_note, opened_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN')
            """,
            (user_id, did, sym, sd, qty, ep, fee, str(entry_note or "").strip(), _utcnow_iso()),
        )
    return True, "Đã mở trade journal."


def close_trade(
    user_id: str,
    trade_id: int,
    exit_price: float,
    *,
    exit_fee: float = 0.0,
    exit_note: str = "",
) -> tuple[bool, str]:
    tid = int(trade_id or 0)
    xp = float(exit_price or 0)
    fee = max(0.0, float(exit_fee or 0))
    if tid <= 0 or xp <= 0:
        return False, "trade_id và giá đóng phải hợp lệ."
    with _conn() as c:
        r = c.execute(
            "SELECT id, status FROM trades WHERE id=? AND user_id=?",
            (tid, user_id),
        ).fetchone()
        if r is None:
            return False, "Không tìm thấy trade."
        if str(r["status"]) != "OPEN":
            return False, "Trade đã đóng trước đó."
        c.execute(
            """
            UPDATE trades
            SET status='CLOSED', exit_price=?, exit_fee=?, exit_note=?, closed_at=?
            WHERE id=? AND user_id=?
            """,
            (xp, fee, str(exit_note or "").strip(), _utcnow_iso(), tid, user_id),
        )
    return True, "Đã đóng trade journal."


def list_trades(user_id: str, limit: int = 200) -> list[dict[str, Any]]:
    lim = max(1, min(int(limit or 200), 1000))
    with _conn() as c:
        rows = c.execute(
            """
            SELECT id, symbol, side, quantity, entry_price, entry_fee, entry_note, opened_at,
                   status, exit_price, exit_fee, exit_note, closed_at, decision_id
            FROM trades
            WHERE user_id=?
            ORDER BY id DESC
            LIMIT ?
            """,
            (user_id, lim),
        ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        qty = float(r["quantity"] or 0)
        ep = float(r["entry_price"] or 0)
        ef = float(r["entry_fee"] or 0)
        xp = float(r["exit_price"] or 0) if r["exit_price"] is not None else None
        xf = float(r["exit_fee"] or 0) if r["exit_fee"] is not None else None
        realized = None
        realized_pct = None
        if xp is not None:
            realized = (xp - ep) * qty - ef - (xf or 0.0)
            basis = ep * qty + ef
            realized_pct = (realized / basis * 100.0) if basis > 0 else 0.0
        out.append(
            {
                "ID": int(r["id"]),
                "Decision ID": int(r["decision_id"]) if r["decision_id"] is not None else None,
                "Mã": str(r["symbol"]),
                "SL": int(round(qty)),
                "Giá mở": round(ep, 2),
                "Phí mở": round(ef, 2),
                "Trạng thái": str(r["status"]),
                "Giá đóng": round(xp, 2) if xp is not None else None,
                "Phí đóng": round(float(xf or 0), 2) if xp is not None else None,
                "Realized PnL": round(realized, 2) if realized is not None else None,
                "Realized %": round(realized_pct, 2) if realized_pct is not None else None,
                "Mở lúc": str(r["opened_at"]),
                "Đóng lúc": str(r["closed_at"] or ""),
                "Ghi chú mở": str(r["entry_note"] or ""),
                "Ghi chú đóng": str(r["exit_note"] or ""),
            }
        )
    return out


def execution_vs_plan_report(user_id: str, limit: int = 200) -> list[dict[str, Any]]:
    trades = list_trades(user_id, limit=limit)
    if not trades:
        return []
    decisions = {d["id"]: d for d in list_decisions(user_id, limit=2000)}
    out: list[dict[str, Any]] = []
    for t in trades:
        did = t.get("Decision ID")
        if did is None or did not in decisions:
            continue
        d = decisions[did]
        plan_entry = float(d.get("entry_price") or 0)
        plan_sl = float(d.get("stop_loss") or 0)
        plan_tp = float(d.get("take_profit") or 0)
        real_entry = float(t.get("Giá mở") or 0)
        real_exit = float(t.get("Giá đóng") or 0) if t.get("Giá đóng") is not None else None
        entry_slippage_pct = ((real_entry - plan_entry) / plan_entry * 100.0) if plan_entry > 0 and real_entry > 0 else 0.0
        exit_vs_tp_pct = (
            ((real_exit - plan_tp) / plan_tp * 100.0) if plan_tp > 0 and real_exit is not None else None
        )
        exit_vs_sl_pct = (
            ((real_exit - plan_sl) / plan_sl * 100.0) if plan_sl > 0 and real_exit is not None else None
        )
        discipline = "GOOD"
        if abs(entry_slippage_pct) > 2.0:
            discipline = "WARN_ENTRY"
        if real_exit is not None and real_exit < plan_sl * 0.99:
            discipline = "BAD_SLIP_BELOW_SL"
        out.append(
            {
                "Trade ID": t.get("ID"),
                "Decision ID": did,
                "Mã": t.get("Mã"),
                "Plan Entry": round(plan_entry, 2),
                "Real Entry": round(real_entry, 2),
                "Entry Slippage %": round(entry_slippage_pct, 2),
                "Plan SL": round(plan_sl, 2),
                "Plan TP": round(plan_tp, 2),
                "Real Exit": round(real_exit, 2) if real_exit is not None else None,
                "Exit vs TP %": round(exit_vs_tp_pct, 2) if exit_vs_tp_pct is not None else None,
                "Exit vs SL %": round(exit_vs_sl_pct, 2) if exit_vs_sl_pct is not None else None,
                "Discipline": discipline,
                "Realized %": t.get("Realized %"),
            }
        )
    return out


def realized_performance(user_id: str, days: int = 30) -> dict[str, Any]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=max(1, int(days)))).isoformat()
    rows = list_trades(user_id, limit=2000)
    closed = [r for r in rows if r.get("Trạng thái") == "CLOSED" and str(r.get("Đóng lúc") or "") >= cutoff[:10]]
    pnl_vals = [float(r.get("Realized PnL") or 0) for r in closed]
    pct_vals = [float(r.get("Realized %") or 0) for r in closed]
    wins = sum(1 for x in pnl_vals if x > 0)
    losses = sum(1 for x in pnl_vals if x < 0)
    return {
        "closed_trades": len(closed),
        "realized_pnl_total": round(sum(pnl_vals), 2) if pnl_vals else 0.0,
        "realized_pct_avg": round(sum(pct_vals) / len(pct_vals), 2) if pct_vals else 0.0,
        "win_rate_pct": round((wins / (wins + losses) * 100.0), 1) if (wins + losses) > 0 else 0.0,
    }


def user_aha_progress(user_id: str) -> dict[str, Any]:
    holdings_n = len(list_holdings(user_id))
    alerts_n = len(list_alerts(user_id))
    decisions_n = len(list_decisions(user_id, limit=500))
    trades = list_trades(user_id, limit=500)
    opened_n = len([t for t in trades if str(t.get("Trạng thái") or "") in ("OPEN", "CLOSED")])
    closed_n = len([t for t in trades if str(t.get("Trạng thái") or "") == "CLOSED"])
    kpi7 = get_kpi_dashboard(user_id, days=7)
    by_event = kpi7.get("by_event") or {}
    analysis_n = int(by_event.get("analysis_completed") or 0)

    steps = [
        {"id": "analysis", "label": "Phân tích ít nhất 1 mã", "done": analysis_n >= 1},
        {"id": "holding", "label": "Thêm 1 vị thế danh mục", "done": holdings_n >= 1},
        {"id": "alert", "label": "Tạo 1 cảnh báo giá", "done": alerts_n >= 1},
        {"id": "decision", "label": "Lưu 1 quyết định đầu tư", "done": decisions_n >= 1},
        {"id": "trade_open", "label": "Mở 1 trade journal", "done": opened_n >= 1},
        {"id": "trade_close", "label": "Đóng 1 trade journal", "done": closed_n >= 1},
    ]
    done_count = sum(1 for s in steps if s["done"])
    pct = round(done_count / len(steps) * 100.0, 1) if steps else 0.0
    next_step = next((s for s in steps if not s["done"]), None)
    return {
        "steps": steps,
        "completed_steps": done_count,
        "total_steps": len(steps),
        "progress_pct": pct,
        "next_step": next_step["label"] if next_step else "Bạn đã hoàn tất hành trình Aha cơ bản.",
    }


def customer_value_snapshot(user_id: str) -> dict[str, Any]:
    rp30 = realized_performance(user_id, days=30)
    mv = monthly_value_report(user_id)
    aha = user_aha_progress(user_id)
    analysis_30 = int(mv.get("analysis_30d") or 0)
    # Conservative estimate: each guided analysis saves ~5 mins research wandering.
    time_saved_min = analysis_30 * 5
    return {
        "realized_pnl_30d": float(rp30.get("realized_pnl_total") or 0.0),
        "realized_win_rate_30d": float(rp30.get("win_rate_pct") or 0.0),
        "discipline_score": float(mv.get("discipline_score") or 0.0),
        "analysis_30d": analysis_30,
        "alerts_30d": int(mv.get("alerts_dispatched_30d") or 0),
        "time_saved_min_est": int(time_saved_min),
        "aha_progress_pct": float(aha.get("progress_pct") or 0.0),
    }


def next_best_action(user_id: str) -> dict[str, Any]:
    aha = user_aha_progress(user_id)
    rp30 = realized_performance(user_id, days=30)
    sc = decision_scorecard(user_id, limit=200)
    kpi7 = get_kpi_dashboard(user_id, days=7)
    by_event = kpi7.get("by_event") or {}
    analyses_7d = int(by_event.get("analysis_completed") or 0)
    alerts_n = len(list_alerts(user_id))
    decisions_n = len(list_decisions(user_id, limit=300))
    trades = list_trades(user_id, limit=300)
    open_trades = [t for t in trades if str(t.get("Trạng thái") or "") == "OPEN"]
    closed_trades = int(rp30.get("closed_trades") or 0)
    discipline = float(sc.get("discipline_score") or 0.0)

    # Priority ladder: drive activation -> discipline -> retention loop.
    if analyses_7d == 0:
        return {
            "id": "do_first_analysis",
            "title": "Phân tích 1 mã ngay hôm nay",
            "reason": "Bạn chưa có phiên phân tích nào trong 7 ngày gần nhất.",
            "target_impact": "Mở khóa dữ liệu định giá + risk plan để ra quyết định đúng hơn.",
            "priority": 100,
        }
    if decisions_n == 0:
        return {
            "id": "log_first_decision",
            "title": "Lưu quyết định đầu tiên (Entry/SL/TP)",
            "reason": "Chưa có quyết định được lưu nên chưa thể hậu kiểm chiến lược.",
            "target_impact": "Bắt đầu tạo data moat cá nhân để cải thiện win-rate theo thời gian.",
            "priority": 95,
        }
    if alerts_n == 0:
        return {
            "id": "create_first_alert",
            "title": "Tạo cảnh báo SL/TP cho mã đang theo dõi",
            "reason": "Bạn chưa bật lớp phòng thủ cảnh báo tự động.",
            "target_impact": "Giảm bỏ lỡ điểm vào/ra và giảm quyết định cảm tính.",
            "priority": 90,
        }
    if open_trades and discipline < 65:
        return {
            "id": "tighten_execution",
            "title": "Rà soát kỷ luật thực thi cho trade đang mở",
            "reason": f"Discipline score hiện {discipline:.1f}/100, dưới ngưỡng an toàn vận hành.",
            "target_impact": "Giảm sai lệch so với kế hoạch, hạn chế drawdown không cần thiết.",
            "priority": 88,
        }
    if closed_trades < 3:
        return {
            "id": "close_trade_samples",
            "title": "Hoàn tất tối thiểu 3 vòng trade journal",
            "reason": "Dữ liệu realized còn mỏng, chưa đủ mẫu để coach cá nhân hóa sâu.",
            "target_impact": "Tăng độ tin cậy cho Adaptive Coach và Position Sizing.",
            "priority": 82,
        }
    if float(aha.get("progress_pct") or 0.0) < 100.0:
        return {
            "id": "complete_aha_journey",
            "title": "Hoàn tất Aha Journey 7 ngày",
            "reason": f"Tiến độ hiện {aha.get('progress_pct', 0)}%, còn bước nền tảng chưa hoàn tất.",
            "target_impact": "Đạt đầy đủ vòng lặp phân tích -> quyết định -> thực thi -> hậu kiểm.",
            "priority": 78,
        }
    return {
        "id": "optimize_rr_quality",
        "title": "Nâng chất lượng RR trung bình lên >= 2.0",
        "reason": "Bạn đã qua giai đoạn kích hoạt, phù hợp bước tối ưu hiệu suất.",
        "target_impact": "Tăng lợi nhuận kỳ vọng trên mỗi đơn vị rủi ro.",
        "priority": 70,
    }


def today_action_board(user_id: str) -> list[dict[str, Any]]:
    """
    Top 3 tasks for today's execution, ordered by impact.
    """
    aha = user_aha_progress(user_id)
    kpi7 = get_kpi_dashboard(user_id, days=7)
    by_event = kpi7.get("by_event") or {}
    analyses_7 = int(by_event.get("analysis_completed") or 0)
    decisions_n = len(list_decisions(user_id, limit=500))
    alerts_n = len(list_alerts(user_id))
    open_trades = [t for t in list_trades(user_id, limit=500) if str(t.get("Trạng thái") or "") == "OPEN"]
    sc = decision_scorecard(user_id, limit=200)
    discipline = float(sc.get("discipline_score") or 0.0)

    tasks: list[dict[str, Any]] = [
        {
            "task": "Phân tích ít nhất 1 mã hôm nay",
            "status": "done" if analyses_7 >= 1 else "todo",
            "impact": "Mở khóa plan hành động theo dữ liệu mới nhất.",
        },
        {
            "task": "Lưu 1 decision có Entry/SL/TP",
            "status": "done" if decisions_n >= 1 else "todo",
            "impact": "Tạo dữ liệu hậu kiểm để app học phong cách của bạn.",
        },
        {
            "task": "Tạo alert SL/TP cho mã ưu tiên",
            "status": "done" if alerts_n >= 1 else "todo",
            "impact": "Giảm bỏ lỡ điểm vào/ra và giảm cảm tính.",
        },
    ]
    if open_trades and discipline < 65:
        tasks.append(
            {
                "task": "Rà soát trade mở có lệch kế hoạch",
                "status": "todo",
                "impact": "Giảm drawdown do phá kỷ luật thực thi.",
            }
        )
    if float(aha.get("progress_pct") or 0) >= 100:
        tasks.append(
            {
                "task": "Nâng RR trung bình lên >= 2.0",
                "status": "todo",
                "impact": "Tăng hiệu suất vốn trên mỗi vị thế.",
            }
        )
    # Prioritize unfinished tasks first, keep top 3.
    tasks = sorted(tasks, key=lambda x: 0 if x["status"] == "todo" else 1)
    return tasks[:3]


def proof_of_value_report(user_id: str) -> dict[str, Any]:
    """
    Compact, customer-facing value proof with conservative estimates.
    """
    rp30 = realized_performance(user_id, days=30)
    sc = decision_scorecard(user_id, limit=200)
    mv = monthly_value_report(user_id)
    analyses = int(mv.get("analysis_30d") or 0)
    alerts_sent = int(mv.get("alerts_dispatched_30d") or 0)
    realized = float(rp30.get("realized_pnl_total") or 0.0)
    win_rate = float(rp30.get("win_rate_pct") or 0.0)
    rr_avg = float(sc.get("rr_avg") or 0.0)
    discipline = float(sc.get("discipline_score") or 0.0)

    # Conservative value proxies:
    # - each alert dispatched considered potential error-avoidance touchpoint
    # - each analysis saves ~5 minutes in manual searching
    time_saved_min = analyses * 5
    risk_shield_score = min(100.0, alerts_sent * 2.0 + max(0.0, discipline - 40.0))

    return {
        "realized_pnl_30d": round(realized, 2),
        "win_rate_realized_30d": round(win_rate, 1),
        "rr_avg": round(rr_avg, 2),
        "discipline_score": round(discipline, 1),
        "analyses_30d": analyses,
        "alerts_dispatched_30d": alerts_sent,
        "time_saved_min_est": int(time_saved_min),
        "risk_shield_score": round(risk_shield_score, 1),
        "value_message": (
            f"30 ngày: Realized PnL {realized:,.0f} VND | Win-rate {win_rate:.1f}% | "
            f"Discipline {discipline:.1f}/100 | Tiết kiệm ~{time_saved_min} phút."
        ),
    }


def overdue_action_reminders(user_id: str) -> list[dict[str, Any]]:
    """
    Build actionable reminders when key behaviors are missing in recent windows.
    """
    out: list[dict[str, Any]] = []
    kpi1 = get_kpi_dashboard(user_id, days=1)
    kpi7 = get_kpi_dashboard(user_id, days=7)
    by1 = kpi1.get("by_event") or {}
    by7 = kpi7.get("by_event") or {}
    analysis_1d = int(by1.get("analysis_completed") or 0)
    analysis_7d = int(by7.get("analysis_completed") or 0)
    decisions_n = len(list_decisions(user_id, limit=500))
    alerts_n = len(list_alerts(user_id))
    open_trades = [t for t in list_trades(user_id, limit=500) if str(t.get("Trạng thái") or "") == "OPEN"]
    sc = decision_scorecard(user_id, limit=200)
    discipline = float(sc.get("discipline_score") or 0.0)

    if analysis_1d == 0:
        out.append(
            {
                "id": "no_analysis_24h",
                "severity": "high",
                "title": "24h chưa có phiên phân tích mới",
                "message": "Thực hiện ít nhất 1 phân tích hôm nay để tránh quyết định theo cảm tính.",
            }
        )
    if analysis_7d == 0:
        out.append(
            {
                "id": "no_analysis_7d",
                "severity": "high",
                "title": "7 ngày chưa phân tích",
                "message": "Rủi ro bỏ lỡ biến động thị trường tăng cao. Nên kích hoạt lại chu kỳ phân tích.",
            }
        )
    if decisions_n == 0:
        out.append(
            {
                "id": "no_decision_logged",
                "severity": "medium",
                "title": "Chưa lưu quyết định đầu tư",
                "message": "Lưu Entry/SL/TP để app có thể hậu kiểm và cải thiện khuyến nghị.",
            }
        )
    if alerts_n == 0:
        out.append(
            {
                "id": "no_alerts",
                "severity": "medium",
                "title": "Chưa bật cảnh báo phòng thủ",
                "message": "Thiết lập alert SL/TP để không bỏ lỡ điểm hành động quan trọng.",
            }
        )
    if open_trades and discipline < 65:
        out.append(
            {
                "id": "discipline_low_with_open_trades",
                "severity": "high",
                "title": "Kỷ luật thấp khi còn trade mở",
                "message": "Ưu tiên rà soát Kế hoạch vs Thực thi để giảm rủi ro phá vỡ nguyên tắc.",
            }
        )
    return out[:5]


def value_maturity_score(user_id: str) -> dict[str, Any]:
    """
    Customer-facing maturity score (0-10): activation + discipline + execution loop.
    """
    aha = user_aha_progress(user_id)
    sc = decision_scorecard(user_id, limit=200)
    rp = realized_performance(user_id, days=30)
    kpi30 = get_kpi_dashboard(user_id, days=30)
    by_event = kpi30.get("by_event") or {}
    analyses = int(by_event.get("analysis_completed") or 0)
    alerts = len(list_alerts(user_id))
    decisions = len(list_decisions(user_id, limit=1000))
    closed = int(rp.get("closed_trades") or 0)
    discipline = float(sc.get("discipline_score") or 0.0)

    score = 0.0
    score += min(3.0, float(aha.get("progress_pct") or 0.0) / 100.0 * 3.0)  # onboarding completion
    score += min(2.0, analyses / 20.0 * 2.0)  # usage rhythm
    score += min(1.0, alerts / 3.0 * 1.0)  # protection setup
    score += min(1.5, decisions / 10.0 * 1.5)  # journal data moat
    score += min(1.5, closed / 10.0 * 1.5)  # realized loop
    score += min(1.0, max(0.0, discipline) / 100.0 * 1.0)  # discipline quality
    score = max(0.0, min(10.0, score))

    level = "Khởi động"
    if score >= 8.5:
        level = "Tinh gọn hiệu suất cao"
    elif score >= 6.5:
        level = "Đang tăng tốc"
    elif score >= 4.5:
        level = "Có nền tảng"

    return {
        "score_10": round(score, 1),
        "level": level,
        "drivers": {
            "aha_progress_pct": float(aha.get("progress_pct") or 0.0),
            "analysis_30d": analyses,
            "alerts_total": alerts,
            "decisions_total": decisions,
            "closed_trades_30d": closed,
            "discipline_score": round(discipline, 1),
        },
    }


def can_auto_execute_symbol(user_id: str, symbol: str, cooldown_hours: int = 24) -> tuple[bool, str]:
    """
    Prevent repetitive 1-click execution for same symbol in short window.
    """
    sym = str(symbol or "").strip().upper()
    if not sym:
        return False, "Mã rỗng."
    now = datetime.now(timezone.utc)
    cooldown = timedelta(hours=max(1, int(cooldown_hours)))
    for d in list_decisions(user_id, limit=500):
        if str(d.get("symbol") or "").upper() != sym:
            continue
        ts = _parse_ts(str(d.get("created_at") or ""))
        if ts is None:
            continue
        if now - ts <= cooldown:
            return False, f"Đã có decision {sym} trong {cooldown_hours}h gần nhất."
    return True, "ok"


def smart_upgrade_prompt(user_id: str, current_plan: str) -> dict[str, Any]:
    """
    Decide if/when to show upgrade prompt based on user's realized value and behavior.
    """
    pid = str(current_plan or "free").strip().lower()
    pov = proof_of_value_report(user_id)
    ms = value_maturity_score(user_id)
    kpi30 = get_kpi_dashboard(user_id, days=30)
    by = kpi30.get("by_event") or {}
    analyses = int(by.get("analysis_completed") or 0)
    alerts = len(list_alerts(user_id))
    roi = float(pov.get("realized_pnl_30d") or 0.0) / 500_000.0
    maturity = float(ms.get("score_10") or 0.0)

    show = False
    variant = "none"
    title = ""
    message = ""
    cta_plan = "pro"

    auto_pick = select_upgrade_variant_auto(days=30)
    preferred_variant = str(auto_pick.get("variant") or "")

    if pid == "free":
        if roi >= 1.0:
            show = True
            variant = "roi_positive"
            title = "Bạn đã tạo giá trị thực, mở khóa full execution để tăng tốc"
            message = (
                f"30 ngày gần nhất ROI so với phí 500K đạt ~{roi:.2f}x. "
                "Nâng Pro để mở full Entry/SL/TP, 1-click execution và tăng xác suất bỏ lỡ ít cơ hội hơn."
            )
        elif maturity >= 6.0 or analyses >= 20:
            show = True
            variant = "high_engagement"
            title = "Bạn đang dùng app rất đều, nên nâng gói để tối đa hiệu suất"
            message = (
                f"Maturity {maturity:.1f}/10, phân tích {analyses} lần/30 ngày. "
                "Pro giúp bạn hành động nhanh hơn với batch execution và quota cao hơn."
            )
        elif alerts >= 3:
            show = True
            variant = "alerts_limit_pressure"
            title = "Bạn chạm ngưỡng cảnh báo của Free"
            message = "Nâng Pro để mở rộng quota alert và bảo vệ vị thế tốt hơn."
        elif preferred_variant in ("high_engagement", "alerts_limit_pressure"):
            show = True
            variant = preferred_variant
            if preferred_variant == "alerts_limit_pressure":
                title = "Nâng Pro để mở rộng vùng an toàn cảnh báo"
                message = "Bạn đang ở nhịp dùng ổn định, Pro giúp không bị giới hạn cảnh báo khi thị trường biến động."
            else:
                title = "Nâng Pro để tăng tốc hiệu suất"
                message = "Mở khóa full execution để rút ngắn thời gian từ tín hiệu đến hành động."
    elif pid == "pro":
        cta_plan = "expert"
        if roi >= 2.0 or maturity >= 8.0:
            show = True
            variant = "expert_ready"
            title = "Bạn đã sẵn sàng lên Expert"
            message = (
                f"Hiệu suất hiện tại tốt (ROI ~{roi:.2f}x, maturity {maturity:.1f}/10). "
                "Expert mở rộng giới hạn quét/alert và hỗ trợ vận hành quy mô lớn hơn."
            )

    return {
        "show": show,
        "variant": variant,
        "title": title,
        "message": message,
        "cta_plan": cta_plan,
        "roi_fee_x": round(roi, 2),
        "maturity_score_10": round(maturity, 1),
        "analysis_30d": analyses,
        "variant_source": str(auto_pick.get("reason") or "rule_based"),
    }


def get_upgrade_funnel(days: int = 30) -> list[dict[str, Any]]:
    """
    Conversion funnel by upgrade prompt variant.
    Tracks view -> info click -> cta click -> success.
    """
    since = (datetime.now(timezone.utc) - timedelta(days=max(1, int(days)))).isoformat()
    with _conn() as c:
        rows = c.execute(
            """
            SELECT event, meta_json
            FROM events
            WHERE ts>=?
              AND event IN ('upgrade_prompt_view','upgrade_info_click','upgrade_cta_click','upgrade_success')
            """,
            (since,),
        ).fetchall()

    bucket: dict[str, dict[str, int]] = {}

    def _inc(var: str, key: str) -> None:
        if var not in bucket:
            bucket[var] = {"views": 0, "info_clicks": 0, "cta_clicks": 0, "success": 0}
        bucket[var][key] += 1

    for r in rows:
        ev = str(r["event"] or "")
        try:
            meta = json.loads(str(r["meta_json"] or "{}"))
            if not isinstance(meta, dict):
                meta = {}
        except ValueError:
            meta = {}
        var = str(meta.get("variant") or "unknown")
        if ev == "upgrade_prompt_view":
            _inc(var, "views")
        elif ev == "upgrade_info_click":
            _inc(var, "info_clicks")
        elif ev == "upgrade_cta_click":
            _inc(var, "cta_clicks")
        elif ev == "upgrade_success":
            _inc(var, "success")

    out: list[dict[str, Any]] = []
    for var, v in sorted(bucket.items(), key=lambda x: x[0]):
        views = int(v["views"])
        cta = int(v["cta_clicks"])
        suc = int(v["success"])
        out.append(
            {
                "variant": var,
                "views": views,
                "info_clicks": int(v["info_clicks"]),
                "cta_clicks": cta,
                "success": suc,
                "CTR %": round((cta / views * 100.0), 1) if views > 0 else 0.0,
                "CVR %": round((suc / cta * 100.0), 1) if cta > 0 else 0.0,
            }
        )
    return out


def select_upgrade_variant_auto(days: int = 30) -> dict[str, Any]:
    """
    Pick best-performing upgrade variant from historical funnel.
    Fallback to balanced defaults when data is sparse.
    """
    rows = get_upgrade_funnel(days=days)
    if not rows:
        return {"variant": "high_engagement", "reason": "cold_start"}

    # Score by weighted objective: prioritize conversions, keep CTR in check.
    best = None
    best_score = -1.0
    for r in rows:
        cta = int(r.get("cta_clicks") or 0)
        views = int(r.get("views") or 0)
        success = int(r.get("success") or 0)
        ctr = float(r.get("CTR %") or 0.0) / 100.0
        cvr = float(r.get("CVR %") or 0.0) / 100.0
        # Bayesian-ish smoothing for low samples.
        cvr_smooth = (success + 1.0) / (cta + 2.0) if cta >= 0 else 0.0
        ctr_smooth = (cta + 1.0) / (views + 2.0) if views >= 0 else 0.0
        score = cvr_smooth * 0.7 + ctr_smooth * 0.3 + cvr * 0.2 + ctr * 0.1
        if score > best_score:
            best_score = score
            best = str(r.get("variant") or "high_engagement")
    return {"variant": best or "high_engagement", "reason": "funnel_optimized", "score": round(best_score, 4)}


def add_decision(
    user_id: str,
    symbol: str,
    side: str,
    entry_price: float,
    stop_loss: float,
    take_profit: float,
    thesis: str = "",
    horizon_days: int = 30,
) -> tuple[bool, str]:
    sym = (symbol or "").strip().upper()
    sd = (side or "BUY").strip().upper()
    if sd not in ("BUY", "WATCH"):
        sd = "BUY"
    if not sym:
        return False, "Mã cổ phiếu rỗng."
    if entry_price <= 0 or stop_loss <= 0 or take_profit <= 0:
        return False, "Entry/SL/TP phải > 0."
    if sd == "BUY" and not (stop_loss < entry_price < take_profit):
        return False, "Cần thỏa SL < Entry < TP."
    hz = max(7, min(int(horizon_days or 30), 365))
    with _conn() as c:
        c.execute(
            """
            INSERT INTO decisions(user_id, symbol, side, entry_price, stop_loss, take_profit, thesis, horizon_days, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (user_id, sym, sd, float(entry_price), float(stop_loss), float(take_profit), str(thesis or "").strip(), hz, _utcnow_iso()),
        )
    return True, "Đã lưu quyết định."


def list_decisions(user_id: str, limit: int = 100) -> list[dict[str, Any]]:
    lim = max(1, min(int(limit or 100), 500))
    with _conn() as c:
        rows = c.execute(
            """
            SELECT id, symbol, side, entry_price, stop_loss, take_profit, thesis, horizon_days, created_at
            FROM decisions
            WHERE user_id=?
            ORDER BY id DESC
            LIMIT ?
            """,
            (user_id, lim),
        ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": int(r["id"]),
                "symbol": str(r["symbol"]),
                "side": str(r["side"]),
                "entry_price": float(r["entry_price"]),
                "stop_loss": float(r["stop_loss"]),
                "take_profit": float(r["take_profit"]),
                "thesis": str(r["thesis"] or ""),
                "horizon_days": int(r["horizon_days"]),
                "created_at": str(r["created_at"]),
            }
        )
    return out


def evaluate_decisions(user_id: str, limit: int = 100) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for d in list_decisions(user_id, limit=limit):
        sym = d["symbol"]
        snap = fetch_financial_snapshot(sym) or {}
        px = float(snap.get("price") or 0)
        entry = float(d["entry_price"] or 0)
        sl = float(d["stop_loss"] or 0)
        tp = float(d["take_profit"] or 0)
        rr_target = (tp - entry) / max(entry - sl, 1e-6) if entry > 0 and sl > 0 else 0.0
        ret_pct = ((px - entry) / entry * 100.0) if entry > 0 and px > 0 else 0.0
        status = "OPEN"
        if px > 0:
            if px <= sl:
                status = "STOP_LOSS_HIT"
            elif px >= tp:
                status = "TAKE_PROFIT_HIT"
        out.append(
            {
                "ID": d["id"],
                "Mã": sym,
                "Side": d["side"],
                "Entry": round(entry, 2),
                "Giá hiện tại": round(px, 2) if px > 0 else None,
                "SL": round(sl, 2),
                "TP": round(tp, 2),
                "RR target": round(rr_target, 2),
                "P/L %": round(ret_pct, 2),
                "Trạng thái": status,
                "Luận điểm": d["thesis"],
                "Horizon": d["horizon_days"],
                "Tạo lúc": d["created_at"],
            }
        )
    return out


def decision_scorecard(user_id: str, limit: int = 120) -> dict[str, Any]:
    rows = evaluate_decisions(user_id, limit=limit)
    if not rows:
        return {
            "decisions": 0,
            "discipline_score": 0.0,
            "rr_avg": 0.0,
            "win_rate_closed_pct": 0.0,
            "pl_avg_pct": 0.0,
        }
    rr_vals = [float(r.get("RR target") or 0) for r in rows if float(r.get("RR target") or 0) > 0]
    rr_avg = sum(rr_vals) / len(rr_vals) if rr_vals else 0.0
    closed = [r for r in rows if r.get("Trạng thái") in ("STOP_LOSS_HIT", "TAKE_PROFIT_HIT")]
    tp_n = sum(1 for r in closed if r.get("Trạng thái") == "TAKE_PROFIT_HIT")
    sl_n = sum(1 for r in closed if r.get("Trạng thái") == "STOP_LOSS_HIT")
    win_rate = (tp_n / (tp_n + sl_n) * 100.0) if (tp_n + sl_n) > 0 else 0.0
    pl_vals = [float(r.get("P/L %") or 0) for r in rows]
    pl_avg = sum(pl_vals) / len(pl_vals) if pl_vals else 0.0

    # Discipline score (0-100): RR quality + downside control + outcome quality
    rr_score = min(40.0, rr_avg / 2.0 * 40.0)  # RR=2 -> full 40
    close_score = min(30.0, win_rate / 100.0 * 30.0)
    pnl_score = max(0.0, min(30.0, (pl_avg + 10.0) / 20.0 * 30.0))  # maps [-10,+10] to [0,30]
    discipline = max(0.0, min(100.0, rr_score + close_score + pnl_score))
    return {
        "decisions": len(rows),
        "discipline_score": round(discipline, 1),
        "rr_avg": round(rr_avg, 2),
        "win_rate_closed_pct": round(win_rate, 1),
        "pl_avg_pct": round(pl_avg, 2),
    }


def postmortem_report(user_id: str, days: int = 30, limit: int = 120) -> list[dict[str, Any]]:
    rows = evaluate_decisions(user_id, limit=limit)
    horizon = max(1, int(days))
    out: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)
    for r in rows:
        created = _parse_ts(str(r.get("Tạo lúc") or ""))
        if created is None:
            continue
        age = int((now - created).total_seconds() // 86400)
        if age < horizon:
            continue
        status = str(r.get("Trạng thái") or "OPEN")
        pl = float(r.get("P/L %") or 0)
        lesson = "Giữ kỷ luật theo kế hoạch."
        if status == "STOP_LOSS_HIT":
            lesson = "Tín hiệu sai hoặc vào lệnh sớm, cần chờ xác nhận tốt hơn."
        elif status == "TAKE_PROFIT_HIT":
            lesson = "Thực thi tốt theo kế hoạch RR."
        elif status == "OPEN" and pl < -5:
            lesson = "Nên rà soát lại luận điểm và điều kiện vô hiệu."
        out.append(
            {
                "Mã": r.get("Mã"),
                "Age (ngày)": age,
                "Trạng thái": status,
                "P/L %": round(pl, 2),
                "Bài học": lesson,
                "Luận điểm": r.get("Luận điểm"),
            }
        )
    return out


def monthly_value_report(user_id: str) -> dict[str, Any]:
    kpi30 = get_kpi_dashboard(user_id, days=30)
    cohort = get_cohort_kpi(user_id)
    sc = decision_scorecard(user_id, limit=200)
    return {
        "plan_id": kpi30.get("plan_id"),
        "events_30d": int(kpi30.get("events_total") or 0),
        "analysis_30d": int(cohort.get("analysis_30") or 0),
        "alerts_dispatched_30d": int(cohort.get("alerts_dispatched_30") or 0),
        "decisions_logged": int(sc.get("decisions") or 0),
        "discipline_score": float(sc.get("discipline_score") or 0.0),
        "avg_pl_pct": float(sc.get("pl_avg_pct") or 0.0),
        "win_rate_closed_pct": float(sc.get("win_rate_closed_pct") or 0.0),
        "value_summary": (
            f"30 ngày qua: {int(cohort.get('analysis_30') or 0)} lần phân tích, "
            f"{int(cohort.get('alerts_dispatched_30') or 0)} cảnh báo gửi đi, "
            f"discipline score {float(sc.get('discipline_score') or 0.0):.1f}/100."
        ),
    }


def coach_decision_quality(
    *,
    side: str,
    entry_price: float,
    stop_loss: float,
    take_profit: float,
    gate_passed: bool | None = None,
    confidence_score: float | None = None,
) -> dict[str, Any]:
    sd = str(side or "BUY").upper()
    entry = float(entry_price or 0)
    sl = float(stop_loss or 0)
    tp = float(take_profit or 0)
    gate = bool(gate_passed) if gate_passed is not None else False
    conf = float(confidence_score or 0)

    warnings: list[str] = []
    strengths: list[str] = []

    if entry <= 0 or sl <= 0 or tp <= 0:
        warnings.append("Thiếu dữ liệu giá Entry/SL/TP hợp lệ.")
        rr = 0.0
    else:
        rr = (tp - entry) / max(entry - sl, 1e-6)
        if sd == "BUY":
            if not (sl < entry < tp):
                warnings.append("Cấu trúc giá chưa đúng chuẩn BUY (cần SL < Entry < TP).")
            else:
                strengths.append("Cấu trúc giá BUY hợp lệ.")
        if rr >= 2.0:
            strengths.append("RR đạt chuẩn tốt (>=2.0).")
        elif rr >= 1.5:
            warnings.append("RR trung bình (1.5-2.0), cân nhắc tối ưu TP/Entry.")
        else:
            warnings.append("RR thấp (<1.5), không nên vào lệnh lớn.")

    if gate_passed is not None:
        if gate:
            strengths.append("Data Confidence Gate: PASS.")
        else:
            warnings.append("Gate chưa PASS, ưu tiên WATCH thay vì BUY.")

    if confidence_score is not None:
        if conf >= 70:
            strengths.append("Confidence cao (>=70).")
        elif conf >= 60:
            warnings.append("Confidence trung bình, nên giảm tỷ trọng.")
        else:
            warnings.append("Confidence thấp, tránh mở vị thế mới.")

    score = 100.0
    score -= 20.0 if any("RR thấp" in w for w in warnings) else 0.0
    score -= 15.0 if any("Gate chưa PASS" in w for w in warnings) else 0.0
    score -= 15.0 if any("Confidence thấp" in w for w in warnings) else 0.0
    score -= 10.0 if any("Cấu trúc giá chưa đúng" in w for w in warnings) else 0.0
    score = max(0.0, min(100.0, score))

    verdict = "GO"
    if score < 60:
        verdict = "NO-GO"
    elif score < 75:
        verdict = "CAUTION"

    return {
        "coach_score": round(score, 1),
        "rr": round(rr, 2) if entry > 0 and sl > 0 and tp > 0 else 0.0,
        "verdict": verdict,
        "strengths": strengths,
        "warnings": warnings,
    }


def adaptive_coach_thresholds(user_id: str) -> dict[str, float]:
    """
    Personalize coach thresholds based on user execution quality.
    Returns dynamic thresholds for rr_min, go_min_score, caution_min_score.
    """
    sc = decision_scorecard(user_id, limit=200)
    rp = realized_performance(user_id, days=90)
    discipline = float(sc.get("discipline_score") or 0)
    win_rate = float(rp.get("win_rate_pct") or 0)
    trades_closed = int(rp.get("closed_trades") or 0)

    rr_min = 1.8
    go_min = 75.0
    caution_min = 60.0

    # If user lacks sufficient history, keep default conservative thresholds.
    if trades_closed < 5:
        return {
            "rr_min": rr_min,
            "go_min_score": go_min,
            "caution_min_score": caution_min,
            "profile": "default_conservative",
        }

    # Better users can tolerate slightly lower RR due to proven execution.
    if discipline >= 80 and win_rate >= 55:
        rr_min = 1.6
        go_min = 70.0
        caution_min = 55.0
        profile = "adaptive_agile"
    elif discipline < 60 or win_rate < 45:
        rr_min = 2.0
        go_min = 80.0
        caution_min = 65.0
        profile = "adaptive_defensive"
    else:
        profile = "adaptive_balanced"

    return {
        "rr_min": rr_min,
        "go_min_score": go_min,
        "caution_min_score": caution_min,
        "profile": profile,
    }


def coach_decision_quality_adaptive(
    user_id: str,
    *,
    side: str,
    entry_price: float,
    stop_loss: float,
    take_profit: float,
    gate_passed: bool | None = None,
    confidence_score: float | None = None,
) -> dict[str, Any]:
    """
    Adaptive wrapper over coach_decision_quality using user-specific thresholds.
    """
    base = coach_decision_quality(
        side=side,
        entry_price=entry_price,
        stop_loss=stop_loss,
        take_profit=take_profit,
        gate_passed=gate_passed,
        confidence_score=confidence_score,
    )
    th = adaptive_coach_thresholds(user_id)

    rr = float(base.get("rr") or 0)
    score = float(base.get("coach_score") or 0)
    warnings = list(base.get("warnings") or [])
    strengths = list(base.get("strengths") or [])

    rr_min = float(th["rr_min"])
    go_min = float(th["go_min_score"])
    caution_min = float(th["caution_min_score"])

    # Re-apply RR against personalized threshold
    if rr > 0 and rr < rr_min:
        warnings.append(f"RR ({rr:.2f}) thấp hơn ngưỡng cá nhân hóa ({rr_min:.2f}).")
        score -= 8.0
    elif rr >= rr_min and rr > 0:
        strengths.append(f"RR đạt ngưỡng cá nhân hóa ({rr_min:.2f}).")

    score = max(0.0, min(100.0, score))
    verdict = "GO"
    if score < caution_min:
        verdict = "NO-GO"
    elif score < go_min:
        verdict = "CAUTION"

    return {
        "coach_score": round(score, 1),
        "rr": round(rr, 2),
        "verdict": verdict,
        "strengths": strengths,
        "warnings": warnings,
        "adaptive_profile": th["profile"],
        "adaptive_thresholds": th,
    }


def adaptive_position_sizing(
    user_id: str,
    *,
    base_max_position_pct: float,
    confidence_score: float,
    gate_passed: bool,
    vol_multiple: float = 1.0,
    coach_verdict: str = "CAUTION",
) -> dict[str, Any]:
    """
    Suggest position size based on user quality + current signal quality.
    Output: suggested_position_pct (0-20), risk_bucket, reason.
    """
    base = max(2.0, min(float(base_max_position_pct or 20.0), 20.0))
    conf = max(0.0, min(float(confidence_score or 0.0), 100.0))
    vol = max(0.1, float(vol_multiple or 1.0))
    verdict = str(coach_verdict or "CAUTION").upper()

    sc = decision_scorecard(user_id, limit=200)
    discipline = float(sc.get("discipline_score") or 0.0)

    # Start from base plan, then apply multipliers.
    mul = 1.0
    reasons: list[str] = []

    if not gate_passed:
        mul *= 0.5
        reasons.append("Gate chưa PASS -> giảm nửa vị thế.")

    if conf < 60:
        mul *= 0.7
        reasons.append("Confidence thấp -> giảm tỷ trọng.")
    elif conf >= 75:
        mul *= 1.05
        reasons.append("Confidence cao -> có thể nhích tỷ trọng.")

    if discipline < 60:
        mul *= 0.75
        reasons.append("Kỷ luật thực thi thấp -> giảm vị thế.")
    elif discipline >= 80:
        mul *= 1.05
        reasons.append("Kỷ luật tốt -> cho phép tăng nhẹ vị thế.")

    if vol > 1.6:
        mul *= 0.75
        reasons.append("Biến động/khối lượng nóng -> giảm vị thế để kiểm soát drawdown.")
    elif vol < 1.1:
        mul *= 0.95
        reasons.append("Thanh khoản/nhịp giá bình thường -> giữ vị thế vừa phải.")

    if verdict == "NO-GO":
        mul *= 0.4
        reasons.append("Coach verdict NO-GO -> chỉ nên quan sát hoặc vị thế rất nhỏ.")
    elif verdict == "GO":
        mul *= 1.05
        reasons.append("Coach verdict GO -> cho phép nhích tỷ trọng.")

    suggested = max(1.0, min(base * mul, 20.0))
    bucket = "Thận trọng"
    if suggested >= 14:
        bucket = "Tăng tốc có kiểm soát"
    elif suggested >= 8:
        bucket = "Cân bằng"

    return {
        "suggested_position_pct": round(suggested, 1),
        "risk_bucket": bucket,
        "discipline_score": round(discipline, 1),
        "reason": " | ".join(reasons) if reasons else "Giữ theo tỷ trọng cơ sở.",
    }


def migrate_legacy_json_to_sqlite(path: Path | None = None) -> dict[str, int]:
    src = path or LEGACY_STATE_PATH
    if not src.exists():
        return {"users": 0, "holdings": 0, "alerts": 0, "events": 0}
    try:
        with open(src, encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, ValueError):
        return {"users": 0, "holdings": 0, "alerts": 0, "events": 0}
    users = raw.get("users") if isinstance(raw, dict) else None
    if not isinstance(users, dict):
        return {"users": 0, "holdings": 0, "alerts": 0, "events": 0}

    migrated_users = migrated_holdings = migrated_alerts = migrated_events = 0
    for user_id, block in users.items():
        if not isinstance(block, dict):
            continue
        uid = str(user_id).strip() or "default_user"
        set_user_plan(uid, str(block.get("plan_id") or "free"))
        migrated_users += 1

        for h in block.get("holdings", []):
            if not isinstance(h, dict):
                continue
            sym = str(h.get("symbol") or "").strip().upper()
            qty = float(h.get("quantity") or 0)
            avg = float(h.get("avg_cost") or 0)
            if sym and qty > 0 and avg > 0:
                add_holding(uid, sym, qty, avg)
                migrated_holdings += 1

        for a in block.get("alerts", []):
            if not isinstance(a, dict):
                continue
            sym = str(a.get("symbol") or "").strip().upper()
            at = str(a.get("type") or "price_above")
            th = float(a.get("threshold") or 0)
            note = str(a.get("note") or "")
            if sym and th > 0:
                add_alert(uid, sym, at, th, note)
                migrated_alerts += 1

        for e in block.get("events", []):
            if not isinstance(e, dict):
                continue
            ev = str(e.get("event") or "unknown")
            meta = e.get("meta") if isinstance(e.get("meta"), dict) else {}
            log_event(uid, ev, meta)
            migrated_events += 1

    return {
        "users": migrated_users,
        "holdings": migrated_holdings,
        "alerts": migrated_alerts,
        "events": migrated_events,
    }
