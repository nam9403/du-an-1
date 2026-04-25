"""
Retention + Monetization support layer (SQLite backend).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import smtplib
import sqlite3
import secrets
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

import requests  # type: ignore
from scrapers.financial_data import fetch_financial_snapshot
from core import auth_service
from core import admin_runtime_service
from core import analytics_service
from core.db_connection_factory import connect_runtime_db
from core.db_runtime import require_sqlite_backend, resolve_db_runtime
from core import decision_service
from core import forecast_accountability_service
from core import forecast_service
from core import notification_queue_repository as notif_queue_repo
from core.observability import log_timing
from core import plan_usage_service
from core import secret_repository
from core import secret_service
from core import trade_journal_service

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "app_state.db"
LEGACY_STATE_PATH = ROOT / "data" / "user_state.json"
SECRETS_PATH = ROOT / "data" / "secrets_store.json"
APP_SECRET_PATH = ROOT / "data" / ".app_secret.key"
SCHEMA_VERSION = 3
logger = logging.getLogger(__name__)

PIN_HASH_PREFIX = "pbkdf2_sha256"
PIN_HASH_ITERATIONS = 210_000

PLAN_FEATURES = {
    "free": {"scan_limit": 20, "llm_live": False, "alerts": 3, "analysis_per_day": 30},
    "pro": {"scan_limit": 80, "llm_live": True, "alerts": 20, "analysis_per_day": 200},
    "expert": {"scan_limit": 200, "llm_live": True, "alerts": 100, "analysis_per_day": 1000},
}


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _db_backend() -> str:
    return resolve_db_runtime(root=ROOT, default_sqlite_path=DB_PATH).backend


def _conn() -> Any:
    db_runtime = resolve_db_runtime(root=ROOT, default_sqlite_path=DB_PATH)
    c = connect_runtime_db(db_runtime, sqlite_default_path=DB_PATH)
    if db_runtime.backend == "sqlite":
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
    return c


def _init_db() -> None:
    if _db_backend() == "postgresql":
        _init_db_postgres()
        return

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
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cx_score_daily (
                user_id TEXT NOT NULL,
                score_date TEXT NOT NULL,
                score INTEGER NOT NULL,
                band TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (user_id, score_date)
            );
            """
        )
        _apply_schema_migrations(c)


def _init_db_postgres() -> None:
    with _conn() as c:
        ddl_statements = [
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                plan_id TEXT NOT NULL DEFAULT 'free',
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS holdings (
                user_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                quantity DOUBLE PRECISION NOT NULL,
                avg_cost DOUBLE PRECISION NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (user_id, symbol)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                type TEXT NOT NULL,
                threshold DOUBLE PRECISION NOT NULL,
                note TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS usage_daily (
                user_id TEXT NOT NULL,
                usage_date TEXT NOT NULL,
                feature TEXT NOT NULL,
                count INTEGER NOT NULL,
                PRIMARY KEY (user_id, usage_date, feature)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS events (
                id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                user_id TEXT NOT NULL,
                ts TEXT NOT NULL,
                event TEXT NOT NULL,
                meta_json TEXT NOT NULL DEFAULT '{}'
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS notification_sent (
                dedup_key TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                sent_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS notification_queue (
                id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                user_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending',
                last_error TEXT NOT NULL DEFAULT '',
                next_retry_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS auth_users (
                user_id TEXT PRIMARY KEY,
                pin_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS decisions (
                id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                user_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_price DOUBLE PRECISION NOT NULL,
                stop_loss DOUBLE PRECISION NOT NULL,
                take_profit DOUBLE PRECISION NOT NULL,
                thesis TEXT NOT NULL DEFAULT '',
                horizon_days INTEGER NOT NULL DEFAULT 30,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS trades (
                id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                user_id TEXT NOT NULL,
                decision_id BIGINT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL DEFAULT 'LONG',
                quantity DOUBLE PRECISION NOT NULL,
                entry_price DOUBLE PRECISION NOT NULL,
                entry_fee DOUBLE PRECISION NOT NULL DEFAULT 0,
                entry_note TEXT NOT NULL DEFAULT '',
                opened_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'OPEN',
                exit_price DOUBLE PRECISION,
                exit_fee DOUBLE PRECISION,
                exit_note TEXT,
                closed_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS forecast_records (
                id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                user_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                created_at TEXT NOT NULL,
                horizon_days INTEGER NOT NULL,
                base_price DOUBLE PRECISION NOT NULL,
                expected_price DOUBLE PRECISION NOT NULL,
                expected_return_pct DOUBLE PRECISION NOT NULL,
                realized_price DOUBLE PRECISION,
                realized_return_pct DOUBLE PRECISION,
                abs_error_pct DOUBLE PRECISION,
                direction_hit INTEGER,
                status TEXT NOT NULL DEFAULT 'pending'
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS premium_trials (
                user_id TEXT PRIMARY KEY,
                trial_start_at TEXT,
                trial_end_at TEXT,
                trial_consumed INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS cx_score_daily (
                user_id TEXT NOT NULL,
                score_date TEXT NOT NULL,
                score INTEGER NOT NULL,
                band TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (user_id, score_date)
            )
            """,
        ]
        for stmt in ddl_statements:
            c.execute(stmt)
        for version in (1, 2, 3):
            c.execute(
                "INSERT INTO schema_migrations(version, applied_at) VALUES (%s, %s) ON CONFLICT (version) DO NOTHING",
                (version, _utcnow_iso()),
            )


def _apply_schema_migrations(c: sqlite3.Connection) -> None:
    applied = {
        int(r["version"])
        for r in c.execute("SELECT version FROM schema_migrations ORDER BY version ASC").fetchall()
    }
    if 1 not in applied:
        cols = [str(r["name"]) for r in c.execute("PRAGMA table_info(trades)").fetchall()]
        if "decision_id" not in cols:
            c.execute("ALTER TABLE trades ADD COLUMN decision_id INTEGER")
        c.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
            (1, _utcnow_iso()),
        )
    if 2 not in applied:
        c.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        c.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
            (2, _utcnow_iso()),
        )
    if 3 not in applied:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS cx_score_daily (
                user_id TEXT NOT NULL,
                score_date TEXT NOT NULL,
                score INTEGER NOT NULL,
                band TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (user_id, score_date)
            )
            """
        )
        c.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        c.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
            (3, _utcnow_iso()),
        )


_init_db()


def get_plan_features(plan_id: str) -> dict[str, Any]:
    return plan_usage_service.get_plan_features(plan_id, plan_features=PLAN_FEATURES)


def set_user_plan(user_id: str, plan_id: str) -> None:
    plan_usage_service.set_user_plan(
        conn_factory=_conn,
        utcnow_iso=_utcnow_iso,
        user_id=user_id,
        plan_id=plan_id,
        plan_features=PLAN_FEATURES,
    )


def get_user_plan(user_id: str, default_plan: str = "free") -> str:
    return plan_usage_service.get_user_plan(
        conn_factory=_conn,
        user_id=user_id,
        default_plan=default_plan,
        plan_features=PLAN_FEATURES,
    )


def get_trial_state(user_id: str) -> dict[str, Any]:
    return plan_usage_service.get_trial_state(
        conn_factory=_conn,
        parse_ts=_parse_ts,
        user_id=user_id,
    )


def trial_is_active(user_id: str) -> bool:
    st = get_trial_state(user_id)
    return plan_usage_service.trial_is_active(trial_state=st)


def start_premium_trial_7d(user_id: str) -> tuple[bool, str]:
    return plan_usage_service.start_premium_trial_7d(
        conn_factory=_conn,
        utcnow_iso=_utcnow_iso,
        parse_ts=_parse_ts,
        user_id=user_id,
    )


def premium_features_unlocked(user_id: str, plan_id: str) -> bool:
    return plan_usage_service.premium_features_unlocked(
        user_id=user_id,
        plan_id=plan_id,
        trial_is_active_func=trial_is_active,
    )


def _usage_key(feature: str) -> str:
    return f"{datetime.now(timezone.utc).date().isoformat()}:{feature}"


def _pin_hash(pin: str) -> str:
    return auth_service.pin_hash(pin, prefix=PIN_HASH_PREFIX, iterations=PIN_HASH_ITERATIONS)


def _pin_hash_legacy(pin: str) -> str:
    return auth_service.pin_hash_legacy(pin)


def _verify_pin_hash(stored_hash: str, pin: str) -> bool:
    return auth_service.verify_pin_hash(stored_hash, pin, prefix=PIN_HASH_PREFIX)


def register_user_pin(user_id: str, pin: str) -> tuple[bool, str]:
    return auth_service.register_user_pin(
        conn_factory=_conn,
        utcnow_iso=_utcnow_iso,
        user_id=user_id,
        pin=pin,
        prefix=PIN_HASH_PREFIX,
        iterations=PIN_HASH_ITERATIONS,
    )


def verify_user_pin(user_id: str, pin: str) -> bool:
    return auth_service.verify_user_pin(
        conn_factory=_conn,
        user_id=user_id,
        pin=pin,
        prefix=PIN_HASH_PREFIX,
        iterations=PIN_HASH_ITERATIONS,
        upsert_user_pin_func=upsert_user_pin,
    )


def upsert_user_pin(user_id: str, pin: str) -> tuple[bool, str]:
    return auth_service.upsert_user_pin(
        conn_factory=_conn,
        utcnow_iso=_utcnow_iso,
        user_id=user_id,
        pin=pin,
        prefix=PIN_HASH_PREFIX,
        iterations=PIN_HASH_ITERATIONS,
    )


def has_auth_user(user_id: str) -> bool:
    return auth_service.has_auth_user(conn_factory=_conn, user_id=user_id)


def _secret_key(user_id: str) -> bytes:
    return secret_service.secret_key(user_id)


def _is_truthy_env(name: str, default: str = "0") -> bool:
    return secret_service.is_truthy_env(os.environ.get, name, default)


def _require_env_secret_key() -> bool:
    return secret_service.require_env_secret_key(os.environ.get)


def _allow_legacy_secret_decrypt() -> bool:
    return secret_service.allow_legacy_secret_decrypt(os.environ.get)


def _short_error(scope: str, exc: Exception) -> str:
    msg = f"{scope}:{exc.__class__.__name__}:{str(exc).strip()}"
    return msg[:220]


def _xor_crypt(data: bytes, key: bytes) -> bytes:
    return secret_service.xor_crypt(data, key)


def _get_fernet():
    return secret_service.get_fernet(env_get=os.environ.get, app_secret_path=APP_SECRET_PATH)


def _load_secrets() -> dict[str, Any]:
    return secret_repository.load_secrets(SECRETS_PATH)


def _save_secrets(data: dict[str, Any]) -> None:
    secret_repository.save_secrets(SECRETS_PATH, data)


def save_secret(user_id: str, name: str, value: str) -> None:
    secret_service.save_secret(
        env_get=os.environ.get,
        app_secret_path=APP_SECRET_PATH,
        secrets_path=SECRETS_PATH,
        user_id=user_id,
        name=name,
        value=value,
    )


def load_secret(user_id: str, name: str, default: str = "") -> str:
    return secret_service.load_secret(
        env_get=os.environ.get,
        app_secret_path=APP_SECRET_PATH,
        secrets_path=SECRETS_PATH,
        save_secret_func=save_secret,
        user_id=user_id,
        name=name,
        default=default,
    )


def record_usage(user_id: str, feature: str, count: int = 1) -> None:
    plan_usage_service.record_usage(
        conn_factory=_conn,
        user_id=user_id,
        feature=feature,
        count=count,
    )


def get_usage_today(user_id: str, feature: str) -> int:
    return plan_usage_service.get_usage_today(
        conn_factory=_conn,
        user_id=user_id,
        feature=feature,
    )


def can_use_feature(user_id: str, feature: str, requested: int = 1, plan_id: str | None = None) -> tuple[bool, str]:
    return plan_usage_service.can_use_feature(
        user_id=user_id,
        feature=feature,
        requested=requested,
        plan_id=plan_id,
        get_user_plan_func=get_user_plan,
        get_plan_features_func=get_plan_features,
        get_usage_today_func=get_usage_today,
        list_alerts_func=list_alerts,
    )


def log_event(user_id: str, event_type: str, meta: dict[str, Any] | None = None) -> None:
    analytics_service.log_event(
        conn_factory=_conn,
        utcnow_iso=_utcnow_iso,
        user_id=user_id,
        event_type=event_type,
        meta=meta,
    )


def upsert_daily_cx_score(user_id: str, score: int, band: str) -> None:
    uid = str(user_id or "").strip() or "default_user"
    d = datetime.now(timezone.utc).date().isoformat()
    sc = max(0, min(100, int(score)))
    bd = str(band or "Good").strip()[:32] or "Good"
    with _conn() as c:
        c.execute(
            """
            INSERT INTO cx_score_daily(user_id, score_date, score, band, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_id, score_date)
            DO UPDATE SET score=excluded.score, band=excluded.band, updated_at=excluded.updated_at
            """,
            (uid, d, sc, bd, _utcnow_iso()),
        )


def get_recent_cx_scores(user_id: str, days: int = 14) -> list[dict[str, Any]]:
    uid = str(user_id or "").strip() or "default_user"
    cap = max(1, min(int(days), 90))
    with _conn() as c:
        rows = c.execute(
            """
            SELECT score_date, score, band, updated_at
            FROM cx_score_daily
            WHERE user_id=?
            ORDER BY score_date DESC
            LIMIT ?
            """,
            (uid, cap),
        ).fetchall()
    return [
        {
            "score_date": str(r["score_date"]),
            "score": int(r["score"]),
            "band": str(r["band"] or ""),
            "updated_at": str(r["updated_at"] or ""),
        }
        for r in rows
    ]


def get_admin_cx_kpi(days: int = 14) -> dict[str, Any]:
    return analytics_service.get_admin_cx_kpi(conn_factory=_conn, days=days)


def record_forecast_snapshot(user_id: str, symbol: str, report: dict[str, Any]) -> None:
    forecast_service.record_forecast_snapshot(
        conn_factory=_conn,
        utcnow_iso=_utcnow_iso,
        user_id=user_id,
        symbol=symbol,
        report=report,
    )


def _refresh_matured_forecasts(user_id: str, symbol: str) -> None:
    def _fetch_snapshot_for_forecast(sym: str) -> dict[str, Any]:
        return fetch_financial_snapshot(sym) or {}

    forecast_service.refresh_matured_forecasts(
        conn_factory=_conn,
        parse_ts=_parse_ts,
        fetch_financial_snapshot_func=_fetch_snapshot_for_forecast,
        user_id=user_id,
        symbol=symbol,
    )


def get_forecast_accuracy_dashboard(user_id: str, symbol: str, lookback_days: int = 540) -> dict[str, Any]:
    return forecast_service.get_forecast_accuracy_dashboard(
        conn_factory=_conn,
        refresh_matured_forecasts_func=_refresh_matured_forecasts,
        user_id=user_id,
        symbol=symbol,
        lookback_days=lookback_days,
    )


def get_forecast_benchmark_by_horizon(
    user_id: str,
    symbol: str,
    horizons: tuple[int, ...] = (30, 60, 90),
    lookback_days: int = 720,
) -> list[dict[str, Any]]:
    return forecast_service.get_forecast_benchmark_by_horizon(
        conn_factory=_conn,
        refresh_matured_forecasts_func=_refresh_matured_forecasts,
        user_id=user_id,
        symbol=symbol,
        horizons=horizons,
        lookback_days=lookback_days,
    )


def get_forecast_leaderboard(user_id: str, limit: int = 20, lookback_days: int = 720) -> list[dict[str, Any]]:
    return forecast_service.get_forecast_leaderboard(
        conn_factory=_conn,
        user_id=user_id,
        limit=limit,
        lookback_days=lookback_days,
    )


def get_forecast_group_benchmark(
    user_id: str,
    symbols: list[str],
    lookback_days: int = 720,
) -> dict[str, Any]:
    return forecast_service.get_forecast_group_benchmark(
        conn_factory=_conn,
        user_id=user_id,
        symbols=symbols,
        lookback_days=lookback_days,
    )


def get_forecast_drift_signal(user_id: str, symbol: str, recent_n: int = 20, baseline_n: int = 60) -> dict[str, Any]:
    return forecast_service.get_forecast_drift_signal(
        conn_factory=_conn,
        user_id=user_id,
        symbol=symbol,
        recent_n=recent_n,
        baseline_n=baseline_n,
    )


def get_forecast_portfolio_dashboard(user_id: str, lookback_days: int = 720) -> dict[str, Any]:
    return forecast_accountability_service.get_forecast_portfolio_dashboard(
        conn_factory=_conn,
        parse_ts=_parse_ts,
        user_id=user_id,
        lookback_days=lookback_days,
    )


def get_forecast_regime_dashboard(user_id: str, lookback_days: int = 720) -> dict[str, Any]:
    return forecast_accountability_service.get_forecast_regime_dashboard(
        conn_factory=_conn,
        user_id=user_id,
        lookback_days=lookback_days,
    )


def get_forecast_drift_streak(user_id: str, symbol: str, checks: int = 3) -> dict[str, Any]:
    return forecast_service.get_forecast_drift_streak(
        get_forecast_drift_signal_func=get_forecast_drift_signal,
        user_id=user_id,
        symbol=symbol,
        checks=checks,
    )


def export_forecast_health_report(user_id: str) -> str:
    return forecast_accountability_service.export_forecast_health_report(
        root=ROOT,
        utcnow_iso=_utcnow_iso,
        user_id=user_id,
        get_forecast_portfolio_dashboard_func=lambda uid: get_forecast_portfolio_dashboard(uid),
        get_forecast_regime_dashboard_func=lambda uid: get_forecast_regime_dashboard(uid),
        get_forecast_leaderboard_func=lambda uid, limit: get_forecast_leaderboard(uid, limit=limit),
    )


def export_model_accountability_report(user_id: str) -> str:
    return forecast_accountability_service.export_model_accountability_report(
        root=ROOT,
        utcnow_iso=_utcnow_iso,
        user_id=user_id,
        get_forecast_portfolio_dashboard_func=lambda uid: get_forecast_portfolio_dashboard(uid),
        get_forecast_regime_dashboard_func=lambda uid: get_forecast_regime_dashboard(uid),
        get_forecast_leaderboard_func=lambda uid, limit: get_forecast_leaderboard(uid, limit=limit),
        get_kpi_dashboard_func=lambda uid, days: get_kpi_dashboard(uid, days=days),
        proof_of_value_report_func=proof_of_value_report,
    )


def export_monthly_accountability_markdown(user_id: str) -> str:
    return forecast_accountability_service.export_monthly_accountability_markdown(
        root=ROOT,
        utcnow_iso=_utcnow_iso,
        user_id=user_id,
        get_forecast_portfolio_dashboard_func=lambda uid, days: get_forecast_portfolio_dashboard(uid, lookback_days=days),
        get_forecast_regime_dashboard_func=lambda uid, days: get_forecast_regime_dashboard(uid, lookback_days=days),
        get_kpi_dashboard_func=lambda uid, days: get_kpi_dashboard(uid, days=days),
        proof_of_value_report_func=proof_of_value_report,
        get_forecast_leaderboard_func=lambda uid, limit: get_forecast_leaderboard(uid, limit=limit),
    )


def send_monthly_accountability_email(
    user_id: str,
    email_to: str,
    *,
    smtp_host: str,
    smtp_port: int = 587,
    smtp_user: str = "",
    smtp_password: str = "",
    smtp_from: str = "",
) -> tuple[bool, str]:
    return forecast_accountability_service.send_monthly_accountability_email(
        user_id=user_id,
        email_to=email_to,
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_password=smtp_password,
        smtp_from=smtp_from,
        export_monthly_accountability_markdown_func=export_monthly_accountability_markdown,
        short_error=_short_error,
        logger_warning=logger.warning,
    )


def get_kpi_dashboard(user_id: str, days: int = 30) -> dict[str, Any]:
    return analytics_service.get_kpi_dashboard(
        conn_factory=_conn,
        user_id=user_id,
        days=days,
        get_user_plan_func=lambda uid: get_user_plan(uid),
    )


def list_known_user_ids(limit: int = 500) -> list[str]:
    cap = max(1, min(int(limit), 5000))
    with _conn() as c:
        rows = c.execute(
            """
            SELECT user_id FROM users
            UNION
            SELECT user_id FROM auth_users
            UNION
            SELECT user_id FROM holdings
            UNION
            SELECT user_id FROM alerts
            UNION
            SELECT user_id FROM events
            ORDER BY user_id ASC
            LIMIT ?
            """,
            (cap,),
        ).fetchall()
    return [str(r["user_id"]) for r in rows if str(r["user_id"] or "").strip()]


def has_delivery_marker(user_id: str, marker_key: str) -> bool:
    uid = str(user_id or "").strip()
    mk = str(marker_key or "").strip()
    if not uid or not mk:
        return False
    with _conn() as c:
        return c.execute(
            "SELECT 1 FROM notification_sent WHERE dedup_key=? AND user_id=?",
            (mk, uid),
        ).fetchone() is not None


def set_delivery_marker(user_id: str, marker_key: str) -> None:
    uid = str(user_id or "").strip()
    mk = str(marker_key or "").strip()
    if not uid or not mk:
        return
    with _conn() as c:
        c.execute(
            "INSERT OR IGNORE INTO notification_sent(dedup_key, user_id, sent_at) VALUES (?, ?, ?)",
            (mk, uid, _utcnow_iso()),
        )


def _parse_ts(s: str) -> datetime | None:
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None


def get_cohort_kpi(user_id: str) -> dict[str, Any]:
    return analytics_service.get_cohort_kpi(conn_factory=_conn, user_id=user_id)


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
        except requests.RequestException as e:
            logger.warning(
                "dispatch_alert_notifications telegram failed user_id=%s symbol=%s error=%s",
                user_id,
                str(a.get("Mã") or ""),
                _short_error("telegram_alert", e),
            )
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
        except requests.RequestException as e:
            logger.warning("dispatch_external_notifications webhook failed error=%s", _short_error("webhook_alert", e))
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
        except (OSError, smtplib.SMTPException, ValueError) as e:
            logger.warning("dispatch_external_notifications email failed error=%s", _short_error("smtp_alert", e))
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
        except requests.RequestException as e:
            logger.warning("dispatch_text_notifications telegram failed error=%s", _short_error("telegram_text", e))
            failed += 1

    hook = (webhook_url or "").strip()
    if hook:
        try:
            r = requests.post(hook, json={"message": msg}, timeout=10)
            if r.status_code < 300:
                sent += 1
            else:
                failed += 1
        except requests.RequestException as e:
            logger.warning("dispatch_text_notifications webhook failed error=%s", _short_error("webhook_text", e))
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
        except (OSError, smtplib.SMTPException, ValueError) as e:
            logger.warning("dispatch_text_notifications email failed error=%s", _short_error("smtp_text", e))
            failed += 1
    return {"sent": sent, "failed": failed}


def enqueue_notification(user_id: str, kind: str, payload: dict[str, Any], delay_seconds: int = 60) -> None:
    notif_queue_repo.enqueue_notification(
        conn_factory=_conn,
        user_id=user_id,
        kind=kind,
        payload=payload,
        delay_seconds=delay_seconds,
    )


def process_notification_queue(max_jobs: int = 20) -> dict[str, int]:
    started_at = datetime.now(timezone.utc)
    stat = notif_queue_repo.process_notification_queue(
        conn_factory=_conn,
        utcnow_iso=_utcnow_iso,
        short_error=_short_error,
        dispatch_text_notifications=dispatch_text_notifications,
        dispatch_alert_notifications=dispatch_alert_notifications,
        max_jobs=max_jobs,
    )
    duration_ms = (datetime.now(timezone.utc) - started_at).total_seconds() * 1000.0
    log_timing(
        "product_layer.process_notification_queue",
        duration_ms,
        max_jobs=int(max_jobs),
        processed=int(stat.get("processed") or 0),
        sent=int(stat.get("sent") or 0),
        failed=int(stat.get("failed") or 0),
        retried=int(stat.get("retried") or 0),
    )
    return stat


def get_admin_kpi(days: int = 30) -> dict[str, Any]:
    started_at = datetime.now(timezone.utc)
    out = admin_runtime_service.get_admin_kpi_enriched(conn_factory=_conn, days=days)
    duration_ms = (datetime.now(timezone.utc) - started_at).total_seconds() * 1000.0
    log_timing("product_layer.get_admin_kpi", duration_ms, days=int(days), decision=str(out.get("release_decision") or ""))
    return out


def get_notification_queue_diagnostics(limit: int = 20) -> list[dict[str, Any]]:
    return notif_queue_repo.get_notification_queue_diagnostics(conn_factory=_conn, limit=limit)


def retry_failed_notifications(max_rows: int = 20) -> int:
    return notif_queue_repo.retry_failed_notifications(
        conn_factory=_conn,
        utcnow_iso=_utcnow_iso,
        max_rows=max_rows,
    )


def get_notification_fail_reason_summary(days: int = 14, limit: int = 10) -> list[dict[str, Any]]:
    return notif_queue_repo.get_notification_fail_reason_summary(
        conn_factory=_conn,
        days=days,
        limit=limit,
    )


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
    return trade_journal_service.open_trade(
        conn_factory=_conn,
        utcnow_iso=_utcnow_iso,
        user_id=user_id,
        symbol=symbol,
        quantity=quantity,
        entry_price=entry_price,
        decision_id=decision_id,
        side=side,
        entry_fee=entry_fee,
        entry_note=entry_note,
    )


def close_trade(
    user_id: str,
    trade_id: int,
    exit_price: float,
    *,
    exit_fee: float = 0.0,
    exit_note: str = "",
) -> tuple[bool, str]:
    return trade_journal_service.close_trade(
        conn_factory=_conn,
        utcnow_iso=_utcnow_iso,
        user_id=user_id,
        trade_id=trade_id,
        exit_price=exit_price,
        exit_fee=exit_fee,
        exit_note=exit_note,
    )


def list_trades(user_id: str, limit: int = 200) -> list[dict[str, Any]]:
    return trade_journal_service.list_trades(conn_factory=_conn, user_id=user_id, limit=limit)


def execution_vs_plan_report(user_id: str, limit: int = 200) -> list[dict[str, Any]]:
    return trade_journal_service.execution_vs_plan_report(
        user_id=user_id,
        limit=limit,
        list_trades_func=lambda uid, lim: list_trades(uid, limit=lim),
        list_decisions_func=lambda uid, lim: list_decisions(uid, limit=lim),
    )


def realized_performance(user_id: str, days: int = 30) -> dict[str, Any]:
    return trade_journal_service.realized_performance(
        user_id=user_id,
        days=days,
        list_trades_func=lambda uid, lim: list_trades(uid, limit=lim),
    )


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
    return analytics_service.get_upgrade_funnel(conn_factory=_conn, days=days)


def select_upgrade_variant_auto(days: int = 30) -> dict[str, Any]:
    return analytics_service.select_upgrade_variant_auto(
        days=days,
        get_upgrade_funnel_func=get_upgrade_funnel,
    )


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
    return decision_service.add_decision(
        conn_factory=_conn,
        utcnow_iso=_utcnow_iso,
        user_id=user_id,
        symbol=symbol,
        side=side,
        entry_price=entry_price,
        stop_loss=stop_loss,
        take_profit=take_profit,
        thesis=thesis,
        horizon_days=horizon_days,
    )


def list_decisions(user_id: str, limit: int = 100) -> list[dict[str, Any]]:
    return decision_service.list_decisions(conn_factory=_conn, user_id=user_id, limit=limit)


def evaluate_decisions(user_id: str, limit: int = 100) -> list[dict[str, Any]]:
    def _fetch_snapshot_for_decision(sym: str) -> dict[str, Any]:
        return fetch_financial_snapshot(sym) or {}

    return decision_service.evaluate_decisions(
        user_id=user_id,
        limit=limit,
        list_decisions_func=lambda uid, lim: list_decisions(uid, limit=lim),
        fetch_financial_snapshot_func=_fetch_snapshot_for_decision,
    )


def decision_scorecard(user_id: str, limit: int = 120) -> dict[str, Any]:
    return decision_service.decision_scorecard(
        user_id=user_id,
        limit=limit,
        evaluate_decisions_func=lambda uid, lim: evaluate_decisions(uid, limit=lim),
    )


def postmortem_report(user_id: str, days: int = 30, limit: int = 120) -> list[dict[str, Any]]:
    return decision_service.postmortem_report(
        user_id=user_id,
        days=days,
        limit=limit,
        evaluate_decisions_func=lambda uid, lim: evaluate_decisions(uid, limit=lim),
        parse_ts=_parse_ts,
    )


def monthly_value_report(user_id: str) -> dict[str, Any]:
    return decision_service.monthly_value_report(
        user_id=user_id,
        get_kpi_dashboard_func=lambda uid, days: get_kpi_dashboard(uid, days=days),
        get_cohort_kpi_func=get_cohort_kpi,
        decision_scorecard_func=lambda uid, lim: decision_scorecard(uid, limit=lim),
    )


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


def adaptive_coach_thresholds(user_id: str) -> dict[str, Any]:
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
