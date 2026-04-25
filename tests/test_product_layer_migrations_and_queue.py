from __future__ import annotations

from datetime import datetime, timezone


def test_schema_migrations_initialized(tmp_path, monkeypatch) -> None:
    import core.product_layer as pl

    monkeypatch.setattr(pl, "DB_PATH", tmp_path / "app_state.db")
    monkeypatch.setattr(pl, "SECRETS_PATH", tmp_path / "secrets.json")
    monkeypatch.setattr(pl, "APP_SECRET_PATH", tmp_path / ".app_secret.key")
    pl._init_db()

    with pl._conn() as c:
        versions = [int(r["version"]) for r in c.execute("SELECT version FROM schema_migrations ORDER BY version ASC").fetchall()]
        user_version = int(c.execute("PRAGMA user_version").fetchone()[0])
    assert versions == [1, 2, 3]
    assert user_version == pl.SCHEMA_VERSION


def test_process_queue_marks_invalid_payload_failed(tmp_path, monkeypatch) -> None:
    import core.product_layer as pl

    monkeypatch.setattr(pl, "DB_PATH", tmp_path / "app_state.db")
    monkeypatch.setattr(pl, "SECRETS_PATH", tmp_path / "secrets.json")
    monkeypatch.setattr(pl, "APP_SECRET_PATH", tmp_path / ".app_secret.key")
    pl._init_db()

    now = datetime.now(timezone.utc).isoformat()
    with pl._conn() as c:
        c.execute(
            """
            INSERT INTO notification_queue(user_id, kind, payload_json, attempts, status, last_error, next_retry_at, created_at, updated_at)
            VALUES (?, ?, ?, 0, 'pending', '', ?, ?, ?)
            """,
            ("u1", "text", "{bad json", now, now, now),
        )
    stat = pl.process_notification_queue(max_jobs=5)
    assert int(stat.get("failed") or 0) == 1

    with pl._conn() as c:
        row = c.execute("SELECT status, attempts, last_error FROM notification_queue").fetchone()
    assert str(row["status"]) == "failed"
    assert int(row["attempts"]) == 1
    assert "invalid_payload" in str(row["last_error"])


def test_retry_failed_notifications_and_diagnostics(tmp_path, monkeypatch) -> None:
    import core.product_layer as pl

    monkeypatch.setattr(pl, "DB_PATH", tmp_path / "app_state.db")
    monkeypatch.setattr(pl, "SECRETS_PATH", tmp_path / "secrets.json")
    monkeypatch.setattr(pl, "APP_SECRET_PATH", tmp_path / ".app_secret.key")
    pl._init_db()

    now = datetime.now(timezone.utc).isoformat()
    with pl._conn() as c:
        c.execute(
            """
            INSERT INTO notification_queue(user_id, kind, payload_json, attempts, status, last_error, next_retry_at, created_at, updated_at)
            VALUES (?, ?, ?, 3, 'failed', 'smtp_down', ?, ?, ?)
            """,
            ("u1", "text", "{}", now, now, now),
        )
    diag = pl.get_notification_queue_diagnostics(limit=10)
    assert len(diag) >= 1
    assert str(diag[0]["status"]) == "failed"

    moved = pl.retry_failed_notifications(max_rows=10)
    assert moved >= 1
    with pl._conn() as c:
        row = c.execute("SELECT status, attempts, last_error FROM notification_queue").fetchone()
    assert str(row["status"]) == "pending"
    assert int(row["attempts"]) == 0
    assert str(row["last_error"]) == ""


def test_notification_fail_reason_summary(tmp_path, monkeypatch) -> None:
    import core.product_layer as pl

    monkeypatch.setattr(pl, "DB_PATH", tmp_path / "app_state.db")
    monkeypatch.setattr(pl, "SECRETS_PATH", tmp_path / "secrets.json")
    monkeypatch.setattr(pl, "APP_SECRET_PATH", tmp_path / ".app_secret.key")
    pl._init_db()

    now = datetime.now(timezone.utc).isoformat()
    with pl._conn() as c:
        c.execute(
            """
            INSERT INTO notification_queue(user_id, kind, payload_json, attempts, status, last_error, next_retry_at, created_at, updated_at)
            VALUES (?, ?, ?, 5, 'failed', 'smtp_alert:SMTPException:auth_failed', ?, ?, ?)
            """,
            ("u1", "text", "{}", now, now, now),
        )
        c.execute(
            """
            INSERT INTO notification_queue(user_id, kind, payload_json, attempts, status, last_error, next_retry_at, created_at, updated_at)
            VALUES (?, ?, ?, 5, 'failed', 'webhook_text:RequestException:timeout', ?, ?, ?)
            """,
            ("u2", "text", "{}", now, now, now),
        )
    summary = pl.get_notification_fail_reason_summary(days=14, limit=10)
    assert len(summary) >= 2
    reasons = {str(x["reason"]) for x in summary}
    assert "smtp_alert" in reasons
    assert "webhook_text" in reasons


def test_process_queue_without_channels_marks_done_not_failed(tmp_path, monkeypatch) -> None:
    import core.product_layer as pl

    monkeypatch.setattr(pl, "DB_PATH", tmp_path / "app_state.db")
    monkeypatch.setattr(pl, "SECRETS_PATH", tmp_path / "secrets.json")
    monkeypatch.setattr(pl, "APP_SECRET_PATH", tmp_path / ".app_secret.key")
    pl._init_db()

    now = datetime.now(timezone.utc).isoformat()
    with pl._conn() as c:
        c.execute(
            """
            INSERT INTO notification_queue(user_id, kind, payload_json, attempts, status, last_error, next_retry_at, created_at, updated_at)
            VALUES (?, ?, ?, 0, 'pending', '', ?, ?, ?)
            """,
            ("u1", "text", '{"text":"hello"}', now, now, now),
        )

    stat = pl.process_notification_queue(max_jobs=5)
    assert int(stat.get("processed") or 0) == 1
    assert int(stat.get("failed") or 0) == 0

    with pl._conn() as c:
        row = c.execute("SELECT status, attempts FROM notification_queue").fetchone()
    assert str(row["status"]) == "done"
    assert int(row["attempts"]) == 0


def test_process_queue_uses_exponential_backoff_on_dispatch_error(tmp_path, monkeypatch) -> None:
    import core.product_layer as pl

    monkeypatch.setattr(pl, "DB_PATH", tmp_path / "app_state.db")
    monkeypatch.setattr(pl, "SECRETS_PATH", tmp_path / "secrets.json")
    monkeypatch.setattr(pl, "APP_SECRET_PATH", tmp_path / ".app_secret.key")
    pl._init_db()

    monkeypatch.setattr(
        pl,
        "dispatch_text_notifications",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("smtp down")),
    )

    now = datetime.now(timezone.utc).isoformat()
    with pl._conn() as c:
        c.execute(
            """
            INSERT INTO notification_queue(user_id, kind, payload_json, attempts, status, last_error, next_retry_at, created_at, updated_at)
            VALUES (?, ?, ?, 1, 'pending', '', ?, ?, ?)
            """,
            ("u1", "text", '{"text":"hello"}', now, now, now),
        )

    stat = pl.process_notification_queue(max_jobs=5)
    assert int(stat.get("retried") or 0) == 1

    with pl._conn() as c:
        row = c.execute("SELECT status, attempts, last_error, next_retry_at, updated_at FROM notification_queue").fetchone()
    assert str(row["status"]) == "pending"
    assert int(row["attempts"]) == 2
    assert "retry_scheduled:dispatch" in str(row["last_error"])
    assert str(row["next_retry_at"]) > str(row["updated_at"])


def test_process_queue_fails_unsupported_kind_fast(tmp_path, monkeypatch) -> None:
    import core.product_layer as pl

    monkeypatch.setattr(pl, "DB_PATH", tmp_path / "app_state.db")
    monkeypatch.setattr(pl, "SECRETS_PATH", tmp_path / "secrets.json")
    monkeypatch.setattr(pl, "APP_SECRET_PATH", tmp_path / ".app_secret.key")
    pl._init_db()

    now = datetime.now(timezone.utc).isoformat()
    with pl._conn() as c:
        c.execute(
            """
            INSERT INTO notification_queue(user_id, kind, payload_json, attempts, status, last_error, next_retry_at, created_at, updated_at)
            VALUES (?, ?, ?, 0, 'pending', '', ?, ?, ?)
            """,
            ("u1", "unknown_kind", '{"text":"hello"}', now, now, now),
        )

    stat = pl.process_notification_queue(max_jobs=5)
    assert int(stat.get("failed") or 0) == 1

    with pl._conn() as c:
        row = c.execute("SELECT status, attempts, last_error FROM notification_queue").fetchone()
    assert str(row["status"]) == "failed"
    assert int(row["attempts"]) == 1
    assert str(row["last_error"]) == "unsupported_kind"


def test_admin_kpi_includes_notification_observability(tmp_path, monkeypatch) -> None:
    import core.product_layer as pl

    monkeypatch.setattr(pl, "DB_PATH", tmp_path / "app_state.db")
    monkeypatch.setattr(pl, "SECRETS_PATH", tmp_path / "secrets.json")
    monkeypatch.setattr(pl, "APP_SECRET_PATH", tmp_path / ".app_secret.key")
    pl._init_db()

    now = datetime.now(timezone.utc).isoformat()
    with pl._conn() as c:
        c.execute(
            """
            INSERT INTO notification_queue(user_id, kind, payload_json, attempts, status, last_error, next_retry_at, created_at, updated_at)
            VALUES (?, ?, ?, 4, 'failed', 'smtp_text:SMTPException:auth_failed', ?, ?, ?)
            """,
            ("u1", "text", "{}", now, now, now),
        )

    admin = pl.get_admin_kpi(days=30)
    obs = admin.get("notification_observability") or {}
    reasons = obs.get("fail_reasons") or []
    tiers = obs.get("retry_tiers") or []
    health = obs.get("runtime_health") or {}
    assert isinstance(reasons, list)
    assert isinstance(tiers, list)
    assert isinstance(health, dict)
    assert any(str(x.get("reason") or "") == "smtp_text" for x in reasons)
    assert any(str(x.get("retry_tier") or "") in ("t3_4", "t5_plus") for x in tiers)
    assert "high_retry_ratio" in health
    assert "oldest_pending_age_minutes" in health


def test_process_queue_emits_timing_metric(tmp_path, monkeypatch) -> None:
    import core.product_layer as pl

    monkeypatch.setattr(pl, "DB_PATH", tmp_path / "app_state.db")
    monkeypatch.setattr(pl, "SECRETS_PATH", tmp_path / "secrets.json")
    monkeypatch.setattr(pl, "APP_SECRET_PATH", tmp_path / ".app_secret.key")
    pl._init_db()

    metrics: list[dict] = []

    def fake_log_timing(metric: str, duration_ms: float, **fields):
        metrics.append({"metric": metric, "duration_ms": duration_ms, **fields})

    monkeypatch.setattr(pl, "log_timing", fake_log_timing)

    now = datetime.now(timezone.utc).isoformat()
    with pl._conn() as c:
        c.execute(
            """
            INSERT INTO notification_queue(user_id, kind, payload_json, attempts, status, last_error, next_retry_at, created_at, updated_at)
            VALUES (?, ?, ?, 0, 'pending', '', ?, ?, ?)
            """,
            ("u1", "text", '{"text":"hello"}', now, now, now),
        )

    stat = pl.process_notification_queue(max_jobs=5)
    assert int(stat.get("processed") or 0) == 1
    assert any(m.get("metric") == "product_layer.process_notification_queue" for m in metrics)
