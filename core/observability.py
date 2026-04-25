from __future__ import annotations

import contextlib
import contextvars
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any

_correlation_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("correlation_id", default="")
_ROOT = Path(__file__).resolve().parent.parent


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        corr = correlation_id()
        if corr:
            payload["correlation_id"] = corr
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def log_timing(metric: str, duration_ms: float, **fields: Any) -> None:
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "metric": metric,
        "duration_ms": round(float(duration_ms), 2),
        **fields,
    }
    corr = correlation_id()
    if corr:
        payload["correlation_id"] = corr
    logging.getLogger("core.metrics").info("timing %s", json.dumps(payload, ensure_ascii=False, sort_keys=True))
    timing_path = Path(
        str(os.environ.get("II_TIMING_LOG_PATH") or (_ROOT / "data" / "reports" / "logs" / "timing_metrics.jsonl"))
    )
    try:
        timing_path.parent.mkdir(parents=True, exist_ok=True)
        with timing_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except OSError:
        # Never break app runtime because local metrics log file is unavailable.
        pass


@contextlib.contextmanager
def timed_operation(metric: str, **fields: Any):
    start = perf_counter()
    try:
        yield
    finally:
        log_timing(metric, (perf_counter() - start) * 1000.0, **fields)


def correlation_id() -> str:
    return _correlation_id_var.get()


def ensure_correlation_id(seed: str = "") -> str:
    existing = correlation_id()
    if existing:
        return existing
    cid = str(seed or os.environ.get("II_CORRELATION_ID") or uuid.uuid4().hex)
    _correlation_id_var.set(cid)
    return cid


def configure_observability(component: str = "app") -> str:
    root = logging.getLogger()
    level_name = str(os.environ.get("II_LOG_LEVEL", "INFO")).strip().upper()
    level = getattr(logging, level_name, logging.INFO)
    root.setLevel(level)

    if not root.handlers:
        handler = logging.StreamHandler()
        if str(os.environ.get("II_LOG_JSON", "1")).strip().lower() in ("0", "false", "no", "off"):
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
        else:
            handler.setFormatter(JsonFormatter())
        root.addHandler(handler)

    cid = ensure_correlation_id(component)
    logging.getLogger(__name__).info("observability_configured component=%s", component)
    return cid
