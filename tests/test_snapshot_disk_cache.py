from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

import time

from core.snapshot_disk_cache import cache_ttl_seconds, get_cached_snapshot, get_disk_snapshot_any_age, put_snapshot


def test_put_and_get_respects_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    import core.snapshot_disk_cache as mod

    monkeypatch.setenv("II_SNAPSHOT_DISK_CACHE", "1")
    d = tempfile.mkdtemp()
    monkeypatch.setattr(mod, "_CACHE_PATH", Path(d) / "c.json")
    monkeypatch.setenv("II_SNAPSHOT_CACHE_TTL_SEC", "3600")
    assert cache_ttl_seconds() == 3600.0

    put_snapshot("AAA", {"symbol": "AAA", "price": 100.0, "eps": 5.0})
    hit = get_cached_snapshot("AAA")
    assert hit is not None
    assert hit.get("price") == 100.0
    assert hit.get("_disk_cache", {}).get("hit") is True


def test_get_disk_snapshot_any_age_when_ttl_expired(monkeypatch: pytest.MonkeyPatch) -> None:
    import core.snapshot_disk_cache as mod

    monkeypatch.setenv("II_SNAPSHOT_DISK_CACHE", "1")
    monkeypatch.setenv("II_SNAPSHOT_CACHE_TTL_SEC", "1")
    d = tempfile.mkdtemp()
    monkeypatch.setattr(mod, "_CACHE_PATH", Path(d) / "c.json")
    put_snapshot("OLD", {"symbol": "OLD", "price": 9.0})
    time.sleep(1.2)
    assert get_cached_snapshot("OLD") is None
    any_age = get_disk_snapshot_any_age("OLD")
    assert any_age is not None
    assert any_age["symbol"] == "OLD"
    assert any_age.get("_disk_cache", {}).get("stale") is True


def test_fetch_uses_disk_cache_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    import core.snapshot_disk_cache as sdc
    from scrapers import financial_data as fd
    from scrapers.finance_scraper import ScraperError

    monkeypatch.setenv("II_SNAPSHOT_DISK_CACHE", "1")
    d = tempfile.mkdtemp()
    monkeypatch.setattr(sdc, "_CACHE_PATH", Path(d) / "snap.json")
    put_snapshot("VNM", {"symbol": "VNM", "price": 1.0, "eps": 1.0, "source": "test"})

    called = {"n": 0}

    def _fake_get_stock_data(sym: str):
        called["n"] += 1
        raise ScraperError("no network")

    import scrapers.finance_scraper as fs

    monkeypatch.setattr(fs, "get_stock_data", _fake_get_stock_data)
    monkeypatch.setattr(fd, "_load_mock_json", lambda: {})

    snap = fd.fetch_financial_snapshot("VNM", bypass_cache=False)
    assert snap is not None
    assert snap.get("symbol") == "VNM"
    assert called["n"] == 0


def test_fetch_uses_stale_disk_when_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    import core.snapshot_disk_cache as sdc
    from scrapers import financial_data as fd

    monkeypatch.setenv("II_SNAPSHOT_DISK_CACHE", "1")
    monkeypatch.setenv("II_SNAPSHOT_CACHE_TTL_SEC", "1")
    monkeypatch.setenv("II_READ_STALE_DISK", "1")
    d = tempfile.mkdtemp()
    monkeypatch.setattr(sdc, "_CACHE_PATH", Path(d) / "snap2.json")
    put_snapshot("STALE", {"symbol": "STALE", "price": 5.0, "eps": 1.0, "source": "test"})
    time.sleep(1.2)

    def _fake_get_stock_data(sym: str, **kw):
        return {"price": 12.0, "source": "test_live"}

    import scrapers.finance_scraper as fs

    monkeypatch.setattr(fs, "get_stock_data", _fake_get_stock_data)
    monkeypatch.setattr(fd, "_load_mock_json", lambda: {})

    snap = fd.fetch_financial_snapshot("STALE", bypass_cache=False)
    assert snap is not None
    assert snap.get("symbol") == "STALE"
    assert float(snap.get("price") or 0) == 12.0
    assert "TTL" in (snap.get("snapshot_fundamentals_note_vi") or "")
