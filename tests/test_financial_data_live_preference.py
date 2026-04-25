from __future__ import annotations


def test_prod_prefers_live_over_mock(monkeypatch) -> None:
    import scrapers.financial_data as fd
    import scrapers.finance_scraper as fs

    monkeypatch.setenv("II_ENV", "prod")
    monkeypatch.delenv("II_SNAPSHOT_PREFER_LIVE", raising=False)
    monkeypatch.setattr(fd, "get_cached_snapshot", lambda sym: None)
    monkeypatch.setattr(fd, "get_disk_snapshot_any_age", lambda sym: None)
    monkeypatch.setattr(fd, "_load_mock_json", lambda: {"FPT": {"price": 1.0}})

    monkeypatch.setattr(fs, "get_stock_data", lambda sym, use_cache=True: {"symbol": sym, "price": 123.0, "source": "vndirect_finfo"})
    monkeypatch.setattr(fd, "_resolve_snapshot_market_price", lambda row, sym: None)
    monkeypatch.setattr(fd, "_merge_stock_metadata", lambda row, sym: None)
    monkeypatch.setattr(fd, "put_snapshot", lambda sym, row: None)

    out = fd.fetch_financial_snapshot("FPT")
    assert out is not None
    assert str(out.get("source")) == "vndirect_finfo"
    assert float(out.get("price") or 0) == 123.0


def test_dev_prefers_mock_when_available(monkeypatch) -> None:
    import scrapers.financial_data as fd

    monkeypatch.setenv("II_ENV", "dev")
    monkeypatch.delenv("II_SNAPSHOT_PREFER_LIVE", raising=False)
    monkeypatch.setattr(fd, "get_cached_snapshot", lambda sym: None)
    monkeypatch.setattr(fd, "get_disk_snapshot_any_age", lambda sym: None)
    monkeypatch.setattr(fd, "_load_mock_json", lambda: {"FPT": {"price": 98_000}})
    monkeypatch.setattr(fd, "_resolve_snapshot_market_price", lambda row, sym: None)
    monkeypatch.setattr(fd, "_merge_stock_metadata", lambda row, sym: None)
    monkeypatch.setattr(fd, "put_snapshot", lambda sym, row: None)

    out = fd.fetch_financial_snapshot("FPT")
    assert out is not None
    assert str(out.get("source")) == "mock_json"
    assert float(out.get("price") or 0) == 98_000
