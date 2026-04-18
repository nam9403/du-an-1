from __future__ import annotations

from unittest.mock import patch

from scrapers.finance_scraper import ScraperError

from core.watchlist_live import build_watchlist_price_dataframe


def test_build_watchlist_live_uses_quote_then_fallback() -> None:
    def fake_quote(sym: str, *, use_cache: bool = True):
        assert use_cache is False
        if sym == "AAA":
            return {"price": 1000.0, "source": "test"}
        raise ScraperError("offline")

    def fake_snap(sym: str):
        if sym == "BBB":
            return {"symbol": "BBB", "price": 50.0, "price_source": "portal", "source": "mock_json"}
        return None

    with (
        patch("core.watchlist_live.get_stock_data", side_effect=fake_quote),
        patch("core.watchlist_live.fetch_financial_snapshot", side_effect=fake_snap),
    ):
        df, ts = build_watchlist_price_dataframe(["AAA", "BBB", "ZZZ"], live=True)
    assert "UTC" in ts
    assert len(df) == 3
    a = df[df["Mã"] == "AAA"].iloc[0]
    assert float(a["Giá hiện tại"]) == 1000.0
    assert a["Trạng thái"] == "Live"


def test_build_watchlist_not_live_snapshot_only() -> None:
    def fake_snap(sym: str):
        return {"symbol": sym, "price": 10.0, "source": "mock_json"}

    with patch("core.watchlist_live.fetch_financial_snapshot", side_effect=fake_snap):
        df, _ = build_watchlist_price_dataframe(["AAA"], live=False)
    assert len(df) == 1
    assert df.iloc[0]["Giá hiện tại"] == 10.0
