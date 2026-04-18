"""Đảm bảo chọn đúng phiên giao dịch mới nhất từ VNDirect stock-prices."""

from __future__ import annotations

from scrapers.finance_scraper import _pick_latest_vndirect_price_row


def test_pick_latest_vndirect_price_row_by_date() -> None:
    rows = [
        {"code": "VNM", "date": "2024-01-02", "close": 100.0},
        {"code": "VNM", "date": "2024-06-01", "close": 120.0},
        {"code": "VNM", "date": "2023-12-01", "close": 80.0},
    ]
    r = _pick_latest_vndirect_price_row(rows, "VNM")
    assert float(r["close"]) == 120.0
