from __future__ import annotations

from scrapers.financial_statements import compute_trend_metrics


def test_compute_trend_metrics_yoy() -> None:
    rows = [
        {"year": 2024, "quarter": 3, "revenue": 110.0, "net_income": 20.0},
        {"year": 2023, "quarter": 3, "revenue": 100.0, "net_income": 18.0},
        {"year": 2023, "quarter": 2, "revenue": 95.0, "net_income": 17.0},
    ]
    t = compute_trend_metrics(rows)
    assert t["revenue_yoy_pct"] is not None
    assert abs(float(t["revenue_yoy_pct"]) - 10.0) < 0.01
    assert t["profit_yoy_pct"] is not None
    assert t.get("yoy_compare_label_vi") == "Q3/2024 so với Q3/2023"
