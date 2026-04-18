from core.graham_guardrails import (
    evaluate_graham_7_criteria,
    mr_market_sentiment_index,
    watcher_governance_debt_flags,
)


def test_evaluate_graham_7_criteria_counts_passed_rules():
    snapshot = {
        "market_cap_bn_vnd": 15000,
        "debt_to_equity": 0.8,
        "current_ratio": 1.7,
        "dividend_paid_years": 9,
        "eps_growth_10y_pct": 40,
        "eps_positive_years_10y": 10,
        "pe": 14,
        "pb": 1.9,
    }
    valuation = {"price": 50, "eps_for_graham": 4, "book_value_per_share": 20}
    out = evaluate_graham_7_criteria(snapshot, valuation)
    assert out["total"] == 7
    assert out["passed"] == 7


def test_mr_market_sentiment_index_penalizes_structural_news():
    out = mr_market_sentiment_index(
        stock_day_change_pct=-4.0,
        index_day_change_pct=-1.2,
        structural_news_count=2,
    )
    assert out["score"] < 40
    assert out["mood_vi"] in ("Sợ hãi cao", "Trung tính")


def test_watcher_governance_debt_flags_detects_risk():
    out = watcher_governance_debt_flags(
        {
            "debt_to_equity": 2.0,
            "debt_to_equity_prior": 1.6,
            "insider_net_sell_pct": 0.8,
        }
    )
    assert out["flag_count"] >= 2
    assert len(out["flags_vi"]) == out["flag_count"]

