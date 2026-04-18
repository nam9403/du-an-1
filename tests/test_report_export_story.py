from core.report_export import premium_storytelling_report_markdown


def test_premium_storytelling_report_contains_sections():
    report = {
        "ticker": "FPT",
        "valuation": {
            "symbol": "FPT",
            "name": "FPT Corp",
            "currency": "VND",
            "industry_cluster_id": "tech_telecom",
            "price": 120000,
            "composite_target_price": 150000,
            "margin_of_safety_composite_pct": 20.0,
            "piotroski_score": 7,
            "growth_rate_pct": 12.0,
        },
        "financials": {"debt_to_equity": 0.4, "revenue_growth_yoy": 15.5},
        "phase": {"phase": "accumulation", "reason": "gia on dinh"},
        "risk_plan": {"take_profit_price": 148000, "stop_loss_price": 108000, "buy_zone": {"low": 115000, "high": 122000}},
        "news": [{"title": "FPT tang truong loi nhuan quy I"}],
    }
    out = premium_storytelling_report_markdown(report)
    assert "Premium Story Report" in out
    assert "Câu chuyện doanh nghiệp" in out
    assert "Góc nhìn định giá Graham" in out
    assert "Kế hoạch hành động" in out
    assert "công nghệ/viễn thông" in out.lower()

