from __future__ import annotations

from core.professional_pdf import build_professional_report_pdf


def test_build_professional_pdf_minimal() -> None:
    report = {
        "ticker": "TST",
        "final_action": "WATCH",
        "confidence_score": 70,
        "whys_steps": ["Bước 1", "Bước 2"],
        "analysis_text": "Kết luận ngắn.",
        "phase": {"phase": "neutral", "reason": "—"},
        "valuation": {"symbol": "TST", "margin_of_safety_composite_pct": 12.0, "advice": "Theo dõi"},
        "risk_plan": {
            "entry_price": 10000,
            "stop_loss_price": 9000,
            "take_profit_price": 12000,
            "max_position_pct": 20,
            "allocated_capital_vnd": 20_000_000,
            "worst_case_loss_vnd": 1_000_000,
        },
        "total_capital_vnd": 100_000_000.0,
    }
    val = report["valuation"]
    pdf = build_professional_report_pdf("TST", report, val, ohlcv_png=None, allocation_png=None)
    assert pdf.startswith(b"%PDF")
    assert len(pdf) > 1000
