from __future__ import annotations

import pytest

from core.valuation import value_investing_summary


def test_value_investing_summary_includes_elite_scenarios() -> None:
    snap = {
        "symbol": "T",
        "name": "Test",
        "price": 100.0,
        "currency": "VND",
        "eps": 10.0,
        "growth_rate_pct": 5.0,
        "book_value_per_share": 50.0,
        "source": "test",
        "piotroski": {},
        "data_trust": {},
        "sector_pe_5y_avg": 12.0,
        "fair_pb_multiple": 2.0,
    }
    v = value_investing_summary(snap, include_extensions=True)
    assert "scenario_valuation" in v
    assert len(v["scenario_valuation"]) == 3
    assert v["scenario_valuation"][1]["id"] == "base"
    assert "intrinsic_band" in v
    assert "valuation_audit" in v and v["valuation_audit"].get("input_hash_sha256")
    assert v.get("valuation_excellence_score") is not None
    assert float(v["valuation_excellence_score"]) <= 9.5


def test_value_investing_summary_no_extensions() -> None:
    snap = {
        "symbol": "T",
        "name": "Test",
        "price": 100.0,
        "currency": "VND",
        "eps": 10.0,
        "growth_rate_pct": 5.0,
        "book_value_per_share": 50.0,
        "source": "test",
        "piotroski": {},
    }
    v = value_investing_summary(snap, include_extensions=False)
    assert "scenario_valuation" not in v


def test_elite_vnm_mock_snapshot() -> None:
    import json
    from pathlib import Path

    p = Path(__file__).resolve().parent.parent / "data" / "mock_financials.json"
    if not p.exists():
        pytest.skip("mock_financials.json missing")
    data = json.loads(p.read_text(encoding="utf-8"))
    snap = data.get("VNM")
    if not isinstance(snap, dict):
        pytest.skip("no VNM in mock")
    snap = {**snap, "source": "mock_json"}
    v = value_investing_summary(snap)
    assert v.get("simple_dcf", {}).get("ok") is True
    assert v["scenario_valuation"][1]["id"] == "base"
