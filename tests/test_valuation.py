from __future__ import annotations

import pytest

from core.valuation import benjamin_graham_value, margin_of_safety_pct, value_investing_summary


def test_graham_and_mos() -> None:
    v = benjamin_graham_value(1000.0, 5.0, bond_yield_pct=4.4)
    assert v > 0
    mos = margin_of_safety_pct(v, 8000.0)
    assert mos is not None and mos > 0


def test_value_investing_summary_includes_data_trust() -> None:
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
        "data_trust": {"fundamentals_source": "test", "price_source": "x", "warnings": []},
    }
    s = value_investing_summary(snap)
    assert "data_trust" in s
    assert s["data_trust"]["fundamentals_source"] == "test"
    assert s.get("growth_rate_pct_source") == "unspecified"
    assert "bond_yield_pct_used" in s
    assert s.get("growth_rate_pct_source_label_vi")
    assert float(s.get("eps_for_graham") or 0) == 10.0
    assert s.get("eps_basis_key") == "reported"


def test_eps_ttm_overrides_reported_for_graham() -> None:
    snap = {
        "symbol": "T",
        "name": "Test",
        "price": 100.0,
        "currency": "VND",
        "eps": 8.0,
        "eps_ttm": 10.0,
        "growth_rate_pct": 5.0,
        "book_value_per_share": 50.0,
        "source": "test",
        "piotroski": {},
        "data_trust": {},
    }
    s = value_investing_summary(snap)
    assert float(s.get("eps_for_graham") or 0) == 10.0
    assert s.get("eps_basis_key") == "ttm"


def test_hybrid_can_infer_de_cr_roe_from_piotroski() -> None:
    snap = {
        "symbol": "T",
        "name": "Test",
        "price": 100.0,
        "currency": "VND",
        "eps": 10.0,
        "growth_rate_pct": 8.0,
        "book_value_per_share": 50.0,
        "source": "test",
        "piotroski": {
            "net_income": 150,
            "total_assets": 1000,
            "long_term_debt": 100,
            "current_assets": 300,
            "current_liabilities": 150,
        },
    }
    s = value_investing_summary(snap)
    assert s["legend_data_ready"]["has_graham_inputs"] is True
    assert float(s.get("debt_to_equity") or 0) > 0
    assert float(s.get("current_ratio") or 0) > 0
    assert float(s.get("roe_5y_avg") or 0) > 0
    assert isinstance(s.get("legend_fallback_notes"), list)


def test_legend_profile_thresholds_exposed() -> None:
    snap = {
        "symbol": "T",
        "name": "Test",
        "price": 80.0,
        "currency": "VND",
        "eps": 10.0,
        "growth_rate_pct": 12.0,
        "book_value_per_share": 50.0,
        "legend_profile": "aggressive",
        "debt_to_equity": 0.3,
        "current_ratio": 2.0,
        "roe": 20.0,
        "source": "test",
        "piotroski": {},
    }
    s = value_investing_summary(snap)
    assert s.get("legend_profile") == "aggressive"
    th = s.get("legend_thresholds") or {}
    assert float(th.get("strong_buy_mos_min") or 0) > 0
    assert float(th.get("max_peg_for_buy") or 0) > 0


def test_legend_threshold_overrides_work() -> None:
    snap = {
        "symbol": "T",
        "name": "Test",
        "price": 80.0,
        "currency": "VND",
        "eps": 10.0,
        "growth_rate_pct": 12.0,
        "book_value_per_share": 50.0,
        "debt_to_equity": 0.3,
        "current_ratio": 2.0,
        "roe": 20.0,
        "legend_profile": "defensive",
        "legend_strong_buy_mos_min": 7.0,
        "legend_max_peg_for_buy": 1.8,
        "source": "test",
    }
    s = value_investing_summary(snap)
    th = s.get("legend_thresholds") or {}
    assert float(th.get("strong_buy_mos_min") or 0) == 7.0
    assert float(th.get("max_peg_for_buy") or 0) == 1.8
