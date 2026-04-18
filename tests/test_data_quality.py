from __future__ import annotations

from core.data_quality import compute_bctc_readiness, compute_snapshot_confidence, enrich_snapshot_quality_fields


def test_confidence_mock_lower() -> None:
    snap = {"source": "mock_json", "price_source": "", "piotroski": {}}
    s, _ = compute_snapshot_confidence(snap)
    assert 1 <= s <= 5


def test_confidence_stronger_with_price() -> None:
    snap = {
        "source": "mock_json",
        "price_source": "portal_ohlcv:live",
        "piotroski": {"a": 1, "b": 2, "c": 3, "d": 4},
    }
    s, reasons = compute_snapshot_confidence(snap)
    assert s >= 3
    assert reasons


def test_enrich_idempotent() -> None:
    row = {"source": "mock_json", "price_source": "quote:x"}
    enrich_snapshot_quality_fields(row)
    enrich_snapshot_quality_fields(row)
    assert row.get("data_confidence_score") is not None
    assert row.get("_dq_enriched") is True


def test_bctc_readiness_scores() -> None:
    snap = {
        "source": "vndirect_finfo",
        "price_source": "portal_ohlcv:x",
        "eps": 5000,
        "book_value_per_share": 15000,
        "growth_rate_pct": 5,
        "sector_pe_5y_avg": 12,
        "piotroski": {k: 1 for k in ("net_income", "net_income_prior", "revenue", "revenue_prior", "total_assets", "total_assets_prior")},
    }
    br = compute_bctc_readiness(snap)
    assert br["score_0_100"] >= 60
    assert br["tier"] in ("A", "B")
    assert br["graham_reliable"] is True
