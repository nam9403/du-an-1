from __future__ import annotations

from scripts.live_data_gate_check import classify_source, evaluate_live_data


def test_classify_source() -> None:
    assert classify_source("mock_json") == "mock"
    assert classify_source("cache:vietstock") == "cache"
    assert classify_source("vietstock") == "live"
    assert classify_source("") == "unknown"


def test_evaluate_live_data_counts_and_ratios() -> None:
    samples = {
        "AAA": {"price": 100.0, "source": "mock_json"},
        "BBB": {"price": 101.0, "source": "cache:vietstock"},
        "CCC": {"price": 102.0, "source": "vietstock"},
        "DDD": None,
    }

    def fake_fetch(sym: str):
        return samples[sym]

    out = evaluate_live_data(["AAA", "BBB", "CCC", "DDD"], fetch_fn=fake_fetch)
    assert int(out["total"]) == 4
    assert int(out["counts"]["mock"]) == 1
    assert int(out["counts"]["cache"]) == 1
    assert int(out["counts"]["live"]) == 1
    assert int(out["counts"]["none"]) == 1
    assert float(out["live_ratio"]) == 0.25
    assert float(out["available_ratio"]) == 0.75
