from __future__ import annotations

import pandas as pd


def test_scan_potential_stocks_returns_sorted_ready_rows(monkeypatch) -> None:
    import core.scanner_service as ss

    def fake_snapshot(sym: str):
        return {"symbol": sym, "price": 100.0}

    def fake_report(sym: str, snap: dict, profile: str, capital: float, quick_mode: bool = True):
        base = {
            "valuation": {
                "price": 100.0,
                "margin_of_safety_composite_pct": 25.0 if sym == "AAA" else 5.0,
                "piotroski_score": 8 if sym == "AAA" else 5,
                "industry_subtype_label_vi": "Test",
            },
            "financials": {"revenue_growth_yoy": 20.0 if sym == "AAA" else 3.0},
            "phase": {"phase": "breakout" if sym == "AAA" else "neutral", "metrics": {"vol_multiple": 1.6 if sym == "AAA" else 1.0}},
        }
        return base

    def fake_ohlcv(sym: str):
        # avg vol20 = 500k for both symbols
        return pd.DataFrame(
            {
                "date": pd.date_range("2026-01-01", periods=30, freq="D"),
                "open": [1.0] * 30,
                "high": [1.0] * 30,
                "low": [1.0] * 30,
                "close": [1.0] * 30,
                "volume": [500_000.0] * 30,
            }
        )

    monkeypatch.setattr(ss, "load_snapshot_cached", fake_snapshot)
    monkeypatch.setattr(ss, "load_strategic_report_cached", fake_report)
    monkeypatch.setattr(ss, "load_ohlcv_cached", fake_ohlcv)

    df = ss.scan_potential_stocks.__wrapped__(("AAA", "BBB"), "growth", 100_000.0)  # type: ignore[attr-defined]
    assert not df.empty
    assert list(df["Mã"]) == ["AAA", "BBB"]
    assert str(df.iloc[0]["Trạng thái"]) == "Sẵn sàng"
    assert float(df.iloc[0]["Expected Return %"]) >= float(df.iloc[1]["Expected Return %"])


def test_load_autopilot_board_keeps_ready_rows_and_limits(monkeypatch) -> None:
    import core.scanner_service as ss

    monkeypatch.setattr(ss, "list_universe_symbols", lambda limit=30: ["AAA", "BBB", "CCC"])
    fake_scan = pd.DataFrame(
        [
            {"Mã": "AAA", "Khuyến nghị": "BUY", "MOS%": 20.0, "Expected Return %": 15.0, "Risk % (ước tính)": 10.0, "Trạng thái": "Sẵn sàng"},
            {"Mã": "BBB", "Khuyến nghị": "HOLD", "MOS%": 12.0, "Expected Return %": 8.0, "Risk % (ước tính)": 12.0, "Trạng thái": "Sẵn sàng"},
            {"Mã": "CCC", "Khuyến nghị": "AVOID", "MOS%": 1.0, "Expected Return %": 1.0, "Risk % (ước tính)": 30.0, "Trạng thái": "Không đạt lọc thanh khoản"},
        ]
    )
    monkeypatch.setattr(ss, "scan_potential_stocks", lambda *a, **k: fake_scan)

    out = ss.load_autopilot_board.__wrapped__("growth", universe_limit=30, min_avg_volume_20=100_000.0)  # type: ignore[attr-defined]
    assert not out.empty
    assert "Mã" in out.columns
    assert out.iloc[0]["Mã"] == "AAA"
    assert "CCC" not in out["Mã"].tolist()


def test_simple_view_for_plan_hides_pro_fields_for_free() -> None:
    import core.scanner_service as ss

    df = pd.DataFrame(
        [
            {
                "Mã": "AAA",
                "Hành động": "BUY",
                "Giá hiện tại": 100.0,
                "Giá trị nội tại": 120.0,
                "Biên an toàn %": 20.0,
                "Giá vào": 98.0,
                "SL": 92.0,
                "TP": 120.0,
                "Tỷ trọng %": 10.0,
            }
        ]
    )
    free_df = ss.simple_view_for_plan(df, "free")
    pro_df = ss.simple_view_for_plan(df, "pro")
    assert "Giá vào" not in free_df.columns
    assert "SL" not in free_df.columns
    assert "TP" not in free_df.columns
    assert "Giá vào" in pro_df.columns


def test_scan_potential_stocks_emits_timing_metric(monkeypatch) -> None:
    import core.scanner_service as ss

    metrics: list[dict] = []

    def fake_log_timing(metric: str, duration_ms: float, **fields):
        metrics.append({"metric": metric, "duration_ms": duration_ms, **fields})

    monkeypatch.setattr(ss, "log_timing", fake_log_timing)
    monkeypatch.setattr(ss, "load_snapshot_cached", lambda sym: {"symbol": sym, "price": 100.0})
    monkeypatch.setattr(
        ss,
        "load_strategic_report_cached",
        lambda sym, snap, profile, capital, quick_mode=True: {
            "valuation": {
                "price": 100.0,
                "margin_of_safety_composite_pct": 12.0,
                "piotroski_score": 7,
                "industry_subtype_label_vi": "Test",
            },
            "financials": {"revenue_growth_yoy": 10.0},
            "phase": {"phase": "accumulation", "metrics": {"vol_multiple": 1.2}},
        },
    )
    monkeypatch.setattr(
        ss,
        "load_ohlcv_cached",
        lambda sym: pd.DataFrame(
            {
                "date": pd.date_range("2026-01-01", periods=25, freq="D"),
                "open": [1.0] * 25,
                "high": [1.0] * 25,
                "low": [1.0] * 25,
                "close": [1.0] * 25,
                "volume": [400_000.0] * 25,
            }
        ),
    )

    out = ss.scan_potential_stocks.__wrapped__(("AAA",), "growth", 100_000.0)  # type: ignore[attr-defined]
    assert not out.empty
    assert any(m.get("metric") == "scanner.scan_potential_stocks" for m in metrics)

