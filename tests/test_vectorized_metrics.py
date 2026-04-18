import pandas as pd

from core.vectorized_metrics import apply_vectorized_scan_metrics


def test_apply_vectorized_scan_metrics_populates_columns():
    df = pd.DataFrame(
        [
            {"MOS%": 30, "Rev YoY%": 12, "F-Score": 7, "Pha": "accumulation", "Vol multiple": 1.2, "Khuyến nghị": "PENDING_VECTORIZE"},
            {"MOS%": 5, "Rev YoY%": 2, "F-Score": 3, "Pha": "distribution", "Vol multiple": 0.8, "Khuyến nghị": "PENDING_VECTORIZE"},
        ]
    )
    out = apply_vectorized_scan_metrics(df)
    assert "Expected Return %" in out.columns
    assert "Risk % (ước tính)" in out.columns
    assert out.iloc[0]["Khuyến nghị"] in ("BUY", "HOLD")
    assert out.iloc[1]["Khuyến nghị"] in ("HOLD", "AVOID")


def test_apply_vectorized_scan_metrics_preserves_custom_recommendation():
    df = pd.DataFrame(
        [
            {"MOS%": 40, "Rev YoY%": 15, "F-Score": 8, "Pha": "accumulation", "Vol multiple": 1.5, "Khuyến nghị": "BUY"},
        ]
    )
    out = apply_vectorized_scan_metrics(df)
    assert out.iloc[0]["Khuyến nghị"] == "BUY"

