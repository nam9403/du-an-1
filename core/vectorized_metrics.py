"""
Vectorized metrics for large-universe scoring.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def apply_vectorized_scan_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute Expected Return / Risk / Recommendation bằng vectorization.
    Yêu cầu các cột: MOS%, Rev YoY%, F-Score, Pha, Vol multiple.
    """
    if df.empty:
        return df
    out = df.copy()
    for col, default in (
        ("MOS%", 0.0),
        ("Rev YoY%", 0.0),
        ("F-Score", 0.0),
        ("Vol multiple", 0.0),
    ):
        if col not in out.columns:
            out[col] = default

    mos = pd.to_numeric(out["MOS%"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    rev = pd.to_numeric(out["Rev YoY%"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    fscore = pd.to_numeric(out["F-Score"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    vol_multiple = pd.to_numeric(out["Vol multiple"], errors="coerce").fillna(0.0).to_numpy(dtype=float)

    expected = np.maximum(mos * 0.6 + rev * 0.2 + fscore * 1.2, 0.0)
    risk = np.clip(12.0 + np.maximum(0.0, (1.2 - vol_multiple) * 6.0) + (9.0 - fscore), 3.0, 35.0)
    out["Expected Return %"] = np.round(expected, 2)
    out["Risk % (ước tính)"] = np.round(risk, 2)

    phase = out.get("Pha", pd.Series(["neutral"] * len(out))).astype(str).str.lower()
    is_buy = (expected >= 12.0) & (risk <= 18.0) & phase.isin(["accumulation", "breakout"]).to_numpy()
    is_hold = (~is_buy) & (expected >= 6.0) & (risk <= 24.0)
    vec_rec = np.where(is_buy, "BUY", np.where(is_hold, "HOLD", "AVOID"))
    existing = out.get("Khuyến nghị")
    if existing is None:
        out["Khuyến nghị"] = vec_rec
    else:
        ex = existing.astype(str).to_numpy()
        out["Khuyến nghị"] = np.where((ex == "") | (ex == "PENDING_VECTORIZE"), vec_rec, ex)
    return out

