"""
Calibration utilities for Hybrid Legend valuation.

Goal:
- compare new model vs a legacy Graham-only baseline on historical price paths
- optimize decision thresholds on available symbol universe
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import product
from typing import Any

import numpy as np

from core.strategy_backtest import fetch_long_ohlcv_for_backtest
from core.valuation import benjamin_graham_value, margin_of_safety_pct, value_investing_summary


@dataclass
class SymbolBacktestStats:
    symbol: str
    days: int
    legend_return_pct: float
    legacy_return_pct: float
    buyhold_return_pct: float
    legend_max_dd_pct: float
    legacy_max_dd_pct: float


def _max_drawdown_pct(equity: np.ndarray) -> float:
    if len(equity) == 0:
        return 0.0
    peak = np.maximum.accumulate(equity)
    dd = np.where(peak > 0, (peak - equity) / peak * 100.0, 0.0)
    return float(np.max(dd)) if len(dd) else 0.0


def _legacy_decision(snapshot: dict[str, Any], price: float) -> str:
    eps = float(snapshot.get("eps_ttm") or snapshot.get("eps") or 0)
    g = float(snapshot.get("growth_rate_pct") or 0)
    y = float(snapshot.get("bond_yield_pct") or 4.4)
    intrinsic = benjamin_graham_value(eps, g, bond_yield_pct=y)
    mos = margin_of_safety_pct(intrinsic, price)
    if intrinsic <= 0 or mos is None:
        return "hold"
    if mos >= 30:
        return "buy"
    if mos < 0:
        return "sell"
    return "hold"


def _simulate_equity(
    closes: np.ndarray,
    decisions: list[str],
    *,
    initial_cash: float = 100_000_000.0,
) -> np.ndarray:
    cash = float(initial_cash)
    shares = 0.0
    invested = False
    eq: list[float] = []
    for i, px in enumerate(closes):
        d = decisions[i]
        if d == "buy" and not invested and px > 0:
            shares = cash / px
            cash = 0.0
            invested = True
        elif d == "sell" and invested and px > 0:
            cash = shares * px
            shares = 0.0
            invested = False
        eq.append(cash + shares * px)
    return np.array(eq, dtype=float)


def _evaluate_on_closes(
    closes: np.ndarray,
    snapshot: dict[str, Any],
    *,
    legend_profile: str,
    strong_buy_mos_min: float | None,
    max_peg_for_buy: float | None,
    watch_buy_mos_min: float = 8.0,
) -> tuple[float, float, float, float]:
    legend_decisions: list[str] = []
    legacy_decisions: list[str] = []
    for px in closes:
        snap_now = dict(snapshot)
        snap_now["price"] = float(px)
        snap_now["legend_profile"] = legend_profile
        if strong_buy_mos_min is not None:
            snap_now["legend_strong_buy_mos_min"] = strong_buy_mos_min
        if max_peg_for_buy is not None:
            snap_now["legend_max_peg_for_buy"] = max_peg_for_buy
        val = value_investing_summary(snap_now, include_extensions=False)
        decision = str(val.get("instant_conclusion") or "Theo dõi")
        mos = val.get("margin_of_safety_composite_pct")
        mos_v = float(mos) if mos is not None else -999.0
        if decision == "Mua mạnh":
            legend_decisions.append("buy")
        elif decision == "Theo dõi" and mos_v >= watch_buy_mos_min:
            legend_decisions.append("buy")
        elif decision == "Bỏ qua":
            legend_decisions.append("sell")
        else:
            legend_decisions.append("hold")
        legacy_decisions.append(_legacy_decision(snapshot, float(px)))

    eq_legend = _simulate_equity(closes, legend_decisions)
    eq_legacy = _simulate_equity(closes, legacy_decisions)
    initial = 100_000_000.0
    legend_ret = (eq_legend[-1] / initial - 1.0) * 100.0 if len(eq_legend) else 0.0
    legacy_ret = (eq_legacy[-1] / initial - 1.0) * 100.0 if len(eq_legacy) else 0.0
    return (
        float(legend_ret),
        float(legacy_ret),
        _max_drawdown_pct(eq_legend),
        _max_drawdown_pct(eq_legacy),
    )


def backtest_legend_vs_legacy_for_symbol(
    symbol: str,
    snapshot: dict[str, Any],
    *,
    range_preference: str = "2y",
    legend_profile: str = "balanced",
    strong_buy_mos_min: float | None = None,
    max_peg_for_buy: float | None = None,
    watch_buy_mos_min: float = 8.0,
) -> SymbolBacktestStats:
    """
    Approximate calibration backtest:
    - keep fundamentals static from snapshot
    - replay daily prices; re-evaluate decision with daily price
    """
    ohlcv, _ = fetch_long_ohlcv_for_backtest(symbol, range_preference=range_preference)
    closes = ohlcv["close"].astype(float).values

    legend_ret, legacy_ret, legend_dd, legacy_dd = _evaluate_on_closes(
        closes,
        snapshot,
        legend_profile=legend_profile,
        strong_buy_mos_min=strong_buy_mos_min,
        max_peg_for_buy=max_peg_for_buy,
        watch_buy_mos_min=watch_buy_mos_min,
    )
    bh_ret = (closes[-1] / closes[0] - 1.0) * 100.0 if len(closes) >= 2 and closes[0] > 0 else 0.0
    return SymbolBacktestStats(
        symbol=symbol,
        days=len(closes),
        legend_return_pct=float(legend_ret),
        legacy_return_pct=float(legacy_ret),
        buyhold_return_pct=float(bh_ret),
        legend_max_dd_pct=float(legend_dd),
        legacy_max_dd_pct=float(legacy_dd),
    )


def calibrate_legend_thresholds(
    snapshots: dict[str, dict[str, Any]],
    *,
    symbols: list[str] | None = None,
    range_preference: str = "2y",
    grid_profiles: tuple[str, ...] | None = None,
    grid_mos: tuple[float, ...] | None = None,
    grid_peg: tuple[float, ...] | None = None,
    grid_watch_mos: tuple[float, ...] | None = None,
) -> dict[str, Any]:
    """
    Grid-search simple objective:
    score = mean(legend_return - legacy_return) - 0.15 * mean(legend_max_dd)
    """
    all_symbols = [s for s in (symbols or sorted(snapshots.keys())) if s in snapshots]
    grid_profiles = grid_profiles or ("defensive", "balanced", "aggressive")
    grid_mos = grid_mos or (10.0, 14.0, 18.0, 22.0, 26.0)
    grid_peg = grid_peg or (1.1, 1.25, 1.4, 1.6)
    grid_watch_mos = grid_watch_mos or (0.0, 5.0, 8.0, 12.0)

    # Fetch close series once per symbol to avoid repeated network calls per grid point.
    close_map: dict[str, np.ndarray] = {}
    for sym in all_symbols:
        try:
            ohlcv, _ = fetch_long_ohlcv_for_backtest(sym, range_preference=range_preference)
            close_map[sym] = ohlcv["close"].astype(float).values
        except Exception:
            continue

    best: dict[str, Any] = {
        "profile": "balanced",
        "strong_buy_mos_min": 18.0,
        "max_peg_for_buy": 1.35,
        "watch_buy_mos_min": 8.0,
        "objective": -10**9,
        "evaluated": 0,
    }
    leaderboard: list[dict[str, Any]] = []
    for profile, mos_min, peg_max, watch_mos in product(grid_profiles, grid_mos, grid_peg, grid_watch_mos):
        rows: list[dict[str, float]] = []
        for sym, closes in close_map.items():
            snap = snapshots.get(sym)
            if not isinstance(snap, dict):
                continue
            try:
                lg_ret, lc_ret, lg_dd, _ = _evaluate_on_closes(
                    closes,
                    snap,
                    legend_profile=profile,
                    strong_buy_mos_min=mos_min,
                    max_peg_for_buy=peg_max,
                    watch_buy_mos_min=watch_mos,
                )
            except Exception:
                continue
            rows.append({"legend_ret": lg_ret, "legacy_ret": lc_ret, "legend_dd": lg_dd})
        if not rows:
            continue
        legend_alpha = np.mean([r["legend_ret"] - r["legacy_ret"] for r in rows])
        dd_penalty = np.mean([r["legend_dd"] for r in rows]) * 0.15
        objective = float(legend_alpha - dd_penalty)
        item = {
            "profile": profile,
            "strong_buy_mos_min": mos_min,
            "max_peg_for_buy": peg_max,
            "watch_buy_mos_min": watch_mos,
            "symbols_used": len(rows),
            "avg_legend_alpha_vs_legacy_pct": float(legend_alpha),
            "avg_legend_max_dd_pct": float(np.mean([r["legend_dd"] for r in rows])),
            "objective": objective,
        }
        leaderboard.append(item)
        if objective > float(best["objective"]):
            best = {**item, "evaluated": int(best["evaluated"])}
        best["evaluated"] = int(best["evaluated"]) + 1

    leaderboard = sorted(leaderboard, key=lambda x: float(x.get("objective") or -10**9), reverse=True)
    top = leaderboard[:10]
    return {
        "best": best,
        "top10": top,
        "universe_size": len(all_symbols),
        "symbols_with_price_history": len(close_map),
    }

