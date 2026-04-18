"""
Investment Intelligence Dashboard (VN stocks).
Run: streamlit run app.py
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import hashlib
import threading
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from core.ai_logic import AILogicError, generate_strategic_report, get_provider_health_snapshot
from core.background_jobs import run_background_cycle
from core.catalyst_engine import calculate_catalyst_score
from core.opportunity_finder import evaluate_stock_opportunity_3m
from core.strategy_backtest import fetch_vn_benchmark_series
from core.product_layer import (
    add_alert,
    add_holding,
    add_decision,
    adaptive_position_sizing,
    can_use_feature,
    coach_decision_quality_adaptive,
    dispatch_alert_notifications,
    dispatch_external_notifications,
    dispatch_text_notifications,
    enqueue_notification,
    evaluate_alerts,
    evaluate_decisions,
    execution_vs_plan_report,
    get_forecast_benchmark_by_horizon,
    get_forecast_accuracy_dashboard,
    get_forecast_drift_signal,
    get_forecast_drift_streak,
    get_forecast_group_benchmark,
    get_forecast_leaderboard,
    get_forecast_portfolio_dashboard,
    export_forecast_health_report,
    open_trade,
    close_trade,
    list_trades,
    list_holdings,
    realized_performance,
    get_admin_kpi,
    get_cohort_kpi,
    get_plan_features,
    get_user_plan,
    has_auth_user,
    get_kpi_dashboard,
    monthly_value_report,
    postmortem_report,
    decision_scorecard,
    customer_value_snapshot,
    user_aha_progress,
    next_best_action,
    today_action_board,
    proof_of_value_report,
    overdue_action_reminders,
    value_maturity_score,
    can_auto_execute_symbol,
    smart_upgrade_prompt,
    get_upgrade_funnel,
    select_upgrade_variant_auto,
    load_secret,
    list_alerts,
    list_decisions,
    log_event,
    migrate_legacy_json_to_sqlite,
    portfolio_snapshot,
    record_usage,
    record_forecast_snapshot,
    register_user_pin,
    save_secret,
    set_user_plan,
    process_notification_queue,
    verify_user_pin,
)
from core.report_export import investment_report_html, investment_report_markdown
from core.valuation import value_investing_summary
from scrapers.financial_data import fetch_financial_snapshot, list_universe_symbols, universe_subtype_map
from scrapers.portal import PortalDataError, fetch_financial_indicators, fetch_ohlcv_history, get_source_sla_report
from app_constants import get_app_version

WATCHLIST_DEFAULT = ["VNM", "FPT", "HPG"]
PROFILE_OPTIONS = {
    "An toàn & Cổ tức": "safe_dividend",
    "Tăng trưởng": "growth",
    "Mạo hiểm/Lướt sóng": "aggressive_trading",
}

SUBTYPE_LABEL_TO_ID = {
    "Ngân hàng": "bank",
    "Chứng khoán": "securities",
    "Bảo hiểm": "insurance",
    "BĐS dân dụng": "real_estate_residential",
    "BĐS khu công nghiệp": "real_estate_kcn",
    "Tiêu dùng thiết yếu": "consumer_staples",
    "Bán lẻ tiêu dùng": "consumer_retail",
    "Dầu khí": "oil_gas",
    "Thép & vật liệu": "steel_materials",
    "Dược & y tế": "pharma_healthcare",
    "Công nghệ dịch vụ": "technology_services",
    "Khác": "other",
}


def _snapshot_impl(ticker: str):
    """Không gắn @st.cache_data — dùng trong ThreadPoolExecutor (tránh missing ScriptRunContext)."""
    return fetch_financial_snapshot(ticker)


@st.cache_data(ttl=300)
def load_snapshot_cached(ticker: str):
    return _snapshot_impl(ticker)


def _ohlcv_impl(ticker: str):
    return fetch_ohlcv_history(ticker, sessions=80)


@st.cache_data(ttl=300)
def load_ohlcv_cached(ticker: str):
    return _ohlcv_impl(ticker)


def _strategic_report_impl(
    ticker: str,
    snapshot: dict,
    profile: str,
    total_capital_vnd: float,
    *,
    quick_mode: bool = True,
    llm_live: bool = True,
    task_mode: str | None = None,
):
    sessions = 60 if quick_mode else 80
    news_limit = 5 if quick_mode else 10
    enable_llm = bool(llm_live) and (not quick_mode)
    return generate_strategic_report(
        ticker,
        snapshot,
        profile=profile,
        total_capital_vnd=float(total_capital_vnd),
        sessions=sessions,
        news_limit=news_limit,
        enable_llm=enable_llm,
        fast_mode=quick_mode,
        task_mode=task_mode,
    )


@st.cache_data(ttl=240)
def load_strategic_report_cached(
    ticker: str,
    snapshot: dict,
    profile: str,
    total_capital_vnd: float,
    *,
    quick_mode: bool = True,
    llm_live: bool = True,
    task_mode: str | None = None,
):
    return _strategic_report_impl(
        ticker,
        snapshot,
        profile,
        total_capital_vnd,
        quick_mode=quick_mode,
        llm_live=llm_live,
        task_mode=task_mode,
    )


def prewarm_watchlist_cache(symbols: list[str], profile: str, total_capital_vnd: float, quick_mode: bool) -> None:
    syms = [s for s in symbols if s][: min(8, len(symbols))]
    if not syms:
        return

    def _warm(sym: str) -> None:
        snap = _snapshot_impl(sym)
        if snap is None:
            return
        try:
            # Warm financial disk cache for detailed panels.
            fetch_financial_indicators(sym, fast_mode=False)
        except Exception:
            pass
        try:
            _strategic_report_impl(sym, snap, profile, total_capital_vnd, quick_mode=quick_mode)
        except Exception:
            return

    max_workers = min(4, max(2, len(syms)))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_warm, s) for s in syms]
        for fut in as_completed(futs):
            try:
                fut.result()
            except Exception:
                pass


def _run_bg_warm_job(
    watchlist: list[str],
    universe_limit: int,
    queue_jobs: int = 10,
    warm_financial_live: bool = False,
) -> dict:
    return run_background_cycle(
        universe_limit=max(0, int(universe_limit)),
        watchlist=watchlist[:12],
        queue_jobs=max(1, int(queue_jobs)),
        warm_financial_live=bool(warm_financial_live),
    )


def extract_ticker(text: str) -> str | None:
    m = re.search(r"\b([A-Z0-9]{2,6})\b", (text or "").strip().upper())
    return m.group(1) if m else None


def health_badge(score: int) -> tuple[str, str]:
    if score >= 7:
        return "🟢 Khỏe", "#0ea35a"
    if score >= 4:
        return "🟡 Trung tính", "#c99800"
    return "🔴 Yếu", "#c9342f"


def _profile_match_note(profile: str, report: dict) -> str:
    val = report.get("valuation", {})
    fin = report.get("financials", {})
    phase = report.get("phase", {})
    if profile == "safe_dividend":
        cond = int(val.get("piotroski_score") or 0) >= 7 and (
            fin.get("debt_to_equity") is None or float(fin.get("debt_to_equity") or 0) < 0.7
        )
        return "Phù hợp profile an toàn." if cond else "Mức phù hợp profile an toàn: trung bình."
    if profile == "growth":
        rev = fin.get("revenue_growth_yoy")
        cond = rev is not None and float(rev) > 15
        return "Phù hợp profile tăng trưởng." if cond else "Mức phù hợp tăng trưởng: trung bình."
    vol_mult = float((phase.get("metrics") or {}).get("vol_multiple") or 0)
    return "Phù hợp profile lướt sóng." if vol_mult > 1.3 else "Mức phù hợp lướt sóng: thấp/trung bình."


@st.cache_data(ttl=300)
def compute_top5_for_profile(symbols: tuple[str, ...], profile: str) -> pd.DataFrame:
    rows: list[dict] = []
    for sym in symbols:
        snap = load_snapshot_cached(sym)
        if snap is None:
            continue
        try:
            rep = load_strategic_report_cached(sym, snap, profile, 100_000_000.0, quick_mode=True)
        except Exception:
            continue
        val = rep.get("valuation", {})
        fin = rep.get("financials", {})
        phase = rep.get("phase", {})
        fscore = int(val.get("piotroski_score") or 0)
        mos = float(val.get("margin_of_safety_composite_pct") or 0)
        rev = float(fin.get("revenue_growth_yoy") or 0)
        de = float(fin.get("debt_to_equity") or 0)
        vol_mult = float((phase.get("metrics") or {}).get("vol_multiple") or 0)
        if profile == "safe_dividend":
            score = fscore * 1.3 + max(0, 1.0 - de) * 10 + max(mos, 0) * 0.2
        elif profile == "growth":
            score = rev * 0.7 + max(mos, 0) * 0.3 + fscore
        else:
            score = vol_mult * 8 + abs(float((phase.get("metrics") or {}).get("day_change_pct") or 0)) * 1.2
        rows.append(
            {
                "Mã": sym,
                "Tiểu ngành": str(val.get("industry_subtype_label_vi") or "Khác"),
                "Điểm profile": round(score, 2),
                "Pha": str(phase.get("phase", "neutral")),
                "MOS%": round(mos, 2),
                "F-Score": fscore,
            }
        )
    if not rows:
        return pd.DataFrame(columns=["Mã", "Tiểu ngành", "Điểm profile", "Pha", "MOS%", "F-Score"])
    df = pd.DataFrame(rows).sort_values("Điểm profile", ascending=False).head(5).reset_index(drop=True)
    return df


@st.cache_data(ttl=300)
def scan_potential_stocks(
    universe: tuple[str, ...],
    profile: str,
    min_avg_volume_20: float,
) -> pd.DataFrame:
    rows: list[dict] = []
    def classify_recommendation(expected_return_pct: float, risk_pct: float, ph: str) -> str:
        if expected_return_pct >= 12 and risk_pct <= 18 and ph in ("accumulation", "breakout"):
            return "BUY"
        if expected_return_pct >= 6 and risk_pct <= 24:
            return "HOLD"
        return "AVOID"

    def estimate_horizon(ph: str, vol_mult: float, expected_return_pct: float) -> str:
        if ph == "breakout" and vol_mult >= 1.4:
            return "Ngắn hạn (2-6 tuần)"
        if ph in ("accumulation", "neutral") and expected_return_pct >= 6:
            return "Trung hạn (2-6 tháng)"
        return "Quan sát thêm"

    def _one_symbol(sym: str) -> dict | None:
        snap = _snapshot_impl(sym)
        if snap is None:
            return {"Mã": sym, "Giá": None, "Trạng thái": "Không có snapshot"}
        try:
            rep = _strategic_report_impl(sym, snap, profile, 100_000_000.0, quick_mode=True)
            try:
                ohlcv = _ohlcv_impl(sym)
            except Exception:
                ohlcv = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        except Exception:
            return {"Mã": sym, "Giá": float(snap.get("price") or 0), "Trạng thái": "Lỗi phân tích"}
        avg_vol20 = (
            float(ohlcv.tail(20)["volume"].mean()) if not ohlcv.empty and len(ohlcv) >= 20
            else (float(ohlcv["volume"].mean()) if not ohlcv.empty else 0.0)
        )
        if avg_vol20 < min_avg_volume_20:
            return {
                "Mã": sym,
                "Giá": float((rep.get("valuation") or {}).get("price") or snap.get("price") or 0),
                "Trạng thái": "Không đạt lọc thanh khoản",
                "Avg Vol20": int(avg_vol20),
            }
        val = rep.get("valuation", {})
        fin = rep.get("financials", {})
        phase = rep.get("phase", {})
        price = float(val.get("price") or 0)
        mos = float(val.get("margin_of_safety_composite_pct") or 0)
        fscore = int(val.get("piotroski_score") or 0)
        rev = float(fin.get("revenue_growth_yoy") or 0)
        vol_mult = float((phase.get("metrics") or {}).get("vol_multiple") or 0)
        expected_return = max(mos * 0.6 + rev * 0.2 + fscore * 1.2, 0.0)
        risk_score = max(3.0, min(35.0, 12.0 + max(0.0, (1.2 - vol_mult) * 6.0) + (9 - fscore)))
        ph = str(phase.get("phase", "neutral"))
        recommendation = classify_recommendation(expected_return, risk_score, ph)
        horizon = estimate_horizon(ph, vol_mult, expected_return)
        return {
            "Mã": sym,
            "Giá": round(price, 2),
            "Tiểu ngành": str(val.get("industry_subtype_label_vi") or "Khác"),
            "Pha": ph,
            "Khuyến nghị": recommendation,
            "Khung thời gian": horizon,
            "MOS%": round(mos, 2),
            "F-Score": fscore,
            "Rev YoY%": round(rev, 2),
            "Avg Vol20": int(avg_vol20),
            "Dữ liệu kỹ thuật": "Đủ" if not ohlcv.empty else "Thiếu (fallback)",
            "Expected Return %": round(expected_return, 2),
            "Risk % (ước tính)": round(risk_score, 2),
            "Trạng thái": "Sẵn sàng",
        }

    max_workers = min(8, max(2, len(universe)))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_one_symbol, sym): sym for sym in universe}
        for fut in as_completed(futs):
            try:
                row = fut.result()
                if row:
                    rows.append(row)
            except Exception:
                rows.append({"Mã": futs[fut], "Giá": None, "Trạng thái": "Lỗi hệ thống"})
    if not rows:
        return pd.DataFrame(
            columns=[
                "Mã",
                "Tiểu ngành",
                "Pha",
                "Khuyến nghị",
                "Khung thời gian",
                "MOS%",
                "F-Score",
                "Rev YoY%",
                "Avg Vol20",
                "Dữ liệu kỹ thuật",
                "Expected Return %",
                "Risk % (ước tính)",
                "Trạng thái",
            ]
        )
    df = pd.DataFrame(rows)
    if "Expected Return %" not in df.columns:
        df["Expected Return %"] = -1.0
    if "F-Score" not in df.columns:
        df["F-Score"] = -1
    if "Trạng thái" not in df.columns:
        df["Trạng thái"] = "Sẵn sàng"
    df["__ready"] = (df["Trạng thái"] == "Sẵn sàng").astype(int)
    df = (
        df.sort_values(["__ready", "Expected Return %", "F-Score"], ascending=[False, False, False])
        .drop(columns=["__ready"])
        .reset_index(drop=True)
    )
    return df


@st.cache_data(ttl=300)
def load_autopilot_board(profile: str, universe_limit: int = 30, min_avg_volume_20: float = 300_000.0) -> pd.DataFrame:
    universe = tuple(list_universe_symbols(limit=max(10, min(int(universe_limit or 30), 120))))
    df = scan_potential_stocks(universe, profile, float(min_avg_volume_20))
    if df.empty:
        return pd.DataFrame(
            columns=[
                "Mã",
                "Tiểu ngành",
                "Khuyến nghị",
                "Khung thời gian",
                "Giá",
                "MOS%",
                "Expected Return %",
                "Risk % (ước tính)",
                "Trạng thái",
            ]
        )
    ready = df[df["Trạng thái"] == "Sẵn sàng"].copy() if "Trạng thái" in df.columns else df.copy()
    base = ready if not ready.empty else df.copy()
    rank_map = {"BUY": 3, "HOLD": 2, "WATCH": 1, "AVOID": 0}
    base["__rank"] = base["Khuyến nghị"].map(rank_map).fillna(0)
    base["__score"] = (
        base["__rank"] * 20
        + base["Expected Return %"].fillna(0) * 0.7
        + base["MOS%"].fillna(0) * 0.3
        - base["Risk % (ước tính)"].fillna(0) * 0.2
    )
    out = (
        base.sort_values(["__score", "MOS%", "Expected Return %"], ascending=[False, False, False])
        .drop(columns=["__rank", "__score"], errors="ignore")
        .head(10)
        .reset_index(drop=True)
    )
    keep_cols = [
        "Mã",
        "Tiểu ngành",
        "Khuyến nghị",
        "Khung thời gian",
        "Giá",
        "MOS%",
        "Expected Return %",
        "Risk % (ước tính)",
        "Trạng thái",
    ]
    return out[[c for c in keep_cols if c in out.columns]]


@st.cache_data(ttl=240)
def build_autopilot_simple_view(
    symbols: tuple[str, ...],
    profile: str,
    total_capital_vnd: float,
    quick_mode: bool = True,
    llm_live: bool = True,
) -> pd.DataFrame:
    rows: list[dict] = []
    for sym in symbols:
        snap = load_snapshot_cached(sym)
        if snap is None:
            continue
        try:
            rep = load_strategic_report_cached(
                sym,
                snap,
                profile,
                float(total_capital_vnd),
                quick_mode=quick_mode,
                llm_live=llm_live,
            )
        except Exception:
            continue
        rp = rep.get("risk_plan") or {}
        val = rep.get("valuation") or {}
        action = str(rep.get("final_action") or "WATCH").upper()
        show_plan = action in ("BUY", "HOLD")
        price_now = float(val.get("price") or snap.get("price") or 0)
        intrinsic = float(val.get("composite_target_price") or val.get("intrinsic_value_graham") or 0)
        mos = val.get("margin_of_safety_composite_pct")
        rows.append(
            {
                "Mã": sym,
                "Hành động": action,
                "Giá hiện tại": round(price_now, 2) if price_now > 0 else None,
                "Giá trị nội tại": round(intrinsic, 2) if intrinsic > 0 else None,
                "Biên an toàn %": round(float(mos), 1) if mos is not None else None,
                "Giá vào": (
                    round(float(rp.get("entry_price") or 0), 2)
                    if show_plan and float(rp.get("entry_price") or 0) > 0
                    else None
                ),
                "SL": (
                    round(float(rp.get("stop_loss_price") or 0), 2)
                    if show_plan and float(rp.get("stop_loss_price") or 0) > 0
                    else None
                ),
                "TP": (
                    round(float(rp.get("take_profit_price") or 0), 2)
                    if show_plan and float(rp.get("take_profit_price") or 0) > 0
                    else None
                ),
                "Tỷ trọng %": round(float(rp.get("max_position_pct") or 0), 1),
            }
        )
    if not rows:
        return pd.DataFrame(
            columns=["Mã", "Hành động", "Giá hiện tại", "Giá trị nội tại", "Biên an toàn %", "Giá vào", "SL", "TP", "Tỷ trọng %"]
        )
    df = pd.DataFrame(rows)
    rank = {"BUY": 3, "HOLD": 2, "WATCH": 1, "AVOID": 0}
    df["__r"] = df["Hành động"].map(rank).fillna(0)
    df = df.sort_values(["__r", "Tỷ trọng %"], ascending=[False, False]).drop(columns=["__r"]).reset_index(drop=True)
    return df


@st.cache_data(ttl=240)
def build_profit_focus_board(
    symbols: tuple[str, ...],
    profile: str,
    total_capital_vnd: float,
    quick_mode: bool = True,
    llm_live: bool = True,
) -> pd.DataFrame:
    """
    Strict actionability board:
    - Gate PASS
    - Output quality PASS
    - Action BUY/HOLD
    - RR >= 2.0
    """
    rows: list[dict] = []
    for sym in symbols:
        snap = load_snapshot_cached(sym)
        if snap is None:
            continue
        try:
            rep = load_strategic_report_cached(
                sym,
                snap,
                profile,
                float(total_capital_vnd),
                quick_mode=quick_mode,
                llm_live=llm_live,
            )
        except Exception:
            continue
        act = str(rep.get("final_action") or "WATCH").upper()
        gate_ok = bool(rep.get("gate_passed"))
        out_ok = bool(rep.get("output_gate_passed", True))
        conf = float(rep.get("confidence_score") or 0)
        rp = rep.get("risk_plan") or {}
        entry = float(rp.get("entry_price") or 0)
        sl = float(rp.get("stop_loss_price") or 0)
        tp = float(rp.get("take_profit_price") or 0)
        rr = ((tp - entry) / max(entry - sl, 1e-6)) if entry > sl > 0 and tp > entry else 0.0
        if act not in ("BUY", "HOLD"):
            continue
        if not gate_ok or not out_ok:
            continue
        if rr < 2.0:
            continue
        val = rep.get("valuation") or {}
        mos = float(val.get("margin_of_safety_composite_pct") or 0)
        score = conf * 0.7 + mos * 0.4 + rr * 10.0
        rows.append(
            {
                "Mã": sym,
                "Hành động": act,
                "Entry": round(entry, 2),
                "SL": round(sl, 2),
                "TP": round(tp, 2),
                "RR": round(rr, 2),
                "Confidence %": round(conf, 1),
                "MOS %": round(mos, 1),
                "Điểm ưu tiên": round(score, 2),
            }
        )
    if not rows:
        return pd.DataFrame(columns=["Mã", "Hành động", "Entry", "SL", "TP", "RR", "Confidence %", "MOS %", "Điểm ưu tiên"])
    return pd.DataFrame(rows).sort_values("Điểm ưu tiên", ascending=False).head(8).reset_index(drop=True)


@st.cache_data(ttl=240)
def build_daily_action_list(
    universe: tuple[str, ...],
    profile: str,
    total_capital_vnd: float,
    held_symbols: tuple[str, ...],
    quick_mode: bool = True,
    llm_live: bool = True,
) -> pd.DataFrame:
    """
    Customer-facing daily list:
    BUY / WATCH / HOLD / SELL with concise reasons and action prices.
    """
    held = {str(x).upper() for x in held_symbols}
    rows: list[dict] = []
    for sym in universe:
        snap = load_snapshot_cached(sym)
        if snap is None:
            continue
        try:
            rep = load_strategic_report_cached(
                sym,
                snap,
                profile,
                float(total_capital_vnd),
                quick_mode=quick_mode,
                llm_live=llm_live,
            )
        except Exception:
            continue
        val = rep.get("valuation") or {}
        rp = rep.get("risk_plan") or {}
        phase = rep.get("phase") or {}
        conf = float(rep.get("confidence_score") or 0)
        gate = bool(rep.get("gate_passed"))
        out_gate = bool(rep.get("output_gate_passed", True))
        act = str(rep.get("final_action") or "WATCH").upper()
        mos = float(val.get("margin_of_safety_composite_pct") or 0)
        fscore = int(val.get("piotroski_score") or 0)
        ph = str(phase.get("phase") or "neutral")
        price = float(val.get("price") or snap.get("price") or 0)
        entry = float(rp.get("entry_price") or 0)
        sl = float(rp.get("stop_loss_price") or 0)
        tp = float(rp.get("take_profit_price") or 0)

        if sym in held:
            if act == "AVOID" or (ph == "distribution" and conf < 55):
                action_today = "SELL"
                reason = "Vị thế đang rủi ro cao (pha phân phối/độ tin cậy thấp), ưu tiên bảo toàn vốn."
            else:
                action_today = "HOLD"
                reason = "Đang có vị thế và cấu trúc chưa xấu, tiếp tục giữ kỷ luật theo kế hoạch."
        else:
            if act == "BUY" and gate and out_gate:
                action_today = "BUY"
                reason = "Đạt chuẩn xuống tiền: gate pass + output pass + cấu trúc kế hoạch hợp lệ."
            elif act == "HOLD":
                action_today = "WATCH"
                reason = "Tín hiệu trung tính, chờ điểm vào đẹp hơn trước khi giải ngân."
            else:
                action_today = "WATCH"
                reason = "Chưa đạt chuẩn hành động, theo dõi thêm để tránh quyết định sớm."

        rows.append(
            {
                "Mã": sym,
                "Hành động hôm nay": action_today,
                "Giá hiện tại": round(price, 2) if price > 0 else None,
                "Entry": round(entry, 2) if entry > 0 else None,
                "SL": round(sl, 2) if sl > 0 else None,
                "TP": round(tp, 2) if tp > 0 else None,
                "Confidence %": round(conf, 1),
                "MOS %": round(mos, 1),
                "F-Score": fscore,
                "Pha": ph,
                "Lý do ngắn": reason,
            }
        )
    if not rows:
        return pd.DataFrame(
            columns=["Mã", "Hành động hôm nay", "Giá hiện tại", "Entry", "SL", "TP", "Confidence %", "MOS %", "F-Score", "Pha", "Lý do ngắn"]
        )
    df = pd.DataFrame(rows)
    rank = {"BUY": 3, "HOLD": 2, "WATCH": 1, "SELL": 0}
    df["__r"] = df["Hành động hôm nay"].map(rank).fillna(0)
    df = (
        df.sort_values(["__r", "Confidence %", "MOS %"], ascending=[False, False, False])
        .drop(columns=["__r"])
        .reset_index(drop=True)
    )
    return df


def _simple_view_for_plan(df: pd.DataFrame, plan_id: str) -> pd.DataFrame:
    pid = str(plan_id or "free").strip().lower()
    if df.empty:
        return df
    if pid in ("pro", "expert"):
        return df
    # Free: keep lightweight insight, hide execution-sensitive levels.
    cols = [c for c in ["Mã", "Hành động", "Giá hiện tại", "Giá trị nội tại", "Biên an toàn %", "Tỷ trọng %"] if c in df.columns]
    return df[cols].copy()


@st.cache_data(ttl=300)
def build_action_center(
    universe: tuple[str, ...],
    profile: str,
    total_capital_vnd: float,
    top_n: int = 8,
) -> pd.DataFrame:
    """
    Build actionable board with strict ranking:
    - Gate passed first
    - BUY/HOLD priority
    - confidence + MOS + RR quality
    """
    rows: list[dict] = []
    for sym in universe:
        snap = load_snapshot_cached(sym)
        if snap is None:
            continue
        try:
            rep = load_strategic_report_cached(sym, snap, profile, total_capital_vnd, quick_mode=True)
        except Exception:
            continue
        val = rep.get("valuation") or {}
        plan = rep.get("risk_plan") or {}
        conf = float(rep.get("confidence_score") or 0)
        mos = float(val.get("margin_of_safety_composite_pct") or 0)
        act = str(rep.get("final_action") or "WATCH")
        gate = bool(rep.get("gate_passed"))
        entry = float(plan.get("entry_price") or 0)
        sl = float(plan.get("stop_loss_price") or 0)
        tp = float(plan.get("take_profit_price") or 0)
        rr = ((tp - entry) / max(entry - sl, 1e-6)) if entry > 0 and tp > entry and sl < entry else 0.0
        score = (25 if gate else -30) + conf * 0.7 + mos * 0.5 + rr * 8.0
        if act == "BUY":
            score += 20
        elif act == "HOLD":
            score += 8
        elif act == "WATCH":
            score -= 5
        else:
            score -= 12
        rows.append(
            {
                "Mã": sym,
                "Khuyến nghị": act,
                "Gate": "PASS" if gate else "WATCH-ONLY",
                "Confidence %": round(conf, 1),
                "MOS %": round(mos, 1),
                "RR": round(rr, 2),
                "Entry": round(entry, 2) if entry > 0 else None,
                "SL": round(sl, 2) if sl > 0 else None,
                "TP": round(tp, 2) if tp > 0 else None,
                "Điểm hành động": round(score, 2),
                "Tiểu ngành": str(val.get("industry_subtype_label_vi") or "Khác"),
            }
        )
    if not rows:
        return pd.DataFrame(
            columns=["Mã", "Khuyến nghị", "Gate", "Confidence %", "MOS %", "RR", "Entry", "SL", "TP", "Điểm hành động"]
        )
    out = pd.DataFrame(rows).sort_values("Điểm hành động", ascending=False).head(max(1, top_n)).reset_index(drop=True)
    return out


def build_daily_playbook_text(action_df: pd.DataFrame, ticker: str, report: dict) -> str:
    lines: list[str] = []
    lines.append("📘 Daily Playbook")
    lines.append(f"- Mã đang theo dõi chính: {ticker}")
    act = str(report.get("final_action") or "WATCH")
    conf = float(report.get("confidence_score") or 0)
    lines.append(f"- Trạng thái hiện tại: {act} | Confidence: {conf:.1f}%")
    if action_df.empty:
        lines.append("- Chưa có cơ hội nổi bật hôm nay theo tiêu chí hiện tại.")
        return "\n".join(lines)
    lines.append("- Top cơ hội ưu tiên:")
    for _, r in action_df.head(3).iterrows():
        lines.append(
            f"  • {r.get('Mã')} | {r.get('Khuyến nghị')} | Gate {r.get('Gate')} | "
            f"MOS {r.get('MOS %')}% | RR {r.get('RR')}"
        )
    return "\n".join(lines)


@st.cache_data(ttl=240)
def build_opportunity_3m_board(
    universe: tuple[str, ...],
    profile: str,
    total_capital_vnd: float,
    quick_mode: bool = True,
    llm_live: bool = False,
    allowed_subtypes: tuple[str, ...] = (),
    min_avg_volume_20: float = 0.0,
    benchmark_return_3m_pct: float | None = None,
    min_data_quality_pct: float = 55.0,
    only_actionable: bool = False,
    min_catalyst_score: float = 0.0,
    dynamic_sector_calibration: bool = True,
    top_n: int = 25,
) -> pd.DataFrame:
    def _sector_calibration(subtype_id: str) -> tuple[float, float, str]:
        sid = str(subtype_id or "other").strip().lower()
        # tech_mult, fund_mult, explanation
        table: dict[str, tuple[float, float, str]] = {
            "bank": (0.92, 1.08, "Ngân hàng: ưu tiên chất lượng tài sản và định giá."),
            "insurance": (0.94, 1.06, "Bảo hiểm: ưu tiên nền tảng tài chính ổn định."),
            "securities": (1.06, 0.94, "Chứng khoán: nhạy chu kỳ, ưu tiên tín hiệu giá/khối lượng."),
            "real_estate_residential": (1.03, 0.97, "BĐS dân dụng: tăng trọng số kỹ thuật để bắt nhịp chu kỳ."),
            "real_estate_kcn": (1.00, 1.00, "BĐS KCN: cân bằng kỹ thuật và cơ bản."),
            "consumer_staples": (0.96, 1.04, "Tiêu dùng thiết yếu: ưu tiên chất lượng lợi nhuận."),
            "consumer_retail": (1.02, 0.98, "Bán lẻ: thiên về động lượng khi nhu cầu cải thiện."),
            "oil_gas": (1.08, 0.92, "Dầu khí: biến động theo chu kỳ hàng hóa."),
            "steel_materials": (1.10, 0.90, "Thép & vật liệu: chu kỳ mạnh, ưu tiên xung lực."),
            "pharma_healthcare": (0.95, 1.05, "Dược & y tế: ưu tiên tính bền vững tài chính."),
            "technology_services": (1.04, 0.96, "Công nghệ: cân nhắc động lượng tăng trưởng."),
            "other": (1.00, 1.00, "Khác: dùng trọng số trung tính."),
        }
        return table.get(sid, table["other"])

    def _profile_reco(
        profile_key: str,
        score: float,
        prob: float,
        conf: float,
        wf: float,
        alpha: float | None,
        catalyst_pass: bool,
    ) -> str:
        a = float(alpha) if alpha is not None else 0.0
        conf_adj = conf + (4.0 if llm_live else 0.0)
        reco = "CHỜ"
        if profile_key == "safe_dividend":
            if score >= 75 and prob >= 72 and conf_adj >= 70 and wf >= 55 and a >= 0:
                reco = "MUA"
            elif score >= 62 and prob >= 62 and conf_adj >= 60:
                reco = "THEO DÕI"
        elif profile_key == "aggressive_trading":
            if score >= 65 and prob >= 65 and conf_adj >= 58:
                reco = "MUA"
            elif score >= 52 and prob >= 55:
                reco = "THEO DÕI"
        else:
            # growth (default)
            if score >= 70 and prob >= 68 and conf_adj >= 62 and wf >= 45:
                reco = "MUA"
            elif score >= 58 and prob >= 58 and conf_adj >= 55:
                reco = "THEO DÕI"

        # Hard guardrail: must pass catalyst to allow BUY.
        if reco == "MUA" and not catalyst_pass:
            return "THEO DÕI THÊM - CHƯA NÊN MUA (CHỜ TÍN HIỆU)"
        return reco

    def _profile_priority(profile_key: str, row: dict[str, object]) -> float:
        s = float(row.get("Điểm cơ hội (hiệu chỉnh ngành)") or row.get("Điểm cơ hội") or 0)
        p = float(row.get("Xác suất tăng 3M %") or 0)
        c = float(row.get("Độ tin cậy %") or 0)
        w = float(row.get("Reliability WF %") or 0)
        a = float(row.get("Alpha 3M vs Benchmark %") or 0)
        if profile_key == "safe_dividend":
            return s * 0.30 + p * 0.25 + c * 0.20 + w * 0.20 + a * 0.05
        if profile_key == "aggressive_trading":
            return s * 0.35 + p * 0.30 + c * 0.15 + w * 0.10 + a * 0.10
        return s * 0.33 + p * 0.28 + c * 0.18 + w * 0.15 + a * 0.06

    def _derive_dynamic_sector_multipliers(
        pool: list[dict[str, float | str]],
    ) -> dict[str, tuple[float, float, str]]:
        result: dict[str, tuple[float, float, str]] = {}
        grouped: dict[str, list[dict[str, float | str]]] = {}
        for row in pool:
            sid = str(row.get("subtype_id") or "other").strip().lower()
            grouped.setdefault(sid, []).append(row)
        for sid, rows_sid in grouped.items():
            if len(rows_sid) < 3:
                continue
            tech_hits = 0
            fund_hits = 0
            for r in rows_sid:
                ret = float(r.get("ret_3m") or 0.0)
                tech = float(r.get("tech") or 0.0)
                fund = float(r.get("fund") or 0.0)
                if ret >= 8.0 and tech >= 25.0:
                    tech_hits += 1
                if ret >= 8.0 and fund >= 25.0:
                    fund_hits += 1
            n = float(len(rows_sid))
            tech_hit_rate = tech_hits / n
            fund_hit_rate = fund_hits / n
            delta = max(-0.10, min(0.10, (tech_hit_rate - fund_hit_rate) * 0.20))
            tech_mult = max(0.90, min(1.10, 1.0 + delta))
            fund_mult = max(0.90, min(1.10, 1.0 - delta))
            result[sid] = (
                round(tech_mult, 2),
                round(fund_mult, 2),
                f"Động theo dữ liệu ngành ({int(n)} mã): tech_hit={tech_hit_rate*100:.0f}%, fund_hit={fund_hit_rate*100:.0f}%",
            )
        return result

    rows: list[dict[str, object]] = []
    staged_rows: list[dict[str, object]] = []
    candidate_pool: list[dict[str, float | str]] = []
    allowed = {x.strip().lower() for x in allowed_subtypes if str(x).strip()}
    for sym in universe:
        snap = load_snapshot_cached(sym)
        if snap is None:
            continue
        try:
            rep = load_strategic_report_cached(
                sym,
                snap,
                profile,
                float(total_capital_vnd),
                quick_mode=quick_mode,
            )
            ohlcv = load_ohlcv_cached(sym)
        except Exception:
            continue
        val = rep.get("valuation") or {}
        fin = rep.get("financials") or {}
        news_items = rep.get("news") or []
        news_blob = " | ".join(str(x.get("title") or "") for x in news_items[:8])
        subtype_id = str(val.get("industry_subtype_id") or "other").strip().lower()
        if allowed and subtype_id not in allowed:
            continue
        out = evaluate_stock_opportunity_3m(
            sym,
            ohlcv,
            val,
            fin,
            benchmark_return_3m_pct=benchmark_return_3m_pct,
        )
        catalyst = calculate_catalyst_score(sym, ohlcv, val, news_text=news_blob)
        if float(out.avg_volume_20 or 0.0) < float(min_avg_volume_20 or 0.0):
            continue
        if float(out.data_quality_pct or 0.0) < float(min_data_quality_pct or 0.0):
            continue
        if float(catalyst.catalyst_score or 0.0) < float(min_catalyst_score or 0.0):
            continue
        if only_actionable and out.status not in ("CƠ HỘI CAO", "THEO DÕI"):
            continue
        candidate_pool.append(
            {
                "subtype_id": subtype_id,
                "tech": float(out.technical_score),
                "fund": float(out.fundamental_score),
                "ret_3m": float(out.return_3m_recent_pct),
            }
        )
        staged_rows.append(
            {
                "symbol": out.symbol,
                "status": out.status,
                "reco_reason": out.recommendation_reason,
                "subtype_id": subtype_id,
                "subtype_label": str(val.get("industry_subtype_label_vi") or "Khác"),
                "out": out,
                "catalyst": catalyst,
            }
        )

    dyn_map = _derive_dynamic_sector_multipliers(candidate_pool) if dynamic_sector_calibration else {}
    for srow in staged_rows:
        out = srow["out"]
        catalyst = srow["catalyst"]
        subtype_id = str(srow["subtype_id"])
        tech_mult, fund_mult, sector_note = _sector_calibration(subtype_id)
        dyn = dyn_map.get(subtype_id)
        if dyn is not None:
            tech_mult, fund_mult, sector_note = dyn
        adjusted_tech = max(0.0, min(50.0, float(out.technical_score) * tech_mult))
        adjusted_fund = max(0.0, min(50.0, float(out.fundamental_score) * fund_mult))
        sector_adjusted_score = round(max(0.0, min(100.0, adjusted_tech + adjusted_fund)), 2)
        rows.append(
            {
                "Mã": str(srow["symbol"]),
                "Trạng thái": str(srow["status"]),
                "Lý do khuyến nghị": str(srow["reco_reason"]),
                "Ngành con": str(srow["subtype_label"]),
                "Điểm cơ hội": out.score,
                "Điểm cơ hội (hiệu chỉnh ngành)": sector_adjusted_score,
                "Catalyst Score": catalyst.catalyst_score,
                "Catalyst Pass": "YES" if catalyst.passed else "NO",
                "Catalyst Lý do": " ; ".join(catalyst.reasons[:3]) if catalyst.reasons else "",
                "Điểm kỹ thuật": out.technical_score,
                "Điểm cơ bản": out.fundamental_score,
                "Hệ số ngành (KT/CB)": f"{tech_mult:.2f}/{fund_mult:.2f}",
                "Xác suất tăng 3M %": out.probability_up_3m_pct,
                "Bull %": out.bull_prob_pct,
                "Base %": out.base_prob_pct,
                "Bear %": out.bear_prob_pct,
                "Lợi nhuận kỳ vọng 3M %": out.expected_return_3m_pct,
                "Độ tin cậy %": out.confidence_pct,
                "Hit-rate 3M lịch sử %": out.historical_hit_rate_3m_pct,
                "Reliability WF %": out.walkforward_reliability_pct,
                "Return 3M gần nhất %": out.return_3m_recent_pct,
                "Alpha 3M vs Benchmark %": out.alpha_3m_vs_benchmark_pct,
                "KL TB20": round(out.avg_volume_20, 0),
                "Data Quality %": out.data_quality_pct,
                "Ghi chú hiệu chỉnh ngành": sector_note,
                "Luận điểm chính": out.thesis,
                "Rủi ro chính": out.risks,
                "Mốc vô hiệu": out.invalidation,
            }
        )
    if not rows:
        return pd.DataFrame(
            columns=[
                "Mã",
                "Trạng thái",
                "Lý do khuyến nghị",
                "Ngành con",
                "Điểm cơ hội",
                "Điểm cơ hội (hiệu chỉnh ngành)",
                "Catalyst Score",
                "Catalyst Pass",
                "Catalyst Lý do",
                "Điểm kỹ thuật",
                "Điểm cơ bản",
                "Hệ số ngành (KT/CB)",
                "Xác suất tăng 3M %",
                "Bull %",
                "Base %",
                "Bear %",
                "Lợi nhuận kỳ vọng 3M %",
                "Độ tin cậy %",
                "Hit-rate 3M lịch sử %",
                "Reliability WF %",
                "Return 3M gần nhất %",
                "Alpha 3M vs Benchmark %",
                "KL TB20",
                "Data Quality %",
                "Ghi chú hiệu chỉnh ngành",
                "Luận điểm chính",
                "Rủi ro chính",
                "Mốc vô hiệu",
            ]
        )
    out_df = pd.DataFrame(rows)
    out_df["Khuyến nghị theo profile"] = [
        _profile_reco(
            profile,
            float(r.get("Điểm cơ hội (hiệu chỉnh ngành)") or r.get("Điểm cơ hội") or 0),
            float(r.get("Xác suất tăng 3M %") or 0),
            float(r.get("Độ tin cậy %") or 0),
            float(r.get("Reliability WF %") or 0),
            r.get("Alpha 3M vs Benchmark %"),
            str(r.get("Catalyst Pass") or "").upper() == "YES",
        )
        for _, r in out_df.iterrows()
    ]
    out_df["__priority"] = [_profile_priority(profile, dict(r)) for _, r in out_df.iterrows()]
    out_df = out_df.sort_values(
        ["__priority", "Catalyst Score", "Data Quality %", "Điểm cơ hội", "Xác suất tăng 3M %", "Reliability WF %", "Hit-rate 3M lịch sử %"],
        ascending=[False, False, False, False, False, False, False],
    )
    return out_df.head(max(1, int(top_n))).drop(columns=["__priority"]).reset_index(drop=True)


@st.cache_data(ttl=600)
def load_benchmark_return_3m() -> tuple[float | None, str]:
    try:
        bench, label = fetch_vn_benchmark_series("2y")
    except Exception:
        return None, ""
    if bench is None or len(bench) < 64:
        return None, label
    try:
        b0 = float(bench.iloc[-64])
        b1 = float(bench.iloc[-1])
        if b0 <= 0:
            return None, label
        ret = (b1 / b0 - 1.0) * 100.0
        return round(ret, 2), label
    except Exception:
        return None, label


def render_5s_panel(report: dict, sla_rows: list[dict]) -> None:
    conf = float(report.get("confidence_score") or 0)
    latency = float(report.get("latency_seconds") or 0)
    gate = bool(report.get("gate_passed"))
    avg_sla = 0.0
    if sla_rows:
        avg_sla = sum(float(x.get("success_rate_pct") or 0) for x in sla_rows) / len(sla_rows)
    score_sort = 80 if gate else 60
    score_setinorder = 85 if latency <= 2.5 else (70 if latency <= 6 else 55)
    score_shine = 82 if conf >= 65 else 65
    score_standardize = 88 if avg_sla >= 70 else 72
    score_sustain = 86 if report.get("data_reliability") else 68
    rows = [
        {"5S": "Seiri (Sàng lọc)", "Điểm": score_sort, "Ý nghĩa": "Ưu tiên cơ hội hành động rõ ràng, loại nhiễu."},
        {"5S": "Seiton (Sắp xếp)", "Điểm": score_setinorder, "Ý nghĩa": "Luồng thao tác nhanh, ít bước chờ."},
        {"5S": "Seiso (Sạch sẽ)", "Điểm": score_shine, "Ý nghĩa": "Dashboard gọn, thông tin dễ đọc."},
        {"5S": "Seiketsu (Chuẩn hóa)", "Điểm": score_standardize, "Ý nghĩa": "SLA dữ liệu và gate thống nhất."},
        {"5S": "Shitsuke (Duy trì)", "Điểm": score_sustain, "Ý nghĩa": "KPI + cohort duy trì cải tiến liên tục."},
    ]
    st.subheader("🧭 5S Product Health")
    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)


def render_aha_and_value(user_id: str) -> None:
    aha = user_aha_progress(user_id)
    val = customer_value_snapshot(user_id)
    nba = next_best_action(user_id)
    st.subheader("🚀 Aha Journey (7 ngày)")
    st.progress(min(max(float(aha.get("progress_pct") or 0) / 100.0, 0.0), 1.0), text=f"Tiến độ: {aha.get('progress_pct', 0)}%")
    st.caption(f"Bước tiếp theo gợi ý: {aha.get('next_step')}")
    steps = aha.get("steps") or []
    if steps:
        st.caption(
            " · ".join([f"{'✅' if s.get('done') else '⬜'} {s.get('label')}" for s in steps])
        )

    st.subheader("💡 Value Snapshot (30 ngày)")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Realized PnL", f"{float(val.get('realized_pnl_30d') or 0):,.0f} VND")
    with c2:
        st.metric("Win rate realized", f"{float(val.get('realized_win_rate_30d') or 0):.1f}%")
    with c3:
        st.metric("Discipline score", f"{float(val.get('discipline_score') or 0):.1f}/100")
    with c4:
        st.metric("Tiết kiệm thời gian", f"~{int(val.get('time_saved_min_est') or 0)} phút")

    st.markdown("### 🎯 Next Best Action (cá nhân hóa)")
    st.info(
        f"**{nba.get('title')}**\n\n"
        f"- Vì sao: {nba.get('reason')}\n"
        f"- Tác động kỳ vọng: {nba.get('target_impact')}"
    )

    st.markdown("### 🗓️ Top 3 việc phải làm hôm nay")
    tasks = today_action_board(user_id)
    for t in tasks:
        mark = "✅" if t.get("status") == "done" else "⬜"
        st.caption(f"{mark} {t.get('task')} — {t.get('impact')}")

    st.markdown("### 💸 Proof of Value")
    pov = proof_of_value_report(user_id)
    p1, p2, p3, p4 = st.columns(4)
    with p1:
        st.metric("Realized PnL 30d", f"{float(pov.get('realized_pnl_30d') or 0):,.0f} VND")
    with p2:
        st.metric("Win-rate 30d", f"{float(pov.get('win_rate_realized_30d') or 0):.1f}%")
    with p3:
        st.metric("Risk Shield", f"{float(pov.get('risk_shield_score') or 0):.1f}/100")
    with p4:
        st.metric("Tiết kiệm thời gian", f"~{int(pov.get('time_saved_min_est') or 0)} phút")
    st.caption(str(pov.get("value_message") or ""))
    monthly_fee = 500_000.0
    realized = float(pov.get("realized_pnl_30d") or 0.0)
    value_ratio = (realized / monthly_fee) if monthly_fee > 0 else 0.0
    st.caption(f"ROI so với phí 500K/tháng (theo Realized PnL 30d): {value_ratio:.2f}x")
    ms = value_maturity_score(user_id)
    st.caption(f"🧭 Maturity Score: {ms.get('score_10')}/10 · {ms.get('level')}")
    reminders = overdue_action_reminders(user_id)
    if reminders:
        st.markdown("### ⏰ Nhắc việc quan trọng")
        for rm in reminders:
            sev = str(rm.get("severity") or "medium")
            tag = "🔴" if sev == "high" else "🟡"
            st.caption(f"{tag} {rm.get('title')}: {rm.get('message')}")


def render_next_best_action_cta(user_id: str, ticker: str, report: dict, plan_id: str) -> None:
    nba = next_best_action(user_id)
    nid = str(nba.get("id") or "")
    plan = report.get("risk_plan") or {}
    sl = float(plan.get("stop_loss_price") or 0)
    tp = float(plan.get("take_profit_price") or 0)
    entry = float(plan.get("entry_price") or 0)
    action = str(report.get("final_action") or "WATCH").upper()
    side = "BUY" if action == "BUY" else "WATCH"

    st.markdown("### ⚡ Thực thi ngay (1-click)")
    if nid == "create_first_alert":
        c1, c2 = st.columns(2)
        with c1:
            if st.button("Tạo alert SL ngay", width="stretch"):
                ok_alert, msg_alert = can_use_feature(user_id, "alert", 1, plan_id=plan_id)
                if not ok_alert:
                    st.error(msg_alert)
                elif sl > 0:
                    add_alert(user_id, ticker, "price_below", sl, "1-click from Next Best Action")
                    log_event(user_id, "alert_created", {"symbol": ticker, "type": "price_below", "auto": True})
                    st.success(f"Đã tạo alert SL cho {ticker} tại {sl:,.2f}.")
                else:
                    st.warning("Không có mức SL hợp lệ từ risk plan.")
        with c2:
            if st.button("Tạo alert TP ngay", width="stretch"):
                ok_alert, msg_alert = can_use_feature(user_id, "alert", 1, plan_id=plan_id)
                if not ok_alert:
                    st.error(msg_alert)
                elif tp > 0:
                    add_alert(user_id, ticker, "price_above", tp, "1-click from Next Best Action")
                    log_event(user_id, "alert_created", {"symbol": ticker, "type": "price_above", "auto": True})
                    st.success(f"Đã tạo alert TP cho {ticker} tại {tp:,.2f}.")
                else:
                    st.warning("Không có mức TP hợp lệ từ risk plan.")
        return

    if nid == "log_first_decision":
        if st.button("Lưu decision từ risk plan", width="stretch"):
            if entry > 0 and sl > 0 and tp > 0:
                ok_dec, msg_dec = add_decision(
                    user_id,
                    ticker,
                    side,
                    float(entry),
                    float(sl),
                    float(tp),
                    "Auto decision from Next Best Action",
                    30,
                )
                if ok_dec:
                    log_event(user_id, "decision_added", {"symbol": ticker, "side": side, "auto": True})
                    st.success("Đã lưu decision mẫu thành công.")
                else:
                    st.error(msg_dec)
            else:
                st.warning("Risk plan chưa đủ Entry/SL/TP để lưu decision.")
        return

    if nid == "do_first_analysis":
        st.caption("Bạn đang ở bước đúng: bấm `🚀 Phân tích ngay` ở sidebar để hoàn tất hành động này.")
        return

    if nid == "tighten_execution":
        st.caption("Gợi ý: mở tab `📊 Sức khỏe tài chính` để xem `Kế hoạch vs Thực thi` và chỉnh điểm vào/ra.")
        return

    if nid == "close_trade_samples":
        st.caption("Gợi ý: hoàn tất thêm các trade đang mở để hệ thống học được phong cách thực thi của bạn.")
        return

    if nid == "complete_aha_journey":
        st.caption("Gợi ý: hoàn tất các bước còn thiếu trong `Aha Journey` để mở khóa cá nhân hóa sâu hơn.")
        return

    st.caption("Bạn đã ở trạng thái tốt. Bước tiếp theo là tối ưu chất lượng RR và tuân thủ kế hoạch.")


def build_portfolio_options(candidates: pd.DataFrame, total_capital_vnd: float) -> dict[str, pd.DataFrame]:
    if not candidates.empty and "Trạng thái" in candidates.columns:
        candidates = candidates[candidates["Trạng thái"] == "Sẵn sàng"]
    if candidates.empty:
        empty = pd.DataFrame(
            columns=[
                "Mã",
                "Khuyến nghị",
                "Khung thời gian",
                "Tỷ trọng %",
                "Vốn (VND)",
                "Lợi nhuận kỳ vọng (VND)",
                "Rủi ro ước tính (VND)",
            ]
        )
        return {"an_toan": empty, "can_bang": empty, "mao_hiem": empty}

    def _build(top_n: int, max_weight: float, tilt_return: float) -> pd.DataFrame:
        d = candidates.head(top_n).copy()
        if d.empty:
            return pd.DataFrame(
                columns=[
                    "Mã",
                    "Khuyến nghị",
                    "Khung thời gian",
                    "Tỷ trọng %",
                    "Vốn (VND)",
                    "Lợi nhuận kỳ vọng (VND)",
                    "Rủi ro ước tính (VND)",
                ]
            )
        score = d["Expected Return %"] * tilt_return + (35 - d["Risk % (ước tính)"]) * (1 - tilt_return)
        score = score.clip(lower=0.1)
        w = score / score.sum()
        w = w.clip(upper=max_weight)
        w = w / w.sum()
        alloc = total_capital_vnd * w
        pnl = alloc * d["Expected Return %"] / 100.0
        risk_vnd = alloc * d["Risk % (ước tính)"] / 100.0
        out = pd.DataFrame(
            {
                "Mã": d["Mã"].values,
                "Khuyến nghị": d["Khuyến nghị"].values,
                "Khung thời gian": d["Khung thời gian"].values,
                "Tỷ trọng %": (w * 100).round(2).values,
                "Vốn (VND)": alloc.round(0).values,
                "Lợi nhuận kỳ vọng (VND)": pnl.round(0).values,
                "Rủi ro ước tính (VND)": risk_vnd.round(0).values,
            }
        )
        return out

    return {
        "an_toan": _build(top_n=3, max_weight=0.4, tilt_return=0.35),
        "can_bang": _build(top_n=4, max_weight=0.35, tilt_return=0.5),
        "mao_hiem": _build(top_n=5, max_weight=0.3, tilt_return=0.7),
    }


def render_health_cards(report: dict) -> None:
    valuation = report.get("valuation", {})
    phase = report.get("phase", {})
    fin = report.get("financials", {})
    fscore = int(valuation.get("piotroski_score") or 0)
    mos = valuation.get("margin_of_safety_composite_pct")
    status, color = health_badge(fscore)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(
            f"<div style='padding:10px;border-radius:10px;border:1px solid {color};'><b>Sức khỏe</b><br>{status}</div>",
            unsafe_allow_html=True,
        )
    with c2:
        st.metric("Pha thị trường", str(phase.get("phase", "neutral")).upper())
    with c3:
        st.metric("MOS tổng hợp", f"{mos:.1f}%" if mos is not None else "N/A")
    with c4:
        de = fin.get("debt_to_equity")
        st.metric("Nợ/VCSH", f"{de:.2f}" if isinstance(de, (float, int)) else "N/A")
    st.caption(
        "Legend sức khỏe: 🟢 F-Score >= 7 (mạnh), 🟡 4–6 (trung tính), 🔴 < 4 (yếu). "
        "Pha thị trường lấy từ Trend & Flow (accumulation/breakout/distribution/neutral)."
    )


def render_readiness_checklist(report: dict, ohlcv: pd.DataFrame) -> None:
    """Show data readiness for technical/fundamental/news/LLM."""
    fin = report.get("financials", {})
    news = report.get("news", [])
    phase = report.get("phase", {})

    ohlcv_ok = not ohlcv.empty and len(ohlcv) >= 50
    ohlcv_src = str(ohlcv.attrs.get("source", "")) if ohlcv_ok else ""
    checks = [
        ("OHLCV 50+ phiên", ohlcv_ok),
        ("Financial ratios", any(fin.get(k) is not None for k in ("debt_to_equity", "gross_margin"))),
        ("Tin tức", len(news) > 0),
        ("Pha thị trường", bool(phase.get("phase")) and phase.get("phase") != "neutral"),
        ("LLM live", bool(report.get("llm_used"))),
    ]
    labels = []
    for name, ok in checks:
        labels.append(f"{'✅' if ok else '⚠️'} {name}")
    st.caption("Readiness: " + " · ".join(labels))
    if ohlcv_src:
        st.caption(f"Nguồn OHLCV hiện tại: `{ohlcv_src}`")
    rel = report.get("data_reliability") or {}
    if rel:
        fin_rel = rel.get("financial") or {}
        bt_rel = rel.get("backtest") or {}
        st.caption(
            "Reliability nguồn dữ liệu: "
            f"Kỹ thuật={'✅' if (rel.get('technical') or {}).get('ok') else '⚠️'} | "
            f"Tài chính={'✅' if fin_rel.get('ok') else '⚠️'} "
            f"(source={fin_rel.get('source', 'unknown')}, quality={fin_rel.get('quality_score', 0)}) | "
            f"Tin tức={'✅' if (rel.get('news') or {}).get('ok') else '⚠️'} | "
            f"Backtest={'✅' if bt_rel.get('ok') else '⚠️'} "
            f"(N={bt_rel.get('samples', 0)}, BUY={bt_rel.get('buy_signals', 0)})."
        )
    val = report.get("valuation") or {}
    src_valuation = str(val.get("data_source") or "unknown")
    src_fin = str(((report.get("data_reliability") or {}).get("financial") or {}).get("source") or "unknown")
    src_ohlcv = str(ohlcv.attrs.get("source") or "unknown") if ohlcv_ok else "unknown"
    ohlcv_age = ""
    saved_at = str(ohlcv.attrs.get("saved_at") or "")
    if saved_at:
        try:
            dt = datetime.fromisoformat(saved_at.replace("Z", "+00:00"))
            age_min = max(0.0, (datetime.now(timezone.utc) - dt).total_seconds() / 60.0)
            ohlcv_age = f" (~{age_min:.0f} phút tuổi)"
        except ValueError:
            ohlcv_age = ""
    st.caption(
        "Nguồn đang dùng: "
        f"Định giá=`{src_valuation}` · OHLCV=`{src_ohlcv}`{ohlcv_age} · Financial=`{src_fin}`"
    )


def render_candlestick_with_intrinsic(
    ohlcv: pd.DataFrame,
    valuation: dict,
    *,
    show_mos_zone: bool = True,
    show_ma200: bool = True,
    show_rsi: bool = True,
) -> None:
    d = ohlcv.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    for c in ("open", "high", "low", "close", "volume"):
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")
    d = d.dropna(subset=["date", "open", "high", "low", "close"]).sort_values("date").reset_index(drop=True)
    if d.empty:
        st.warning("Dữ liệu OHLCV rỗng sau làm sạch.")
        return

    intrinsic = float(valuation.get("composite_target_price") or valuation.get("intrinsic_value_graham") or 0)
    close = d["close"]
    ma200 = close.rolling(200, min_periods=50).mean()
    delta = close.diff()
    up = delta.clip(lower=0.0)
    down = -delta.clip(upper=0.0)
    rs = up.ewm(alpha=1 / 14, adjust=False).mean() / down.ewm(alpha=1 / 14, adjust=False).mean().replace(0, pd.NA)
    rsi14 = 100.0 - (100.0 / (1.0 + rs))

    mos_series = pd.Series(0.0, index=d.index)
    if intrinsic > 0:
        mos_series = ((intrinsic - close) / intrinsic) * 100.0
    mos_safe = (mos_series >= 20.0) & (close < intrinsic if intrinsic > 0 else False)
    hover_mos = [f"Biên an toàn hiện tại: {float(x):.1f}%" if intrinsic > 0 else "Biên an toàn hiện tại: N/A" for x in mos_series]

    rows = 2 if show_rsi else 1
    fig = make_subplots(
        rows=rows,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.06 if show_rsi else 0.02,
        row_heights=[0.72, 0.28] if show_rsi else [1.0],
    )
    fig.add_trace(
        go.Candlestick(
            x=d["date"],
            open=d["open"],
            high=d["high"],
            low=d["low"],
            close=d["close"],
            name="Giá thị trường",
            customdata=hover_mos,
            hovertemplate="Ngày: %{x|%Y-%m-%d}<br>O:%{open:.2f} H:%{high:.2f} L:%{low:.2f} C:%{close:.2f}<br>%{customdata}<extra></extra>",
        ),
        row=1,
        col=1,
    )
    if show_ma200:
        fig.add_trace(
            go.Scatter(
                x=d["date"],
                y=ma200,
                mode="lines",
                name="MA200",
                line={"width": 1.6, "color": "#9aa0a6"},
            ),
            row=1,
            col=1,
        )
    if intrinsic > 0:
        fig.add_trace(
            go.Scatter(
                x=d["date"],
                y=[intrinsic] * len(d),
                mode="lines",
                name="Giá trị nội tại",
                line={"dash": "dot", "width": 2, "color": "#2a9d8f"},
            ),
            row=1,
            col=1,
        )
        if show_mos_zone:
            safe_close = d["close"].where(mos_safe)
            safe_intrinsic = pd.Series([intrinsic] * len(d)).where(mos_safe)
            fig.add_trace(
                go.Scatter(
                    x=d["date"],
                    y=safe_close,
                    mode="lines",
                    line={"width": 0, "color": "rgba(34,197,94,0.00)"},
                    showlegend=False,
                    hoverinfo="skip",
                ),
                row=1,
                col=1,
            )
            fig.add_trace(
                go.Scatter(
                    x=d["date"],
                    y=safe_intrinsic,
                    mode="lines",
                    line={"width": 0, "color": "rgba(34,197,94,0.00)"},
                    fill="tonexty",
                    fillcolor="rgba(34,197,94,0.20)",
                    name="Vùng MOS >= 20%",
                    hoverinfo="skip",
                ),
                row=1,
                col=1,
            )

    if show_rsi:
        fig.add_trace(
            go.Scatter(
                x=d["date"],
                y=rsi14,
                mode="lines",
                name="RSI (14)",
                line={"width": 1.8, "color": "#f59e0b"},
            ),
            row=2,
            col=1,
        )
        fig.add_hline(y=30, line_dash="dot", line_color="#ef4444", row=2, col=1)
        fig.add_hline(y=70, line_dash="dot", line_color="#ef4444", row=2, col=1)
    fig.update_yaxes(title_text="Giá", row=1, col=1)
    if show_rsi:
        fig.update_yaxes(title_text="RSI", range=[0, 100], row=2, col=1)
    fig.update_layout(
        height=620 if show_rsi else 500,
        xaxis_rangeslider_visible=False,
        margin={"l": 10, "r": 10, "t": 20, "b": 10},
    )
    st.plotly_chart(fig, width="stretch")
    src = str(ohlcv.attrs.get("source") or "")
    if src in ("disk_cache", "disk_cache_fastpath"):
        st.caption(f"OHLCV từ cache đĩa (source={src}, saved_at={ohlcv.attrs.get('saved_at', 'unknown')}).")
    elif src:
        st.caption(f"OHLCV source: {src}")
    else:
        st.caption("OHLCV source: unknown")


def fetch_ohlcv_yfinance_fallback(symbol: str, sessions: int = 200) -> pd.DataFrame:
    try:
        import yfinance as yf  # type: ignore
    except Exception:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
    candidates = [symbol.upper(), f"{symbol.upper()}.VN", f"{symbol.upper()}.HM"]
    for t in candidates:
        try:
            hist = yf.Ticker(t).history(period="1y", interval="1d")
            if hist is None or hist.empty:
                continue
            out = hist.reset_index().rename(
                columns={
                    "Date": "date",
                    "Open": "open",
                    "High": "high",
                    "Low": "low",
                    "Close": "close",
                    "Volume": "volume",
                }
            )
            out = out[["date", "open", "high", "low", "close", "volume"]].tail(max(60, sessions)).copy()
            out.attrs["source"] = "yfinance_fallback"
            return out
        except Exception:
            continue
    return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])


def render_whys(report: dict) -> None:
    st.subheader("Phân tích 7 Whys")
    st.caption(f"Nguồn AI: {report.get('llm_provider')} · LLM used: {report.get('llm_used')}")
    for i, text in enumerate(report.get("whys_steps") or [], start=1):
        st.markdown(f"**{i}.** {text}")
    st.markdown("### Kết luận chiến lược")
    st.write(report.get("analysis_text", "Chưa có kết luận."))
    bz = report.get("buy_zone") or {}
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Vùng mua", f"{bz.get('low', 'N/A')} - {bz.get('high', 'N/A')}")
    with c2:
        st.metric("Chốt lời mục tiêu", f"{report.get('take_profit', 'N/A')}")
    with c3:
        st.metric("Stop-loss tuyệt đối", f"{report.get('stop_loss', 'N/A')}")


def render_evidence(report: dict) -> None:
    st.subheader("Bảng số liệu BCTC")
    fin = report.get("financials", {})
    rows = [
        {"Chỉ tiêu": "Nợ/Vốn chủ sở hữu", "Giá trị": fin.get("debt_to_equity")},
        {"Chỉ tiêu": "Biên lợi nhuận gộp", "Giá trị": fin.get("gross_margin")},
        {"Chỉ tiêu": "Tăng trưởng doanh thu YoY", "Giá trị": fin.get("revenue_growth_yoy")},
        {"Chỉ tiêu": "Tăng trưởng doanh thu QoQ", "Giá trị": fin.get("revenue_growth_qoq")},
        {"Chỉ tiêu": "Tăng trưởng lợi nhuận YoY", "Giá trị": fin.get("profit_growth_yoy")},
        {"Chỉ tiêu": "Tăng trưởng lợi nhuận QoQ", "Giá trị": fin.get("profit_growth_qoq")},
    ]
    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
    st.subheader("Tin tức mới nhất")
    for item in report.get("news", []):
        st.markdown(f"- [{item.get('title')}]({item.get('url')}) ({item.get('source')})")


def render_report_downloads(report: dict) -> None:
    summary = report.get("valuation", {})
    ticker = summary.get("symbol", "stock")
    md = investment_report_markdown(summary)
    html_doc = investment_report_html(summary)
    json_doc = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    c1, c2, c3 = st.columns(3)
    with c1:
        st.download_button(
            "Tải báo cáo Markdown",
            data=md.encode("utf-8"),
            file_name=f"{ticker}_bao_cao.md",
            mime="text/markdown",
            width="stretch",
        )
    with c2:
        st.download_button(
            "Tải báo cáo HTML (in ra PDF)",
            data=html_doc.encode("utf-8"),
            file_name=f"{ticker}_bao_cao.html",
            mime="text/html",
            width="stretch",
        )
    with c3:
        st.download_button(
            "Tải báo cáo JSON",
            data=json_doc.encode("utf-8"),
            file_name=f"{ticker}_strategic_report.json",
            mime="application/json",
            width="stretch",
        )


def render_risk_plan(report: dict) -> None:
    st.subheader("🛡️ Kế hoạch tác chiến an toàn")
    plan = report.get("risk_plan") or {}
    if not plan:
        st.info("Chưa có risk plan.")
        return
    table = pd.DataFrame(
        [
            {
                "Mua giá nào?": plan.get("entry_price"),
                "Bán giá nào?": plan.get("take_profit_price") or report.get("take_profit"),
                "Cắt lỗ giá nào?": plan.get("stop_loss_price"),
            }
        ]
    )
    st.dataframe(table, width="stretch", hide_index=True)

    alloc = float(plan.get("allocated_capital_vnd") or 0)
    total = float(report.get("total_capital_vnd") or 0)
    cash = max(total - alloc, 0.0)
    fig = go.Figure(
        data=[
            go.Pie(
                labels=["Mã đang phân tích", "Phần vốn còn lại"],
                values=[alloc, cash],
                hole=0.45,
            )
        ]
    )
    fig.update_layout(height=320, margin={"l": 10, "r": 10, "t": 30, "b": 10})
    st.plotly_chart(fig, width="stretch")
    st.caption(
        f"Giải ngân tối đa: {plan.get('max_position_pct')}% ~ {alloc:,.0f} VND | "
        f"SL xấu nhất: {plan.get('worst_case_loss_vnd', 0):,.0f} VND "
        f"({plan.get('worst_case_loss_pct_total_capital', 0)}% tổng vốn)."
    )


def build_final_opportunity_table(candidates: pd.DataFrame, discount_threshold_pct: float = 20.0) -> pd.DataFrame:
    """Create concise final recommendations from scanned candidates."""
    if candidates.empty:
        return pd.DataFrame(
            columns=["Mã", "Tiểu ngành", "Giá hiện tại", "Giá mục tiêu tích sản", "Chiết khấu", "Thông điệp cuối cùng"]
        )
    d = candidates.copy()
    d = d[d["MOS%"] >= discount_threshold_pct].copy()
    if d.empty:
        return pd.DataFrame(
            columns=["Mã", "Tiểu ngành", "Giá hiện tại", "Giá mục tiêu tích sản", "Chiết khấu", "Thông điệp cuối cùng"]
        )
    out_rows: list[dict] = []
    for _, r in d.head(8).iterrows():
        price = float(r.get("Giá", 0) or 0)
        target_buy = price * (1.0 - discount_threshold_pct / 100.0) if price > 0 else 0
        out_rows.append(
            {
                "Mã": r.get("Mã"),
                "Tiểu ngành": r.get("Tiểu ngành", "Khác"),
                "Giá hiện tại": round(price, 2),
                "Giá mục tiêu tích sản": round(target_buy, 2),
                "Chiết khấu": f">= {discount_threshold_pct:.0f}%",
                "Thông điệp cuối cùng": "Giá đang tốt, có thể mua tích sản dần.",
            }
        )
    return pd.DataFrame(out_rows)


def _inject_custom_styles() -> None:
    st.markdown(
        """
        <style>
        /* ==== Light UI ==== */
        .stApp {
            background: #f5f7fb;
            color: #111827;
            font-family: Roboto, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        }

        .main .block-container {
            max-width: 1080px;
            padding-top: 0.5rem;
            padding-bottom: 1.2rem;
        }

        /* Sidebar light */
        div[data-testid="stSidebar"] {
            background: #ffffff;
            border-right: 1px solid #e5e7eb;
        }

        div[data-testid="stSidebar"] h1,
        div[data-testid="stSidebar"] h2,
        div[data-testid="stSidebar"] h3,
        div[data-testid="stSidebar"] label,
        div[data-testid="stSidebar"] p {
            color: #111827 !important;
        }

        /* Nút bo tròn kiểu mobile */
        div[data-testid="stSidebar"] .stButton button[kind="primary"],
        .stButton button[kind="primary"] {
            background: linear-gradient(135deg, #ff4d5b, #ff3747);
            color: #ffffff;
            border: none;
            font-weight: 700;
            border-radius: 999px;
            padding-top: 0.45rem;
            padding-bottom: 0.45rem;
            box-shadow: 0 8px 20px rgba(255, 55, 71, 0.35);
        }

        .stButton button:hover[kind="primary"] {
            background: linear-gradient(135deg, #ff5966, #ff3747);
            transform: translateY(-1px);
        }

        /* Nút thường: chip tròn, border nhẹ */
        .stButton button {
            border-radius: 999px;
            border: 1px solid #e5e7eb;
            background: #ffffff;
            color: #111827;
            padding-top: 0.4rem;
            padding-bottom: 0.4rem;
        }

        .stButton button:hover {
            border-color: #ff3747;
            color: #111827;
        }

        /* Input light */
        .stTextInput>div>div>input,
        .stNumberInput input,
        .stSelectbox>div>div>div,
        .stTextArea textarea {
            background-color: #f9fafb;
            color: #111827;
            border-radius: 14px;
            border: 1px solid #e5e7eb;
            padding-top: 0.4rem;
            padding-bottom: 0.4rem;
        }

        .stTextInput>div>div>input:focus,
        .stNumberInput input:focus,
        .stSelectbox>div>div>div:focus,
        .stTextArea textarea:focus {
            outline: none;
            border-color: #ff3747;
            box-shadow: 0 0 0 1px #ff374766;
            background-color: #ffffff;
        }

        /* Bảng dữ liệu */
        div[data-testid="stDataFrame"] {
            background: #ffffff;
            border-radius: 14px;
            border: 1px solid #e5e7eb;
            padding-top: 4px;
        }

        div[data-testid="stDataFrame"] table {
            font-size: 13px;
            color: #111827;
        }

        div[data-testid="stDataFrame"] thead tr {
            background: #f3f4f6;
            border-bottom: 1px solid #e5e7eb;
        }

        div[data-testid="stDataFrame"] thead th {
            color: #6b7280;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.04em;
        }

        div[data-testid="stDataFrame"] tbody tr:nth-child(even) {
            background: #ffffff;
        }

        div[data-testid="stDataFrame"] tbody tr:nth-child(odd) {
            background: #f9fafb;
        }

        div[data-testid="stDataFrame"] tbody tr:hover {
            background: #eaf2ff;
        }

        /* Box rủi ro & badge */
        .risk-box {
            background: #ffffff;
            color: #111827;
            border-radius: 16px;
            padding: 14px;
            border: 1px solid #e5e7eb;
            box-shadow: none;
        }

        .badge-ok {
            display:inline-block;
            background:#dcfce7;
            color:#166534;
            border-radius:999px;
            padding:4px 10px;
            font-size:12px;
            font-weight:700;
            border: 1px solid #22c55e;
        }

        .badge-warn {
            display:inline-block;
            background:#fee2e2;
            color:#991b1b;
            border-radius:999px;
            padding:4px 10px;
            font-size:12px;
            font-weight:700;
            border: 1px solid #ef4444;
        }

        /* Metric cards */
        div[data-testid="stMetric"] {
            background: #ffffff;
            border-radius: 18px;
            padding: 10px 14px;
            border: 1px solid #e5e7eb;
            box-shadow: 0 8px 20px rgba(15,23,42,0.08);
        }

        /* Tabs dạng pill */
        .stTabs [data-baseweb="tab-list"] {
            gap: 8px;
        }

        .stTabs [data-baseweb="tab"] {
            background-color: #e5e7eb;
            border-radius: 999px;
            padding: 6px 14px;
            color: #4b5563;
            border: 1px solid #d1d5db;
            min-height: 38px;
        }

        .stTabs [data-baseweb="tab"][aria-selected="true"] {
            background-color: #ffffff;
            color: #ff3747;
            border-color: #ff3747;
        }

        /* Plotly chart nền sáng */
        .js-plotly-plot .plotly .main-svg {
            background-color: #ffffff !important;
        }

        .streamlit-expanderHeader {
            background: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 12px;
            color: #111827 !important;
        }

        .quick-card {
            background: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 16px;
            padding: 12px 14px;
            min-height: 118px;
            box-shadow: 0 6px 18px rgba(15,23,42,0.06);
        }
        .quick-title {
            color: #6b7280;
            font-size: 12px;
            margin-bottom: 6px;
        }
        .quick-value {
            color: #111827;
            font-size: 24px;
            font-weight: 700;
            line-height: 1.2;
        }
        .quick-sub {
            color: #4b5563;
            font-size: 12px;
            margin-top: 6px;
        }
        .quick-pill {
            display: inline-block;
            border-radius: 999px;
            padding: 4px 10px;
            font-size: 12px;
            font-weight: 700;
            border: 1px solid #d1d5db;
            margin-top: 4px;
        }
        .quick-card.status-buy {
            border-color: #00f4b0;
            box-shadow: 0 0 0 1px #00f4b033 inset, 0 0 18px rgba(0, 244, 176, 0.12);
        }
        .quick-card.status-hold {
            border-color: #fbac20;
            box-shadow: 0 0 0 1px #fbac2033 inset, 0 0 16px rgba(251, 172, 32, 0.10);
        }
        .quick-card.status-watch {
            border-color: #ff3747;
            box-shadow: 0 0 0 1px #ff374733 inset, 0 0 16px rgba(255, 55, 71, 0.12);
        }
        @keyframes statusPulse {
            0% { box-shadow: 0 0 0 1px rgba(255,255,255,0.12) inset; }
            50% { box-shadow: 0 0 0 1px rgba(255,255,255,0.20) inset; }
            100% { box-shadow: 0 0 0 1px rgba(255,255,255,0.12) inset; }
        }
        .quick-card.status-buy,
        .quick-card.status-hold,
        .quick-card.status-watch {
            animation: statusPulse 2.2s ease-in-out infinite;
        }

        </style>
        """,
        unsafe_allow_html=True,
    )


def render_main_metrics(report: dict) -> None:
    val = report.get("valuation", {})
    c1, c2, c3, c4 = st.columns(4)
    price = float(val.get("price") or 0)
    intrinsic = float(val.get("composite_target_price") or val.get("intrinsic_value_graham") or 0)
    mos = val.get("margin_of_safety_composite_pct")
    action = str(report.get("final_action", "HOLD"))
    conf = float(report.get("confidence_score") or 0)
    with c1:
        st.metric("💰 Giá hiện tại", f"{price:,.0f} VND" if price > 0 else "N/A")
    with c2:
        st.metric("💎 Giá trị nội tại", f"{intrinsic:,.0f} VND" if intrinsic > 0 else "N/A")
    with c3:
        st.metric("🧷 Biên an toàn", f"{mos:.1f}%" if mos is not None else "N/A")
    with c4:
        st.metric("🧭 Khuyến nghị cuối", f"{action} ({conf:.0f}%)")
    gate_min = float(report.get("confidence_gate_min") or 65.0)
    if not bool(report.get("gate_passed", True)):
        st.error(
            f"🚧 Data Confidence Gate: {conf:.0f}% < {gate_min:.0f}%.\n"
            "Hệ thống chuyển trạng thái sang **WATCH** (chỉ quan sát), chưa khuyến nghị vào lệnh."
        )
    out_gate = report.get("output_quality") or {}
    if report.get("output_gate_passed") is False:
        reasons = ", ".join([str(x) for x in (out_gate.get("reasons") or [])]) or "unknown"
        st.warning(
            "🧪 Output Quality Gate: kế hoạch giá chưa đạt chuẩn thực chiến, "
            f"hệ thống hạ về **WATCH**. Lý do: `{reasons}`"
        )
    latency_s = report.get("latency_seconds")
    if isinstance(latency_s, (float, int)) and latency_s > 0:
        st.caption(f"⏱️ Thời gian phản hồi phân tích: {float(latency_s):.2f}s")


def render_action_explanation(report: dict) -> None:
    val = report.get("valuation") or {}
    fin = report.get("financials") or {}
    phase = report.get("phase") or {}
    action_raw = str(report.get("final_action") or "WATCH").upper()
    # App core currently emits BUY/HOLD/AVOID/WATCH. Show SELL label for user clarity.
    action_label = "SELL" if action_raw == "AVOID" else action_raw
    conf = float(report.get("confidence_score") or 0)
    mos = float(val.get("margin_of_safety_composite_pct") or 0)
    fscore = int(val.get("piotroski_score") or 0)
    phase_name = str(phase.get("phase") or "neutral").upper()
    rev = fin.get("revenue_growth_yoy")
    de = fin.get("debt_to_equity")
    rev_txt = f"{float(rev):.1f}%" if isinstance(rev, (float, int)) else "N/A"
    de_txt = f"{float(de):.2f}" if isinstance(de, (float, int)) else "N/A"
    st.markdown("#### 🎯 Vì sao hệ thống đang khuyến nghị như vậy?")
    st.markdown(
        f"- Khuyến nghị hiện tại: **{action_label}** (độ tin cậy `{conf:.0f}%`)\n"
        f"- Định giá: MOS `{mos:.1f}%` · F-Score `{fscore}/9`\n"
        f"- Kỹ thuật: pha thị trường `{phase_name}`\n"
        f"- Cơ bản doanh nghiệp: Rev YoY `{rev_txt}` · D/E `{de_txt}`"
    )
    if report.get("gate_passed") is False:
        st.warning("Confidence Gate chưa đạt, nên hệ thống ưu tiên WATCH để tránh vào lệnh sớm.")
    if report.get("output_gate_passed") is False:
        st.warning("Output Quality Gate chưa đạt (plan giá chưa thực chiến), nên hệ thống tự hạ về WATCH.")


def render_macro_micro_outlook(report: dict) -> None:
    val = report.get("valuation") or {}
    phase = report.get("phase") or {}
    fin = report.get("financials") or {}
    news = report.get("news") or []
    industry = str(val.get("industry_subtype_label_vi") or val.get("industry_cluster_label_vi") or "Khác")
    phase_name = str(phase.get("phase") or "neutral")
    vol_mult = float((phase.get("metrics") or {}).get("vol_multiple") or 0)
    rev = fin.get("revenue_growth_yoy")
    rev_num = float(rev) if isinstance(rev, (float, int)) else None
    macro_hint = "trung tính"
    if len(news) >= 3:
        macro_hint = "tin tức dày hơn bình thường"
    if phase_name == "distribution":
        macro_hint = "áp lực thị trường ngắn hạn đang tăng"
    elif phase_name == "breakout":
        macro_hint = "dòng tiền thị trường đang ủng hộ xu hướng tăng"
    micro_hint = "nội tại ổn định"
    if rev_num is not None:
        if rev_num >= 15:
            micro_hint = "nội tại tăng trưởng tốt"
        elif rev_num < 0:
            micro_hint = "nội tại suy yếu, cần thận trọng"
    st.markdown("#### 🌐 Góc nhìn vi mô/vĩ mô ảnh hưởng giá thời gian tới")
    st.caption(
        f"Vĩ mô/ngắn hạn: {macro_hint}. "
        f"Vi mô/ngành: {industry} - {micro_hint}. "
        f"Xung lực kỹ thuật hiện tại: phase={phase_name}, volume multiple={vol_mult:.2f}."
    )
    if news:
        top_news = " | ".join([str(x.get("title") or "") for x in news[:2]])
        st.caption(f"Headline tác động gần nhất: {top_news}")


def render_probabilistic_forecast(report: dict) -> None:
    fc = report.get("probabilistic_forecast") or {}
    rel = report.get("forecast_reliability") or {}
    cal = report.get("forecast_calibration") or {}
    scenarios = fc.get("scenarios") or []
    st.markdown("#### 🔮 Dự phóng xác suất giá")
    exp_px = fc.get("expected_price")
    exp_ret = fc.get("expected_return_pct")
    hz = int(fc.get("horizon_days") or 90)
    if not scenarios or exp_px is None:
        st.info("Chưa đủ dữ liệu để tạo dự phóng xác suất.")
        return
    c1, c2 = st.columns(2)
    with c1:
        st.metric(f"Giá kỳ vọng {hz} ngày", f"{float(exp_px):,.0f} VND")
    with c2:
        st.metric("Lợi suất kỳ vọng", f"{float(exp_ret):.2f}%")
    df = pd.DataFrame(scenarios)
    if not df.empty:
        df["probability"] = df["probability"].map(lambda x: f"{float(x) * 100:.1f}%")
        df["target_price"] = df["target_price"].map(lambda x: f"{float(x):,.0f}")
        df["return_pct"] = df["return_pct"].map(lambda x: f"{float(x):.2f}%")
        df = df.rename(
            columns={
                "name": "Kịch bản",
                "probability": "Xác suất",
                "target_price": "Giá mục tiêu",
                "return_pct": "Lợi suất",
            }
        )
        st.dataframe(df, width="stretch", hide_index=True)
    st.caption(str(fc.get("summary") or ""))
    hit = rel.get("hit_rate_proxy_pct")
    err = rel.get("expected_abs_error_pct")
    quality = str(rel.get("quality_label") or "insufficient_data").upper()
    if isinstance(hit, (float, int)) and isinstance(err, (float, int)):
        st.caption(f"Độ tin cậy dự phóng (proxy): {hit:.1f}% | Sai số kỳ vọng: ±{err:.1f}% | Mức: {quality}")
    notes = str(rel.get("notes") or "")
    if notes:
        st.caption(notes)
    if cal.get("applied"):
        st.caption(
            "Calibrated theo dữ liệu thực tế: "
            f"bias={float(cal.get('bias_pct') or 0):.2f}% | "
            f"factor={float(cal.get('factor') or 0):.2f} | "
            f"resolved={int(cal.get('resolved_records') or 0)}."
        )


def render_forecast_accuracy(user_id: str, ticker: str) -> None:
    st.markdown("#### 🎯 Độ chính xác dự báo thực tế")
    acc = get_forecast_accuracy_dashboard(user_id, ticker)
    total = int(acc.get("records") or 0)
    resolved = int(acc.get("resolved") or 0)
    hit = acc.get("hit_rate_pct")
    mape = acc.get("mape_pct")
    bias = acc.get("bias_pct")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Tổng bản ghi", total)
    with c2:
        st.metric("Đã đối chiếu", resolved)
    with c3:
        st.metric("Hit-rate thực tế", f"{float(hit):.1f}%" if isinstance(hit, (float, int)) else "N/A")
    with c4:
        st.metric("Sai số TB (MAPE proxy)", f"{float(mape):.1f}%" if isinstance(mape, (float, int)) else "N/A")
    if isinstance(bias, (float, int)):
        if bias > 0:
            st.caption(f"Bias: +{float(bias):.2f}% (mô hình đang hơi bảo thủ, thực tế tốt hơn dự báo).")
        elif bias < 0:
            st.caption(f"Bias: {float(bias):.2f}% (mô hình đang hơi lạc quan, cần thận trọng).")
        else:
            st.caption("Bias: gần 0% (khá cân bằng giữa kỳ vọng và thực tế).")


def render_forecast_benchmark(user_id: str, ticker: str) -> None:
    st.markdown("#### 🧪 Benchmark thực chiến 30/60/90 ngày")
    rows = get_forecast_benchmark_by_horizon(user_id, ticker, horizons=(30, 60, 90))
    if not rows:
        st.caption("Chưa có dữ liệu benchmark.")
        return
    df = pd.DataFrame(rows)
    if df.empty:
        st.caption("Chưa có dữ liệu benchmark.")
        return
    view = df.copy()
    for col in (
        "hit_rate_pct",
        "mape_pct",
        "expected_return_pct_avg",
        "realized_return_pct_avg",
        "alpha_pct",
        "beat_expected_pct",
    ):
        if col in view.columns:
            view[col] = view[col].map(lambda x: f"{float(x):.2f}%" if isinstance(x, (float, int)) else "N/A")
    view = view.rename(
        columns={
            "horizon_days": "Horizon",
            "samples": "Mẫu",
            "hit_rate_pct": "Hit-rate",
            "mape_pct": "Sai số TB",
            "expected_return_pct_avg": "Dự báo TB",
            "realized_return_pct_avg": "Thực tế TB",
            "alpha_pct": "Alpha (thực tế-dự báo)",
            "beat_expected_pct": "Tỷ lệ vượt dự báo",
        }
    )
    st.dataframe(view, width="stretch", hide_index=True)
    st.caption("Alpha dương: thực tế tốt hơn dự báo trung bình. Alpha âm: mô hình đang hơi lạc quan.")


def recommend_task_mode(user_id: str, ticker: str, quick_mode: bool) -> str:
    if quick_mode:
        return "speed"
    bench_rows = get_forecast_benchmark_by_horizon(user_id, ticker, horizons=(30, 60, 90))
    valid = [x for x in bench_rows if int(x.get("samples") or 0) >= 3]
    # Sector-aware fallback when single-symbol history is thin.
    if len(valid) < 2:
        s_map = universe_subtype_map()
        subtype = str(s_map.get(str(ticker or "").strip().upper(), "other"))
        peers = [s for s, st in s_map.items() if str(st) == subtype][:25]
        gb = get_forecast_group_benchmark(user_id, peers)
        if int(gb.get("samples") or 0) >= 8:
            hit = float(gb.get("hit_rate_pct") or 0.0)
            mape = float(gb.get("mape_pct") or 99.0)
            alpha = float(gb.get("alpha_pct") or 0.0)
            if hit >= 62 and mape <= 9 and alpha >= -1:
                return "speed"
            if hit < 52 or mape > 14 or alpha < -3:
                return "quality"
            return "balanced"
    if not valid:
        return "balanced"
    hit = sum(float(x.get("hit_rate_pct") or 0.0) for x in valid) / len(valid)
    mape = sum(float(x.get("mape_pct") or 0.0) for x in valid) / len(valid)
    alpha = sum(float(x.get("alpha_pct") or 0.0) for x in valid) / len(valid)
    if hit >= 62 and mape <= 9 and alpha >= -1:
        return "speed"
    if hit < 52 or mape > 14 or alpha < -3:
        return "quality"
    return "balanced"


def render_model_drift_panel(user_id: str, ticker: str) -> None:
    st.markdown("#### 📉 Theo dõi model drift")
    d = get_forecast_drift_signal(user_id, ticker, recent_n=20, baseline_n=60)
    status = str(d.get("status") or "unknown")
    msg = str(d.get("message") or "")
    dh = d.get("delta_hit_pct")
    dm = d.get("delta_mape_pct")
    if status == "drift_down":
        st.warning(msg)
    elif status == "improving":
        st.success(msg)
    else:
        st.caption(msg)
    if isinstance(dh, (float, int)) and isinstance(dm, (float, int)):
        st.caption(f"So với baseline: delta hit-rate = {float(dh):+.2f}% | delta sai số = {float(dm):+.2f}%")
    h_th = d.get("hit_drop_threshold_pct")
    m_th = d.get("mape_rise_threshold_pct")
    if isinstance(h_th, (float, int)) and isinstance(m_th, (float, int)):
        st.caption(f"Ngưỡng drift thích ứng: hit <= -{float(h_th):.2f}% hoặc sai số >= +{float(m_th):.2f}%")
    streak = get_forecast_drift_streak(user_id, ticker, checks=3)
    st.caption(
        f"Drift down streak: {int(streak.get('drift_down_streak') or 0)}/{int(streak.get('checks') or 0)} kỳ gần nhất."
    )


def render_forecast_portfolio_dashboard(user_id: str) -> None:
    st.markdown("#### 📊 Dashboard hiệu năng danh mục")
    d = get_forecast_portfolio_dashboard(user_id)
    w = d.get("weekly") or {}
    m = d.get("monthly") or {}
    o = d.get("overall") or {}
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Weekly hit-rate", f"{float(w.get('hit_rate_pct')):.1f}%" if isinstance(w.get("hit_rate_pct"), (float, int)) else "N/A")
        st.caption(f"Mẫu: {int(w.get('samples') or 0)}")
    with c2:
        st.metric("Monthly hit-rate", f"{float(m.get('hit_rate_pct')):.1f}%" if isinstance(m.get("hit_rate_pct"), (float, int)) else "N/A")
        st.caption(f"Mẫu: {int(m.get('samples') or 0)}")
    with c3:
        st.metric("Overall MAPE", f"{float(o.get('mape_pct')):.1f}%" if isinstance(o.get("mape_pct"), (float, int)) else "N/A")
        st.caption(f"Alpha tổng: {float(o.get('alpha_pct')):+.2f}%" if isinstance(o.get("alpha_pct"), (float, int)) else "Alpha tổng: N/A")


def apply_drift_guardrails(user_id: str, ticker: str, report: dict, task_mode: str) -> tuple[dict, str]:
    d = get_forecast_drift_signal(user_id, ticker, recent_n=20, baseline_n=60)
    streak = get_forecast_drift_streak(user_id, ticker, checks=3)
    drift_streak = int(streak.get("drift_down_streak") or 0)
    if str(d.get("status")) != "drift_down" and drift_streak < 2:
        report["drift_guardrails"] = {"applied": False, "status": d.get("status")}
        return report, task_mode
    # Auto reaction: escalate mode and reduce risk sizing.
    forced_mode = "quality"
    rp = dict(report.get("risk_plan") or {})
    orig_pct = float(rp.get("max_position_pct") or 20.0)
    new_pct = max(5.0, round(orig_pct * 0.75, 2))
    if rp:
        rp["max_position_pct"] = new_pct
        alloc = float(rp.get("allocated_capital_vnd") or 0.0)
        if alloc > 0:
            rp["allocated_capital_vnd"] = round(alloc * 0.75, 2)
        report["risk_plan"] = rp
    report["drift_guardrails"] = {
        "applied": True,
        "status": "drift_down",
        "drift_streak": drift_streak,
        "forced_task_mode": forced_mode,
        "max_position_pct_before": orig_pct,
        "max_position_pct_after": new_pct,
    }
    return report, forced_mode


def render_forecast_leaderboard(user_id: str) -> None:
    st.markdown("#### 🏆 Leaderboard hiệu suất dự báo")
    rows = get_forecast_leaderboard(user_id, limit=12)
    if not rows:
        st.caption("Chưa đủ dữ liệu resolved để xếp hạng.")
        return
    df = pd.DataFrame(rows)
    view = df.copy()
    for col in ("hit_rate_pct", "mape_pct", "alpha_pct"):
        if col in view.columns:
            view[col] = view[col].map(lambda x: f"{float(x):.2f}%")
    view = view.rename(
        columns={
            "symbol": "Mã",
            "samples": "Mẫu",
            "hit_rate_pct": "Hit-rate",
            "mape_pct": "Sai số TB",
            "alpha_pct": "Alpha",
            "score": "Điểm tổng hợp",
        }
    )
    st.dataframe(view, width="stretch", hide_index=True)


def render_sector_mode_suggestions(user_id: str) -> None:
    st.markdown("#### 🧭 Gợi ý mode theo ngành")
    lb = get_forecast_leaderboard(user_id, limit=50)
    if not lb:
        st.caption("Chưa đủ dữ liệu leaderboard để gợi ý theo ngành.")
        return
    s_map = universe_subtype_map()
    by_subtype: dict[str, list[dict]] = {}
    for row in lb:
        sym = str(row.get("symbol") or "").upper()
        stype = str(s_map.get(sym, "other"))
        by_subtype.setdefault(stype, []).append(row)
    out: list[dict] = []
    for stype, grp in by_subtype.items():
        n = len(grp)
        if n < 2:
            continue
        hit = sum(float(x.get("hit_rate_pct") or 0.0) for x in grp) / n
        mape = sum(float(x.get("mape_pct") or 0.0) for x in grp) / n
        alpha = sum(float(x.get("alpha_pct") or 0.0) for x in grp) / n
        if hit >= 62 and mape <= 9 and alpha >= -1:
            mode = "speed"
        elif hit < 52 or mape > 14 or alpha < -3:
            mode = "quality"
        else:
            mode = "balanced"
        out.append(
            {
                "Ngành con": stype,
                "Mẫu": n,
                "Hit-rate TB": f"{hit:.2f}%",
                "Sai số TB": f"{mape:.2f}%",
                "Alpha TB": f"{alpha:+.2f}%",
                "Mode gợi ý": mode,
            }
        )
    if not out:
        st.caption("Chưa đủ dữ liệu theo ngành để gợi ý mode.")
        return
    st.dataframe(pd.DataFrame(out), width="stretch", hide_index=True)


def calibrate_report_forecast_with_history(user_id: str, ticker: str, report: dict) -> dict:
    fc = dict((report or {}).get("probabilistic_forecast") or {})
    scenarios = list(fc.get("scenarios") or [])
    if not fc or not scenarios:
        report["forecast_calibration"] = {"applied": False, "reason": "missing_forecast"}
        return report

    acc = get_forecast_accuracy_dashboard(user_id, ticker)
    resolved = int(acc.get("resolved") or 0)
    bias = acc.get("bias_pct")
    min_records_raw = os.environ.get("AI_FORECAST_CALIB_MIN_RECORDS", "8").strip()
    try:
        min_records = max(3, min(100, int(min_records_raw)))
    except ValueError:
        min_records = 8
    if not isinstance(bias, (float, int)) or resolved < min_records:
        report["forecast_calibration"] = {
            "applied": False,
            "reason": "insufficient_history",
            "resolved_records": resolved,
            "min_required": min_records,
        }
        return report

    # realized - expected = bias; move forecast toward realized partially (shrink to avoid overfit).
    factor_raw = os.environ.get("AI_FORECAST_BIAS_FACTOR", "0.35").strip()
    try:
        factor = float(factor_raw)
    except ValueError:
        factor = 0.35
    factor = max(0.1, min(0.8, factor))
    shift_pct = float(bias) * factor

    base_price = float((report.get("valuation") or {}).get("price") or 0.0)
    if base_price <= 0:
        report["forecast_calibration"] = {"applied": False, "reason": "missing_base_price"}
        return report

    exp_ret = fc.get("expected_return_pct")
    if isinstance(exp_ret, (float, int)):
        new_exp_ret = float(exp_ret) + shift_pct
        fc["expected_return_pct"] = round(new_exp_ret, 2)
        fc["expected_price"] = round(base_price * (1.0 + new_exp_ret / 100.0), 2)

    new_scenarios: list[dict] = []
    for row in scenarios:
        r = dict(row)
        r_ret = r.get("return_pct")
        if isinstance(r_ret, (float, int)):
            r_ret = float(r_ret) + shift_pct
            r["return_pct"] = round(r_ret, 2)
            r["target_price"] = round(base_price * (1.0 + r_ret / 100.0), 2)
        new_scenarios.append(r)
    fc["scenarios"] = new_scenarios
    fc["summary"] = str(fc.get("summary") or "") + f" (đã hiệu chỉnh bias {shift_pct:+.2f}%)."
    report["probabilistic_forecast"] = fc
    report["forecast_calibration"] = {
        "applied": True,
        "bias_pct": round(float(bias), 2),
        "shift_pct": round(shift_pct, 2),
        "factor": round(factor, 2),
        "resolved_records": resolved,
    }
    return report


def render_risk_box(report: dict, profile: str) -> None:
    plan = report.get("risk_plan") or {}
    if not plan:
        st.info("Chưa có kế hoạch rủi ro.")
        return
    note = _profile_match_note(profile, report)
    is_fit = "phù hợp" in note.lower()
    badge_html = (
        "<span class='badge-ok'>Phù hợp hoàn hảo</span>"
        if is_fit
        else "<span class='badge-warn'>Rủi ro cao - Không hợp gu</span>"
    )
    st.markdown("<div class='risk-box'>", unsafe_allow_html=True)
    st.markdown(f"**🛡️ Kế hoạch đi vốn** &nbsp; {badge_html}", unsafe_allow_html=True)
    st.markdown(
        f"- Mua vùng: **{plan.get('buy_zone_low')} - {plan.get('buy_zone_high')}**\n"
        f"- Cắt lỗ: **{plan.get('stop_loss_price')}**\n"
        f"- Chốt lời: **{plan.get('take_profit_price') or report.get('take_profit')}**\n"
        f"- Tỷ trọng vốn: **{plan.get('max_position_pct')}%** (~{float(plan.get('allocated_capital_vnd') or 0):,.0f} VND)\n"
        f"- Số lượng dự kiến: **{plan.get('estimated_quantity')} cp**"
    )
    risk_pct = float(plan.get("worst_case_loss_pct_total_capital") or 0)
    risk_norm = min(max(risk_pct / 5.0, 0.0), 1.0)
    st.progress(risk_norm, text=f"Mức rủi ro vị thế: {risk_pct:.2f}% tổng vốn")
    st.markdown("</div>", unsafe_allow_html=True)


def render_ai_chat_style(report: dict) -> None:
    st.caption(
        f"Model: {report.get('llm_provider')} | "
        f"LLM live: {report.get('llm_used')} | "
        f"Cache hit: {report.get('llm_cache_hit', False)} | "
        f"Escalated: {report.get('llm_escalated', False)} | "
        f"Escalation tried: {report.get('llm_escalation_attempted', False)}"
    )
    with st.chat_message("assistant"):
        st.markdown(report.get("analysis_text", "Chưa có kết luận."))
    for i, x in enumerate(report.get("whys_steps") or [], start=1):
        with st.chat_message("assistant"):
            st.markdown(f"**Why {i}:** {x}")


def render_ai_health_panel() -> None:
    st.markdown("### 🩺 AI Health")
    rows = get_provider_health_snapshot()
    if not rows:
        st.caption("Chưa có dữ liệu health. Hãy chạy phân tích thêm vài lần.")
        return
    df = pd.DataFrame(rows)
    if "avg_ms" in df.columns:
        df["avg_ms"] = df["avg_ms"].map(lambda x: round(float(x), 1))
    st.dataframe(df, width="stretch", hide_index=True)
    st.caption("Key được ẩn dạng ***xxxx. cooldown_sec > 0 nghĩa là key đang tạm nghỉ.")


def render_financial_health_tab(report: dict) -> None:
    val = report.get("valuation", {})
    pio = val.get("piotroski_block") or {}
    rev = float(pio.get("revenue") or 0)
    rev_p = float(pio.get("revenue_prior") or 0)
    ni = float(pio.get("net_income") or 0)
    ni_p = float(pio.get("net_income_prior") or 0)
    fscore = int(val.get("piotroski_score") or 0)

    c1, c2 = st.columns(2)
    with c1:
        if rev > 0 or rev_p > 0:
            df_rev = pd.DataFrame({"Kỳ trước": [rev_p], "Hiện tại": [rev]}, index=["Doanh thu"])
            st.bar_chart(df_rev.T)
        else:
            st.info("Thiếu dữ liệu doanh thu.")
    with c2:
        if ni > 0 or ni_p > 0:
            df_ni = pd.DataFrame({"Kỳ trước": [ni_p], "Hiện tại": [ni]}, index=["Lợi nhuận"])
            st.bar_chart(df_ni.T)
        else:
            st.info("Thiếu dữ liệu lợi nhuận.")
    c3, c4 = st.columns(2)
    with c3:
        st.metric("📊 F-Score", f"{fscore}/9")
    with c4:
        bt = report.get("backtest_summary") or {}
        st.metric(
            "🧪 Backtest BUY win-rate",
            f"{float(bt.get('buy_win_rate_pct') or 0):.1f}%",
            help=f"Mẫu: {bt.get('buy_signals', 0)} tín hiệu BUY / {bt.get('samples', 0)} phiên",
        )


def main() -> None:
    app_version = get_app_version()
    st.set_page_config(page_title="Investment Intelligence Dashboard", page_icon="📊", layout="wide")
    _inject_custom_styles()
    st.title("📡 Investment Intelligence Dashboard")
    st.caption(f"v{app_version} | Technical + Fundamental + News + AI 7 Whys")

    with st.sidebar:
        st.header("⚙️ Bảng điều khiển")
        with st.expander("Cài đặt tài khoản", expanded=True):
            user_id = st.text_input("Mã khách hàng", value="default_user")
            user_pin = st.text_input("PIN truy cập", value="", type="password")
            c_auth1, c_auth2 = st.columns(2)
            with c_auth1:
                if st.button("Đăng nhập", width="stretch"):
                    if has_auth_user(user_id):
                        if verify_user_pin(user_id, user_pin):
                            st.session_state["auth_ok"] = True
                            st.success("Đăng nhập thành công.")
                        else:
                            st.session_state["auth_ok"] = False
                            st.error("PIN không đúng.")
                    else:
                        st.warning("Tài khoản chưa tồn tại. Bấm 'Đăng ký'.")
            with c_auth2:
                if st.button("Đăng ký", width="stretch"):
                    ok_reg, msg_reg = register_user_pin(user_id, user_pin)
                    if ok_reg:
                        st.success(msg_reg)
                    else:
                        st.warning(msg_reg)

            auth_ok = bool(st.session_state.get("auth_ok", False)) if has_auth_user(user_id) else True
            if has_auth_user(user_id) and not auth_ok:
                st.info("Bạn cần đăng nhập để dùng đầy đủ tính năng.")
            persisted_plan = get_user_plan(user_id, "free")
            plan_opts = ["Free", "Pro", "Expert"]
            plan_index = max(0, [x.lower() for x in plan_opts].index(persisted_plan)) if persisted_plan in ("free", "pro", "expert") else 0
            plan_label = st.selectbox("Gói dịch vụ", options=plan_opts, index=plan_index)
            plan_id = plan_label.lower()
            plan = get_plan_features(plan_id)
            set_user_plan(user_id, plan_id)
            profile_label = st.selectbox("Khẩu vị đầu tư", options=list(PROFILE_OPTIONS.keys()), index=1)
            profile = PROFILE_OPTIONS[profile_label]
            total_capital_vnd = float(
                st.number_input("Tổng vốn đầu tư (VND)", min_value=10_000_000.0, value=100_000_000.0, step=10_000_000.0)
            )
            st.caption(
                f"Plan limits: scan<= {plan['scan_limit']} mã | alerts<= {plan['alerts']} | "
                f"LLM live: {'✅' if plan['llm_live'] else '❌'}"
            )
            tg_bot = st.text_input(
                "Telegram Bot Token",
                value=load_secret(user_id, "telegram_bot_token", os.environ.get("TELEGRAM_BOT_TOKEN", "")),
                type="password",
                help="Dùng để gửi alert realtime ra Telegram.",
            )
            tg_chat = st.text_input(
                "Telegram Chat ID",
                value=load_secret(user_id, "telegram_chat_id", os.environ.get("TELEGRAM_CHAT_ID", "")),
                help="Chat cá nhân/nhóm nhận alert.",
            )
            webhook_url = st.text_input(
                "Webhook URL (tuỳ chọn)",
                value=load_secret(user_id, "alert_webhook_url", os.environ.get("ALERT_WEBHOOK_URL", "")),
                help="Nhận JSON alert để tích hợp hệ thống ngoài.",
            )
            email_to = st.text_input("Email nhận alert (tuỳ chọn)", value=load_secret(user_id, "alert_email_to", os.environ.get("ALERT_EMAIL_TO", "")))
            smtp_host = st.text_input("SMTP host", value=load_secret(user_id, "smtp_host", os.environ.get("SMTP_HOST", "")))
            smtp_port = int(load_secret(user_id, "smtp_port", os.environ.get("SMTP_PORT", "587")) or 587)
            smtp_user = st.text_input("SMTP user", value=load_secret(user_id, "smtp_user", os.environ.get("SMTP_USER", "")))
            smtp_password = st.text_input("SMTP password", value=load_secret(user_id, "smtp_password", os.environ.get("SMTP_PASSWORD", "")), type="password")
            smtp_from = st.text_input("SMTP from", value=load_secret(user_id, "smtp_from", os.environ.get("SMTP_FROM", "")))
            if st.button("💾 Lưu cấu hình kênh gửi an toàn", width="stretch"):
                save_secret(user_id, "telegram_bot_token", tg_bot)
                save_secret(user_id, "telegram_chat_id", tg_chat)
                save_secret(user_id, "alert_webhook_url", webhook_url)
                save_secret(user_id, "alert_email_to", email_to)
                save_secret(user_id, "smtp_host", smtp_host)
                save_secret(user_id, "smtp_port", str(smtp_port))
                save_secret(user_id, "smtp_user", smtp_user)
                save_secret(user_id, "smtp_password", smtp_password)
                save_secret(user_id, "smtp_from", smtp_from)
                st.success("Đã lưu cấu hình kênh gửi vào kho bí mật cục bộ.")
            auto_notify = st.toggle("🔔 Tự động gửi Telegram khi alert kích hoạt", value=False)
            with st.expander("💳 Billing / Upgrade", expanded=False):
                target_plan_label = st.selectbox("Nâng lên gói", options=["Pro", "Expert"], index=0)
                target_plan_id = target_plan_label.lower()
                if st.button("✅ Xác nhận nâng cấp (mock checkout)", width="stretch"):
                    set_user_plan(user_id, target_plan_id)
                    log_event(user_id, "upgrade_success", {"from": plan_id, "to": target_plan_id})
                    st.success(f"Đã nâng gói lên {target_plan_label} (mock).")
                if st.button("🧪 Tôi quan tâm nâng cấp", width="stretch"):
                    log_event(user_id, "upgrade_intent", {"from": plan_id, "to": target_plan_id})
                    st.info("Đã ghi nhận tín hiệu quan tâm nâng cấp.")
            with st.expander("🛠️ Notification Queue", expanded=False):
                if st.button("Xử lý hàng đợi notification", width="stretch"):
                    q_stat = process_notification_queue(max_jobs=30)
                    st.caption(
                        f"Queue processed={q_stat['processed']} | sent={q_stat['sent']} | "
                        f"retried={q_stat['retried']} | failed={q_stat['failed']}"
                    )
                if st.button("Migrate user_state.json -> SQLite", width="stretch"):
                    mg = migrate_legacy_json_to_sqlite()
                    st.caption(
                        f"Migrated users={mg['users']} | holdings={mg['holdings']} | "
                        f"alerts={mg['alerts']} | events={mg['events']}"
                    )
                if st.button("Chạy 1 vòng background jobs", width="stretch"):
                    bj = run_background_cycle(universe_limit=30, watchlist=watchlist or WATCHLIST_DEFAULT, queue_jobs=30)
                    st.caption(
                        f"Prefetch ok={bj['prefetch']['ok']}/{bj['prefetch']['total']} | "
                        f"Queue processed={bj['queue']['processed']}"
                    )
        st.divider()
        with st.expander("Danh mục theo dõi", expanded=True):
            raw_watch = st.text_area("Watchlist", value=", ".join(WATCHLIST_DEFAULT), height=80)
            watchlist = [x for x in re.split(r"[\s,;]+", raw_watch.upper().strip()) if re.fullmatch(r"[A-Z0-9]{2,6}", x)]
            picked = st.selectbox("Chọn mã từ watchlist", options=watchlist or WATCHLIST_DEFAULT, index=0)
            manual = st.text_input("Hoặc nhập mã bất kỳ", value=picked)
            ticker = extract_ticker(manual) or picked
            autopilot_mode = st.toggle("🤖 Auto Pilot cơ hội", value=True, help="Mở app là có ngay danh sách cơ hội ưu tiên.")
            profit_focus_mode = st.toggle(
                "💰 Profit Focus Mode",
                value=True,
                help="Chỉ hiện cơ hội đạt chuẩn xuống tiền (Gate + Output + RR).",
            )
            autopilot_limit = int(st.slider("Số mã Auto Pilot quét", min_value=10, max_value=80, value=30, step=5))
            autopilot_refresh = st.button("♻️ Làm mới Auto Pilot", width="stretch")
            quick_mode = st.toggle(
                "⚡ Chế độ phản hồi nhanh",
                value=False,
                help="Bật khi cần tốc độ; tắt để phân tích sâu hơn (tin tức, ngữ cảnh và chất lượng giải thích tốt hơn).",
            )
            ultra_fast_mode = st.toggle(
                "🚀 Ultra Fast (cache-first)",
                value=True,
                help="Ưu tiên cache có kiểm soát tuổi dữ liệu để phản hồi gần như tức thời.",
            )
            if ultra_fast_mode:
                os.environ["II_OHLCV_DISK_FIRST"] = "1"
                os.environ.setdefault("II_OHLCV_DISK_MAX_AGE_SEC", "7200")
                os.environ.setdefault("II_PORTAL_LIVE_BUDGET_SEC", "6")
            st.caption(
                "Mode: "
                + ("Ultra Fast (cache-first + budget)" if ultra_fast_mode else "Standard live-first")
            )
            auto_bg_warm = st.toggle(
                "♨️ Tự làm nóng cache nền",
                value=True,
                help="Tự chạy prefetch nhẹ theo chu kỳ để giảm thời gian chờ ở các lần phân tích sau.",
            )
            bg_interval_min = int(
                st.slider("Chu kỳ làm nóng nền (phút)", min_value=3, max_value=30, value=8, step=1)
            )
            bg_universe_limit = int(
                st.slider("Số mã nền prefetch", min_value=10, max_value=80, value=30, step=5)
            )
            prewarm_now = st.button("🔥 Làm nóng dữ liệu watchlist", width="stretch")
            prewarm_now_async = st.button("♨️ Warm now (non-blocking)", width="stretch")
            st.caption("Gợi ý chạy nền: `python scripts/prefetch_cache.py --watchlist FPT,HPG,VNM --universe-limit 30`")
            run_analysis = st.button("🚀 Phân tích ngay", width="stretch", type="primary")
        st.divider()
        with st.expander("💼 Danh mục & Cảnh báo", expanded=False):
            c1, c2 = st.columns(2)
            with c1:
                h_symbol = st.text_input("Mã thêm vào danh mục", value=ticker)
                h_qty = st.number_input("Số lượng", min_value=0.0, value=0.0, step=100.0)
            with c2:
                h_cost = st.number_input("Giá vốn", min_value=0.0, value=0.0, step=100.0)
                if st.button("➕ Lưu vị thế", width="stretch"):
                    if h_symbol and h_qty > 0 and h_cost > 0:
                        add_holding(user_id, h_symbol, h_qty, h_cost)
                        log_event(user_id, "holding_added", {"symbol": h_symbol.upper(), "qty": h_qty})
                        st.success("Đã lưu vị thế.")
                    else:
                        st.warning("Nhập đủ Mã, Số lượng và Giá vốn > 0.")

            current_alerts = list_alerts(user_id)
            st.caption(f"Đã tạo {len(current_alerts)}/{plan['alerts']} cảnh báo theo gói {plan_label}.")
            a_symbol = st.text_input("Mã cảnh báo", value=ticker)
            a_type = st.selectbox(
                "Loại cảnh báo",
                options=["price_above", "price_below"],
                format_func=lambda x: "Giá vượt ngưỡng" if x == "price_above" else "Giá thủng ngưỡng",
            )
            a_threshold = st.number_input("Ngưỡng giá", min_value=0.0, value=0.0, step=100.0)
            a_note = st.text_input("Ghi chú", value="")
            if st.button("🔔 Tạo cảnh báo", width="stretch"):
                ok_alert, msg_alert = can_use_feature(user_id, "alert", 1, plan_id=plan_id)
                if not ok_alert:
                    st.error(msg_alert)
                elif a_symbol and a_threshold > 0:
                    add_alert(user_id, a_symbol, a_type, a_threshold, a_note)
                    log_event(user_id, "alert_created", {"symbol": a_symbol.upper(), "type": a_type})
                    st.success("Đã tạo cảnh báo.")
                else:
                    st.warning("Nhập mã và ngưỡng giá hợp lệ.")

            st.markdown("#### 📝 Nhật ký quyết định đầu tư")
            d_symbol = st.text_input("Mã quyết định", value=ticker)
            d_side = st.selectbox("Loại quyết định", options=["BUY", "WATCH"])
            d_entry = st.number_input("Entry quyết định", min_value=0.0, value=0.0, step=100.0)
            d_sl = st.number_input("Stop-loss quyết định", min_value=0.0, value=0.0, step=100.0)
            d_tp = st.number_input("Take-profit quyết định", min_value=0.0, value=0.0, step=100.0)
            d_hz = int(st.slider("Khung hậu kiểm (ngày)", min_value=7, max_value=180, value=30, step=1))
            d_thesis = st.text_area("Luận điểm đầu tư", value="", height=80)
            cache_rep = st.session_state.get("report_cache") or {}
            coach = coach_decision_quality_adaptive(
                user_id,
                side=d_side,
                entry_price=float(d_entry),
                stop_loss=float(d_sl),
                take_profit=float(d_tp),
                gate_passed=bool(cache_rep.get("gate_passed")),
                confidence_score=float(cache_rep.get("confidence_score") or 0),
            )
            st.caption(
                f"Coach score: {coach['coach_score']}/100 | RR: {coach['rr']} | Verdict: {coach['verdict']} "
                f"| Adaptive: {coach.get('adaptive_profile')}"
            )
            th = coach.get("adaptive_thresholds") or {}
            st.caption(
                f"Ngưỡng cá nhân hóa -> RR tối thiểu: {float(th.get('rr_min') or 0):.2f}, "
                f"GO >= {float(th.get('go_min_score') or 0):.0f}, "
                f"CAUTION >= {float(th.get('caution_min_score') or 0):.0f}"
            )
            phase_metrics = (cache_rep.get("phase") or {}).get("metrics") or {}
            vol_mult = float(phase_metrics.get("vol_multiple") or 1.0)
            base_pos = float((cache_rep.get("risk_plan") or {}).get("max_position_pct") or 20.0)
            sizing = adaptive_position_sizing(
                user_id,
                base_max_position_pct=base_pos,
                confidence_score=float(cache_rep.get("confidence_score") or 0),
                gate_passed=bool(cache_rep.get("gate_passed")),
                vol_multiple=vol_mult,
                coach_verdict=str(coach.get("verdict") or "CAUTION"),
            )
            st.caption(
                f"Tỷ trọng gợi ý (adaptive): {sizing['suggested_position_pct']}% | "
                f"Bucket: {sizing['risk_bucket']} | Discipline: {sizing['discipline_score']}"
            )
            st.caption(sizing["reason"])
            if coach["strengths"]:
                st.caption("✅ " + " | ".join(coach["strengths"]))
            if coach["warnings"]:
                st.warning(" | ".join(coach["warnings"]))
            if st.button("💾 Lưu quyết định", width="stretch"):
                if coach["verdict"] == "NO-GO":
                    st.error("Coach Mode: NO-GO. Vui lòng chỉnh kế hoạch trước khi lưu.")
                    st.stop()
                ok_dec, msg_dec = add_decision(
                    user_id,
                    d_symbol,
                    d_side,
                    float(d_entry),
                    float(d_sl),
                    float(d_tp),
                    d_thesis,
                    d_hz,
                )
                if ok_dec:
                    log_event(
                        user_id,
                        "decision_added",
                        {
                            "symbol": d_symbol.upper(),
                            "side": d_side,
                            "coach_score": coach["coach_score"],
                            "verdict": coach["verdict"],
                        },
                    )
                    st.success(msg_dec)
                else:
                    st.error(msg_dec)

            st.markdown("#### 📒 Trade Journal (Realized PnL)")
            tj1, tj2 = st.columns(2)
            with tj1:
                t_symbol = st.text_input("Mã mở lệnh", value=ticker)
                d_opts = list_decisions(user_id, limit=100)
                d_map = {f"{x['id']} | {x['symbol']} | {x['side']}": int(x["id"]) for x in d_opts}
                selected_d = st.selectbox("Link tới Decision (tuỳ chọn)", options=["(không link)"] + list(d_map.keys()))
                t_qty = st.number_input("Số lượng mở", min_value=0.0, value=0.0, step=100.0)
                t_entry = st.number_input("Giá mở thực tế", min_value=0.0, value=0.0, step=100.0)
                t_entry_fee = st.number_input("Phí mở", min_value=0.0, value=0.0, step=1000.0)
                t_entry_note = st.text_input("Ghi chú mở", value="")
                if st.button("🟢 Mở trade", width="stretch"):
                    linked_decision_id = d_map.get(selected_d) if selected_d != "(không link)" else None
                    ok_tr, msg_tr = open_trade(
                        user_id,
                        t_symbol,
                        float(t_qty),
                        float(t_entry),
                        decision_id=linked_decision_id,
                        entry_fee=float(t_entry_fee),
                        entry_note=t_entry_note,
                    )
                    if ok_tr:
                        log_event(user_id, "trade_opened", {"symbol": t_symbol.upper(), "qty": t_qty})
                        st.success(msg_tr)
                    else:
                        st.error(msg_tr)
            with tj2:
                close_id = int(st.number_input("ID trade đóng", min_value=0.0, value=0.0, step=1.0))
                close_price = st.number_input("Giá đóng thực tế", min_value=0.0, value=0.0, step=100.0)
                close_fee = st.number_input("Phí đóng", min_value=0.0, value=0.0, step=1000.0)
                close_note = st.text_input("Ghi chú đóng", value="")
                if st.button("🔴 Đóng trade", width="stretch"):
                    ok_cl, msg_cl = close_trade(
                        user_id,
                        close_id,
                        float(close_price),
                        exit_fee=float(close_fee),
                        exit_note=close_note,
                    )
                    if ok_cl:
                        log_event(user_id, "trade_closed", {"trade_id": close_id})
                        st.success(msg_cl)
                    else:
                        st.error(msg_cl)

        st.divider()
        with st.expander("Bộ lọc cơ hội tự động", expanded=False):
            st.markdown("### Top 5 theo profile")
            run_top5 = st.button("Tính Top 5", width="stretch")
            if run_top5:
                top5 = compute_top5_for_profile(tuple((watchlist or WATCHLIST_DEFAULT)[:12]), profile)
                st.session_state["top5_df"] = top5
            if "top5_df" in st.session_state:
                st.dataframe(st.session_state["top5_df"], width="stretch", hide_index=True)
            else:
                st.caption("Chưa chạy Top 5. Bấm **Tính Top 5** khi cần.")
            min_avg_volume_20 = float(
                st.number_input("Thanh khoản tối thiểu (Avg Vol20)", min_value=0.0, value=300_000.0, step=100_000.0)
            )
            universe_limit = int(st.slider("Số mã quét từ universe", min_value=10, max_value=100, value=40, step=5))
            subtype_labels = st.multiselect(
                "Lọc theo tiểu ngành",
                options=list(SUBTYPE_LABEL_TO_ID.keys()),
                default=[],
                help="Để trống = quét toàn bộ tiểu ngành trong universe.",
            )
            run_scan = st.button("🔎 Quét cơ hội tự động", width="stretch")

    if prewarm_now:
        with st.spinner("Đang làm nóng cache dữ liệu cho watchlist..."):
            effective_quick_mode = bool(quick_mode)
            prewarm_watchlist_cache(
                watchlist or WATCHLIST_DEFAULT,
                profile,
                float(total_capital_vnd),
                effective_quick_mode,
            )
        st.success("Đã làm nóng xong cache. Lần phân tích kế tiếp sẽ nhanh hơn.")

    if prewarm_now_async and not bool(st.session_state.get("bg_warm_job_running", False)):
        ex = st.session_state.get("bg_warm_executor")
        if ex is None:
            ex = ThreadPoolExecutor(max_workers=1)
            st.session_state["bg_warm_executor"] = ex
        fut = ex.submit(
            _run_bg_warm_job,
            (watchlist or WATCHLIST_DEFAULT),
            int(bg_universe_limit),
            10,
            False,
        )
        st.session_state["bg_warm_future"] = fut
        st.session_state["bg_warm_job_running"] = True
        st.session_state["bg_warm_job_status"] = "running"
        st.info("Đã khởi chạy warm nền. Bạn có thể tiếp tục thao tác, kết quả sẽ cập nhật tự động.")

    fut = st.session_state.get("bg_warm_future")
    if fut is not None and bool(st.session_state.get("bg_warm_job_running", False)):
        if fut.done():
            st.session_state["bg_warm_job_running"] = False
            try:
                stat = fut.result()
                st.session_state["bg_warm_last_stat"] = stat
                st.session_state["bg_warm_job_status"] = "done"
                st.session_state["bg_warm_last_ts"] = time.time()
            except Exception as e:
                st.session_state["bg_warm_job_status"] = "error"
                st.session_state["bg_warm_job_error"] = str(e)
                st.session_state["bg_warm_last_ts"] = time.time()
        else:
            st.caption("Warm nền đang chạy...")

    if auto_bg_warm:
        now_ts = time.time()
        interval_s = max(180, int(bg_interval_min) * 60)
        last_ts = float(st.session_state.get("bg_warm_last_ts") or 0.0)
        if now_ts - last_ts >= interval_s:
            try:
                bj = run_background_cycle(
                    universe_limit=int(bg_universe_limit),
                    watchlist=(watchlist or WATCHLIST_DEFAULT)[:12],
                    queue_jobs=10,
                    warm_financial_live=False,
                )
                st.session_state["bg_warm_last_ts"] = now_ts
                st.session_state["bg_warm_last_stat"] = bj
            except Exception:
                st.session_state["bg_warm_last_ts"] = now_ts
        last_info = st.session_state.get("bg_warm_last_stat") or {}
        pre = last_info.get("prefetch") or {}
        if pre:
            st.caption(
                f"Auto warm: ok={pre.get('ok', 0)}/{pre.get('total', 0)} "
                f"(chu kỳ {int(bg_interval_min)} phút, universe {int(bg_universe_limit)} mã)."
            )
    status = str(st.session_state.get("bg_warm_job_status") or "")
    if status == "done":
        st.caption("Warm now (non-blocking): hoàn tất.")
    elif status == "error":
        st.warning(f"Warm now (non-blocking) lỗi: {st.session_state.get('bg_warm_job_error')}")

    effective_quick_mode = bool(quick_mode)
    if has_auth_user(user_id) and not bool(st.session_state.get("auth_ok", False)):
        st.warning("🔒 Vui lòng đăng nhập bằng PIN để sử dụng đầy đủ tính năng phân tích/giao dịch.")
        st.stop()

    # Quick watchlist board: show available prices first
    watch_rows = []
    for sym in (watchlist or WATCHLIST_DEFAULT):
        snap = load_snapshot_cached(sym)
        if snap is None:
            watch_rows.append({"Mã": sym, "Giá hiện tại": None, "Nguồn giá": "N/A", "Trạng thái": "Chưa có dữ liệu"})
            continue
        watch_rows.append(
            {
                "Mã": sym,
                "Giá hiện tại": float(snap.get("price") or 0),
                "Nguồn giá": str(snap.get("price_source") or snap.get("source") or "unknown"),
                "Trạng thái": "Sẵn sàng",
            }
        )
    watch_df = pd.DataFrame(watch_rows)
    watch_df["__ready"] = (watch_df["Trạng thái"] == "Sẵn sàng").astype(int)
    watch_df = watch_df.sort_values(["__ready", "Mã"], ascending=[False, True]).drop(columns=["__ready"])
    with st.container():
        st.markdown("### 💹 Bảng giá theo dõi nhanh")
        st.dataframe(watch_df, width="stretch", hide_index=True)

    st.markdown("### 📌 Daily Action List (hôm nay nên làm gì)")
    if st.button("Tải Daily Action List", key="load_daily_action_list_btn", width="content"):
        held_syms = tuple(str(x.get("symbol") or "").upper() for x in list_holdings(user_id))
        daily_universe = tuple(list_universe_symbols(limit=min(60, int(plan["scan_limit"]))))
        with st.spinner("Đang dựng Daily Action List (chạy nền dữ liệu)..."):
            daily_actions = build_daily_action_list(
                daily_universe,
                profile,
                float(total_capital_vnd),
                held_syms,
                quick_mode=effective_quick_mode,
                llm_live=bool(plan["llm_live"]),
            )
        st.session_state["daily_actions_df"] = daily_actions
    daily_actions = st.session_state.get("daily_actions_df")
    if isinstance(daily_actions, pd.DataFrame):
        if daily_actions.empty:
            st.caption("Chưa đủ dữ liệu để dựng Daily Action List.")
        else:
            st.dataframe(daily_actions.head(12), width="stretch", hide_index=True)
            st.caption("Ưu tiên thực thi từ trên xuống: BUY -> HOLD -> WATCH -> SELL.")
    else:
        st.caption("Mặc định không tải khối này khi mở app để tối ưu tốc độ. Bấm 'Tải Daily Action List' khi cần.")

    if autopilot_mode:
        if autopilot_refresh:
            load_autopilot_board.clear()
            build_autopilot_simple_view.clear()
            build_profit_focus_board.clear()
        autopilot_df = load_autopilot_board(profile, universe_limit=autopilot_limit, min_avg_volume_20=300_000.0)
        st.session_state["autopilot_df"] = autopilot_df
        st.markdown("### 🤖 Auto Pilot - Cơ hội ưu tiên hôm nay")
        if autopilot_df.empty:
            st.caption("Auto Pilot chưa tìm thấy cơ hội đủ dữ liệu ở lần quét này.")
        else:
            st.dataframe(autopilot_df, width="stretch", hide_index=True)
            top_symbol = str(autopilot_df.iloc[0].get("Mã") or "").upper()
            st.caption(f"Gợi ý mặc định: **{top_symbol}** (xếp hạng #1 từ Auto Pilot)")
            simple_symbols = tuple(str(x).upper() for x in autopilot_df["Mã"].head(5).tolist())
            simple_df = build_autopilot_simple_view(
                simple_symbols,
                profile,
                float(total_capital_vnd),
                quick_mode=effective_quick_mode,
                llm_live=bool(plan["llm_live"]),
            )
            st.markdown("#### ✅ Simple View (ra quyết định nhanh)")
            if simple_df.empty:
                st.caption("Chưa dựng được bảng Simple View ở lần quét này.")
            else:
                simple_view = _simple_view_for_plan(simple_df, plan_id)
                st.dataframe(simple_view, width="stretch", hide_index=True)
                if plan_id == "free":
                    st.info(
                        "🔒 Gói Free chỉ hiển thị bản rút gọn. Nâng Pro/Expert để mở full "
                        "Entry/SL/TP và hành động 1-click."
                    )
                    if st.button("🚀 Mở khóa Pro để xem full plan", width="stretch", key="cta_unlock_simple_view"):
                        log_event(user_id, "upgrade_cta_click", {"placement": "simple_view_paywall", "target_plan": "pro"})
                        st.success("Đã ghi nhận nhu cầu nâng cấp Pro.")
                cta_cols = st.columns([2, 1])
                with cta_cols[0]:
                    apply_symbol = st.selectbox(
                        "Chọn mã để áp dụng kế hoạch 1-click",
                        options=simple_df["Mã"].tolist(),
                        key="autopilot_apply_symbol",
                    )
                with cta_cols[1]:
                    apply_now = st.button(
                        "✅ Áp dụng kế hoạch 1-click",
                        width="stretch",
                        disabled=(plan_id == "free"),
                    )
                if apply_now:
                    sel = simple_df[simple_df["Mã"] == apply_symbol].head(1)
                    if sel.empty:
                        st.warning("Không tìm thấy mã đã chọn trong Simple View.")
                    else:
                        can_exec, msg_exec = can_auto_execute_symbol(user_id, str(apply_symbol).upper(), cooldown_hours=24)
                        if not can_exec:
                            st.warning(msg_exec)
                        else:
                            row = sel.iloc[0]
                            side = str(row.get("Hành động") or "WATCH").upper()
                            entry = float(row.get("Giá vào") or 0)
                            sl = float(row.get("SL") or 0)
                            tp = float(row.get("TP") or 0)
                            if entry <= 0 or sl <= 0 or tp <= 0:
                                st.warning("Dữ liệu Entry/SL/TP chưa đủ để áp dụng 1-click.")
                            else:
                                ok_dec, msg_dec = add_decision(
                                    user_id,
                                    str(apply_symbol).upper(),
                                    "BUY" if side == "BUY" else "WATCH",
                                    entry,
                                    sl,
                                    tp,
                                    "1-click from Auto Pilot Simple View",
                                    30,
                                )
                                ok_a1, ok_a2 = False, False
                                if ok_dec:
                                    ok_check1, _ = can_use_feature(user_id, "alert", 1, plan_id=plan_id)
                                    if ok_check1:
                                        add_alert(
                                            user_id,
                                            str(apply_symbol).upper(),
                                            "price_below",
                                            sl,
                                            "1-click SL from Auto Pilot",
                                        )
                                        ok_a1 = True
                                    ok_check2, _ = can_use_feature(user_id, "alert", 1, plan_id=plan_id)
                                    if ok_check2:
                                        add_alert(
                                            user_id,
                                            str(apply_symbol).upper(),
                                            "price_above",
                                            tp,
                                            "1-click TP from Auto Pilot",
                                        )
                                        ok_a2 = True
                                    log_event(
                                        user_id,
                                        "autopilot_oneclick_applied",
                                        {
                                            "symbol": str(apply_symbol).upper(),
                                            "side": side,
                                            "decision_saved": True,
                                            "alert_sl": ok_a1,
                                            "alert_tp": ok_a2,
                                        },
                                    )
                                    st.success(
                                        f"Đã áp dụng kế hoạch cho {str(apply_symbol).upper()}: "
                                        f"decision=OK | alert SL={'OK' if ok_a1 else 'SKIP'} | "
                                        f"alert TP={'OK' if ok_a2 else 'SKIP'}."
                                    )
                                else:
                                    st.error(msg_dec)
                buy_df = simple_df[simple_df["Hành động"] == "BUY"].head(3).copy()
                if not buy_df.empty:
                    if st.button(
                        "🚀 Batch 1-click Top 3 BUY",
                        width="stretch",
                        disabled=(plan_id == "free"),
                    ):
                        batch_results: list[str] = []
                        for _, b in buy_df.iterrows():
                            sym = str(b.get("Mã") or "").upper()
                            can_exec, msg_exec = can_auto_execute_symbol(user_id, sym, cooldown_hours=24)
                            if not can_exec:
                                batch_results.append(f"{sym}: SKIP ({msg_exec})")
                                continue
                            entry = float(b.get("Giá vào") or 0)
                            sl = float(b.get("SL") or 0)
                            tp = float(b.get("TP") or 0)
                            if not sym or entry <= 0 or sl <= 0 or tp <= 0:
                                batch_results.append(f"{sym or 'N/A'}: SKIP (thiếu Entry/SL/TP)")
                                continue
                            ok_dec, msg_dec = add_decision(
                                user_id,
                                sym,
                                "BUY",
                                entry,
                                sl,
                                tp,
                                "Batch 1-click from Auto Pilot Simple View",
                                30,
                            )
                            if not ok_dec:
                                batch_results.append(f"{sym}: FAIL decision ({msg_dec})")
                                continue
                            ok_sl = ok_tp = False
                            can1, _ = can_use_feature(user_id, "alert", 1, plan_id=plan_id)
                            if can1:
                                add_alert(user_id, sym, "price_below", sl, "Batch 1-click SL from Auto Pilot")
                                ok_sl = True
                            can2, _ = can_use_feature(user_id, "alert", 1, plan_id=plan_id)
                            if can2:
                                add_alert(user_id, sym, "price_above", tp, "Batch 1-click TP from Auto Pilot")
                                ok_tp = True
                            batch_results.append(f"{sym}: decision=OK | SL={'OK' if ok_sl else 'SKIP'} | TP={'OK' if ok_tp else 'SKIP'}")
                        log_event(
                            user_id,
                            "autopilot_batch_oneclick_applied",
                            {"symbols": [str(x).upper() for x in buy_df["Mã"].tolist()], "count": int(len(buy_df))},
                        )
                        st.success("Batch 1-click hoàn tất cho Top BUY.")
                        for line in batch_results:
                            st.caption(f"- {line}")
                elif plan_id == "free":
                    st.caption("Nâng Pro để dùng chế độ batch execution khi có tín hiệu BUY.")
            if profit_focus_mode:
                profit_symbols = tuple(str(x).upper() for x in autopilot_df["Mã"].head(15).tolist())
                pf_df = build_profit_focus_board(
                    profit_symbols,
                    profile,
                    float(total_capital_vnd),
                    quick_mode=effective_quick_mode,
                    llm_live=bool(plan["llm_live"]),
                )
                st.markdown("#### 💎 Profit Focus Board (đạt chuẩn xuống tiền)")
                if pf_df.empty:
                    st.caption("Chưa có mã đạt đủ chuẩn Profit Focus ở lần quét này.")
                else:
                    st.dataframe(pf_df, width="stretch", hide_index=True)
                    if plan_id in ("pro", "expert"):
                        pf_top3 = pf_df.head(3).copy()
                        if not pf_top3.empty and st.button("🚀 1-click Execute Top 3 Profit Focus", width="stretch"):
                            results: list[str] = []
                            for _, row in pf_top3.iterrows():
                                sym = str(row.get("Mã") or "").upper()
                                can_exec, msg_exec = can_auto_execute_symbol(user_id, sym, cooldown_hours=24)
                                if not can_exec:
                                    results.append(f"{sym}: SKIP ({msg_exec})")
                                    continue
                                entry = float(row.get("Entry") or 0)
                                sl = float(row.get("SL") or 0)
                                tp = float(row.get("TP") or 0)
                                action = str(row.get("Hành động") or "WATCH").upper()
                                if not sym or entry <= 0 or sl <= 0 or tp <= 0:
                                    results.append(f"{sym or 'N/A'}: SKIP (thiếu Entry/SL/TP)")
                                    continue
                                ok_dec, msg_dec = add_decision(
                                    user_id,
                                    sym,
                                    "BUY" if action == "BUY" else "WATCH",
                                    entry,
                                    sl,
                                    tp,
                                    "1-click from Profit Focus Board",
                                    30,
                                )
                                if not ok_dec:
                                    results.append(f"{sym}: FAIL decision ({msg_dec})")
                                    continue
                                ok_sl = ok_tp = False
                                can1, _ = can_use_feature(user_id, "alert", 1, plan_id=plan_id)
                                if can1:
                                    add_alert(user_id, sym, "price_below", sl, "Profit Focus SL")
                                    ok_sl = True
                                can2, _ = can_use_feature(user_id, "alert", 1, plan_id=plan_id)
                                if can2:
                                    add_alert(user_id, sym, "price_above", tp, "Profit Focus TP")
                                    ok_tp = True
                                results.append(f"{sym}: decision=OK | SL={'OK' if ok_sl else 'SKIP'} | TP={'OK' if ok_tp else 'SKIP'}")
                            log_event(
                                user_id,
                                "profit_focus_batch_executed",
                                {"symbols": [str(x).upper() for x in pf_top3["Mã"].tolist()], "count": int(len(pf_top3))},
                            )
                            st.success("Đã execute Top 3 Profit Focus.")
                            for line in results:
                                st.caption(f"- {line}")
                    else:
                        st.info("🔒 1-click execute Profit Focus dành cho Pro/Expert.")
            if top_symbol and st.button("⚡ Dùng mã #1 để phân tích ngay", width="stretch"):
                ticker = top_symbol
                run_analysis = True

        st.markdown("### 🎯 Finder: Cổ phiếu cơ hội tăng trưởng trong 3 tháng tới")
        st.caption(
            "Quét universe, kết hợp lịch sử giá + thanh khoản + tài chính + định giá để ước lượng xác suất tăng trưởng giá trong 3 tháng tới."
        )
        opp_limit = int(st.slider("Số mã quét Finder tăng trưởng 3 tháng tới", min_value=20, max_value=200, value=60, step=10))
        subtype_map = universe_subtype_map()
        subtype_ids = sorted(set(str(x or "other").strip().lower() for x in subtype_map.values()))
        subtype_label_by_id = {v: k for k, v in SUBTYPE_LABEL_TO_ID.items()}
        subtype_options = ["Tất cả ngành con"] + [subtype_label_by_id.get(x, x) for x in subtype_ids]
        selected_subtype_labels = st.multiselect(
            "Lọc theo ngành con",
            options=subtype_options,
            default=["Tất cả ngành con"],
        )
        selected_subtypes: tuple[str, ...]
        if "Tất cả ngành con" in selected_subtype_labels or not selected_subtype_labels:
            selected_subtypes = tuple()
        else:
            selected_subtypes = tuple(
                SUBTYPE_LABEL_TO_ID.get(lbl, lbl).strip().lower() for lbl in selected_subtype_labels if lbl != "Tất cả ngành con"
            )
        min_avg_vol20 = float(
            st.number_input(
                "Thanh khoản tối thiểu (KL TB20)",
                min_value=0.0,
                value=300000.0,
                step=50000.0,
            )
        )
        min_data_quality = float(
            st.slider("Data Quality tối thiểu (%)", min_value=30, max_value=95, value=55, step=5)
        )
        min_catalyst_score = float(
            st.slider("Catalyst Score tối thiểu", min_value=0, max_value=100, value=70, step=5)
        )
        only_actionable = st.toggle(
            "Chỉ hiện mã actionable (CƠ HỘI CAO/THEO DÕI)",
            value=True,
            help="Ẩn các mã THẬN TRỌNG để tập trung danh sách hành động.",
        )
        dynamic_sector = st.toggle(
            "Hiệu chỉnh ngành động theo dữ liệu",
            value=True,
            help="Tự cân bằng trọng số kỹ thuật/cơ bản theo hiệu quả lịch sử gần đây của từng ngành.",
        )
        bm_ret_3m, bm_label = load_benchmark_return_3m()
        if bm_ret_3m is not None:
            st.caption(f"Benchmark 3M: {bm_ret_3m:.2f}% ({bm_label or 'market proxy'})")
        else:
            st.caption("Benchmark 3M: chưa khả dụng ở lần tải này.")
        opp_symbols = tuple(list_universe_symbols(limit=opp_limit))
        opportunity_df = build_opportunity_3m_board(
            opp_symbols,
            profile,
            float(total_capital_vnd),
            quick_mode=effective_quick_mode,
            llm_live=bool(plan["llm_live"]),
            allowed_subtypes=selected_subtypes,
            min_avg_volume_20=min_avg_vol20,
            benchmark_return_3m_pct=bm_ret_3m,
            min_data_quality_pct=min_data_quality,
            min_catalyst_score=min_catalyst_score,
            only_actionable=only_actionable,
            dynamic_sector_calibration=dynamic_sector,
            top_n=25,
        )
        if opportunity_df.empty:
            st.caption("Chưa đủ dữ liệu để dựng Finder cổ phiếu cơ hội tăng trưởng trong 3 tháng tới.")
        else:
            ict_tz = timezone(timedelta(hours=7), "ICT")
            export_dt = datetime.now(ict_tz)
            export_ts = export_dt.strftime("%Y%m%d_%H%M%S")
            export_tag = f"{export_ts}_ICT"
            run_seed = (
                f"{export_tag}|{profile}|"
                f"{','.join(str(x) for x in opportunity_df.get('Mã', pd.Series(dtype=str)).astype(str).tolist())}"
            )
            run_id = hashlib.sha1(run_seed.encode("utf-8")).hexdigest()[:10]
            st.caption(f"Run ID: `{run_id}` | Export time: {export_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            c_opp1, c_opp2, c_opp3 = st.columns(3)
            with c_opp1:
                st.metric("Mã actionable", int(len(opportunity_df)))
            with c_opp2:
                st.metric("Xác suất tăng TB", f"{float(opportunity_df['Xác suất tăng 3M %'].mean()):.1f}%")
            with c_opp3:
                st.metric("Data Quality TB", f"{float(opportunity_df['Data Quality %'].mean()):.1f}%")
            st.dataframe(opportunity_df, width="stretch", hide_index=True)
            with st.expander("Sector Calibration Report", expanded=False):
                if "Ngành con" in opportunity_df.columns:
                    sector_df = (
                        opportunity_df.groupby("Ngành con", dropna=False)
                        .agg(
                            so_ma=("Mã", "count"),
                            diem_hieu_chinh_tb=("Điểm cơ hội (hiệu chỉnh ngành)", "mean"),
                            xs_tang_tb=("Xác suất tăng 3M %", "mean"),
                            data_quality_tb=("Data Quality %", "mean"),
                        )
                        .reset_index()
                    )
                    if "Hệ số ngành (KT/CB)" in opportunity_df.columns:
                        mult_map = (
                            opportunity_df[["Ngành con", "Hệ số ngành (KT/CB)"]]
                            .dropna()
                            .drop_duplicates(subset=["Ngành con"])
                            .set_index("Ngành con")["Hệ số ngành (KT/CB)"]
                            .to_dict()
                        )
                        sector_df["Hệ số KT/CB"] = sector_df["Ngành con"].map(mult_map)
                    sector_df = sector_df.rename(
                        columns={
                            "Ngành con": "Ngành",
                            "so_ma": "Số mã",
                            "diem_hieu_chinh_tb": "Điểm hiệu chỉnh TB",
                            "xs_tang_tb": "Xác suất tăng TB %",
                            "data_quality_tb": "Data Quality TB %",
                        }
                    )
                    for col in ("Điểm hiệu chỉnh TB", "Xác suất tăng TB %", "Data Quality TB %"):
                        if col in sector_df.columns:
                            sector_df[col] = sector_df[col].map(lambda x: round(float(x), 2))
                    sector_df = sector_df.sort_values(
                        ["Điểm hiệu chỉnh TB", "Xác suất tăng TB %", "Data Quality TB %"],
                        ascending=[False, False, False],
                    )
                    st.dataframe(sector_df, width="stretch", hide_index=True)
                    sector_csv = sector_df.to_csv(index=False).encode("utf-8-sig")
                    st.download_button(
                        "⬇️ Export Sector Report CSV",
                        data=sector_csv,
                        file_name=f"sector_calibration_report_{export_tag}_{run_id}.csv",
                        mime="text/csv",
                        width="content",
                    )
                    sector_json_payload = {
                        "exported_at": export_dt.isoformat(),
                        "timezone": "ICT",
                        "run_id": run_id,
                        "rows": json.loads(sector_df.to_json(orient="records", force_ascii=False)),
                    }
                    sector_json = json.dumps(sector_json_payload, ensure_ascii=False).encode("utf-8")
                    st.download_button(
                        "⬇️ Export Sector Report JSON",
                        data=sector_json,
                        file_name=f"sector_calibration_report_{export_tag}_{run_id}.json",
                        mime="application/json",
                        width="content",
                    )
                    if "Ghi chú hiệu chỉnh ngành" in opportunity_df.columns:
                        note_df = (
                            opportunity_df[["Ngành con", "Ghi chú hiệu chỉnh ngành"]]
                            .drop_duplicates(subset=["Ngành con"])
                            .rename(columns={"Ngành con": "Ngành", "Ghi chú hiệu chỉnh ngành": "Ghi chú"})
                        )
                        st.dataframe(note_df, width="stretch", hide_index=True)
                        note_csv = note_df.to_csv(index=False).encode("utf-8-sig")
                        st.download_button(
                            "⬇️ Export Sector Notes CSV",
                            data=note_csv,
                            file_name=f"sector_calibration_notes_{export_tag}_{run_id}.csv",
                            mime="text/csv",
                            width="content",
                        )
                else:
                    st.caption("Chưa có dữ liệu ngành để dựng report.")
            with st.expander("Giải thích điểm số Finder", expanded=False):
                explain_cols = [
                    "Mã",
                    "Trạng thái",
                    "Catalyst Score",
                    "Catalyst Pass",
                    "Điểm kỹ thuật",
                    "Điểm cơ bản",
                    "Điểm cơ hội",
                    "Data Quality %",
                    "Xác suất tăng 3M %",
                    "Khuyến nghị theo profile",
                    "Lý do khuyến nghị",
                ]
                explain_df = opportunity_df[[c for c in explain_cols if c in opportunity_df.columns]].head(10).copy()
                st.dataframe(explain_df, width="stretch", hide_index=True)
                st.caption(
                    "Điểm cơ hội (hiệu chỉnh ngành) = Điểm kỹ thuật/Điểm cơ bản sau hiệu chỉnh theo ngành con. "
                    "Data Quality càng cao thì độ tin cậy khuyến nghị càng tốt."
                )
            csv_bytes = opportunity_df.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "⬇️ Export Finder CSV",
                data=csv_bytes,
                file_name=f"finder_co_phieu_co_hoi_tang_truong_3_thang_toi_{export_tag}_{run_id}.csv",
                mime="text/csv",
                width="content",
            )
            finder_json_payload = {
                "exported_at": export_dt.isoformat(),
                "timezone": "ICT",
                "run_id": run_id,
                "rows": json.loads(opportunity_df.to_json(orient="records", force_ascii=False)),
            }
            json_bytes = json.dumps(finder_json_payload, ensure_ascii=False).encode("utf-8")
            st.download_button(
                "⬇️ Export Finder JSON",
                data=json_bytes,
                file_name=f"finder_co_phieu_co_hoi_tang_truong_3_thang_toi_{export_tag}_{run_id}.json",
                mime="application/json",
                width="content",
            )
            manifest_payload = {
                "run_id": run_id,
                "exported_at": export_dt.isoformat(),
                "timezone": "ICT",
                "profile": profile,
                "version_stamp": {
                    "finder_model_version": "opportunity_finder_v5",
                    "app_schema_version": "finder_manifest_v2",
                    "dynamic_sector_calibration": bool(dynamic_sector),
                    "quick_mode": bool(effective_quick_mode),
                    "llm_live": bool(plan["llm_live"]),
                    "filters": {
                        "min_avg_volume_20": float(min_avg_vol20),
                        "min_data_quality_pct": float(min_data_quality),
                        "min_catalyst_score": float(min_catalyst_score),
                        "only_actionable": bool(only_actionable),
                        "universe_limit": int(opp_limit),
                    },
                },
                "files": {
                    "finder_csv": f"finder_co_phieu_co_hoi_tang_truong_3_thang_toi_{export_tag}_{run_id}.csv",
                    "finder_json": f"finder_co_phieu_co_hoi_tang_truong_3_thang_toi_{export_tag}_{run_id}.json",
                    "sector_report_csv": f"sector_calibration_report_{export_tag}_{run_id}.csv",
                    "sector_report_json": f"sector_calibration_report_{export_tag}_{run_id}.json",
                    "sector_notes_csv": f"sector_calibration_notes_{export_tag}_{run_id}.csv",
                },
                "checksums": {
                    "finder_csv_sha1": hashlib.sha1(csv_bytes).hexdigest(),
                    "finder_json_sha1": hashlib.sha1(json_bytes).hexdigest(),
                    "rows": int(len(opportunity_df)),
                },
            }
            manifest_bytes = json.dumps(manifest_payload, ensure_ascii=False, indent=2).encode("utf-8")
            st.download_button(
                "⬇️ Export Session Manifest JSON",
                data=manifest_bytes,
                file_name=f"finder_manifest_{export_tag}_{run_id}.json",
                mime="application/json",
                width="content",
            )

    reminders_now = overdue_action_reminders(user_id)
    if reminders_now:
        with st.container():
            st.markdown("### 🔔 Action Reminders")
            for rm in reminders_now:
                sev = str(rm.get("severity") or "medium")
                if sev == "high":
                    st.warning(f"{rm.get('title')}: {rm.get('message')}")
                else:
                    st.info(f"{rm.get('title')}: {rm.get('message')}")
            if st.button("📨 Gửi nhắc việc qua kênh đã cấu hình", width="stretch"):
                text = "Nhắc việc quan trọng:\n" + "\n".join(
                    [f"- {x.get('title')}: {x.get('message')}" for x in reminders_now]
                )
                ds = dispatch_text_notifications(
                    text,
                    telegram_bot_token=tg_bot,
                    telegram_chat_id=tg_chat,
                    webhook_url=webhook_url,
                    email_to=email_to,
                    smtp_host=smtp_host,
                    smtp_port=smtp_port,
                    smtp_user=smtp_user,
                    smtp_password=smtp_password,
                    smtp_from=smtp_from,
                )
                log_event(user_id, "reminder_dispatched", ds)
                st.caption(f"Dispatch: sent={ds['sent']} | failed={ds['failed']}")

    if run_scan:
        capped_limit = min(universe_limit, int(plan["scan_limit"]))
        universe_all = list_universe_symbols(limit=capped_limit)
        if capped_limit < universe_limit:
            st.info(f"Gói {plan_label} giới hạn quét {capped_limit} mã. Nâng cấp gói để quét nhiều hơn.")
        selected_subtype_ids = {SUBTYPE_LABEL_TO_ID[x] for x in subtype_labels}
        if selected_subtype_ids:
            s_map = universe_subtype_map()
            universe = tuple([s for s in universe_all if s_map.get(s, "other") in selected_subtype_ids])
        else:
            universe = tuple(universe_all)
        scan_allowed = True
        if not universe:
            st.warning("Không có mã nào khớp bộ lọc tiểu ngành đã chọn.")
            candidates = pd.DataFrame()
            scan_allowed = False
        else:
            ok_scan, msg_scan = can_use_feature(user_id, "scan", len(universe), plan_id=plan_id)
            if not ok_scan:
                st.error(msg_scan)
                candidates = pd.DataFrame()
                scan_allowed = False
            else:
                log_event(user_id, "scan_started", {"symbols": len(universe), "profile": profile})
                with st.spinner(f"Đang quét {len(universe)} mã..."):
                    candidates = scan_potential_stocks(universe, profile, min_avg_volume_20)
        relaxed_used = False
        if scan_allowed and candidates.empty and min_avg_volume_20 > 0:
            with st.spinner("Lần quét đầu rỗng, đang thử lại với bộ lọc thanh khoản nới lỏng..."):
                candidates = scan_potential_stocks(universe, profile, 0.0)
            relaxed_used = True
        st.session_state["scan_candidates"] = candidates
        st.session_state["scan_portfolios"] = build_portfolio_options(candidates, total_capital_vnd)
        st.session_state["scan_profile"] = profile
        st.session_state["scan_capital"] = float(total_capital_vnd)
        st.session_state["scan_relaxed_used"] = relaxed_used
        st.session_state["scan_filter_min_vol"] = float(min_avg_volume_20)

    prompt = st.chat_input("Nhập mã hoặc yêu cầu nhanh (VD: phân tích VCB)")
    if prompt:
        t2 = extract_ticker(prompt)
        if t2:
            st.session_state["chat_ticker"] = t2
        else:
            st.info("Chat hiện dùng để nhận diện mã cổ phiếu. Ví dụ: `FPT` hoặc `phân tích VCB`.")
    if st.session_state.get("chat_ticker"):
        ticker = st.session_state["chat_ticker"]

    if (
        not run_analysis
        and "report_cache" in st.session_state
        and st.session_state.get("report_ticker") == ticker
        and st.session_state.get("report_profile") == profile
        and float(st.session_state.get("report_capital", 0)) == float(total_capital_vnd)
    ):
        report = st.session_state["report_cache"]
        ohlcv = st.session_state.get("ohlcv_cache", pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"]))
        valuation = report.get("valuation", {})
        report.setdefault("latency_seconds", 0.0)
        report = calibrate_report_forecast_with_history(user_id, ticker, report)
        mode_cached = report.setdefault("task_mode_used", recommend_task_mode(user_id, ticker, effective_quick_mode))
        report, mode_cached = apply_drift_guardrails(user_id, ticker, report, mode_cached)
        report["task_mode_used"] = mode_cached
    else:
        run_analysis = True

    if run_analysis:
        ok_analysis, msg_analysis = can_use_feature(user_id, "analysis", 1, plan_id=plan_id)
        if not ok_analysis:
            st.error(msg_analysis + " Nâng cấp gói để tăng hạn mức.")
            st.stop()
        record_usage(user_id, "analysis", 1)
        log_event(user_id, "analysis_started", {"ticker": ticker, "plan": plan_id, "quick_mode": effective_quick_mode})
        started = time.time()
        with st.status(f"🤖 Đang phân tích {ticker}...", expanded=True) as status:
            status.write("Đang quét BCTC và định giá...")
            snapshot = load_snapshot_cached(ticker)
            if snapshot is None:
                st.error(f"Không lấy được snapshot cho `{ticker}`.")
                st.stop()

            valuation = value_investing_summary(snapshot)
            report = {}
            ohlcv = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
            try:
                status.write("Đang phân tích kỹ thuật & tin tức...")
                task_mode = recommend_task_mode(user_id, ticker, effective_quick_mode)
                report = load_strategic_report_cached(
                    ticker,
                    snapshot,
                    profile,
                    total_capital_vnd,
                    quick_mode=effective_quick_mode,
                    llm_live=bool(plan["llm_live"]),
                    task_mode=task_mode,
                )
                report["task_mode_used"] = task_mode
                report, guarded_mode = apply_drift_guardrails(user_id, ticker, report, task_mode)
                report["task_mode_used"] = guarded_mode
                try:
                    ohlcv = load_ohlcv_cached(ticker)
                except PortalDataError:
                    ohlcv = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
            except (AILogicError, PortalDataError, ValueError) as e:
                st.warning(f"Không tải đủ dữ liệu nâng cao cho `{ticker}`: {e}")
                report = {
                    "ticker": ticker,
                    "valuation": valuation,
                    "phase": {"phase": "neutral", "reason": "Thiếu dữ liệu", "metrics": {}},
                    "financials": {},
                    "news": [],
                    "whys_steps": [],
                    "analysis_text": "Chưa đủ dữ liệu để dựng phân tích 7 Whys.",
                    "buy_zone": {},
                    "take_profit": "N/A",
                    "stop_loss": "N/A",
                    "llm_used": False,
                    "llm_provider": "fallback_error",
                }
            status.write("Đang áp dụng 7 Whys và quản trị rủi ro...")
            report = calibrate_report_forecast_with_history(user_id, ticker, report)
            st.session_state["report_ticker"] = ticker
            st.session_state["report_profile"] = profile
            st.session_state["report_capital"] = float(total_capital_vnd)
            st.session_state["report_cache"] = report
            st.session_state["ohlcv_cache"] = ohlcv
            status.update(label=f"✅ Hoàn tất phân tích {ticker}", state="complete")
        report["latency_seconds"] = round(time.time() - started, 2)
        log_event(
            user_id,
            "analysis_completed",
            {"ticker": ticker, "latency_s": report.get("latency_seconds"), "action": report.get("final_action")},
        )
        record_forecast_snapshot(user_id, ticker, report)

    val = report.get("valuation", {})
    phase_q = report.get("phase", {}) or {}
    phase_metrics_q = phase_q.get("metrics", {}) or {}
    action_q_raw = str(report.get("final_action") or "HOLD").upper()
    action_q = "SELL" if action_q_raw == "AVOID" else action_q_raw
    conf_q = float(report.get("confidence_score") or 0)
    price_q = float(val.get("price") or 0)
    intrinsic_q = float(val.get("composite_target_price") or val.get("intrinsic_value_graham") or 0)
    mos_q = float(val.get("margin_of_safety_composite_pct") or 0)
    vol_q = float(phase_metrics_q.get("vol_multiple") or 0)
    phase_name_q = str(phase_q.get("phase") or "neutral").upper()
    action_color = "#00f4b0" if action_q == "BUY" else ("#fbac20" if action_q == "HOLD" else "#ff3747")
    action_class = "status-buy" if action_q == "BUY" else ("status-hold" if action_q == "HOLD" else "status-watch")

    st.markdown("### ⚡ Bảng điều khiển 3 giây")
    q1, q2, q3, q4 = st.columns(4)
    with q1:
        st.markdown(
            f"""
            <div class="quick-card {action_class}">
              <div class="quick-title">Mã đang phân tích</div>
              <div class="quick-value">{ticker}</div>
              <div class="quick-pill" style="color:{action_color}; border-color:{action_color};">{action_q} · {conf_q:.0f}%</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with q2:
        st.markdown(
            f"""
            <div class="quick-card">
              <div class="quick-title">Giá hiện tại / Giá trị</div>
              <div class="quick-value">{price_q:,.0f}</div>
              <div class="quick-sub">Target: {intrinsic_q:,.0f} · MOS: {mos_q:.1f}%</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with q3:
        st.markdown(
            f"""
            <div class="quick-card">
              <div class="quick-title">Pha thị trường</div>
              <div class="quick-value">{phase_name_q}</div>
              <div class="quick-sub">Volume multiple: {vol_q:.2f}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with q4:
        st.markdown(
            """
            <div class="quick-card">
              <div class="quick-title">Hành động nhanh</div>
              <div class="quick-sub">1) Xem tab Định giá</div>
              <div class="quick-sub">2) Kiểm tra Risk Box</div>
              <div class="quick-sub">3) Bấm nút cảnh báo SL/TP</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    render_main_metrics(report)
    tab1, tab2, tab3, tab4 = st.tabs(
        ["🎯 Cần làm gì ngay", "📈 Biểu đồ", "🧠 AI phân tích", "📊 Tài chính & Danh mục"]
    )

    with tab1:
        st.markdown("#### ✅ Quyết định nhanh")
        render_next_best_action_cta(user_id, ticker, report, plan_id)
        render_risk_box(report, profile)
        render_action_explanation(report)
        render_macro_micro_outlook(report)
        render_probabilistic_forecast(report)
        render_forecast_accuracy(user_id, ticker)
        render_forecast_benchmark(user_id, ticker)
        render_model_drift_panel(user_id, ticker)
        render_forecast_portfolio_dashboard(user_id)
        with st.expander("Bảng xếp hạng dự báo", expanded=False):
            render_forecast_leaderboard(user_id)
        with st.expander("Gợi ý mode theo ngành", expanded=False):
            render_sector_mode_suggestions(user_id)
        c_rep1, c_rep2 = st.columns(2)
        with c_rep1:
            if st.button("Xuất report forecast health", width="stretch"):
                out_path = export_forecast_health_report(user_id)
                st.success(f"Đã xuất report: {out_path}")
                log_event(user_id, "forecast_health_exported", {"path": out_path, "ticker": ticker})
        with c_rep2:
            if st.button("Đánh giá drift kéo dài", width="stretch"):
                ds = get_forecast_drift_streak(user_id, ticker, checks=3)
                st.info(f"Drift streak hiện tại: {int(ds.get('drift_down_streak') or 0)}/{int(ds.get('checks') or 0)}")
        st.caption(f"Chế độ AI tự chọn cho mã này: `{report.get('task_mode_used', 'balanced')}`")
        dg = report.get("drift_guardrails") or {}
        if dg.get("applied"):
            st.warning(
                "Đã kích hoạt drift guardrails: ép mode `quality` và giảm tỷ trọng vị thế "
                f"({float(dg.get('max_position_pct_before') or 0):.2f}% -> {float(dg.get('max_position_pct_after') or 0):.2f}%)."
            )
        st.info(_profile_match_note(profile, report))
        rp = report.get("risk_plan") or {}
        default_sl = float(rp.get("stop_loss_price") or 0)
        default_tp = float(rp.get("take_profit_price") or 0)
        c_alert1, c_alert2 = st.columns(2)
        with c_alert1:
            if st.button("Tạo cảnh báo thủng SL", width="stretch"):
                ok_alert, msg_alert = can_use_feature(user_id, "alert", 1, plan_id=plan_id)
                if not ok_alert:
                    st.error(msg_alert)
                elif default_sl > 0:
                    add_alert(user_id, ticker, "price_below", default_sl, "Auto from risk plan (SL)")
                    log_event(user_id, "alert_created", {"symbol": ticker, "type": "price_below", "auto": True})
                    st.success(f"Đã tạo alert SL cho {ticker} tại {default_sl:,.2f}.")
                else:
                    st.warning("Không có mức SL hợp lệ để tạo alert.")
        with c_alert2:
            if st.button("Tạo cảnh báo chạm TP", width="stretch"):
                ok_alert, msg_alert = can_use_feature(user_id, "alert", 1, plan_id=plan_id)
                if not ok_alert:
                    st.error(msg_alert)
                elif default_tp > 0:
                    add_alert(user_id, ticker, "price_above", default_tp, "Auto from risk plan (TP)")
                    log_event(user_id, "alert_created", {"symbol": ticker, "type": "price_above", "auto": True})
                    st.success(f"Đã tạo alert TP cho {ticker} tại {default_tp:,.2f}.")
                else:
                    st.warning("Không có mức TP hợp lệ để tạo alert.")

        with st.expander("📌 Phân tích bổ sung (mở khi cần)", expanded=False):
            render_aha_and_value(user_id)
            render_health_cards(report)
            render_readiness_checklist(report, ohlcv)

        with st.expander("📋 Kế hoạch vốn chi tiết", expanded=False):
            render_risk_plan(report)
            plan_now = report.get("risk_plan") or {}
            phase_now = (report.get("phase") or {}).get("metrics") or {}
            sizing_now = adaptive_position_sizing(
                user_id,
                base_max_position_pct=float(plan_now.get("max_position_pct") or 20.0),
                confidence_score=float(report.get("confidence_score") or 0),
                gate_passed=bool(report.get("gate_passed")),
                vol_multiple=float(phase_now.get("vol_multiple") or 1.0),
                coach_verdict="GO" if str(report.get("final_action") or "") == "BUY" else "CAUTION",
            )
            st.caption(
                f"🧠 Adaptive Position Sizing: {sizing_now['suggested_position_pct']}% "
                f"({sizing_now['risk_bucket']}) | Discipline {sizing_now['discipline_score']}"
            )
            st.caption(sizing_now["reason"])

        with st.expander("🎯 Action Center (danh sách cơ hội)", expanded=False):
            action_universe = tuple((watchlist or WATCHLIST_DEFAULT)[: min(12, len(watchlist or WATCHLIST_DEFAULT))])
            action_df = build_action_center(action_universe, profile, float(total_capital_vnd), top_n=6)
            if action_df.empty:
                st.caption("Chưa đủ dữ liệu để dựng Action Center.")
            else:
                st.dataframe(action_df, width="stretch", hide_index=True)
                log_event(user_id, "action_center_viewed", {"rows": int(len(action_df))})

        if "scan_candidates" in st.session_state:
            with st.expander("✅ Kết quả quét mở rộng", expanded=False):
                final_df = build_final_opportunity_table(st.session_state["scan_candidates"], discount_threshold_pct=20.0)
                if final_df.empty:
                    st.info("Chưa có mã nào đạt tiêu chí định giá rẻ >=20% trong lần quét gần nhất.")
                else:
                    st.dataframe(final_df, width="stretch", hide_index=True)
                    with st.expander("Mở rộng dẫn chứng từng mã"):
                        for _, row in final_df.iterrows():
                            sym = str(row["Mã"])
                            st.markdown(f"**{sym}** · {row['Thông điệp cuối cùng']}")
                            snap_sym = load_snapshot_cached(sym)
                            if snap_sym is None:
                                st.caption("Không lấy được snapshot.")
                                continue
                            rep_sym = load_strategic_report_cached(
                                sym,
                                snap_sym,
                                profile,
                                total_capital_vnd,
                                quick_mode=effective_quick_mode,
                                llm_live=bool(plan["llm_live"]),
                            )
                            st.caption(
                                f"MOS: {float(rep_sym.get('valuation', {}).get('margin_of_safety_composite_pct') or 0):.1f}% | "
                                f"Pha: {rep_sym.get('phase', {}).get('phase', 'neutral')} | "
                                f"F-Score: {rep_sym.get('valuation', {}).get('piotroski_score', 0)}/9"
                            )
        render_report_downloads(report)

    with tab2:
        c1, c2 = st.columns([2, 1])
        with c1:
            ohlcv_to_plot = ohlcv
            show_mos_zone = st.toggle("Hiển thị vùng MOS >= 20%", value=True)
            show_ma200 = st.toggle("Hiển thị MA200", value=True)
            show_rsi = st.toggle("Hiển thị RSI(14)", value=True)
            if ohlcv_to_plot.empty:
                try:
                    # Retry direct fetch for chart so user still gets technical view.
                    ohlcv_to_plot = fetch_ohlcv_history(ticker, sessions=120)
                except Exception:
                    ohlcv_to_plot = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
            if ohlcv_to_plot.empty:
                ohlcv_to_plot = fetch_ohlcv_yfinance_fallback(ticker, sessions=240)
            if not ohlcv_to_plot.empty:
                render_candlestick_with_intrinsic(
                    ohlcv_to_plot,
                    report.get("valuation", valuation),
                    show_mos_zone=show_mos_zone,
                    show_ma200=show_ma200,
                    show_rsi=show_rsi,
                )
            else:
                st.warning("Chưa lấy được OHLCV để vẽ nến. Kiểm tra kết nối dữ liệu hoặc bấm phân tích lại.")
        with c2:
            phase = report.get("phase", {})
            st.metric("Pha hiện tại", str(phase.get("phase", "neutral")).upper())
            st.caption(phase.get("reason", ""))
            vm = float((phase.get("metrics") or {}).get("vol_multiple") or 0)
            st.progress(min(max(vm / 2.0, 0.0), 1.0), text=f"Volume multiple: {vm:.2f}")

    with tab3:
        render_ai_chat_style(report)
        with st.expander("Theo dõi AI Health", expanded=False):
            render_ai_health_panel()

    with tab4:
        with st.expander("📌 Tóm tắt tài chính cốt lõi", expanded=True):
            render_financial_health_tab(report)
        with st.expander("🧾 Bằng chứng dữ liệu chi tiết", expanded=False):
            render_evidence(report)
        st.markdown("---")
        st.subheader("💼 Danh mục cá nhân")
        pf_rows = portfolio_snapshot(user_id)
        if pf_rows:
            pf_df = pd.DataFrame(pf_rows)
            st.dataframe(pf_df, width="stretch", hide_index=True)
            total_value = float(pf_df["Giá trị"].sum())
            total_pnl = float(pf_df["Lãi/Lỗ"].sum())
            st.caption(f"Tổng giá trị: {total_value:,.0f} VND | Lãi/Lỗ: {total_pnl:,.0f} VND")
        else:
            st.caption("Chưa có vị thế nào trong danh mục.")

        st.subheader("📒 Trade Journal - Realized")
        trades = list_trades(user_id, limit=200)
        if trades:
            tdf = pd.DataFrame(trades)
            st.dataframe(tdf, width="stretch", hide_index=True)
            rp = realized_performance(user_id, days=30)
            r1, r2, r3, r4 = st.columns(4)
            with r1:
                st.metric("Closed trades (30d)", int(rp.get("closed_trades") or 0))
            with r2:
                st.metric("Realized PnL (30d)", f"{float(rp.get('realized_pnl_total') or 0):,.0f}")
            with r3:
                st.metric("Realized % avg", f"{float(rp.get('realized_pct_avg') or 0):.2f}%")
            with r4:
                st.metric("Win rate realized", f"{float(rp.get('win_rate_pct') or 0):.1f}%")
            st.markdown("#### 🧪 Kế hoạch vs Thực thi")
            evp = execution_vs_plan_report(user_id, limit=200)
            if evp:
                evp_df = pd.DataFrame(evp)
                st.dataframe(evp_df, width="stretch", hide_index=True)
            else:
                st.caption("Chưa có trade nào được link với decision để so sánh.")
        else:
            st.caption("Chưa có trade journal nào.")

        fired = evaluate_alerts(user_id)
        st.subheader("🔔 Cảnh báo kích hoạt")
        if fired:
            st.dataframe(pd.DataFrame(fired), width="stretch", hide_index=True)
            if auto_notify:
                sent_stat = dispatch_alert_notifications(user_id, fired, tg_bot, tg_chat)
                ext_stat = dispatch_external_notifications(
                    fired,
                    webhook_url=webhook_url,
                    email_to=email_to,
                    smtp_host=smtp_host,
                    smtp_port=smtp_port,
                    smtp_user=smtp_user,
                    smtp_password=smtp_password,
                    smtp_from=smtp_from,
                )
                st.caption(
                    f"Telegram dispatch: sent={sent_stat['sent']} | "
                    f"skipped={sent_stat['skipped']} | failed={sent_stat['failed']}"
                )
                st.caption(f"Webhook/Email dispatch: sent={ext_stat['sent']} | failed={ext_stat['failed']}")
                log_event(user_id, "alert_dispatched", {"telegram": sent_stat, "external": ext_stat})
                if int(sent_stat.get("failed") or 0) > 0:
                    enqueue_notification(
                        user_id,
                        "alerts",
                        {
                            "fired_alerts": fired,
                            "telegram_bot_token": tg_bot,
                            "telegram_chat_id": tg_chat,
                        },
                        delay_seconds=60,
                    )
                    st.caption("Một số alert lỗi gửi đã được đưa vào hàng đợi retry.")
        else:
            st.caption("Chưa có cảnh báo nào kích hoạt tại thời điểm hiện tại.")

        st.subheader("📈 KPI sử dụng")
        kpi = get_kpi_dashboard(user_id, days=30)
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Tổng sự kiện", int(kpi.get("events_total") or 0))
        with c2:
            st.metric("Số cảnh báo", int(kpi.get("alerts_total") or 0))
        with c3:
            st.metric("Số vị thế", int(kpi.get("holdings_total") or 0))
        by_event = kpi.get("by_event") or {}
        if by_event:
            ev_df = pd.DataFrame([{"event": k, "count": v} for k, v in by_event.items()]).sort_values(
                "count", ascending=False
            )
            st.dataframe(ev_df, width="stretch", hide_index=True)

        cohort = get_cohort_kpi(user_id)
        st.subheader("📅 Cohort KPI (7/30 ngày)")
        cohort_df = pd.DataFrame(
            [
                {"Chỉ số": "Active days", "7 ngày": cohort["active_days_7"], "30 ngày": cohort["active_days_30"]},
                {"Chỉ số": "Analyses completed", "7 ngày": cohort["analysis_7"], "30 ngày": cohort["analysis_30"]},
                {"Chỉ số": "Alerts created", "7 ngày": cohort["alerts_created_7"], "30 ngày": cohort["alerts_created_30"]},
                {
                    "Chỉ số": "Alerts dispatched",
                    "7 ngày": cohort["alerts_dispatched_7"],
                    "30 ngày": cohort["alerts_dispatched_30"],
                },
                {"Chỉ số": "Upgrade intent", "7 ngày": cohort["upgrade_intent_7"], "30 ngày": cohort["upgrade_intent_30"]},
                {
                    "Chỉ số": "Upgrade success",
                    "7 ngày": cohort["upgrade_success_7"],
                    "30 ngày": cohort["upgrade_success_30"],
                },
            ]
        )
        st.dataframe(cohort_df, width="stretch", hide_index=True)

        st.subheader("🎯 Outcome Tracker (7/30/90)")
        dec_rows = evaluate_decisions(user_id, limit=120)
        if dec_rows:
            dec_df = pd.DataFrame(dec_rows)
            st.dataframe(dec_df, width="stretch", hide_index=True)
            # quick outcome summary
            closed = dec_df[dec_df["Trạng thái"].isin(["STOP_LOSS_HIT", "TAKE_PROFIT_HIT"])]
            tp_n = int((closed["Trạng thái"] == "TAKE_PROFIT_HIT").sum()) if not closed.empty else 0
            sl_n = int((closed["Trạng thái"] == "STOP_LOSS_HIT").sum()) if not closed.empty else 0
            win_rate = (tp_n / max(tp_n + sl_n, 1) * 100.0) if (tp_n + sl_n) > 0 else 0.0
            c_out1, c_out2, c_out3 = st.columns(3)
            with c_out1:
                st.metric("Quyết định đã lưu", len(dec_df))
            with c_out2:
                st.metric("TP hit / SL hit", f"{tp_n}/{sl_n}")
            with c_out3:
                st.metric("Win rate đã đóng", f"{win_rate:.1f}%")
            sc = decision_scorecard(user_id, limit=200)
            c_sc1, c_sc2, c_sc3, c_sc4 = st.columns(4)
            with c_sc1:
                st.metric("Discipline score", f"{float(sc.get('discipline_score') or 0):.1f}/100")
            with c_sc2:
                st.metric("RR trung bình", f"{float(sc.get('rr_avg') or 0):.2f}")
            with c_sc3:
                st.metric("P/L trung bình", f"{float(sc.get('pl_avg_pct') or 0):.2f}%")
            with c_sc4:
                st.metric("Win rate đóng", f"{float(sc.get('win_rate_closed_pct') or 0):.1f}%")

            pm7 = postmortem_report(user_id, days=7, limit=200)
            pm30 = postmortem_report(user_id, days=30, limit=200)
            st.markdown("#### 🧠 Auto Post-mortem")
            c_pm1, c_pm2 = st.columns(2)
            with c_pm1:
                st.caption("Post-mortem 7 ngày")
                if pm7:
                    st.dataframe(pd.DataFrame(pm7).head(10), width="stretch", hide_index=True)
                else:
                    st.caption("Chưa có quyết định đủ tuổi 7 ngày.")
            with c_pm2:
                st.caption("Post-mortem 30 ngày")
                if pm30:
                    st.dataframe(pd.DataFrame(pm30).head(10), width="stretch", hide_index=True)
                else:
                    st.caption("Chưa có quyết định đủ tuổi 30 ngày.")

            mv = monthly_value_report(user_id)
            st.markdown("#### 💎 Monthly Value Report")
            st.caption(str(mv.get("value_summary") or ""))
            mv_df = pd.DataFrame(
                [
                    {"Chỉ số": "Plan", "Giá trị": mv.get("plan_id")},
                    {"Chỉ số": "Analyses 30d", "Giá trị": mv.get("analysis_30d")},
                    {"Chỉ số": "Alerts dispatched 30d", "Giá trị": mv.get("alerts_dispatched_30d")},
                    {"Chỉ số": "Decisions logged", "Giá trị": mv.get("decisions_logged")},
                    {"Chỉ số": "Discipline score", "Giá trị": mv.get("discipline_score")},
                    {"Chỉ số": "Avg P/L %", "Giá trị": mv.get("avg_pl_pct")},
                    {"Chỉ số": "Win rate closed %", "Giá trị": mv.get("win_rate_closed_pct")},
                ]
            )
            st.dataframe(mv_df, width="stretch", hide_index=True)
        else:
            st.caption("Chưa có quyết định nào. Hãy lưu quyết định để app hậu kiểm giá trị thực.")

        if plan_id == "free":
            sup = smart_upgrade_prompt(user_id, plan_id)
            if bool(sup.get("show")):
                log_event(
                    user_id,
                    "upgrade_prompt_view",
                    {
                        "variant": sup.get("variant"),
                        "variant_source": sup.get("variant_source"),
                        "plan": sup.get("cta_plan"),
                        "roi_x": sup.get("roi_fee_x"),
                        "maturity": sup.get("maturity_score_10"),
                    },
                )
                st.info(f"💡 {sup.get('title')}\n\n{sup.get('message')}")
                cta1, cta2 = st.columns(2)
                with cta1:
                    if st.button("🚀 Nâng cấp ngay", width="stretch"):
                        log_event(
                            user_id,
                            "upgrade_cta_click",
                            {
                                "variant": sup.get("variant"),
                                "plan": sup.get("cta_plan"),
                                "roi_x": sup.get("roi_fee_x"),
                                "maturity": sup.get("maturity_score_10"),
                            },
                        )
                        st.success("Đã ghi nhận nhu cầu nâng cấp. Vào mục Billing để xác nhận.")
                with cta2:
                    if st.button("❓ Xem chi tiết giá trị", width="stretch"):
                        log_event(user_id, "upgrade_info_click", {"variant": sup.get("variant")})
                        st.caption(
                            f"ROI fee x: {sup.get('roi_fee_x')} | "
                            f"Maturity: {sup.get('maturity_score_10')}/10 | "
                            f"Analysis 30d: {sup.get('analysis_30d')}"
                        )

        st.subheader("🛜 SLA nguồn dữ liệu")
        sla_rows = get_source_sla_report()
        if sla_rows:
            st.dataframe(pd.DataFrame(sla_rows), width="stretch", hide_index=True)
        else:
            st.caption("Chưa có số liệu SLA nguồn.")
        render_5s_panel(report, sla_rows)

        st.subheader("📬 Daily Playbook")
        action_df_for_playbook = build_action_center(
            tuple((watchlist or WATCHLIST_DEFAULT)[: min(12, len(watchlist or WATCHLIST_DEFAULT))]),
            profile,
            float(total_capital_vnd),
            top_n=6,
        )
        playbook_text = build_daily_playbook_text(action_df_for_playbook, ticker, report)
        st.code(playbook_text)
        if st.button("Gửi Daily Playbook ngay", width="stretch"):
            ds = dispatch_text_notifications(
                playbook_text,
                telegram_bot_token=tg_bot,
                telegram_chat_id=tg_chat,
                webhook_url=webhook_url,
                email_to=email_to,
                smtp_host=smtp_host,
                smtp_port=smtp_port,
                smtp_user=smtp_user,
                smtp_password=smtp_password,
                smtp_from=smtp_from,
            )
            log_event(user_id, "playbook_dispatched", ds)
            st.caption(f"Dispatch: sent={ds['sent']} | failed={ds['failed']}")
            if int(ds.get("failed") or 0) > 0:
                enqueue_notification(
                    user_id,
                    "text",
                    {
                        "text": playbook_text,
                        "telegram_bot_token": tg_bot,
                        "telegram_chat_id": tg_chat,
                        "webhook_url": webhook_url,
                        "email_to": email_to,
                        "smtp_host": smtp_host,
                        "smtp_port": smtp_port,
                        "smtp_user": smtp_user,
                        "smtp_password": smtp_password,
                        "smtp_from": smtp_from,
                    },
                    delay_seconds=90,
                )
                st.caption("Playbook lỗi gửi đã được đưa vào hàng đợi retry.")
        if "scan_candidates" in st.session_state:
            st.markdown("---")
            st.subheader("🔎 Danh sách cổ phiếu tiềm năng tự động")
            candidates = st.session_state["scan_candidates"]
            if st.session_state.get("scan_relaxed_used"):
                st.info(
                    f"Không có mã đạt ngưỡng Avg Vol20 >= {int(st.session_state.get('scan_filter_min_vol', 0)):,}. "
                    "Hệ thống đã tự quét lại với bộ lọc nới lỏng để vẫn trả ra cơ hội đầu tư."
                )
            if candidates.empty:
                st.warning("Không tìm thấy mã phù hợp bộ lọc hiện tại.")
            else:
                st.caption("Các mã có trạng thái `Sẵn sàng` được ưu tiên hiển thị trước.")
                st.dataframe(candidates.head(20), width="stretch", hide_index=True)
                plans = st.session_state.get("scan_portfolios") or build_portfolio_options(candidates, total_capital_vnd)
                st.markdown("### 📦 Gợi ý phân bổ danh mục theo mức rủi ro")
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.caption("An toàn")
                    st.dataframe(plans["an_toan"], width="stretch", hide_index=True)
                with c2:
                    st.caption("Cân bằng")
                    st.dataframe(plans["can_bang"], width="stretch", hide_index=True)
                with c3:
                    st.caption("Mạo hiểm")
                    st.dataframe(plans["mao_hiem"], width="stretch", hide_index=True)

        st.markdown("---")
        st.subheader("🧑‍💼 Admin Growth KPI (30 ngày)")
        admin = get_admin_kpi(days=30)
        a1, a2, a3, a4 = st.columns(4)
        with a1:
            st.metric("Users", int(admin.get("users_total") or 0))
        with a2:
            st.metric("Active users (30d)", int(admin.get("active_users_period") or 0))
        with a3:
            st.metric("Events (30d)", int(admin.get("events_period") or 0))
        with a4:
            st.metric("Alerts total", int(admin.get("alerts_total") or 0))
        plans_df = pd.DataFrame(admin.get("plans") or [])
        if not plans_df.empty:
            st.caption("Phân bổ gói dịch vụ")
            st.dataframe(plans_df, width="stretch", hide_index=True)
        ev_df = pd.DataFrame(admin.get("events_by_type") or [])
        if not ev_df.empty:
            st.caption("Sự kiện theo loại (30 ngày)")
            st.dataframe(ev_df, width="stretch", hide_index=True)
        q_df = pd.DataFrame(admin.get("notification_queue") or [])
        if not q_df.empty:
            st.caption("Trạng thái hàng đợi notification")
            st.dataframe(q_df, width="stretch", hide_index=True)
        st.caption("Funnel nâng cấp theo variant (30 ngày)")
        uf = get_upgrade_funnel(days=30)
        if uf:
            st.dataframe(pd.DataFrame(uf), width="stretch", hide_index=True)
            pick = select_upgrade_variant_auto(days=30)
            st.caption(
                f"Auto-selected variant hiện tại: `{pick.get('variant')}` "
                f"(reason={pick.get('reason')}, score={pick.get('score', 'n/a')})"
            )
        else:
            st.caption("Chưa có dữ liệu funnel nâng cấp.")


if __name__ == "__main__":
    main()
