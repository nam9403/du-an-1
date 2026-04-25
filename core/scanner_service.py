from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import perf_counter

import pandas as pd
import streamlit as st

from core.analysis_runtime_service import load_ohlcv_cached, load_snapshot_cached, load_strategic_report_cached
from core.observability import log_timing
from scrapers.financial_data import list_universe_symbols


@st.cache_data(ttl=300)
def scan_potential_stocks(
    universe: tuple[str, ...],
    profile: str,
    min_avg_volume_20: float,
) -> pd.DataFrame:
    started_at = perf_counter()
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
        snap = load_snapshot_cached(sym)
        if snap is None:
            return {"Mã": sym, "Giá": None, "Trạng thái": "Không có snapshot"}
        try:
            rep = load_strategic_report_cached(sym, snap, profile, 100_000_000.0, quick_mode=True)
            try:
                ohlcv = load_ohlcv_cached(sym)
            except Exception:
                ohlcv = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        except Exception:
            return {"Mã": sym, "Giá": float(snap.get("price") or 0), "Trạng thái": "Lỗi phân tích"}
        avg_vol20 = (
            float(ohlcv.tail(20)["volume"].mean())
            if not ohlcv.empty and len(ohlcv) >= 20
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
        out = pd.DataFrame(
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
    else:
        df = pd.DataFrame(rows)
        if "Expected Return %" not in df.columns:
            df["Expected Return %"] = -1.0
        if "F-Score" not in df.columns:
            df["F-Score"] = -1
        if "Trạng thái" not in df.columns:
            df["Trạng thái"] = "Sẵn sàng"
        df["__ready"] = (df["Trạng thái"] == "Sẵn sàng").astype(int)
        out = (
            df.sort_values(["__ready", "Expected Return %", "F-Score"], ascending=[False, False, False])
            .drop(columns=["__ready"])
            .reset_index(drop=True)
        )
    ready_count = int((out["Trạng thái"] == "Sẵn sàng").sum()) if "Trạng thái" in out.columns else 0
    log_timing(
        "scanner.scan_potential_stocks",
        (perf_counter() - started_at) * 1000.0,
        symbols=len(universe),
        rows=len(out),
        ready=ready_count,
        profile=str(profile or ""),
        min_avg_volume_20=float(min_avg_volume_20),
    )
    return out


@st.cache_data(ttl=300)
def load_autopilot_board(profile: str, universe_limit: int = 30, min_avg_volume_20: float = 300_000.0) -> pd.DataFrame:
    started_at = perf_counter()
    universe = tuple(list_universe_symbols(limit=max(10, min(int(universe_limit or 30), 120))))
    df = scan_potential_stocks(universe, profile, float(min_avg_volume_20))
    if df.empty:
        out = pd.DataFrame(
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
    else:
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
        ranked = (
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
        out = ranked[[c for c in keep_cols if c in ranked.columns]]
    log_timing(
        "scanner.load_autopilot_board",
        (perf_counter() - started_at) * 1000.0,
        profile=str(profile or ""),
        universe_size=len(universe),
        out_rows=len(out),
    )
    return out


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
                "Giá vào": round(float(rp.get("entry_price") or 0), 2) if show_plan and float(rp.get("entry_price") or 0) > 0 else None,
                "SL": round(float(rp.get("stop_loss_price") or 0), 2) if show_plan and float(rp.get("stop_loss_price") or 0) > 0 else None,
                "TP": round(float(rp.get("take_profit_price") or 0), 2) if show_plan and float(rp.get("take_profit_price") or 0) > 0 else None,
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
    df = df.sort_values(["__r", "Confidence %", "MOS %"], ascending=[False, False, False]).drop(columns=["__r"]).reset_index(drop=True)
    return df


def simple_view_for_plan(df: pd.DataFrame, plan_id: str) -> pd.DataFrame:
    pid = str(plan_id or "free").strip().lower()
    if df.empty:
        return df
    if pid in ("pro", "expert"):
        return df
    cols = [c for c in ["Mã", "Hành động", "Giá hiện tại", "Giá trị nội tại", "Biên an toàn %", "Tỷ trọng %"] if c in df.columns]
    return df[cols].copy()
