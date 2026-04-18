"""
Chứng minh hiệu quả (backtest đơn giản): tín hiệu pha kỹ thuật vs benchmark thị trường VN.

Cảnh báo: mô phỏng học tập; không đảm bảo lợi nhuận tương lai.

Không so sánh trực tiếp với lợi nhuận chứng chỉ quỹ (CCQ): quỹ là danh mục/đòn bẩy/cơ cấu
khác; số % ở đây là rule pha trên **một mã** + có thời gian **ngồi tiền mặt** khi không có tín hiệu.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import numpy as np
import pandas as pd
import requests

from core.engine import compute_technical_indicators, detect_market_phase_from_ohlcv
from scrapers.finance_scraper import DEFAULT_HEADERS


def _yahoo_chart(symbol: str, range_str: str = "2y") -> pd.DataFrame:
    """Lấy OHLCV từ Yahoo chart API (tương tự portal)."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(symbol)}?interval=1d&range={range_str}"
    r = requests.get(url, headers=DEFAULT_HEADERS, timeout=25)
    r.raise_for_status()
    js = r.json()
    result = (((js.get("chart") or {}).get("result") or [None])[0]) or {}
    timestamps = result.get("timestamp") or []
    quote_arr = (((result.get("indicators") or {}).get("quote") or [None])[0]) or {}
    opens = quote_arr.get("open") or []
    highs = quote_arr.get("high") or []
    lows = quote_arr.get("low") or []
    closes = quote_arr.get("close") or []
    vols = quote_arr.get("volume") or []
    n = min(len(timestamps), len(opens), len(highs), len(lows), len(closes))
    rows: list[dict[str, Any]] = []
    for i in range(n):
        o, h, l, c = opens[i], highs[i], lows[i], closes[i]
        if o is None or h is None or l is None or c is None:
            continue
        dt = datetime.fromtimestamp(int(timestamps[i]), tz=timezone.utc).date().isoformat()
        v = vols[i] if i < len(vols) and vols[i] is not None else 0.0
        rows.append({"date": dt, "open": float(o), "high": float(h), "low": float(l), "close": float(c), "volume": float(v)})
    if len(rows) < 80:
        raise ValueError(f"Không đủ dữ liệu Yahoo cho {symbol}")
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)


def fetch_long_ohlcv_for_backtest(ticker: str, range_preference: str | None = None) -> tuple[pd.DataFrame, str]:
    """
    Yahoo chart: thử lần lượt các `range` cho đến khi đủ dữ liệu (>=80 phiên trong `_yahoo_chart`).

    Mặc định: **5y → max → 10y → 2y** — tránh lỗi cũ (luôn dừng ở 2y nên cửa sổ quá ngắn,
    dễ lệch so với lợi suất cả năm / CCQ). Truyền `range_preference` (vd. \"max\") để ép một range.
    Trả về (DataFrame, range đã dùng) để đồng bộ benchmark.
    """
    sym = (ticker or "").strip().upper()
    suffixes = (f"{sym}.VN", f"{sym}.HM", f"{sym}.HN")
    ranges = [range_preference] if range_preference else ["5y", "max", "10y", "2y"]
    last_err: Exception | None = None
    for rng in ranges:
        for ysym in suffixes:
            try:
                return _yahoo_chart(ysym, rng), rng
            except Exception as e:
                last_err = e
                continue
    raise ValueError(f"Không lấy được OHLCV dài cho {sym}: {last_err}")


def fetch_vn_benchmark_series(range_str: str = "max") -> tuple[pd.Series | None, str]:
    """
    Chuỗi giá đóng cửa benchmark (VN-Index / ETF / proxy).
    Trả về (series | None, label). None nếu không tải được — gọi run_full_backtest_for_ticker sẽ dùng mua&giữ cùng mã.
    """
    candidates = [
        ("^VNI", "Chỉ số ^VNI (Yahoo)"),
        ("^VNINDEX", "VN-Index (Yahoo ^VNINDEX)"),
        ("VNINDEX.VN", "VN-Index (Yahoo VNINDEX.VN)"),
        ("E1VFVN30.VN", "ETF E1VFVN30 (proxy VN30)"),
        ("FUEVFVN30.VN", "ETF FUEVFVN30 (proxy VN30)"),
        ("VN30.VN", "VN30 (Yahoo)"),
        ("FPT.VN", "FPT (proxy thanh khoản VN)"),
        ("VNM.VN", "VNM (proxy blue-chip VN)"),
    ]
    for sym, label in candidates:
        try:
            df = _yahoo_chart(sym, range_str)
            s = pd.Series(df["close"].values, index=df["date"].dt.normalize())
            s = s[~s.index.duplicated(keep="last")]
            if len(s) >= 60:
                return s, label
        except Exception:
            continue
    try:
        import contextlib
        import io
        import logging

        import yfinance as yf

        logging.getLogger("yfinance").setLevel(logging.CRITICAL)
        yf_tickers = [
            ("^VNI", "VN-Index (yfinance ^VNI)"),
            ("^VNINDEX", "VN-Index (yfinance)"),
            ("E1VFVN30.VN", "ETF E1VFVN30 (yfinance)"),
            ("VN30.VN", "VN30 (yfinance)"),
            ("FPT.VN", "FPT buy-hold proxy (yfinance)"),
        ]
        yf_period = {"max": "max", "10y": "10y", "5y": "5y", "2y": "2y"}.get(range_str, "5y")
        for ysym, label in yf_tickers:
            try:
                with contextlib.redirect_stderr(io.StringIO()):
                    hist = yf.download(ysym, period=yf_period, progress=False, auto_adjust=True, threads=False)
            except Exception:
                continue
            if hist is None or hist.empty:
                continue
            try:
                close_s = hist["Close"]
            except Exception:
                close_s = hist.iloc[:, 0]
            s = close_s.squeeze().copy()
            s.index = pd.to_datetime(s.index).normalize()
            s = s[~s.index.duplicated(keep="last")]
            if len(s) >= 60:
                return s, label
    except Exception:
        pass
    return None, ""


@dataclass
class BacktestResult:
    dates: list[pd.Timestamp]
    equity_app: np.ndarray
    equity_bench: np.ndarray
    # equity_buyhold_stock: mua full tại ngày đầu kỳ (sau warmup), giữ đến cuối — so sánh cùng mã.
    equity_buyhold_stock: np.ndarray
    app_return_pct: float
    bench_return_pct: float
    buyhold_stock_return_pct: float
    alpha_pct: float
    win_rate_pct: float
    max_drawdown_pct: float
    benchmark_label: str
    note: str


def run_phase_signal_backtest(
    ohlcv: pd.DataFrame,
    benchmark_close: pd.Series,
    *,
    initial_cash: float = 100_000_000.0,
    warmup: int = 60,
    buy_on_breakout: bool = True,
    skip_distribution_if_above_ma50: bool = True,
) -> BacktestResult:
    """
    - Mua: pha **Tích lũy**; tùy chọn thêm **Bứt phá** (`buy_on_breakout`) để bám sóng tăng.
    - Bán khi **Phân phối**, trừ khi giá vẫn trên **MA50** (giảm bán sớm trong uptrend).

    Không đảm bảo lợi nhuận mục tiêu bất kỳ — chỉ cải thiện hành vi mô phỏng so với rule quá đơn giản.
    """
    df = ohlcv.copy()
    if "date" not in df.columns:
        raise ValueError("OHLCV thiếu cột date")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    if len(df) < warmup + 5:
        raise ValueError("Không đủ lịch sử để backtest")

    bench = benchmark_close.sort_index()
    cash = float(initial_cash)
    shares = 0.0
    invested = False
    entry_px = 0.0
    trades_pnl: list[float] = []

    eq_app: list[float] = []
    eq_bench: list[float] = []
    out_dates: list[pd.Timestamp] = []

    bench0 = float(bench.iloc[0]) if len(bench) > 0 else 1.0
    bench_shares = initial_cash / bench0 if bench0 > 0 else 0.0

    px_start = float(df.iloc[warmup]["close"])
    buyhold_shares = initial_cash / px_start if px_start > 0 else 0.0
    eq_bh: list[float] = []

    for i in range(warmup, len(df)):
        window = df.iloc[: i + 1].copy()
        try:
            ph = detect_market_phase_from_ohlcv(window).phase
        except Exception:
            ph = "neutral"
        row = df.iloc[i]
        price = float(row["close"])
        d = pd.Timestamp(row["date"]).normalize()

        want_buy = ph == "accumulation" or (buy_on_breakout and ph == "breakout")
        if want_buy and not invested and price > 0:
            shares = cash / price
            entry_px = price
            cash = 0.0
            invested = True
        elif ph == "distribution" and invested and price > 0:
            do_exit = True
            if skip_distribution_if_above_ma50 and len(window) >= 55:
                try:
                    ti = compute_technical_indicators(window)
                    ma50 = float(ti.iloc[-1]["ma50"])
                    if pd.notna(ma50) and ma50 > 0 and price > ma50:
                        do_exit = False
                except Exception:
                    do_exit = True
            if do_exit:
                cash = shares * price
                if shares > 0 and entry_px > 0:
                    trades_pnl.append((price - entry_px) / entry_px * 100.0)
                shares = 0.0
                entry_px = 0.0
                invested = False

        mtm = cash + shares * price
        eq_app.append(mtm)

        bpx = bench.reindex([d]).iloc[0] if d in bench.index else np.nan
        if pd.isna(bpx):
            sub = bench[bench.index <= d]
            bpx = float(sub.iloc[-1]) if len(sub) else bench0
        else:
            bpx = float(bpx)
        eq_bench.append(bench_shares * bpx)
        eq_bh.append(buyhold_shares * price)
        out_dates.append(d)

    eq_app_a = np.array(eq_app, dtype=float)
    eq_bench_a = np.array(eq_bench, dtype=float)
    eq_bh_a = np.array(eq_bh, dtype=float)
    if len(eq_app_a) == 0:
        raise ValueError("Không sinh được chuỗi equity")

    ret_app = (eq_app_a[-1] / initial_cash - 1.0) * 100.0
    ret_bench = (eq_bench_a[-1] / initial_cash - 1.0) * 100.0
    ret_bh_stock = (eq_bh_a[-1] / initial_cash - 1.0) * 100.0
    alpha = ret_app - ret_bench

    wins = sum(1 for x in trades_pnl if x > 0)
    ntr = len(trades_pnl)
    win_rate = (wins / ntr * 100.0) if ntr > 0 else 0.0

    peak = np.maximum.accumulate(eq_app_a)
    dd = np.where(peak > 0, (peak - eq_app_a) / peak * 100.0, 0.0)
    max_dd = float(np.max(dd)) if len(dd) else 0.0

    note = (
        "Mô phỏng: mua Tích lũy"
        + ("+Bứt phá" if buy_on_breakout else "")
        + "; bán Phân phối"
        + (" (không bán nếu giá>MA50)" if skip_distribution_if_above_ma50 else "")
        + ". Không phí/trượt. Không cam kết lợi nhuận."
    )
    return BacktestResult(
        dates=out_dates,
        equity_app=eq_app_a,
        equity_bench=eq_bench_a,
        equity_buyhold_stock=eq_bh_a,
        app_return_pct=float(ret_app),
        bench_return_pct=float(ret_bench),
        buyhold_stock_return_pct=float(ret_bh_stock),
        alpha_pct=float(alpha),
        win_rate_pct=float(win_rate),
        max_drawdown_pct=float(max_dd),
        benchmark_label="benchmark",
        note=note,
    )


def equity_yearly_returns_pct(
    dates: list[pd.Timestamp],
    equity: np.ndarray,
    *,
    series_label: str = "mô phỏng pha",
) -> pd.DataFrame:
    """
    Tăng/giảm % theo năm dương lịch: **phiên giao dịch đầu tiên** → **phiên cuối cùng** trong năm
    (không phải 1/1 hay 31/12 nếu không có phiên). Năm đang chạy dễ thấp hơn CCQ cả năm nếu so sánh
    với NAV quỹ đoạn 1/1–31/12.
    """
    if len(dates) != len(equity):
        raise ValueError("dates/equity length mismatch")
    s = pd.Series(equity.astype(float), index=pd.to_datetime(dates))
    s = s[~s.index.duplicated(keep="last")].sort_index()
    rows: list[dict[str, Any]] = []
    pct_col = f"Tăng % ({series_label})"
    for yr, grp in s.groupby(s.index.year):
        if len(grp) < 2:
            continue
        a, b = float(grp.iloc[0]), float(grp.iloc[-1])
        pct = ((b / a) - 1.0) * 100.0 if a > 0 else 0.0
        rows.append(
            {
                "Năm": int(yr),
                pct_col: round(pct, 2),
                "Giá trị phiên đầu trong năm (VND)": a,
                "Giá trị phiên cuối trong năm (VND)": b,
            }
        )
    return pd.DataFrame(rows)


def run_full_backtest_for_ticker(
    ticker: str,
    *,
    yahoo_range: str | None = None,
    buy_on_breakout: bool = True,
    skip_distribution_if_above_ma50: bool = True,
) -> tuple[BacktestResult, str]:
    sym_u = (ticker or "").strip().upper()
    ohlcv, rng_used = fetch_long_ohlcv_for_backtest(sym_u, range_preference=yahoo_range)
    bench_series, bench_label = fetch_vn_benchmark_series(rng_used)
    if bench_series is None or len(bench_series) < 60:
        bench_series = pd.Series(
            ohlcv["close"].values,
            index=pd.to_datetime(ohlcv["date"]).dt.normalize(),
        )
        bench_series = bench_series[~bench_series.index.duplicated(keep="last")]
        bench_label = f"Mua & giữ {sym_u} (proxy — không tải được chỉ số thị trường VN)"
    # Căn chỉnh mốc thời gian giao nhau
    start = max(ohlcv["date"].min(), bench_series.index.min().to_pydatetime())
    end = min(ohlcv["date"].max(), bench_series.index.max().to_pydatetime())
    ohlcv = ohlcv[(ohlcv["date"] >= start) & (ohlcv["date"] <= end)].reset_index(drop=True)
    bench_series = bench_series[(bench_series.index >= pd.Timestamp(start)) & (bench_series.index <= pd.Timestamp(end))]
    if len(ohlcv) < 80:
        raise ValueError("Không đủ dữ liệu chồng lấp sau khi căn thời gian.")
    res = run_phase_signal_backtest(
        ohlcv,
        bench_series,
        initial_cash=100_000_000.0,
        warmup=60,
        buy_on_breakout=buy_on_breakout,
        skip_distribution_if_above_ma50=skip_distribution_if_above_ma50,
    )
    res.benchmark_label = bench_label
    res.note = res.note + f" Yahoo range={rng_used}. Benchmark: {bench_label}."
    return res, bench_label
