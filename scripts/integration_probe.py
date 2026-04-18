"""
Chạy thử tích hợp: snapshot -> báo cáo -> PDF -> backtest (mã VN mẫu).

Usage:
  python scripts/integration_probe.py
  set II_HEALTH_SKIP_NETWORK=1   # bỏ bước cần gọi mạng nặng (OHLCV live, backtest Yahoo, …) — phù hợp CI/offline
"""
from __future__ import annotations

import os
import sys
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

TICKERS = ["FPT", "VNM", "HPG", "VCB"]


def _skip_network() -> bool:
    return os.environ.get("II_HEALTH_SKIP_NETWORK", "").strip().lower() in ("1", "true", "yes", "on")


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    failures: list[str] = []

    print("=== 1. Snapshot (fetch_financial_snapshot) ===")
    from scrapers.financial_data import fetch_financial_snapshot

    for sym in TICKERS:
        try:
            snap = fetch_financial_snapshot(sym)
            if snap is None:
                failures.append(f"snapshot {sym}: None")
                print(f"  {sym}: None")
            else:
                price = snap.get("price")
                src = snap.get("source")
                print(f"  {sym}: OK price={price} source={src}")
        except Exception as e:
            failures.append(f"snapshot {sym}: {e}")
            print(f"  {sym}: EXC {e}")

    print("\n=== 2. Strategic report (generate_strategic_report) ===")
    from core.ai_logic import generate_strategic_report

    for sym in TICKERS:
        snap = fetch_financial_snapshot(sym)
        if not snap:
            print(f"  {sym}: SKIP (no snapshot)")
            continue
        try:
            rep = generate_strategic_report(
                sym,
                snap,
                profile="growth",
                total_capital_vnd=100_000_000.0,
                sessions=60,
                news_limit=3,
                enable_llm=False,
                fast_mode=True,
            )
            fa = rep.get("final_action")
            whys = len(rep.get("whys_steps") or [])
            val = rep.get("valuation") or {}
            print(f"  {sym}: action={fa} whys={whys} mos={val.get('margin_of_safety_composite_pct')}")
        except Exception as e:
            failures.append(f"report {sym}: {e}")
            print(f"  {sym}: FAIL {e}")
            traceback.print_exc()

    print("\n=== 3. Professional PDF ===")
    from core.professional_pdf import build_professional_report_pdf

    sym = "FPT"
    snap = fetch_financial_snapshot(sym)
    if snap:
        try:
            rep = generate_strategic_report(
                sym,
                snap,
                profile="growth",
                total_capital_vnd=100_000_000.0,
                sessions=60,
                news_limit=3,
                enable_llm=False,
                fast_mode=True,
            )
            val = rep.get("valuation") or {}
            pdf = build_professional_report_pdf(sym, rep, val, ohlcv_png=None, allocation_png=None)
            print(f"  PDF bytes={len(pdf)} starts_pdf={pdf[:4]==b'%PDF'}")
            if len(pdf) < 500:
                failures.append("pdf too small")
        except Exception as e:
            failures.append(f"pdf: {e}")
            print(f"  FAIL {e}")
            traceback.print_exc()
    else:
        print("  SKIP no snapshot")

    print("\n=== 4. PDF with charts (Kaleido hoặc fallback Matplotlib) ===")
    if _skip_network():
        print("  SKIP (II_HEALTH_SKIP_NETWORK=1 — không gọi OHLCV live)")
    else:
        try:
            from core.chart_export import allocation_png_for_pdf, candlestick_png_for_pdf
            from scrapers.portal import fetch_ohlcv_history

            if snap:
                ohlcv = fetch_ohlcv_history(sym, sessions=80)
                val = rep.get("valuation") or {}
                png1 = candlestick_png_for_pdf(ohlcv, val)
                plan = rep.get("risk_plan") or {}
                alloc = float(plan.get("allocated_capital_vnd") or 0)
                png2 = allocation_png_for_pdf(alloc, max(100_000_000 - alloc, 0))
                pdf2 = build_professional_report_pdf(sym, rep, val, ohlcv_png=png1, allocation_png=png2)
                print(f"  PNG ok len={len(png1)},{len(png2)} pdf2={len(pdf2)}")
        except Exception as e:
            failures.append(f"pdf+kaleido: {e}")
            print(f"  FAIL {e}")
            traceback.print_exc()

    print("\n=== 5. Backtest full (network) ===")
    if _skip_network():
        print("  SKIP (II_HEALTH_SKIP_NETWORK=1)")
    else:
        from core.strategy_backtest import run_full_backtest_for_ticker

        for sym_bt in ("FPT", "VNM"):
            try:
                res, lbl = run_full_backtest_for_ticker(sym_bt)
                print(
                    f"  {sym_bt}: app_ret={res.app_return_pct:.2f}% bench={res.bench_return_pct:.2f}% label={lbl[:40]}"
                )
            except Exception as e:
                failures.append(f"backtest {sym_bt}: {e}")
                print(f"  {sym_bt}: FAIL {e}")

    print("\n=== 6. Alert center scan ===")
    if _skip_network():
        print("  SKIP (II_HEALTH_SKIP_NETWORK=1)")
    else:
        from core.alert_center import scan_watchlist_danger_alerts

        try:
            rows = scan_watchlist_danger_alerts(["FPT", "VNM"], holding_symbols=["FPT"])
            print(f"  rows={len(rows)}")
        except Exception as e:
            failures.append(f"alerts: {e}")
            print(f"  FAIL {e}")

    print("\n=== Kết quả ===")
    if failures:
        print(f"Có {len(failures)} lỗi:")
        for f in failures:
            print(" -", f)
        sys.exit(1)
    print("Tất cả bước hoàn thành không lỗi.")
    sys.exit(0)


if __name__ == "__main__":
    main()
