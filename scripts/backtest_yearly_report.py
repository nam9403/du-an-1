"""
Báo cáo backtest theo năm (dữ liệu Yahoo, lịch sử dài).

Chạy: python scripts/backtest_yearly_report.py FPT

Lưu ý: không dùng kết quả để hứa lợi nhuận với khách hàng.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    sym = (sys.argv[1] if len(sys.argv) > 1 else "FPT").strip().upper()
    from core.strategy_backtest import equity_yearly_returns_pct, run_full_backtest_for_ticker

    print(f"=== Backtest đa năm: {sym} (Yahoo range=max nếu có) ===\n")
    res, lbl = run_full_backtest_for_ticker(sym, yahoo_range="max")
    print(f"Tổng mô phỏng pha: {res.app_return_pct:.2f}%")
    print(f"Mua & giữ cùng mã: {res.buyhold_stock_return_pct:.2f}%")
    print(f"Benchmark: {res.bench_return_pct:.2f}% — {lbl}\n")
    ydf = equity_yearly_returns_pct(res.dates, res.equity_app)
    print(ydf.to_string(index=False))
    print("\n(CAGR trung bình không được tối ưu hóa theo mục tiêu lợi nhuận — chỉ thống kê minh họa.)")


if __name__ == "__main__":
    main()
