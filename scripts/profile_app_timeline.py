from __future__ import annotations

import time
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.ai_logic import generate_strategic_report
from core.valuation import value_investing_summary
from scrapers.financial_data import fetch_financial_snapshot
from scrapers.portal import fetch_financial_indicators, fetch_latest_news, fetch_ohlcv_history


def main() -> None:
    sym = "FPT"
    rows: list[tuple[str, float, bool, str]] = []

    def run(name: str, fn):
        t0 = time.perf_counter()
        ok = True
        err = ""
        val = None
        try:
            val = fn()
        except Exception as e:  # pragma: no cover - diagnostic script
            ok = False
            err = repr(e)
        dt = time.perf_counter() - t0
        extra = ""
        if name == "fetch_financial_snapshot" and isinstance(val, dict):
            extra = f" source={val.get('source')} price_source={val.get('price_source')}"
        elif name.startswith("fetch_ohlcv_history") and hasattr(val, "attrs"):
            extra = f" source={val.attrs.get('source')} rows={len(val)}"
        elif name.startswith("fetch_latest_news") and isinstance(val, list):
            extra = f" items={len(val)}"
        elif name.startswith("generate_strategic_report") and isinstance(val, dict):
            extra = f" final_action={val.get('final_action')}"
        rows.append((name, dt, ok, extra if ok else err))
        return val

    snap = run("fetch_financial_snapshot", lambda: fetch_financial_snapshot(sym))
    if isinstance(snap, dict):
        run("value_investing_summary(no_elite)", lambda: value_investing_summary(snap, include_extensions=False))
        run("value_investing_summary(elite)", lambda: value_investing_summary(snap, include_extensions=True))
    run("fetch_ohlcv_history(80)", lambda: fetch_ohlcv_history(sym, sessions=80))
    run("fetch_financial_indicators", lambda: fetch_financial_indicators(sym, fast_mode=False))
    run("fetch_latest_news(5)", lambda: fetch_latest_news(sym, limit=5))
    if isinstance(snap, dict):
        run(
            "generate_strategic_report(no_llm)",
            lambda: generate_strategic_report(
                sym,
                snap,
                profile="growth",
                total_capital_vnd=100_000_000.0,
                sessions=60,
                news_limit=5,
                enable_llm=False,
                fast_mode=True,
            ),
        )

    rows_sorted = sorted(rows, key=lambda x: x[1], reverse=True)
    print("=== Timeline (actual runtime) ===")
    for i, (name, dt, ok, meta) in enumerate(rows, start=1):
        print(f"{i:02d}. {name}: {dt:.3f}s ok={ok}{meta}")

    print("\n=== Slowest stages ===")
    for i, (name, dt, ok, meta) in enumerate(rows_sorted[:5], start=1):
        print(f"{i}. {name}: {dt:.3f}s ok={ok}{meta}")


if __name__ == "__main__":
    main()

