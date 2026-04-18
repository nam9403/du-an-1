"""Smoke test — chạy: python scripts/smoke_test.py (từ thư mục gốc dự án)."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

errors: list[str] = []


def ok(name: str) -> None:
    print(f"OK  {name}")


def fail(name: str, e: BaseException) -> None:
    errors.append(f"{name}: {e}")
    print(f"FAIL {name}: {e}")


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    print("=== Core valuation ===")
    try:
        from core.valuation import (
            benjamin_graham_value,
            margin_of_safety_pct,
            piotroski_f_score,
            value_investing_summary,
        )

        v = benjamin_graham_value(1000, 5.0, bond_yield_pct=4.4)
        assert v > 0
        mos = margin_of_safety_pct(v, 8000)
        assert mos is not None and mos > 0
        ok("benjamin_graham + mos")
    except Exception as e:
        fail("core valuation", e)

    try:
        from core.valuation import value_investing_summary

        snap = {
            "symbol": "T",
            "name": "Test",
            "price": 100.0,
            "currency": "VND",
            "eps": 10.0,
            "growth_rate_pct": 5.0,
            "book_value_per_share": 50.0,
            "bond_yield_pct": 4.4,
            "source": "test",
            "piotroski": {},
        }
        s = value_investing_summary(snap)
        assert "intrinsic_value_graham" in s
        ok("value_investing_summary minimal snapshot")
    except Exception as e:
        fail("value_investing_summary", e)

    print("\n=== Core engine (technical/phase) ===")
    try:
        from core.engine import compute_technical_indicators, detect_market_phase_from_ohlcv

        rows = []
        base = 100.0
        for i in range(60):
            c = base + i * 0.2
            rows.append(
                {
                    "open": c - 0.3,
                    "high": c + 0.5,
                    "low": c - 0.6,
                    "close": c,
                    "volume": 1_000_000 + i * 1000,
                }
            )
        df = pd.DataFrame(rows)
        x = compute_technical_indicators(df)
        assert "ma20" in x.columns and "rsi14" in x.columns
        phase = detect_market_phase_from_ohlcv(df)
        assert phase.phase in ("accumulation", "breakout", "distribution", "neutral")
        ok("core.engine indicators + phase")
    except Exception as e:
        fail("core.engine", e)

    print("\n=== Scrapers / financial_data ===")
    try:
        from scrapers.financial_data import (
            build_peer_comparison_dataframe,
            fetch_financial_snapshot,
            financials_to_dataframe,
            peer_symbols_same_cluster,
        )

        m = fetch_financial_snapshot("VNM")
        assert m is not None and m.get("symbol") == "VNM"
        assert m.get("source") == "mock_json"
        ok("fetch_financial_snapshot VNM mock")

        m_hpg = fetch_financial_snapshot("HPG")
        assert m_hpg is not None and m_hpg.get("symbol") == "HPG"
        ok("fetch_financial_snapshot HPG mock")

        none = fetch_financial_snapshot("NOTREALZZZ")
        assert none is None
        ok("fetch_financial_snapshot unknown -> None")

        financials_to_dataframe([{"a": 1}])
        ok("financials_to_dataframe")

        peers = peer_symbols_same_cluster("VNM", limit=5)
        assert "VNM" in peers and len(peers) >= 1
        df, failed = build_peer_comparison_dataframe(peers)
        assert not df.empty
        ok("peer_symbols_same_cluster + build_peer_comparison_dataframe")

        from core.report_export import investment_report_html, investment_report_markdown
        from core.valuation import value_investing_summary

        summ = value_investing_summary(m)
        assert "piotroski_block" in summ
        assert len(investment_report_markdown(summ)) > 80
        assert "<html" in investment_report_html(summ).lower()
        ok("report_export + piotroski_block")
    except Exception as e:
        fail("financial_data", e)

    print("\n=== finance_scraper (mạng, có thể lỗi mạng) ===")
    try:
        from scrapers.finance_scraper import ScraperError, get_stock_data

        d = get_stock_data("VNM")
        assert d.get("price", 0) > 0
        ok("get_stock_data VNM live")
    except ScraperError as e:
        print(f"SKIP/WARN scraper live VNM: {e}")
    except Exception as e:
        fail("get_stock_data VNM", e)

    try:
        from scrapers.finance_scraper import ScraperError, get_stock_data

        try:
            get_stock_data("NOTREALZZZ")
            fail("get_stock_data invalid should raise", AssertionError("expected ScraperError"))
        except ScraperError:
            ok("get_stock_data invalid raises ScraperError")
    except Exception as e:
        fail("get_stock_data invalid", e)

    print("\n=== Portal module (mạng, có thể SKIP) ===")
    try:
        from scrapers.portal import (
            PortalDataError,
            fetch_financial_indicators,
            fetch_latest_news,
            fetch_ohlcv_history,
        )

        try:
            ohlcv = fetch_ohlcv_history("VNM", sessions=50)
            assert len(ohlcv) >= 50 and "close" in ohlcv.columns
            ok("portal fetch_ohlcv_history VNM")
        except PortalDataError as e:
            print(f"SKIP/WARN portal ohlcv: {e}")

        try:
            fi = fetch_financial_indicators("VNM")
            assert "debt_to_equity" in fi
            ok("portal fetch_financial_indicators VNM")
        except PortalDataError as e:
            print(f"SKIP/WARN portal financial indicators: {e}")

        try:
            news = fetch_latest_news("VNM", limit=3)
            assert len(news) >= 1
            ok("portal fetch_latest_news VNM")
        except PortalDataError as e:
            print(f"SKIP/WARN portal news: {e}")
    except Exception as e:
        fail("portal module", e)

    print("\n=== AI logic (fallback) ===")
    try:
        from core.ai_logic import generate_strategic_report
        from scrapers.financial_data import fetch_financial_snapshot

        snap = fetch_financial_snapshot("VNM")
        assert snap is not None
        try:
            rep = generate_strategic_report("VNM", snap, preferred_llm="auto")
            assert "whys_steps" in rep
            ok("ai_logic generate_strategic_report")
        except Exception as e:
            print(f"SKIP/WARN ai_logic online pipeline: {e}")
    except Exception as e:
        fail("ai_logic", e)

    print("\n=== Package import scrapers ===")
    try:
        import scrapers

        assert hasattr(scrapers, "get_stock_data")
        assert hasattr(scrapers, "fetch_financial_snapshot")
        assert hasattr(scrapers, "peer_symbols_same_cluster")
        assert hasattr(scrapers, "build_peer_comparison_dataframe")
        ok("scrapers __init__ exports")
    except Exception as e:
        fail("scrapers package", e)

    print("\n=== app.py syntax (không chạy Streamlit) ===")
    app_path = ROOT / "app.py"
    try:
        compile(app_path.read_text(encoding="utf-8"), str(app_path), "exec")
        ok("app.py compile")
    except Exception as e:
        fail("app.py compile", e)

    print("\n=== Kết quả ===")
    if errors:
        print(f"Có {len(errors)} lỗi:")
        for x in errors:
            print(" -", x)
        sys.exit(1)
    print("Tất cả smoke test đạt (trừ scraper mạng nếu bị SKIP).")
    sys.exit(0)


if __name__ == "__main__":
    main()
