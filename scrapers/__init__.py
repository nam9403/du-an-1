"""Scrapers & data ingestion (mở rộng sau: API công khai, trang BCTC)."""

from .finance_scraper import (
    ScraperBlockedError,
    ScraperError,
    ScraperParseError,
    TickerNotFoundError,
    clear_scrape_cache,
    get_stock_data,
)
from .financial_data import (
    build_peer_comparison_dataframe,
    fetch_financial_snapshot,
    list_universe_symbols,
    peer_symbols_same_cluster,
    universe_subtype_map,
)
from .portal import (
    PortalDataError,
    fetch_financial_indicators,
    fetch_latest_news,
    fetch_ohlcv_history,
)

__all__ = [
    "ScraperBlockedError",
    "ScraperError",
    "ScraperParseError",
    "TickerNotFoundError",
    "clear_scrape_cache",
    "build_peer_comparison_dataframe",
    "fetch_financial_snapshot",
    "list_universe_symbols",
    "universe_subtype_map",
    "get_stock_data",
    "peer_symbols_same_cluster",
    "PortalDataError",
    "fetch_ohlcv_history",
    "fetch_financial_indicators",
    "fetch_latest_news",
]
