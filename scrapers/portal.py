"""Portal scraper: OHLCV, financial indicators, and latest news."""

from __future__ import annotations

import os
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .finance_scraper import DEFAULT_HEADERS, VNDIRECT_HEADERS

_TTL_OHLCV_S = 300
_TTL_FIN_S = 900
_TTL_NEWS_S = 300
_PORTAL_TIMEOUT = float(os.environ.get("VALUE_INVESTOR_PORTAL_TIMEOUT", "8"))
_MEM_CACHE: dict[str, tuple[datetime, Any]] = {}
_ROOT = Path(__file__).resolve().parent.parent
_OHLCV_DISK_CACHE_PATH = _ROOT / "data" / "ohlcv_cache.json"
_FIN_DISK_CACHE_PATH = _ROOT / "data" / "financial_indicators_cache.json"
_SLA_PATH = _ROOT / "data" / "source_sla.json"


class PortalDataError(Exception):
    """Raised when portal data cannot be fetched or parsed."""


def _bool_env(name: str, default: bool = False) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _float_env(name: str, default: float) -> float:
    try:
        return float(str(os.environ.get(name, str(default))).strip())
    except ValueError:
        return float(default)


def _timeout_ladder() -> list[float]:
    raw = str(os.environ.get("II_PORTAL_TIMEOUT_LADDER_SEC", "") or "").strip()
    if not raw:
        # Keep first probe fast, second probe moderate.
        return [2.5, max(3.0, _PORTAL_TIMEOUT)]
    out: list[float] = []
    for x in raw.split(","):
        try:
            v = float(x.strip())
            if v > 0:
                out.append(v)
        except ValueError:
            continue
    return out or [max(1.0, _PORTAL_TIMEOUT)]


def _pick_timeout_for_probe(probe_idx: int, started: float, live_budget: float) -> float:
    ladder = _timeout_ladder()
    base = ladder[min(max(0, probe_idx), len(ladder) - 1)]
    elapsed = max(0.0, time.perf_counter() - started)
    remain = max(0.2, live_budget - elapsed)
    return max(0.6, min(base, remain))


def _load_sla() -> dict[str, Any]:
    if not _SLA_PATH.exists():
        return {}
    try:
        with open(_SLA_PATH, encoding="utf-8") as f:
            raw = json.load(f)
        return raw if isinstance(raw, dict) else {}
    except (OSError, ValueError):
        return {}


def _save_sla(payload: dict[str, Any]) -> None:
    _SLA_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(_SLA_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError:
        return


def _sla_mark(source: str, success: bool) -> None:
    data = _load_sla()
    key = (source or "unknown").strip().lower()
    block = data.get(key) if isinstance(data.get(key), dict) else {}
    ok = int(block.get("ok") or 0)
    fail = int(block.get("fail") or 0)
    block["ok"] = ok + (1 if success else 0)
    block["fail"] = fail + (0 if success else 1)
    total = block["ok"] + block["fail"]
    block["success_rate_pct"] = round((block["ok"] / total * 100.0), 2) if total > 0 else 0.0
    block["last_status"] = "ok" if success else "fail"
    block["updated_at"] = datetime.now(timezone.utc).isoformat()
    data[key] = block
    _save_sla(data)


def get_source_sla_report() -> list[dict[str, Any]]:
    data = _load_sla()
    rows: list[dict[str, Any]] = []
    for k, v in data.items():
        if not isinstance(v, dict):
            continue
        rows.append(
            {
                "source": k,
                "ok": int(v.get("ok") or 0),
                "fail": int(v.get("fail") or 0),
                "success_rate_pct": float(v.get("success_rate_pct") or 0.0),
                "last_status": str(v.get("last_status") or "unknown"),
                "updated_at": str(v.get("updated_at") or ""),
            }
        )
    rows.sort(key=lambda x: (x["success_rate_pct"], x["ok"]), reverse=True)
    return rows


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _norm_ticker(ticker: str) -> str:
    sym = (ticker or "").strip().upper()
    if not sym:
        raise PortalDataError("Ticker rỗng.")
    return sym


def _cache_get(key: str, ttl_seconds: int) -> Any | None:
    item = _MEM_CACHE.get(key)
    if not item:
        return None
    saved_at, payload = item
    if datetime.now(timezone.utc) - saved_at > timedelta(seconds=ttl_seconds):
        _MEM_CACHE.pop(key, None)
        return None
    return payload


def _cache_set(key: str, payload: Any) -> None:
    _MEM_CACHE[key] = (datetime.now(timezone.utc), payload)


def _read_ohlcv_disk_cache(sym: str) -> pd.DataFrame | None:
    if not _OHLCV_DISK_CACHE_PATH.exists():
        return None
    try:
        with open(_OHLCV_DISK_CACHE_PATH, encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, ValueError):
        return None
    block = raw.get(sym)
    if not isinstance(block, dict):
        return None
    rows = block.get("rows")
    if not isinstance(rows, list) or len(rows) < 50:
        return None
    df = pd.DataFrame(rows)
    needed = {"date", "open", "high", "low", "close", "volume"}
    if not needed.issubset(set(df.columns)):
        return None
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for c in ("open", "high", "low", "close", "volume"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["date", "open", "high", "low", "close", "volume"]).sort_values("date")
    if len(df) < 50:
        return None
    df.attrs["source"] = "disk_cache"
    df.attrs["saved_at"] = block.get("saved_at")
    return df.reset_index(drop=True)


def _ohlcv_disk_cache_age_seconds(df: pd.DataFrame) -> float | None:
    saved_at = str(df.attrs.get("saved_at") or "").strip()
    if not saved_at:
        return None
    try:
        ts = datetime.fromisoformat(saved_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    return max(0.0, (datetime.now(timezone.utc) - ts).total_seconds())


def _write_ohlcv_disk_cache(sym: str, df: pd.DataFrame) -> None:
    payload: dict[str, Any] = {}
    if _OHLCV_DISK_CACHE_PATH.exists():
        try:
            with open(_OHLCV_DISK_CACHE_PATH, encoding="utf-8") as f:
                raw = json.load(f)
                if isinstance(raw, dict):
                    payload = raw
        except (OSError, ValueError):
            payload = {}

    out_df = df.tail(180).copy()
    out_df["date"] = out_df["date"].dt.strftime("%Y-%m-%d")
    payload[sym] = {
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "rows": out_df.to_dict(orient="records"),
    }
    _OHLCV_DISK_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(_OHLCV_DISK_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError:
        return


def _read_fin_disk_cache(sym: str) -> dict[str, Any] | None:
    if not _FIN_DISK_CACHE_PATH.exists():
        return None
    try:
        with open(_FIN_DISK_CACHE_PATH, encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, ValueError):
        return None
    block = raw.get(sym)
    if not isinstance(block, dict):
        return None
    payload = block.get("payload")
    if not isinstance(payload, dict):
        return None
    out = dict(payload)
    out["source"] = f"{out.get('source', 'unknown')}:disk_cache"
    out["saved_at"] = block.get("saved_at")
    return out


def _fin_disk_cache_age_seconds(payload: dict[str, Any]) -> float | None:
    saved_at = str(payload.get("saved_at") or "").strip()
    if not saved_at:
        return None
    try:
        ts = datetime.fromisoformat(saved_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    return max(0.0, (datetime.now(timezone.utc) - ts).total_seconds())


def _write_fin_disk_cache(sym: str, payload: dict[str, Any]) -> None:
    data: dict[str, Any] = {}
    if _FIN_DISK_CACHE_PATH.exists():
        try:
            with open(_FIN_DISK_CACHE_PATH, encoding="utf-8") as f:
                raw = json.load(f)
                if isinstance(raw, dict):
                    data = raw
        except (OSError, ValueError):
            data = {}
    clean = dict(payload)
    clean.pop("saved_at", None)
    data[sym] = {"saved_at": datetime.now(timezone.utc).isoformat(), "payload": clean}
    _FIN_DISK_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(_FIN_DISK_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except OSError:
        return


def _fetch_ohlcv_history_uncached(ticker: str, sessions: int = 80) -> pd.DataFrame:
    """
    Fetch historical OHLCV for at least 50 recent sessions.

    Returns columns: date, open, high, low, close, volume.
    """
    sym = _norm_ticker(ticker)
    size = max(80, sessions)
    now = datetime.now(timezone.utc).date()
    since = (now - timedelta(days=220)).isoformat()
    urls = [
        (
            "https://finfo-api.vndirect.com.vn/v4/stock_prices"
            f"?sort=date&size={size}&q=code:{quote(sym)}~date:gte:{since}"
        ),
        (
            "https://finfo-api.vndirect.com.vn/v4/stock-prices"
            f"?sort=date&size={size}&q=code:{quote(sym)}~date:gte:{since}"
        ),
    ]
    rows: list[dict[str, Any]] | None = None
    errors: list[str] = []
    started = time.perf_counter()
    live_budget = max(1.0, _float_env("II_PORTAL_LIVE_BUDGET_SEC", 12.0))
    for i, url in enumerate(urls):
        if time.perf_counter() - started > live_budget:
            errors.append("live_budget_exceeded")
            break
        try:
            req_timeout = _pick_timeout_for_probe(i, started, live_budget)
            r = requests.get(url, headers=VNDIRECT_HEADERS, timeout=req_timeout)
            r.raise_for_status()
            js = r.json()
            data = js.get("data") if isinstance(js, dict) else None
            if isinstance(data, list) and data:
                rows = data
                break
            errors.append(f"empty:{url}")
        except (requests.RequestException, ValueError) as e:
            errors.append(f"{url} => {e}")
    if not rows:
        raise PortalDataError(f"Không có dữ liệu OHLCV cho {sym}. Chi tiết: {' | '.join(errors)}")

    parsed: list[dict[str, Any]] = []
    for row in rows:
        o = _to_float(row.get("open"))
        h = _to_float(row.get("high"))
        l = _to_float(row.get("low"))
        c = _to_float(row.get("close") or row.get("lastPrice"))
        v = _to_float(row.get("nmVolume") or row.get("volume"))
        d = row.get("date") or row.get("tradingDate")
        if o is None or h is None or l is None or c is None or v is None or not d:
            continue
        parsed.append({"date": str(d), "open": o, "high": h, "low": l, "close": c, "volume": v})

    if len(parsed) < 50:
        raise PortalDataError(f"Dữ liệu OHLCV {sym} chưa đủ 50 phiên (hiện {len(parsed)}).")

    df = pd.DataFrame(parsed)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = (
        df.dropna(subset=["date"])
        .sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .tail(max(50, sessions))
        .reset_index(drop=True)
    )
    return df


def _fetch_ohlcv_yahoo_uncached(ticker: str, sessions: int = 80) -> pd.DataFrame:
    """
    Fallback OHLCV provider via Yahoo chart API.

    Tries Vietnam suffixes and returns DataFrame with:
    date, open, high, low, close, volume
    """
    sym = _norm_ticker(ticker)
    # HOSE: .VN — HNX: .HN — tránh ưu tiên .HM (dễ nhầm mã / sàn)
    suffixes = (".VN", ".HN", ".HM")
    errors: list[str] = []
    target = max(80, sessions)
    started = time.perf_counter()
    live_budget = max(1.0, _float_env("II_PORTAL_LIVE_BUDGET_SEC", 12.0))

    for i, sx in enumerate(suffixes):
        if time.perf_counter() - started > live_budget:
            errors.append("live_budget_exceeded")
            break
        yahoo_sym = f"{sym}{sx}"
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(yahoo_sym)}?interval=1d&range=1y"
        try:
            req_timeout = _pick_timeout_for_probe(i, started, live_budget)
            r = requests.get(url, headers=DEFAULT_HEADERS, timeout=req_timeout)
            if r.status_code != 200:
                errors.append(f"{yahoo_sym}:HTTP {r.status_code}")
                continue
            js = r.json()
            result = (((js.get("chart") or {}).get("result") or [None])[0]) or {}
            timestamps = result.get("timestamp") or []
            quote_arr = (((result.get("indicators") or {}).get("quote") or [None])[0]) or {}
            opens = quote_arr.get("open") or []
            highs = quote_arr.get("high") or []
            lows = quote_arr.get("low") or []
            closes = quote_arr.get("close") or []
            vols = quote_arr.get("volume") or []
            n = min(len(timestamps), len(opens), len(highs), len(lows), len(closes), len(vols))
            if n < 50:
                errors.append(f"{yahoo_sym}:insufficient {n}")
                continue
            rows: list[dict[str, Any]] = []
            for i in range(n):
                o = _to_float(opens[i])
                h = _to_float(highs[i])
                l = _to_float(lows[i])
                c = _to_float(closes[i])
                v = _to_float(vols[i])
                if o is None or h is None or l is None or c is None or v is None:
                    continue
                rows.append(
                    {
                        "date": datetime.fromtimestamp(int(timestamps[i]), tz=timezone.utc).date().isoformat(),
                        "open": o,
                        "high": h,
                        "low": l,
                        "close": c,
                        "volume": v,
                    }
                )
            if len(rows) < 50:
                errors.append(f"{yahoo_sym}:valid_rows {len(rows)}")
                continue
            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = (
                df.dropna(subset=["date"])
                .sort_values("date")
                .tail(target)
                .reset_index(drop=True)
            )
            return df
        except (requests.RequestException, ValueError, TypeError) as e:
            errors.append(f"{yahoo_sym}:{e}")
    raise PortalDataError(f"Yahoo OHLCV không khả dụng cho {sym}. Chi tiết: {' | '.join(errors)}")


def fetch_ohlcv_history(ticker: str, sessions: int = 80) -> pd.DataFrame:
    """Cached OHLCV history fetch."""
    sym = _norm_ticker(ticker)
    key = f"ohlcv:{sym}:{max(80, sessions)}"
    hit = _cache_get(key, _TTL_OHLCV_S)
    if hit is not None:
        _sla_mark("ohlcv_mem_cache", True)
        return hit.copy()
    prefer_disk_first = _bool_env("II_OHLCV_DISK_FIRST", False)
    allow_stale_disk = _bool_env("II_READ_STALE_DISK", True)
    max_age_s = max(60.0, _float_env("II_OHLCV_DISK_MAX_AGE_SEC", 7200.0))
    disk_first = _read_ohlcv_disk_cache(sym) if prefer_disk_first else None
    if disk_first is not None:
        age_s = _ohlcv_disk_cache_age_seconds(disk_first)
        fresh_enough = age_s is not None and age_s <= max_age_s
        if fresh_enough or allow_stale_disk:
            out = disk_first.tail(max(50, sessions)).reset_index(drop=True)
            out.attrs["source"] = "disk_cache_fastpath"
            _cache_set(key, out.copy())
            _sla_mark("ohlcv_disk_cache_fastpath", True)
            return out
    try:
        out = _fetch_ohlcv_history_uncached(sym, sessions=sessions)
        out.attrs["source"] = "live"
        _write_ohlcv_disk_cache(sym, out)
        _sla_mark("ohlcv_vndirect", True)
    except PortalDataError as e_live:
        _sla_mark("ohlcv_vndirect", False)
        try:
            out = _fetch_ohlcv_yahoo_uncached(sym, sessions=sessions)
            out.attrs["source"] = "yahoo_fallback"
            _write_ohlcv_disk_cache(sym, out)
            _sla_mark("ohlcv_yahoo", True)
        except PortalDataError:
            _sla_mark("ohlcv_yahoo", False)
            cached = _read_ohlcv_disk_cache(sym)
            if cached is None:
                raise e_live
            out = cached.tail(max(50, sessions)).reset_index(drop=True)
            _sla_mark("ohlcv_disk_cache", True)
    _cache_set(key, out.copy())
    return out


def fetch_ohlcv_history_batch(
    tickers: list[str] | tuple[str, ...],
    sessions: int = 80,
    max_concurrency: int = 4,
) -> dict[str, pd.DataFrame]:
    """
    Backward-compatible batch OHLCV fetch helper.
    Returns mapping {SYMBOL: dataframe}. Any failed symbol is skipped.
    """
    _ = max_concurrency  # keep signature compatible; current implementation is sequential.
    syms = [str(t or "").strip().upper() for t in (tickers or []) if str(t or "").strip()]
    if not syms:
        return {}
    out: dict[str, pd.DataFrame] = {}
    for sym in syms:
        try:
            out[sym] = fetch_ohlcv_history(sym, sessions=sessions)
        except Exception:
            continue
    return out


def _fetch_financial_indicators_uncached(ticker: str, max_probes: int | None = None) -> dict[str, Any]:
    """
    Fetch core financial indicators from VNDirect ratios endpoint.

    Output keys:
    - debt_to_equity
    - gross_margin
    - revenue_growth_yoy, revenue_growth_qoq
    - profit_growth_yoy, profit_growth_qoq
    """
    sym = _norm_ticker(ticker)
    filters = (f"code:eq:{sym}", f"ticker:eq:{sym}", f"code:{sym}~", f"ticker:{sym}~")
    errors: list[str] = []

    def _extract_ratio_row(x: dict[str, Any], src: str) -> dict[str, Any]:
        out = {
            "symbol": sym,
            "debt_to_equity": _to_float(
                x.get("debtToEquity")
                or x.get("debt_equity")
                or x.get("debt/equity")
                or x.get("d/e")
            ),
            "gross_margin": _to_float(
                x.get("grossMargin")
                or x.get("grossProfitMargin")
                or x.get("gross_margin")
            ),
            "revenue_growth_yoy": _to_float(
                x.get("revenueGrowthYoy")
                or x.get("revenueGrowthYoY")
                or x.get("revenue_growth_yoy")
            ),
            "revenue_growth_qoq": _to_float(
                x.get("revenueGrowthQoq")
                or x.get("revenueGrowthQoQ")
                or x.get("revenue_growth_qoq")
            ),
            "profit_growth_yoy": _to_float(
                x.get("netIncomeGrowthYoy")
                or x.get("profitGrowthYoy")
                or x.get("profitGrowthYoY")
                or x.get("profit_growth_yoy")
            ),
            "profit_growth_qoq": _to_float(
                x.get("netIncomeGrowthQoq")
                or x.get("profitGrowthQoq")
                or x.get("profitGrowthQoQ")
                or x.get("profit_growth_qoq")
            ),
            "source": src,
            "raw": x,
        }
        quality = sum(
            out[k] is not None
            for k in (
                "debt_to_equity",
                "gross_margin",
                "revenue_growth_yoy",
                "revenue_growth_qoq",
                "profit_growth_yoy",
                "profit_growth_qoq",
            )
        )
        out["data_quality_score"] = quality
        return out

    urls: list[tuple[str, str]] = []
    for flt in filters:
        if "eq:" in flt:
            urls.append(
                (
                    "vndirect_v4_ratios_latest_filter",
                    "https://finfo-api.vndirect.com.vn/v4/ratios/latest"
                    f"?filter={quote(flt)}&order=reportType&direction=desc&size=1",
                )
            )
        else:
            urls.append(
                (
                    "vndirect_v4_ratios_latest_q",
                    "https://finfo-api.vndirect.com.vn/v4/ratios/latest"
                    f"?sort=code:asc&size=1&q={quote(flt)}",
                )
            )

    best: dict[str, Any] | None = None
    started = time.perf_counter()
    live_budget = max(1.0, _float_env("II_PORTAL_LIVE_BUDGET_SEC", 12.0))
    for i, (src, url) in enumerate(urls):
        if max_probes is not None and i >= max_probes:
            break
        if time.perf_counter() - started > live_budget:
            errors.append("live_budget_exceeded")
            break
        try:
            req_timeout = _pick_timeout_for_probe(i, started, live_budget)
            r = requests.get(url, headers=VNDIRECT_HEADERS, timeout=req_timeout)
            if r.status_code != 200:
                errors.append(f"{src}:HTTP {r.status_code}")
                continue
            js = r.json()
            rows = js.get("data") if isinstance(js, dict) else None
            if not isinstance(rows, list) or not rows:
                errors.append(f"{src}:empty")
                continue
            cand = _extract_ratio_row(rows[0], src)
            if best is None or int(cand.get("data_quality_score") or 0) > int(best.get("data_quality_score") or 0):
                best = cand
            if int(cand.get("data_quality_score") or 0) >= 4:
                return cand
        except (requests.RequestException, ValueError) as e:
            errors.append(f"{src}:{e}")

    if best is not None:
        return best
    raise PortalDataError(f"Không lấy được chỉ số tài chính cho {sym}. Chi tiết: {' | '.join(errors)}")


def fetch_financial_indicators(ticker: str, *, fast_mode: bool = False) -> dict[str, Any]:
    """Cached financial indicators fetch."""
    sym = _norm_ticker(ticker)
    key = f"fin:{sym}"
    hit = _cache_get(key, _TTL_FIN_S)
    if hit is not None:
        _sla_mark("financial_mem_cache", True)
        return dict(hit)
    prefer_disk_first = _bool_env("II_FINANCIAL_DISK_FIRST", False)
    allow_stale_disk = _bool_env("II_READ_STALE_DISK", True)
    max_age_s = max(120.0, _float_env("II_FINANCIAL_DISK_MAX_AGE_SEC", 21600.0))
    cached = _read_fin_disk_cache(sym)
    if cached is not None and prefer_disk_first:
        age_s = _fin_disk_cache_age_seconds(cached)
        fresh_enough = age_s is not None and age_s <= max_age_s
        if fresh_enough or allow_stale_disk:
            out_fast = dict(cached)
            out_fast["source"] = f"{out_fast.get('source', 'unknown')}:fastpath"
            _sla_mark("financial_disk_cache_fastpath", True)
            _cache_set(key, dict(out_fast))
            return out_fast
    if cached is not None and not prefer_disk_first:
        _sla_mark("financial_disk_cache", True)
        _cache_set(key, dict(cached))
        return cached
    if fast_mode:
        _sla_mark("financial_fast_mode_skip", False)
        raise PortalDataError(f"Fast mode: chưa có financial cache cho {sym}.")
    max_probes_env = int(max(1, _float_env("II_FINANCIAL_MAX_PROBES", 4.0)))
    try:
        out = _fetch_financial_indicators_uncached(sym, max_probes=max_probes_env)
        _write_fin_disk_cache(sym, out)
        _sla_mark("financial_vndirect_live", True)
    except PortalDataError:
        _sla_mark("financial_vndirect_live", False)
        cached2 = _read_fin_disk_cache(sym)
        if cached2 is None:
            raise
        out = cached2
        _sla_mark("financial_disk_cache", True)
    _cache_set(key, dict(out))
    return out


def _normalize_news_item(title: str, link: str, source: str) -> dict[str, str]:
    return {"title": title.strip(), "url": link.strip(), "source": source}


def _fetch_cafef_news(ticker: str, limit: int = 10) -> list[dict[str, str]]:
    sym = _norm_ticker(ticker)
    url = f"https://cafef.vn/tim-kiem.chn?keywords={quote(sym)}"
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=_PORTAL_TIMEOUT)
        r.raise_for_status()
    except requests.RequestException as e:
        raise PortalDataError(f"CafeF news lỗi mạng: {e}") from e

    soup = BeautifulSoup(r.text, "html.parser")
    out: list[dict[str, str]] = []
    seen: set[str] = set()

    for a in soup.select("a"):
        title = (a.get("title") or a.get_text(" ", strip=True) or "").strip()
        href = (a.get("href") or "").strip()
        if len(title) < 20 or sym not in title.upper():
            continue
        if not href:
            continue
        if href.startswith("/"):
            href = "https://cafef.vn" + href
        if "cafef.vn" not in href:
            continue
        key = f"{title}|{href}"
        if key in seen:
            continue
        seen.add(key)
        out.append(_normalize_news_item(title, href, "cafef"))
        if len(out) >= limit:
            break
    return out


def _fetch_vietstock_news(ticker: str, limit: int = 10) -> list[dict[str, str]]:
    sym = _norm_ticker(ticker)
    url = f"https://vietstock.vn/tim-kiem.htm?keyword={quote(sym)}"
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=_PORTAL_TIMEOUT)
        r.raise_for_status()
    except requests.RequestException as e:
        raise PortalDataError(f"Vietstock news lỗi mạng: {e}") from e

    soup = BeautifulSoup(r.text, "html.parser")
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for a in soup.select("a"):
        title = (a.get("title") or a.get_text(" ", strip=True) or "").strip()
        href = (a.get("href") or "").strip()
        if len(title) < 20 or sym not in title.upper():
            continue
        if not href:
            continue
        if href.startswith("/"):
            href = "https://vietstock.vn" + href
        if "vietstock.vn" not in href:
            continue
        key = f"{title}|{href}"
        if key in seen:
            continue
        seen.add(key)
        out.append(_normalize_news_item(title, href, "vietstock"))
        if len(out) >= limit:
            break
    return out


def _fetch_latest_news_uncached(ticker: str, limit: int = 10) -> list[dict[str, str]]:
    """Fetch latest headlines from CafeF/Vietstock, merged and deduplicated."""
    lim = max(1, min(limit, 30))
    items: list[dict[str, str]] = []
    errors: list[str] = []

    for fn in (_fetch_cafef_news, _fetch_vietstock_news):
        try:
            items.extend(fn(ticker, lim))
        except PortalDataError as e:
            errors.append(str(e))

    dedup: list[dict[str, str]] = []
    seen: set[str] = set()
    for x in items:
        key = f"{x.get('title')}|{x.get('url')}"
        if key in seen:
            continue
        seen.add(key)
        dedup.append(x)
        if len(dedup) >= lim:
            break

    if not dedup:
        raise PortalDataError(f"Không lấy được tin tức cho {ticker}. Chi tiết: {' | '.join(errors)}")
    return dedup


def fetch_latest_news(ticker: str, limit: int = 10) -> list[dict[str, str]]:
    """Cached latest headlines fetch."""
    sym = _norm_ticker(ticker)
    lim = max(1, min(limit, 30))
    key = f"news:{sym}:{lim}"
    hit = _cache_get(key, _TTL_NEWS_S)
    if hit is not None:
        _sla_mark("news_mem_cache", True)
        return [dict(x) for x in hit]
    try:
        out = _fetch_latest_news_uncached(sym, lim)
        _sla_mark("news_live", True)
    except PortalDataError:
        _sla_mark("news_live", False)
        raise
    _cache_set(key, [dict(x) for x in out])
    return out
