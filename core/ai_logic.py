"""AI Thinking Center: 7 Whys strategic report orchestration."""

from __future__ import annotations

import json
import os
import time
import hashlib
from datetime import datetime, timezone
from typing import Any

import requests

from core.engine import backtest_action_strategy, build_investment_context, detect_market_phase_from_ohlcv
from core.valuation import value_investing_summary
from scrapers.portal import PortalDataError, fetch_financial_indicators, fetch_latest_news, fetch_ohlcv_history


class AILogicError(Exception):
    """Raised for AI orchestration failures."""


PROFILE_LABELS = {
    "safe_dividend": "An toàn & Cổ tức",
    "growth": "Tăng trưởng",
    "aggressive_trading": "Mạo hiểm/Lướt sóng",
}
CONFIDENCE_GATE_MIN = 65.0
_KEY_HEALTH: dict[str, dict[str, Any]] = {}
_LLM_RESPONSE_CACHE: dict[str, dict[str, Any]] = {}


def _mask_key(key: str) -> str:
    if not key:
        return "unknown"
    tail = key[-4:] if len(key) >= 4 else key
    return f"***{tail}"


def _collect_api_keys(primary_env: str, multi_env: str | None = None) -> list[str]:
    keys: list[str] = []
    if multi_env:
        multi_raw = os.environ.get(multi_env, "").strip()
        if multi_raw:
            keys.extend([k.strip() for k in multi_raw.replace(";", ",").split(",") if k.strip()])
    one_key = os.environ.get(primary_env, "").strip()
    if one_key and one_key not in keys:
        keys.append(one_key)
    return keys


def _sort_keys_by_health(provider: str, keys: list[str]) -> list[str]:
    now = time.time()

    def _score(api_key: str) -> tuple[float, float]:
        state = _KEY_HEALTH.get(f"{provider}:{api_key}", {})
        cooldown_until = float(state.get("cooldown_until", 0.0) or 0.0)
        if cooldown_until > now:
            return (1e9, 1e9)
        fail_count = float(state.get("fail_count", 0.0) or 0.0)
        avg_ms = float(state.get("avg_ms", 900.0) or 900.0)
        return (fail_count, avg_ms)

    return sorted(keys, key=_score)


def _record_key_health(provider: str, api_key: str, elapsed_ms: float, success: bool, error_text: str = "") -> None:
    now = time.time()
    key_id = f"{provider}:{api_key}"
    state = _KEY_HEALTH.get(key_id, {"ok_count": 0, "fail_count": 0, "avg_ms": elapsed_ms, "cooldown_until": 0.0})
    prev_avg = float(state.get("avg_ms", elapsed_ms) or elapsed_ms)
    state["avg_ms"] = round(prev_avg * 0.7 + elapsed_ms * 0.3, 2)
    if success:
        state["ok_count"] = int(state.get("ok_count", 0)) + 1
        state["cooldown_until"] = 0.0
    else:
        state["fail_count"] = int(state.get("fail_count", 0)) + 1
        err = (error_text or "").lower()
        if any(token in err for token in ("429", "rate", "quota", "unauthorized", "401", "403", "timeout")):
            cooldown_sec = min(180, 30 * int(state["fail_count"]))
            state["cooldown_until"] = now + cooldown_sec
    _KEY_HEALTH[key_id] = state


def get_provider_health_snapshot() -> list[dict[str, Any]]:
    now = time.time()
    rows: list[dict[str, Any]] = []
    for key_id, state in _KEY_HEALTH.items():
        provider, _, raw_key = key_id.partition(":")
        cooldown_until = float(state.get("cooldown_until", 0.0) or 0.0)
        rows.append(
            {
                "provider": provider,
                "key_masked": _mask_key(raw_key),
                "ok_count": int(state.get("ok_count", 0) or 0),
                "fail_count": int(state.get("fail_count", 0) or 0),
                "avg_ms": float(state.get("avg_ms", 0.0) or 0.0),
                "cooldown_sec": int(max(0.0, cooldown_until - now)),
            }
        )
    rows.sort(key=lambda x: (x["provider"], x["fail_count"], x["avg_ms"]))
    return rows


def _get_llm_cache_ttl_sec() -> int:
    raw = os.environ.get("AI_LLM_CACHE_TTL_SEC", "180").strip()
    try:
        ttl = int(raw)
    except ValueError:
        ttl = 180
    return max(0, min(ttl, 3600))


def _messages_fingerprint(messages: list[dict[str, str]]) -> str:
    payload = json.dumps(messages, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _llm_cache_get(cache_key: str) -> tuple[str, str] | None:
    row = _LLM_RESPONSE_CACHE.get(cache_key)
    if not row:
        return None
    if float(row.get("expires_at", 0.0) or 0.0) < time.time():
        _LLM_RESPONSE_CACHE.pop(cache_key, None)
        return None
    return str(row.get("text", "")), str(row.get("provider", ""))


def _llm_cache_set(cache_key: str, text: str, provider: str) -> None:
    ttl = _get_llm_cache_ttl_sec()
    if ttl <= 0:
        return
    _LLM_RESPONSE_CACHE[cache_key] = {
        "text": text,
        "provider": provider,
        "expires_at": time.time() + ttl,
    }


def _build_auto_provider_order(
    available: dict[str, Any], *, fast_mode: bool, news_count: int, force_mode: str | None = None
) -> list[tuple[str, Any]]:
    # Modes: speed | balanced | quality
    mode = (force_mode or os.environ.get("AI_AUTO_TASK_MODE", "balanced")).strip().lower()
    if mode not in ("speed", "balanced", "quality"):
        mode = "balanced"
    if mode == "balanced":
        mode = "speed" if fast_mode else ("quality" if news_count >= 8 else "balanced")

    mode_defaults = {
        "speed": "groq,gemini,openai",
        "balanced": "groq,openai,gemini",
        "quality": "openai,gemini,groq",
    }
    order_raw = os.environ.get(f"AI_AUTO_PROVIDER_ORDER_{mode.upper()}", "").strip()
    if not order_raw:
        order_raw = os.environ.get("AI_AUTO_PROVIDER_ORDER", mode_defaults[mode])
    requested_order = [x.strip().lower() for x in order_raw.split(",") if x.strip()]
    providers: list[tuple[str, Any]] = []
    for provider_name in requested_order:
        call = available.get(provider_name)
        if call is not None:
            providers.append((provider_name, call))
    for provider_name, call in available.items():
        if provider_name not in [x[0] for x in providers]:
            providers.append((provider_name, call))
    return providers


def _confidence_escalation_threshold() -> float:
    raw = os.environ.get("AI_ESCALATE_CONFIDENCE_BELOW", "70").strip()
    try:
        val = float(raw)
    except ValueError:
        val = 70.0
    return min(95.0, max(40.0, val))


def _probabilistic_forecast(
    valuation: dict[str, Any],
    phase: dict[str, Any],
    financials: dict[str, Any],
    news: list[dict[str, Any]],
    *,
    horizon_days: int = 90,
) -> dict[str, Any]:
    price = _safe_num(valuation.get("price"), 0.0)
    intrinsic = _safe_num(
        valuation.get("composite_target_price") or valuation.get("intrinsic_value_graham"),
        0.0,
    )
    if price <= 0:
        return {
            "horizon_days": horizon_days,
            "expected_price": None,
            "expected_return_pct": None,
            "scenarios": [],
            "summary": "Chưa đủ dữ liệu giá hiện tại để dự phóng xác suất.",
        }

    phase_name = str(phase.get("phase") or "neutral")
    vol_mult = _safe_num((phase.get("metrics") or {}).get("vol_multiple"), 1.0)
    rev_yoy = financials.get("revenue_growth_yoy")
    rev_yoy = float(rev_yoy) if isinstance(rev_yoy, (float, int)) else 0.0
    dte = financials.get("debt_to_equity")
    dte = float(dte) if isinstance(dte, (float, int)) else 0.8
    news_count = len(news or [])

    # Regime-adjusted expected return anchors.
    phase_bias = {
        "breakout": 0.10,
        "accumulation": 0.06,
        "neutral": 0.02,
        "distribution": -0.06,
    }.get(phase_name, 0.01)
    fundamental_bias = max(-0.05, min(0.07, (rev_yoy / 200.0) - max(0.0, dte - 1.2) * 0.04))
    valuation_bias = 0.0
    if intrinsic > 0:
        valuation_gap = (intrinsic - price) / max(price, 1e-6)
        valuation_bias = max(-0.08, min(0.10, valuation_gap * 0.35))
    noise_penalty = -0.02 if vol_mult >= 1.8 else (0.01 if vol_mult < 0.9 else 0.0)
    news_bias = min(0.02, news_count * 0.003)
    base_ret = phase_bias + fundamental_bias + valuation_bias + noise_penalty + news_bias

    bull_ret = base_ret + 0.12
    bear_ret = base_ret - 0.12

    # Scenario probabilities adapt by regime and data quality hints.
    p_base = 0.45
    p_bull = 0.25
    p_bear = 0.30
    if phase_name in ("breakout", "accumulation"):
        p_bull += 0.08
        p_bear -= 0.08
    elif phase_name == "distribution":
        p_bull -= 0.10
        p_bear += 0.10
    if rev_yoy >= 15:
        p_bull += 0.05
        p_bear -= 0.05
    elif rev_yoy < 0:
        p_bull -= 0.05
        p_bear += 0.05

    p_bull = min(max(p_bull, 0.1), 0.7)
    p_bear = min(max(p_bear, 0.1), 0.7)
    p_base = max(0.1, 1.0 - p_bull - p_bear)
    s = p_bull + p_base + p_bear
    p_bull, p_base, p_bear = p_bull / s, p_base / s, p_bear / s

    bull_px = price * (1.0 + bull_ret)
    base_px = price * (1.0 + base_ret)
    bear_px = price * (1.0 + bear_ret)
    expected_price = (p_bull * bull_px) + (p_base * base_px) + (p_bear * bear_px)
    expected_ret = (expected_price / price - 1.0) * 100.0

    scenarios = [
        {"name": "bull", "probability": round(p_bull, 3), "target_price": round(bull_px, 2), "return_pct": round(bull_ret * 100.0, 2)},
        {"name": "base", "probability": round(p_base, 3), "target_price": round(base_px, 2), "return_pct": round(base_ret * 100.0, 2)},
        {"name": "bear", "probability": round(p_bear, 3), "target_price": round(bear_px, 2), "return_pct": round(bear_ret * 100.0, 2)},
    ]
    summary = (
        f"Dự phóng {horizon_days} ngày: expected return {expected_ret:.2f}% "
        f"(bull {p_bull*100:.0f}% / base {p_base*100:.0f}% / bear {p_bear*100:.0f}%)."
    )
    return {
        "horizon_days": horizon_days,
        "expected_price": round(expected_price, 2),
        "expected_return_pct": round(expected_ret, 2),
        "scenarios": scenarios,
        "summary": summary,
    }


def _forecast_reliability_summary(
    forecast: dict[str, Any],
    backtest: dict[str, Any],
    confidence_score: float,
) -> dict[str, Any]:
    scenarios = forecast.get("scenarios") or []
    if not scenarios:
        return {
            "quality_label": "insufficient_data",
            "hit_rate_proxy_pct": None,
            "expected_abs_error_pct": None,
            "notes": "Chưa đủ dữ liệu để ước tính độ tin cậy dự phóng.",
        }
    buy_wr = float(backtest.get("buy_win_rate_pct") or 0.0)
    buy_samples = int(backtest.get("buy_signals") or 0)
    total_samples = int(backtest.get("samples") or 0)
    conf = float(confidence_score or 0.0)

    sample_factor = min(1.0, total_samples / 120.0)
    wr_factor = buy_wr / 100.0 if buy_samples > 0 else 0.4
    conf_factor = conf / 100.0
    hit_rate_proxy = max(0.25, min(0.9, 0.2 + 0.45 * wr_factor + 0.35 * conf_factor * sample_factor))
    expected_abs_error = max(4.0, min(30.0, (1.0 - hit_rate_proxy) * 28.0))

    if hit_rate_proxy >= 0.68:
        quality_label = "high"
    elif hit_rate_proxy >= 0.56:
        quality_label = "medium"
    else:
        quality_label = "low"

    notes = (
        f"Proxy accuracy dựa trên backtest BUY win-rate {buy_wr:.1f}% "
        f"({buy_samples}/{total_samples} mẫu) và confidence hiện tại {conf:.1f}%."
    )
    return {
        "quality_label": quality_label,
        "hit_rate_proxy_pct": round(hit_rate_proxy * 100.0, 2),
        "expected_abs_error_pct": round(expected_abs_error, 2),
        "notes": notes,
    }


def _system_prompt() -> str:
    return (
        "Bạn là chuyên gia phân tích đầu tư Việt Nam. "
        "Sử dụng nguyên tắc 7 Whys để truy vấn sâu dữ liệu theo 7 lớp: "
        "1) vì sao xu hướng giá hiện tại, 2) vì sao dòng tiền như vậy, "
        "3) vì sao nội tại doanh nghiệp hỗ trợ/không hỗ trợ, "
        "4) vì sao tăng trưởng doanh thu/lợi nhuận như hiện tại, "
        "5) vì sao bối cảnh ngành định hình định giá, "
        "6) vì sao tin tức đang dẫn dắt tâm lý, "
        "7) vì sao chiến lược hành động (mua/chốt/cắt lỗ) là phù hợp. "
        "Bắt buộc kết luận rõ: cổ phiếu đang tích lũy gom hàng hay rủi ro phân phối. "
        "Không bịa số, chỉ dùng dữ liệu được cung cấp."
    )


def calculate_risk_allocation(
    ticker: str,
    current_price: float,
    buy_zone_low: float,
    buy_zone_high: float,
    total_capital_vnd: float,
    *,
    max_position_pct: float = 0.2,
    support_price: float | None = None,
) -> dict[str, Any]:
    """
    Calculate a practical risk plan:
    - max position size 20% of portfolio
    - stop loss at nearest support if valid, otherwise -7% from entry
    - estimated quantity and worst-case loss
    """
    low = float(buy_zone_low or 0)
    high = float(buy_zone_high or 0)
    if low > 0 and high > 0 and low > high:
        low, high = high, low
    if low <= 0 and high > 0:
        low = high * 0.97
    if high <= 0 and low > 0:
        high = low * 1.03

    # Entry at zone midpoint to avoid overly optimistic/harsh sizing.
    entry = ((low + high) / 2.0) if low > 0 and high > 0 else (max(current_price, 1.0))

    # Rule: SL = support nearby OR -7% from entry, but always below buy zone.
    sl_from_entry = entry * 0.93
    if low > 0 and sl_from_entry >= low:
        sl_from_entry = low * 0.97
    valid_support = None
    if support_price is not None and support_price > 0 and (low <= 0 or support_price < low):
        valid_support = support_price
    stop_loss = max(valid_support, sl_from_entry) if valid_support is not None else sl_from_entry

    alloc_vnd = max(total_capital_vnd, 0.0) * max_position_pct
    quantity = int(alloc_vnd // entry) if entry > 0 else 0
    used_vnd = quantity * entry
    loss_per_share = max(entry - stop_loss, 0.0)
    risk_vnd = quantity * loss_per_share
    risk_pct_total = (risk_vnd / total_capital_vnd * 100.0) if total_capital_vnd > 0 else 0.0

    return {
        "ticker": ticker,
        "entry_price": round(entry, 2),
        "buy_zone_low": round(low, 2),
        "buy_zone_high": round(high, 2),
        "take_profit_price": None,
        "stop_loss_price": round(stop_loss, 2),
        "max_position_pct": round(max_position_pct * 100, 2),
        "allocated_capital_vnd": round(alloc_vnd, 2),
        "estimated_used_capital_vnd": round(used_vnd, 2),
        "estimated_quantity": quantity,
        "worst_case_loss_vnd": round(risk_vnd, 2),
        "worst_case_loss_pct_total_capital": round(risk_pct_total, 3),
    }


def _safe_num(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def _normalize_buy_zone_with_market_price(
    current_price: float,
    buy_low: float,
    buy_high: float,
) -> tuple[float, float]:
    """
    Keep buy zone realistic around current market price.
    If model/fallback returns a zone too far from current price, clamp it.
    """
    cp = _safe_num(current_price, 0.0)
    low = _safe_num(buy_low, 0.0)
    high = _safe_num(buy_high, 0.0)
    if low > 0 and high > 0 and low > high:
        low, high = high, low
    if cp <= 0:
        return low, high
    if low <= 0 and high <= 0:
        return cp * 0.97, cp * 1.03
    if low <= 0 < high:
        low = min(high * 0.97, cp * 0.99)
    if high <= 0 < low:
        high = max(low * 1.03, cp * 1.01)

    mid = (low + high) / 2.0 if low > 0 and high > 0 else cp
    # If the buy zone drifts too far (>10%) from market price, reset near market.
    if cp > 0 and abs(mid / cp - 1.0) > 0.1:
        return cp * 0.97, cp * 1.03
    # Soft clamp zone edges to avoid unrealistic far bands in fallback mode.
    low = max(low, cp * 0.88)
    high = min(high, cp * 1.12)
    if low > high:
        return cp * 0.97, cp * 1.03
    return low, high


def _build_fallback_7whys(
    ticker: str,
    phase: dict[str, Any],
    valuation: dict[str, Any],
    financials: dict[str, Any],
    news: list[dict[str, str]],
    profile: str,
    total_capital_vnd: float,
) -> dict[str, Any]:
    """Fallback logic when no LLM API key is configured."""
    price = _safe_num(valuation.get("price"))
    intrinsic = _safe_num(valuation.get("intrinsic_value_graham"))
    composite = _safe_num(valuation.get("composite_target_price"))
    ref_value = composite if composite > 0 else intrinsic

    if ref_value > 0:
        buy_low = ref_value * 0.85
        buy_high = ref_value * 0.92
        tp = ref_value * 1.05
        sl = buy_low * 0.93
    else:
        buy_low = price * 0.9
        buy_high = price * 0.96
        tp = price * 1.08
        sl = price * 0.9

    phase_name = str(phase.get("phase", "neutral"))
    phase_label = {
        "accumulation": "tích lũy/gom hàng",
        "breakout": "bùng nổ",
        "distribution": "suy yếu/phân phối",
    }.get(phase_name, "trung tính")
    profile_label = PROFILE_LABELS.get(profile, profile)

    head = news[0]["title"] if news else "Chưa có headline nổi bật"
    whys = [
        f"Hồ sơ nhà đầu tư: {profile_label}; tổng vốn giả định {total_capital_vnd:,.0f} VND.",
        f"Why 1 (xu hướng): Pha thị trường hiện tại là {phase_name} ({phase.get('reason', '')}).",
        f"Why 2 (dòng tiền): Volume multiple ~{_safe_num(phase.get('metrics', {}).get('vol_multiple')):.2f}.",
        f"Why 3 (nội tại): F-Score={valuation.get('piotroski_score', 0)}/9, D/E={financials.get('debt_to_equity')}.",
        f"Why 4 (tăng trưởng): Rev YoY={financials.get('revenue_growth_yoy')}, Profit YoY={financials.get('profit_growth_yoy')}.",
        f"Why 5 (định giá ngành): {valuation.get('valuation_transparency_line', '')}",
        f"Why 6 (tin tức): Headline dẫn dắt gần nhất: {head}",
        f"Why 7 (hành động): Ưu tiên kỷ luật điểm mua/chốt/cắt lỗ theo vùng giá định lượng.",
    ]

    return {
        "ticker": ticker,
        "as_of_utc": datetime.now(timezone.utc).isoformat(),
        "phase_assessment": phase_label,
        "buy_zone": {"low": round(buy_low, 2), "high": round(buy_high, 2)},
        "take_profit": round(tp, 2),
        "stop_loss": round(sl, 2),
        "whys_steps": whys,
        "analysis_text": (
            f"{ticker} đang ở trạng thái {phase_label}. "
            "Kết hợp xu hướng kỹ thuật, dữ liệu tài chính và tin tức để giao dịch theo vùng giá."
        ),
        "llm_used": False,
        "llm_provider": "fallback_template",
    }


def _derive_financials_from_snapshot(sym: str, snapshot: dict[str, Any]) -> dict[str, Any]:
    """Best-effort financial ratios when live ratio API is unavailable."""
    pio = snapshot.get("piotroski") if isinstance(snapshot.get("piotroski"), dict) else {}
    rev = _safe_num(pio.get("revenue"))
    rev_p = _safe_num(pio.get("revenue_prior"))
    ni = _safe_num(pio.get("net_income"))
    ni_p = _safe_num(pio.get("net_income_prior"))
    cogs = _safe_num(pio.get("cogs"))
    debt = _safe_num(pio.get("long_term_debt"))
    equity = _safe_num(snapshot.get("shareholders_equity"))
    if equity <= 0:
        bvps = _safe_num(snapshot.get("book_value_per_share"))
        shares = _safe_num(pio.get("shares_outstanding"))
        if bvps > 0 and shares > 0:
            equity = bvps * shares

    gross_margin = ((rev - cogs) / rev * 100.0) if rev > 0 else None
    rev_yoy = ((rev - rev_p) / rev_p * 100.0) if rev > 0 and rev_p > 0 else None
    profit_yoy = ((ni - ni_p) / ni_p * 100.0) if ni > 0 and ni_p > 0 else None
    debt_to_equity = (debt / equity) if debt > 0 and equity > 0 else None

    return {
        "symbol": sym,
        "debt_to_equity": debt_to_equity,
        "gross_margin": gross_margin,
        "revenue_growth_yoy": rev_yoy,
        "revenue_growth_qoq": None,
        "profit_growth_yoy": profit_yoy,
        "profit_growth_qoq": None,
        "source": "snapshot_derived",
    }


def _call_openai(messages: list[dict[str, str]]) -> str:
    model = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
    url = "https://api.openai.com/v1/chat/completions"
    payload = {"model": model, "messages": messages, "temperature": 0.2}
    keys = _collect_api_keys("OPENAI_API_KEY", "OPENAI_API_KEYS")
    if not keys:
        raise AILogicError("Thiếu OPENAI_API_KEY/OPENAI_API_KEYS.")
    keys = _sort_keys_by_health("openai", keys)

    last_err: Exception | None = None
    for idx, api_key in enumerate(keys):
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        started = time.perf_counter()
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=45)
            r.raise_for_status()
            js = r.json()
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            _record_key_health("openai", api_key, elapsed_ms, True)
            return js["choices"][0]["message"]["content"]
        except (requests.RequestException, KeyError, ValueError) as e:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            _record_key_health("openai", api_key, elapsed_ms, False, str(e))
            print(f"[AI][openai] key={_mask_key(api_key)} failed, trying next key ({idx + 1}/{len(keys)}).")
            last_err = e
            time.sleep(min(0.2 * (idx + 1), 0.8))
            continue

    raise AILogicError(f"Lỗi gọi OpenAI (đã thử {len(keys)} key): {last_err}") from last_err


def _call_groq(messages: list[dict[str, str]]) -> str:
    model = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")
    url = "https://api.groq.com/openai/v1/chat/completions"
    payload = {"model": model, "messages": messages, "temperature": 0.2}
    keys = _collect_api_keys("GROQ_API_KEY", "GROQ_API_KEYS")
    if not keys:
        raise AILogicError("Thiếu GROQ_API_KEY/GROQ_API_KEYS.")
    keys = _sort_keys_by_health("groq", keys)

    last_err: Exception | None = None
    for idx, api_key in enumerate(keys):
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        started = time.perf_counter()
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=45)
            r.raise_for_status()
            js = r.json()
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            _record_key_health("groq", api_key, elapsed_ms, True)
            return js["choices"][0]["message"]["content"]
        except (requests.RequestException, KeyError, ValueError) as e:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            _record_key_health("groq", api_key, elapsed_ms, False, str(e))
            print(f"[AI][groq] key={_mask_key(api_key)} failed, trying next key ({idx + 1}/{len(keys)}).")
            last_err = e
            time.sleep(min(0.2 * (idx + 1), 0.8))
            continue

    raise AILogicError(f"Lỗi gọi Groq (đã thử {len(keys)} key): {last_err}") from last_err


def _call_gemini(messages: list[dict[str, str]]) -> str:
    model = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash")
    keys = _collect_api_keys("GEMINI_API_KEY", "GEMINI_API_KEYS")
    if not keys:
        raise AILogicError("Thiếu GEMINI_API_KEY/GEMINI_API_KEYS.")
    keys = _sort_keys_by_health("gemini", keys)
    prompt = "\n\n".join(f"{m['role'].upper()}:\n{m['content']}" for m in messages)
    body = {"contents": [{"parts": [{"text": prompt}]}]}

    last_err: Exception | None = None
    for idx, api_key in enumerate(keys):
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
        started = time.perf_counter()
        try:
            r = requests.post(url, json=body, timeout=45)
            r.raise_for_status()
            js = r.json()
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            _record_key_health("gemini", api_key, elapsed_ms, True)
            return js["candidates"][0]["content"]["parts"][0]["text"]
        except (requests.RequestException, KeyError, ValueError) as e:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            _record_key_health("gemini", api_key, elapsed_ms, False, str(e))
            print(f"[AI][gemini] key={_mask_key(api_key)} failed, trying next key ({idx + 1}/{len(keys)}).")
            last_err = e
            time.sleep(min(0.2 * (idx + 1), 0.8))
            continue

    raise AILogicError(f"Lỗi gọi Gemini (đã thử {len(keys)} key): {last_err}") from last_err


def _try_parse_json(text: str) -> dict[str, Any] | None:
    text = (text or "").strip()
    if not text:
        return None
    if text.startswith("```"):
        text = text.strip("`")
        if "\n" in text:
            text = text.split("\n", 1)[1]
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else None
    except ValueError:
        return None


def _confidence_score(
    phase: dict[str, Any],
    financials: dict[str, Any],
    news: list[dict[str, str]],
    llm_used: bool,
    backtest: dict[str, Any] | None = None,
) -> float:
    score = 0.0
    if phase.get("metrics"):
        score += 30
    dq = float(financials.get("data_quality_score") or 0)
    fin_source = str(financials.get("source") or "")
    basic_fields = ("debt_to_equity", "gross_margin", "revenue_growth_yoy")
    present_count = sum(financials.get(k) is not None for k in basic_fields)
    if dq >= 2:
        score += 30
    elif present_count >= 2:
        score += 12
    if dq >= 4:
        score += 5
    elif 0 < dq < 2:
        score -= 5
    if fin_source.startswith("snapshot_derived"):
        score -= 8
    if news:
        score += 15
    if llm_used:
        score += 15
    if str(phase.get("phase", "neutral")) in ("accumulation", "breakout", "distribution"):
        score += 10
    if backtest:
        wr = float(backtest.get("buy_win_rate_pct") or 0)
        n = int(backtest.get("buy_signals") or 0)
        if n >= 3:
            if wr >= 55:
                score += 8
            elif wr <= 45:
                score -= 8
    return min(100.0, score)


def _final_action(valuation: dict[str, Any], phase: dict[str, Any], confidence: float) -> str:
    if confidence < CONFIDENCE_GATE_MIN:
        return "WATCH"
    mos = float(valuation.get("margin_of_safety_composite_pct") or 0)
    fscore = int(valuation.get("piotroski_score") or 0)
    ph = str(phase.get("phase", "neutral"))
    if confidence >= 65 and mos >= 15 and fscore >= 6 and ph in ("accumulation", "breakout"):
        return "BUY"
    if mos >= 3 and fscore >= 4:
        return "HOLD"
    return "AVOID"


def _data_reliability(
    phase: dict[str, Any],
    financials: dict[str, Any],
    news: list[dict[str, str]],
    llm_used: bool,
    backtest: dict[str, Any] | None,
) -> dict[str, Any]:
    ph = str(phase.get("phase", "neutral"))
    metrics = phase.get("metrics") or {}
    tech_ok = bool(metrics) and ph in ("accumulation", "breakout", "distribution", "neutral")
    fin_quality = int(float(financials.get("data_quality_score") or 0))
    news_count = len(news or [])
    bt_samples = int((backtest or {}).get("samples") or 0)
    bt_buy = int((backtest or {}).get("buy_signals") or 0)
    bt_wr = float((backtest or {}).get("buy_win_rate_pct") or 0)
    return {
        "technical": {"ok": tech_ok, "source": "portal_ohlcv", "phase": ph},
        "financial": {
            "ok": fin_quality >= 3,
            "source": str(financials.get("source") or "unknown"),
            "quality_score": fin_quality,
        },
        "news": {"ok": news_count > 0, "source": "cafef_vietstock", "count": news_count},
        "llm": {"ok": bool(llm_used), "source": "live_api_or_fallback"},
        "backtest": {
            "ok": bt_samples >= 40 and bt_buy >= 3 and bt_wr >= 45.0,
            "samples": bt_samples,
            "buy_signals": bt_buy,
            "buy_win_rate_pct": bt_wr,
        },
    }


def _output_quality_gate(valuation: dict[str, Any], risk_plan: dict[str, Any]) -> dict[str, Any]:
    """
    Guardrail for practical actionability of generated plan.
    Returns pass/fail with machine-readable reasons.
    """
    reasons: list[str] = []
    price = _safe_num(valuation.get("price"), 0.0)
    entry = _safe_num(risk_plan.get("entry_price"), 0.0)
    sl = _safe_num(risk_plan.get("stop_loss_price"), 0.0)
    tp = _safe_num(risk_plan.get("take_profit_price"), 0.0)

    if entry <= 0 or sl <= 0 or tp <= 0:
        reasons.append("missing_entry_sl_tp")
    if not (sl < entry < tp):
        reasons.append("invalid_price_structure")
    if price > 0 and entry > 0 and abs(entry / price - 1.0) > 0.12:
        reasons.append("entry_too_far_from_market")
    if entry > 0 and sl > 0:
        risk_pct = (entry - sl) / entry * 100.0
        if risk_pct <= 0:
            reasons.append("non_positive_risk")
        elif risk_pct > 12.0:
            reasons.append("risk_too_wide")
    if entry > 0 and sl > 0 and tp > 0:
        rr = (tp - entry) / max(entry - sl, 1e-6)
        if rr < 1.8:
            reasons.append("rr_too_low")
        elif rr > 4.0:
            reasons.append("rr_too_optimistic")

    return {"passed": len(reasons) == 0, "reasons": reasons}


def generate_strategic_report(
    ticker: str,
    snapshot: dict[str, Any],
    *,
    preferred_llm: str = "auto",
    profile: str = "growth",
    total_capital_vnd: float = 100_000_000.0,
    sessions: int = 80,
    news_limit: int = 10,
    enable_llm: bool = True,
    fast_mode: bool = False,
    task_mode: str | None = None,
) -> dict[str, Any]:
    """
    Generate strategic report from:
    - technical trend/flow
    - financial indicators
    - latest news
    - valuation summary
    """
    sym = (ticker or "").strip().upper()
    if not sym:
        raise AILogicError("Ticker rỗng.")

    valuation = value_investing_summary(snapshot)
    phase = {"phase": "neutral", "reason": "Thiếu OHLCV để xác định xu hướng.", "metrics": {}}
    financials: dict[str, Any] = {}
    news: list[dict[str, str]] = []
    backtest_summary: dict[str, Any] = {}
    try:
        context_pack = build_investment_context(
            sym,
            snapshot,
            sessions=sessions,
            news_limit=news_limit,
            fast_mode=fast_mode,
        )
        phase = dict(context_pack["technical"]["market_phase"])
        valuation = dict(context_pack["valuation"])
        financials = dict(context_pack["financials"])
        news = list(context_pack["news"])
        try:
            backtest_summary = backtest_action_strategy(context_pack["technical"]["ohlcv"], horizon_days=10)
        except Exception:
            backtest_summary = {}
    except Exception:
        # Fallback mềm: lấy từng mảnh độc lập, tránh lỗi một nguồn làm hỏng toàn bộ report.
        try:
            ohlcv = fetch_ohlcv_history(sym, sessions=sessions)
            ph = detect_market_phase_from_ohlcv(ohlcv)
            phase = {"phase": ph.phase, "reason": ph.reason, "metrics": dict(ph.metrics)}
            try:
                backtest_summary = backtest_action_strategy(ohlcv, horizon_days=10)
            except Exception:
                backtest_summary = {}
        except Exception:
            pass
        try:
            financials = fetch_financial_indicators(sym, fast_mode=fast_mode)
        except PortalDataError:
            financials = _derive_financials_from_snapshot(sym, snapshot)
        try:
            news = fetch_latest_news(sym, limit=news_limit)
        except PortalDataError:
            news = []

    context = {
        "ticker": sym,
        "investor_profile": PROFILE_LABELS.get(profile, profile),
        "total_capital_vnd": float(total_capital_vnd),
        "phase": phase,
        "valuation": valuation,
        "financials": {k: v for k, v in financials.items() if k != "raw"},
        "news": news,
    }
    messages = [
        {"role": "system", "content": _system_prompt()},
        {
            "role": "user",
            "content": (
                "Phân tích theo 7 Whys và trả về JSON với các key: "
                "phase_assessment, buy_zone{low,high}, take_profit, stop_loss, "
                "whys_steps(list 7 mục), analysis_text. "
                "Bắt buộc cá nhân hóa theo investor_profile và total_capital_vnd; "
                "có câu trả lời rõ cho: vì sao mã phù hợp profile, nên giải ngân bao nhiêu %/VND, "
                "và kịch bản xấu nhất nếu thủng stop-loss. "
                f"Dữ liệu: {json.dumps(context, ensure_ascii=False)}"
            ),
        },
    ]

    llm_text = ""
    llm_used = False
    llm_provider = "fallback_template"
    llm_cache_hit = False
    llm_escalated = False
    llm_escalation_attempted = False

    if enable_llm:
        try:
            base_conf_no_llm = _confidence_score(phase, financials, news, False, backtest_summary)
            llm_cache_key = ""
            if preferred_llm in ("groq", "openai", "gemini", "auto"):
                llm_cache_key = (
                    f"{preferred_llm}|{profile}|{int(float(total_capital_vnd))}|{_messages_fingerprint(messages)}"
                )
                cached = _llm_cache_get(llm_cache_key)
                if cached:
                    llm_text, llm_provider = cached
                    llm_used = True
                    llm_cache_hit = True

            if llm_used:
                pass
            elif preferred_llm == "groq":
                llm_text = _call_groq(messages)
                llm_used = True
                llm_provider = "groq"
            elif preferred_llm == "openai":
                llm_text = _call_openai(messages)
                llm_used = True
                llm_provider = "openai"
            elif preferred_llm == "gemini":
                llm_text = _call_gemini(messages)
                llm_used = True
                llm_provider = "gemini"
            elif preferred_llm == "auto":
                available: dict[str, Any] = {}
                if os.environ.get("GROQ_API_KEY") or os.environ.get("GROQ_API_KEYS"):
                    available["groq"] = _call_groq
                if os.environ.get("OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEYS"):
                    available["openai"] = _call_openai
                if os.environ.get("GEMINI_API_KEY") or os.environ.get("GEMINI_API_KEYS"):
                    available["gemini"] = _call_gemini
                providers = _build_auto_provider_order(
                    available, fast_mode=fast_mode, news_count=len(news), force_mode=task_mode
                )
                last_llm_err: Exception | None = None
                for provider_name, provider_call in providers:
                    try:
                        llm_text = provider_call(messages)
                        llm_used = True
                        llm_provider = provider_name
                        if llm_cache_key:
                            _llm_cache_set(llm_cache_key, llm_text, llm_provider)
                        break
                    except AILogicError as e:
                        last_llm_err = e
                        continue
                if not llm_used and last_llm_err is not None:
                    raise last_llm_err

                # Two-stage orchestration: if base confidence is low, escalate to quality mode.
                if llm_used and base_conf_no_llm < _confidence_escalation_threshold():
                    llm_escalation_attempted = True
                    quality_cache_key = f"{llm_cache_key}|quality"
                    cached_quality = _llm_cache_get(quality_cache_key) if quality_cache_key else None
                    if cached_quality:
                        llm_text, llm_provider = cached_quality
                        llm_cache_hit = True
                        llm_escalated = True
                    else:
                        quality_providers = _build_auto_provider_order(
                            available, fast_mode=fast_mode, news_count=len(news), force_mode="quality"
                        )
                        for provider_name, provider_call in quality_providers:
                            try:
                                quality_text = provider_call(messages)
                                # Keep higher-quality narration if parseable JSON.
                                if _try_parse_json(quality_text) is not None:
                                    llm_text = quality_text
                                    llm_provider = provider_name
                                    llm_escalated = True
                                    if quality_cache_key:
                                        _llm_cache_set(quality_cache_key, llm_text, llm_provider)
                                    break
                            except AILogicError:
                                continue
            if llm_used and (not llm_cache_hit) and preferred_llm in ("groq", "openai", "gemini"):
                if llm_cache_key:
                    _llm_cache_set(llm_cache_key, llm_text, llm_provider)
        except AILogicError:
            llm_text = ""
            llm_used = False
            llm_provider = "fallback_template"
            llm_cache_hit = False

    parsed = _try_parse_json(llm_text) if llm_used else None
    if parsed is None:
        parsed = _build_fallback_7whys(sym, phase, valuation, financials, news, profile, total_capital_vnd)

    buy_zone = parsed.get("buy_zone") or {}
    current_price = _safe_num(valuation.get("price"), 0.0)
    buy_low = _safe_num(buy_zone.get("low"), current_price)
    buy_high = _safe_num(buy_zone.get("high"), buy_low)
    buy_low, buy_high = _normalize_buy_zone_with_market_price(current_price, buy_low, buy_high)
    stop_loss_raw = _safe_num(parsed.get("stop_loss"), 0.0)
    support = stop_loss_raw if stop_loss_raw > 0 else None
    risk_plan = calculate_risk_allocation(
        sym,
        _safe_num(valuation.get("price"), 0.0),
        buy_low,
        buy_high,
        float(total_capital_vnd),
        support_price=support,
    )
    tp_raw = _safe_num(parsed.get("take_profit"), 0.0)
    # Enforce minimum RR 1:2 for displayed take-profit, but cap excessive target.
    rr_min_tp = risk_plan["entry_price"] + 2.0 * (risk_plan["entry_price"] - risk_plan["stop_loss_price"])
    tp_candidate = max(tp_raw, rr_min_tp)
    entry_px = float(risk_plan.get("entry_price") or 0.0)
    stop_px = float(risk_plan.get("stop_loss_price") or 0.0)
    # Conservative cap to avoid over-promising TP in fallback/noisy data regimes.
    tp_cap_pct = entry_px * 1.25 if entry_px > 0 else tp_candidate
    tp_cap_rr = entry_px + 3.5 * max(entry_px - stop_px, 0.0) if entry_px > 0 and stop_px > 0 else tp_candidate
    tp_final = min(tp_candidate, tp_cap_pct, tp_cap_rr)
    risk_plan["take_profit_price"] = round(tp_final, 2) if tp_final > 0 else None

    parsed["ticker"] = sym
    parsed["investor_profile"] = PROFILE_LABELS.get(profile, profile)
    parsed["total_capital_vnd"] = float(total_capital_vnd)
    parsed["phase"] = phase
    parsed["valuation"] = valuation
    parsed["financials"] = financials
    parsed["news"] = news
    parsed["risk_plan"] = risk_plan
    parsed["backtest_summary"] = backtest_summary
    parsed["probabilistic_forecast"] = _probabilistic_forecast(
        valuation, phase, financials, news, horizon_days=90 if not fast_mode else 45
    )
    parsed["llm_used"] = llm_used and ("analysis_text" in parsed)
    parsed["llm_provider"] = llm_provider
    parsed["llm_cache_hit"] = llm_cache_hit
    parsed["llm_escalated"] = llm_escalated
    parsed["llm_escalation_attempted"] = llm_escalation_attempted
    parsed["data_reliability"] = _data_reliability(phase, financials, news, parsed["llm_used"], backtest_summary)
    conf = _confidence_score(phase, financials, news, parsed["llm_used"], backtest_summary)
    parsed["forecast_reliability"] = _forecast_reliability_summary(
        parsed["probabilistic_forecast"], backtest_summary, conf
    )
    parsed["confidence_score"] = round(conf, 1)
    parsed["confidence_gate_min"] = CONFIDENCE_GATE_MIN
    parsed["gate_passed"] = conf >= CONFIDENCE_GATE_MIN
    parsed["final_action"] = _final_action(valuation, phase, conf)
    out_gate = _output_quality_gate(valuation, risk_plan)
    parsed["output_quality"] = out_gate
    parsed["output_gate_passed"] = bool(out_gate.get("passed"))
    if not parsed["output_gate_passed"]:
        parsed["final_action"] = "WATCH"
    parsed["as_of_utc"] = parsed.get("as_of_utc") or datetime.now(timezone.utc).isoformat()
    return parsed
