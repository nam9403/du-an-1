from __future__ import annotations

import os

import pytest

from core.config import AppSettings, get_settings


def test_default_bond_yield(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("II_DEFAULT_BOND_YIELD_PCT", raising=False)
    monkeypatch.delenv("BOND_YIELD_PCT", raising=False)
    get_settings.cache_clear()
    s = get_settings()
    assert isinstance(s, AppSettings)
    assert s.default_bond_yield_pct == 4.4
    assert s.merge_live_fundamentals is True
    assert s.skip_mock_snapshot is False


def test_bond_yield_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BOND_YIELD_PCT", raising=False)
    monkeypatch.setenv("II_DEFAULT_BOND_YIELD_PCT", "5.5")
    get_settings.cache_clear()
    assert get_settings().default_bond_yield_pct == 5.5


def test_bond_yield_clamped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("II_DEFAULT_BOND_YIELD_PCT", raising=False)
    monkeypatch.setenv("BOND_YIELD_PCT", "99")
    get_settings.cache_clear()
    assert get_settings().default_bond_yield_pct == 25.0
