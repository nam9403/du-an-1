from __future__ import annotations

from core.product_layer import (
    get_trial_state,
    premium_features_unlocked,
    start_premium_trial_7d,
    trial_is_active,
)


def test_trial_once_per_user(tmp_path, monkeypatch) -> None:
    import core.product_layer as pl

    monkeypatch.setattr(pl, "DB_PATH", tmp_path / "t.db")
    monkeypatch.setattr(pl, "SECRETS_PATH", tmp_path / "sec.json")
    pl._init_db()

    uid = "trial_test_user"
    assert premium_features_unlocked(uid, "free") is False
    ok, _ = start_premium_trial_7d(uid)
    assert ok is True
    assert trial_is_active(uid) is True
    assert premium_features_unlocked(uid, "free") is True
    ok2, _ = start_premium_trial_7d(uid)
    assert ok2 is False
    st = get_trial_state(uid)
    assert int(st.get("trial_consumed") or 0) == 1
