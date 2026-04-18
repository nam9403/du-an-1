from __future__ import annotations

from core.alert_center import _dedupe_badges, BADGE_DUMP, BADGE_NEWS, BADGE_SUPPORT


def test_dedupe_badges() -> None:
    rows = [BADGE_DUMP, BADGE_DUMP, BADGE_NEWS, BADGE_SUPPORT]
    out = _dedupe_badges(rows)
    assert len(out) == 3
    ids = [x[0] for x in out]
    assert ids == ["dump", "bad_news", "support_broken"]
