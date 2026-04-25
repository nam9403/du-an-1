from __future__ import annotations

from pathlib import Path

from scripts.build_timing_dashboard import build_summary, load_events


def test_load_events_filters_invalid_lines(tmp_path: Path) -> None:
    p = tmp_path / "timing.jsonl"
    p.write_text(
        "\n".join(
            [
                '{"metric":"scanner.scan","duration_ms":120.5}',
                '{"metric":"", "duration_ms":50}',
                '{"metric":"bad.duration","duration_ms":"x"}',
                "not-json",
                '{"metric":"scanner.scan","duration_ms":80}',
            ]
        ),
        encoding="utf-8",
    )
    events = load_events(p)
    assert len(events) == 2
    assert all(str(x.get("metric")).startswith("scanner.") for x in events)


def test_build_summary_calculates_percentiles() -> None:
    events = [
        {"metric": "flow.a", "duration_ms": 10.0},
        {"metric": "flow.a", "duration_ms": 20.0},
        {"metric": "flow.a", "duration_ms": 30.0},
        {"metric": "flow.b", "duration_ms": 100.0},
    ]
    summary = build_summary(events, min_samples=2)
    assert len(summary) == 1
    assert summary[0]["metric"] == "flow.a"
    assert float(summary[0]["p50_ms"]) == 20.0
    assert float(summary[0]["p95_ms"]) >= 29.0
