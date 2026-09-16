import json
from pathlib import Path

from feedback_lakehouse.pipeline import LakehousePipeline


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def event(review_id: str, created_at: str, **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "review_id": review_id,
        "product_id": "phone-1",
        "rating": 4,
        "review_text": "The camera is excellent.",
        "source": "web",
        "created_at": created_at,
        "schema_version": 1,
    }
    payload.update(overrides)
    return payload


def test_pipeline_is_idempotent_and_quarantines_bad_events(tmp_path: Path) -> None:
    source = tmp_path / "feedback.jsonl"
    write_jsonl(
        source,
        [
            event("new", "2026-01-05T00:00:00Z"),
            event("late", "2026-01-01T00:00:00Z"),
            event("bad", "2026-01-05T01:00:00Z", rating=8),
        ],
    )
    with LakehousePipeline(tmp_path / "lakehouse.duckdb") as pipeline:
        first = pipeline.run(source, watermark_hours=24)
        second = pipeline.run(source, watermark_hours=24)
        assert first["counts"]["silver_feedback"] == 2
        assert first["counts"]["quarantined_events"] == 1
        assert first["counts"]["late_events"] == 1
        assert second["ingest"]["skipped_file"] is True
        assert second["ingest"]["records_inserted"] == 0
        assert all(pipeline.quality_checks().values())


def test_newer_update_wins_and_stale_update_is_recorded(tmp_path: Path) -> None:
    first_file = tmp_path / "first.jsonl"
    second_file = tmp_path / "second.jsonl"
    stale_file = tmp_path / "stale.jsonl"
    write_jsonl(first_file, [event("review-1", "2026-01-01T00:00:00Z", rating=2)])
    write_jsonl(
        second_file,
        [event("review-1", "2026-01-01T00:00:00Z", rating=5, updated_at="2026-01-02T00:00:00Z")],
    )
    write_jsonl(
        stale_file,
        [event("review-1", "2026-01-01T00:00:00Z", rating=1, updated_at="2026-01-01T12:00:00Z")],
    )
    with LakehousePipeline(tmp_path / "lakehouse.duckdb") as pipeline:
        pipeline.run(first_file)
        pipeline.run(second_file)
        stale = pipeline.run(stale_file)
        rating = pipeline.connection.execute(
            "select rating from silver.feedback where review_id = 'review-1'"
        ).fetchone()[0]
        assert rating == 5
        assert stale["transform"]["stale_updates"] == 1
