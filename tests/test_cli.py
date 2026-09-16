import json
from pathlib import Path

from typer.testing import CliRunner

from feedback_lakehouse.cli import app

runner = CliRunner()


def write_source(path: Path) -> None:
    payload = {
        "review_id": "cli-review",
        "product_id": "cli-product",
        "rating": 5,
        "review_text": "The quality is excellent.",
        "source": "cli",
        "created_at": "2026-01-01T00:00:00Z",
        "schema_version": 1,
    }
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")


def test_run_and_report_commands(tmp_path: Path) -> None:
    source = tmp_path / "feedback.jsonl"
    database = tmp_path / "lakehouse.duckdb"
    write_source(source)

    result = runner.invoke(
        app,
        ["run", str(source), "--database", str(database), "--watermark-hours", "12"],
    )
    assert result.exit_code == 0, result.output
    assert '"silver_feedback": 1' in result.output

    report = runner.invoke(app, ["report", "--database", str(database)])
    assert report.exit_code == 0, report.output
    assert '"all_bronze_processed": true' in report.output


def test_report_exits_non_zero_when_a_quality_check_fails(tmp_path: Path) -> None:
    source = tmp_path / "feedback.jsonl"
    database = tmp_path / "lakehouse.duckdb"
    write_source(source)
    assert runner.invoke(app, ["run", str(source), "--database", str(database)]).exit_code == 0

    # Break an invariant behind the pipeline's back: an aspect row with no parent review.
    import duckdb

    connection = duckdb.connect(str(database))
    connection.execute(
        "insert into silver.aspect_sentiment "
        "values ('orphan', 'camera', 0.5, 'positive', 'x', now())"
    )
    connection.close()

    report = runner.invoke(app, ["report", "--database", str(database)])
    assert report.exit_code == 1, report.output
    assert '"no_orphan_aspects": false' in report.output


def test_demo_command_proves_duplicate_file_idempotency(tmp_path: Path) -> None:
    source = tmp_path / "feedback.jsonl"
    database = tmp_path / "demo.duckdb"
    write_source(source)

    result = runner.invoke(
        app,
        ["demo", "--database", str(database), "--source", str(source)],
    )
    assert result.exit_code == 0, result.output
    assert '"skipped_file": true' in result.output
