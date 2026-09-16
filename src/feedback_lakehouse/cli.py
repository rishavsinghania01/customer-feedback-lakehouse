"""Command-line entry point for local development and operations."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from .pipeline import LakehousePipeline

app = typer.Typer(no_args_is_help=True, help="Customer feedback lakehouse commands")


@app.command()
def run(
    source: Annotated[Path, typer.Argument(exists=True, readable=True)],
    database: Annotated[Path, typer.Option()] = Path("build/lakehouse.duckdb"),
    watermark_hours: Annotated[int, typer.Option(min=1)] = 24,
) -> None:
    """Incrementally ingest one file, transform new events and rebuild marts."""

    with LakehousePipeline(database) as pipeline:
        result = pipeline.run(source, watermark_hours)
        checks = pipeline.quality_checks()
    typer.echo(json.dumps({"run": result, "quality_checks": checks}, indent=2, default=str))
    if not all(checks.values()):
        raise typer.Exit(code=1)


@app.command()
def report(
    database: Annotated[Path, typer.Option()] = Path("build/lakehouse.duckdb"),
) -> None:
    """Print pipeline counts and data-quality status."""

    with LakehousePipeline(database) as pipeline:
        payload = {"counts": pipeline.counts(), "quality_checks": pipeline.quality_checks()}
    typer.echo(json.dumps(payload, indent=2))


@app.command()
def demo(
    database: Annotated[Path, typer.Option()] = Path("build/lakehouse.duckdb"),
    source: Annotated[Path, typer.Option()] = Path("data/sample_feedback.jsonl"),
) -> None:
    """Run the deterministic portfolio demonstration."""

    database.unlink(missing_ok=True)
    with LakehousePipeline(database) as pipeline:
        first = pipeline.run(source)
        second = pipeline.run(source)
        checks = pipeline.quality_checks()
        product_rows = pipeline.connection.execute(
            "select * from gold.product_health_daily order by event_date, product_id, source"
        ).fetchall()
        aspect_rows = pipeline.connection.execute(
            "select * from gold.aspect_health_daily order by event_date, product_id, aspect"
        ).fetchall()
    output = {
        "first_run": first,
        "duplicate_file_run": second,
        "quality_checks": checks,
        "product_health_rows": product_rows,
        "aspect_health_rows": aspect_rows,
    }
    typer.echo(json.dumps(output, indent=2, default=str))
    if not all(checks.values()):
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
