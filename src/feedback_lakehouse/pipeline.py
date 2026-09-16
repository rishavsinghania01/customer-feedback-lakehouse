"""Incremental local pipeline used for development, CI and demonstrations."""

from __future__ import annotations

import csv
import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
from pydantic import ValidationError

from .contracts import FeedbackEvent
from .sentiment import score_aspects


@dataclass(frozen=True)
class IngestResult:
    file_hash: str
    records_seen: int
    records_inserted: int
    skipped_file: bool


@dataclass(frozen=True)
class TransformResult:
    accepted: int
    quarantined: int
    stale_updates: int


class LakehousePipeline:
    """A local analogue of the cloud pipeline, backed by one DuckDB file."""

    def __init__(self, database: str | Path = "build/lakehouse.duckdb") -> None:
        self.database = Path(database)
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self.connection = duckdb.connect(str(self.database))
        self._initialise()

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> LakehousePipeline:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _initialise(self) -> None:
        for schema in ("control", "bronze", "silver", "gold", "quarantine"):
            self.connection.execute(f"create schema if not exists {schema}")

        self.connection.execute(
            """
            create table if not exists control.ingested_files (
                file_hash varchar primary key,
                file_path varchar not null,
                file_size bigint not null,
                records_seen integer not null,
                ingested_at timestamptz not null
            )
            """
        )
        self.connection.execute(
            """
            create table if not exists control.processed_events (
                event_hash varchar primary key,
                outcome varchar not null,
                processed_at timestamptz not null
            )
            """
        )
        self.connection.execute(
            """
            create table if not exists bronze.feedback_events (
                event_hash varchar primary key,
                file_hash varchar not null,
                line_number integer not null,
                payload_json varchar not null,
                ingested_at timestamptz not null
            )
            """
        )
        self.connection.execute(
            """
            create table if not exists quarantine.feedback_events (
                event_hash varchar primary key,
                payload_json varchar not null,
                error_message varchar not null,
                quarantined_at timestamptz not null,
                replayed_at timestamptz
            )
            """
        )
        self.connection.execute(
            """
            create table if not exists silver.feedback (
                review_id varchar primary key,
                product_id varchar not null,
                rating integer not null,
                review_text varchar not null,
                source varchar not null,
                created_at timestamptz not null,
                updated_at timestamptz not null,
                schema_version integer not null,
                customer_id_hash varchar,
                event_hash varchar not null,
                is_late boolean not null,
                processed_at timestamptz not null
            )
            """
        )
        self.connection.execute(
            """
            create table if not exists silver.aspect_sentiment (
                review_id varchar not null,
                aspect varchar not null,
                sentiment_score double not null,
                sentiment_label varchar not null,
                evidence varchar not null,
                processed_at timestamptz not null,
                primary key (review_id, aspect)
            )
            """
        )

    @staticmethod
    def _file_hash(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _event_hash(payload: dict[str, Any]) -> str:
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @staticmethod
    def _read_records(path: Path) -> Iterable[tuple[int, dict[str, Any]]]:
        suffix = path.suffix.lower()
        if suffix in {".jsonl", ".ndjson"}:
            with path.open(encoding="utf-8") as source:
                for line_number, line in enumerate(source, start=1):
                    if line.strip():
                        yield line_number, json.loads(line)
            return
        if suffix == ".csv":
            with path.open(encoding="utf-8", newline="") as source:
                for line_number, row in enumerate(csv.DictReader(source), start=2):
                    yield line_number, dict(row)
            return
        if suffix == ".json":
            payload = json.loads(path.read_text(encoding="utf-8"))
            records = payload if isinstance(payload, list) else [payload]
            for line_number, row in enumerate(records, start=1):
                yield line_number, row
            return
        raise ValueError("supported file types are .csv, .json, .jsonl and .ndjson")

    def ingest_file(self, path: str | Path) -> IngestResult:
        source_path = Path(path)
        if not source_path.is_file():
            raise FileNotFoundError(source_path)
        file_hash = self._file_hash(source_path)
        exists = self.connection.execute(
            "select 1 from control.ingested_files where file_hash = ?", [file_hash]
        ).fetchone()
        if exists:
            seen = self.connection.execute(
                "select records_seen from control.ingested_files where file_hash = ?", [file_hash]
            ).fetchone()[0]
            return IngestResult(file_hash, seen, 0, True)

        records_seen = 0
        inserted = 0
        now = datetime.now(UTC)
        for line_number, payload in self._read_records(source_path):
            records_seen += 1
            event_hash = self._event_hash(payload)
            payload_json = json.dumps(payload, sort_keys=True, default=str)
            before = self.connection.execute(
                "select count(*) from bronze.feedback_events where event_hash = ?", [event_hash]
            ).fetchone()[0]
            self.connection.execute(
                """
                insert into bronze.feedback_events values (?, ?, ?, ?, ?)
                on conflict (event_hash) do nothing
                """,
                [event_hash, file_hash, line_number, payload_json, now],
            )
            inserted += int(before == 0)

        self.connection.execute(
            "insert into control.ingested_files values (?, ?, ?, ?, ?)",
            [file_hash, str(source_path.resolve()), source_path.stat().st_size, records_seen, now],
        )
        return IngestResult(file_hash, records_seen, inserted, False)

    def transform_pending(self, watermark_hours: int = 24) -> TransformResult:
        pending = self.connection.execute(
            """
            select b.event_hash, b.payload_json
            from bronze.feedback_events b
            left join control.processed_events p using (event_hash)
            where p.event_hash is null
            order by b.ingested_at, b.line_number
            """
        ).fetchall()
        max_event_time = self.connection.execute(
            "select max(created_at) from silver.feedback"
        ).fetchone()[0]
        accepted = quarantined = stale_updates = 0

        for event_hash, payload_json in pending:
            processed_at = datetime.now(UTC)
            try:
                event = FeedbackEvent.model_validate_json(payload_json)
            except (ValidationError, ValueError) as exc:
                message = str(exc).replace("\n", " ")[:1000]
                self.connection.execute(
                    """
                    insert into quarantine.feedback_events values (?, ?, ?, ?, null)
                    on conflict (event_hash) do update set error_message = excluded.error_message
                    """,
                    [event_hash, payload_json, message, processed_at],
                )
                self._mark_processed(event_hash, "quarantined", processed_at)
                quarantined += 1
                continue

            existing = self.connection.execute(
                "select updated_at from silver.feedback where review_id = ?", [event.review_id]
            ).fetchone()
            if existing and existing[0] > event.effective_updated_at:
                self._mark_processed(event_hash, "stale_update", processed_at)
                stale_updates += 1
                continue

            current_max = max_event_time
            is_late = bool(
                current_max and event.created_at < current_max - timedelta(hours=watermark_hours)
            )
            self.connection.execute(
                """
                insert or replace into silver.feedback values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    event.review_id,
                    event.product_id,
                    event.rating,
                    event.review_text,
                    event.source,
                    event.created_at,
                    event.effective_updated_at,
                    event.schema_version,
                    event.customer_id_hash,
                    event_hash,
                    is_late,
                    processed_at,
                ],
            )
            self.connection.execute(
                "delete from silver.aspect_sentiment where review_id = ?", [event.review_id]
            )
            for result in score_aspects(event.review_text):
                self.connection.execute(
                    "insert into silver.aspect_sentiment values (?, ?, ?, ?, ?, ?)",
                    [
                        event.review_id,
                        result.aspect,
                        result.sentiment_score,
                        result.sentiment_label,
                        result.evidence,
                        processed_at,
                    ],
                )
            self._mark_processed(event_hash, "accepted", processed_at)
            accepted += 1
            if max_event_time is None or event.created_at > max_event_time:
                max_event_time = event.created_at

        return TransformResult(accepted, quarantined, stale_updates)

    def _mark_processed(self, event_hash: str, outcome: str, processed_at: datetime) -> None:
        self.connection.execute(
            "insert into control.processed_events values (?, ?, ?)",
            [event_hash, outcome, processed_at],
        )

    def build_gold(self) -> None:
        self.connection.execute("drop table if exists gold.product_health_daily")
        self.connection.execute(
            """
            create table gold.product_health_daily as
            select
                cast(created_at as date) as event_date,
                product_id,
                source,
                count(*) as review_count,
                round(avg(rating), 2) as average_rating,
                round(100.0 * sum(case when rating <= 2 then 1 else 0 end) / count(*), 2)
                    as negative_review_pct,
                sum(case when is_late then 1 else 0 end) as late_event_count
            from silver.feedback
            group by 1, 2, 3
            """
        )
        self.connection.execute("drop table if exists gold.aspect_health_daily")
        self.connection.execute(
            """
            create table gold.aspect_health_daily as
            select
                cast(f.created_at as date) as event_date,
                f.product_id,
                a.aspect,
                count(*) as mention_count,
                round(avg(a.sentiment_score), 4) as average_sentiment,
                sum(case when a.sentiment_label = 'negative' then 1 else 0 end)
                    as negative_mentions
            from silver.aspect_sentiment a
            join silver.feedback f using (review_id)
            group by 1, 2, 3
            """
        )

    def run(self, path: str | Path, watermark_hours: int = 24) -> dict[str, Any]:
        ingest = self.ingest_file(path)
        transform = self.transform_pending(watermark_hours)
        self.build_gold()
        return {
            "ingest": ingest.__dict__,
            "transform": transform.__dict__,
            "counts": self.counts(),
        }

    def counts(self) -> dict[str, int]:
        relations = {
            "files": "control.ingested_files",
            "bronze_events": "bronze.feedback_events",
            "silver_feedback": "silver.feedback",
            "aspect_rows": "silver.aspect_sentiment",
            "quarantined_events": "quarantine.feedback_events",
            "late_events": "silver.feedback where is_late",
        }
        return {
            name: self.connection.execute(f"select count(*) from {relation}").fetchone()[0]
            for name, relation in relations.items()
        }

    def quality_checks(self) -> dict[str, bool]:
        checks = {
            "unique_review_ids": "select count(*) = count(distinct review_id) from silver.feedback",
            "ratings_in_range": (
                "select count(*) = 0 from silver.feedback where rating not between 1 and 5"
            ),
            "no_orphan_aspects": """
                select count(*) = 0
                from silver.aspect_sentiment a
                left join silver.feedback f using (review_id)
                where f.review_id is null
            """,
            "all_bronze_processed": """
                select count(*) = 0
                from bronze.feedback_events b
                left join control.processed_events p using (event_hash)
                where p.event_hash is null
            """,
        }
        return {
            name: bool(self.connection.execute(sql).fetchone()[0]) for name, sql in checks.items()
        }
