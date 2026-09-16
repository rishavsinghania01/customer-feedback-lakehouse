"""The Spark jobs, run for real on a local SparkSession with a local Iceberg catalog.

PySpark is an optional extra (`pip install -e ".[spark]"`) and a JVM is needed,
so this module skips itself where they are absent. CI installs both and runs
it in its own job; the streaming job's transformation, the Iceberg merge and
the enrichment UDF are exercised here exactly as the production job calls them,
minus Kafka.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark", reason="pyspark is an optional extra")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

JOBS_DIR = str(Path(__file__).resolve().parents[1] / "jobs")
sys.path.insert(0, JOBS_DIR)

import spark_enrichment_job as enrichment  # noqa: E402
import spark_streaming_job as streaming  # noqa: E402

CATALOG = "lakehouse"

RAW_SCHEMA = StructType(
    [
        StructField("value", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
    ]
)


def iceberg_package() -> str:
    """The Iceberg runtime that matches the installed Spark, unless overridden."""
    override = os.getenv("ICEBERG_SPARK_PACKAGE")
    if override:
        return override
    major, minor = pyspark.__version__.split(".")[:2]
    if major == "3":
        # Same versions as the compose image, tabulario/spark-iceberg:3.5.5_1.8.1.
        return f"org.apache.iceberg:iceberg-spark-runtime-{major}.{minor}_2.12:1.8.1"
    return f"org.apache.iceberg:iceberg-spark-runtime-{major}.{minor}_2.13:1.10.2"


@pytest.fixture(scope="module")
def spark(tmp_path_factory: pytest.TempPathFactory) -> SparkSession:
    # Timestamps cross the Python/JVM boundary as naive local-time values, so both
    # sides are pinned to UTC or every assertion below would depend on the machine.
    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):
        time.tzset()
    # Executors must run the same interpreter as the driver, not whatever `python3` is on PATH.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    warehouse = tmp_path_factory.mktemp("warehouse")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("feedback-lakehouse-tests")
        .config("spark.jars.packages", iceberg_package())
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse.as_uri())
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def ts(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC).replace(tzinfo=None)


def payload(review_id: str, drop: tuple[str, ...] = (), **overrides: object) -> str:
    """A contract-shaped JSON payload; `drop` leaves fields out entirely."""
    event: dict[str, object] = {
        "review_id": review_id,
        "product_id": "phone-42",
        "rating": 4,
        "review_text": "The camera is excellent, but the battery drains quickly.",
        "source": "web",
        "created_at": "2026-01-05T10:00:00Z",
        "schema_version": 1,
    }
    event.update(overrides)
    for key in drop:
        del event[key]
    return json.dumps(event)


def raw_frame(spark: SparkSession, rows: list[tuple[str, int, int, str]]):
    """Rows of (json, partition, offset, kafka timestamp) shaped like the Kafka source."""
    return spark.createDataFrame(
        [(value, partition, offset, ts(stamp)) for value, partition, offset, stamp in rows],
        RAW_SCHEMA,
    )


KAFKA_TIME = "2026-01-05T10:05:00Z"


def test_every_row_lands_in_exactly_one_stream_with_a_named_reason(spark: SparkSession) -> None:
    rows = [
        (payload("ok"), 0, 1, KAFKA_TIME),
        (payload("rating-too-high", rating=8), 0, 2, KAFKA_TIME),
        (payload("rating-missing", drop=("rating",)), 0, 3, KAFKA_TIME),
        (payload("wrong-version", schema_version=2), 0, 4, KAFKA_TIME),
        (payload("no-review-id", drop=("review_id",)), 0, 5, KAFKA_TIME),
        (payload("no-created-at", drop=("created_at",)), 0, 6, KAFKA_TIME),
        ("{this is not json", 0, 7, KAFKA_TIME),
        ("{}", 0, 8, KAFKA_TIME),
    ]
    parsed = streaming.parse_events(raw_frame(spark, rows))
    valid, invalid = streaming.validate(parsed)

    assert [r.review_id for r in valid.select("event.review_id").collect()] == ["ok"]
    reasons = {r.offset: r.error_reason for r in invalid.select("offset", "error_reason").collect()}
    assert reasons == {
        2: "rating must be between 1 and 5",
        # Offset 3 used to be lost: a null rating made the whole predicate null,
        # and a null passes neither `filter(required)` nor `filter(~required)`.
        3: "rating must be between 1 and 5",
        4: "unsupported schema_version",
        5: "review_id is required",
        6: "created_at is required",
        7: "payload is not valid JSON",
        8: "review_id is required",
    }
    assert valid.count() + invalid.count() == len(rows)


def test_event_hash_is_of_the_raw_bytes(spark: SparkSession) -> None:
    same = payload("dup")
    rows = [(same, 0, 1, KAFKA_TIME), (same, 1, 9, KAFKA_TIME), (same + " ", 0, 2, KAFKA_TIME)]
    hashes = [r.event_hash for r in streaming.parse_events(raw_frame(spark, rows)).collect()]
    assert hashes[0] == hashes[1], "an identical redelivery hashes identically"
    assert hashes[0] != hashes[2], "a byte-different payload is a different event"


def test_valid_batches_merge_newer_updates_and_ignore_stale_ones(spark: SparkSession) -> None:
    streaming.ensure_tables(spark, CATALOG)

    def process(rows: list[tuple[str, int, int, str]]) -> None:
        valid, _ = streaming.validate(streaming.parse_events(raw_frame(spark, rows)))
        streaming.process_valid_batch(valid, batch_id=0, catalog=CATALOG)

    def silver() -> dict[str, tuple[int, datetime]]:
        rows = spark.table(f"{CATALOG}.silver.feedback").collect()
        return {r.review_id: (r.rating, r.updated_at) for r in rows}

    # Batch 1: two new reviews, no updated_at, so updated_at defaults to created_at.
    process(
        [(payload("r1", rating=2), 0, 1, KAFKA_TIME), (payload("r2", rating=4), 0, 2, KAFKA_TIME)]
    )
    created = ts("2026-01-05T10:00:00Z")
    assert silver() == {"r1": (2, created), "r2": (4, created)}

    # Batch 2: a newer update to r1 replaces the current state.
    newer = payload("r1", rating=5, updated_at="2026-01-06T00:00:00Z")
    process([(newer, 0, 3, "2026-01-06T00:01:00Z")])
    assert silver()["r1"] == (5, ts("2026-01-06T00:00:00Z"))

    # Batch 3: an older update to r1 arrives late and must not win.
    stale = payload("r1", rating=1, updated_at="2026-01-05T12:00:00Z")
    process([(stale, 0, 4, "2026-01-06T00:02:00Z")])
    assert silver()["r1"] == (5, ts("2026-01-06T00:00:00Z"))

    # Batch 4: two versions of r3 in one micro-batch with the same updated_at.
    # MERGE needs one source row per key; the higher Kafka offset is the later write.
    same_time = {"updated_at": "2026-01-07T00:00:00Z"}
    process(
        [
            (payload("r3", rating=1, **same_time), 0, 10, "2026-01-07T00:01:00Z"),
            (payload("r3", rating=3, **same_time), 0, 11, "2026-01-07T00:01:00Z"),
        ]
    )
    assert silver()["r3"] == (3, ts("2026-01-07T00:00:00Z"))

    # Bronze is append-only: every valid event that arrived is there, including the stale one.
    assert spark.table(f"{CATALOG}.bronze.feedback_events").count() == 6
    assert len(silver()) == 3, "one current row per review_id"


def test_late_flag_compares_event_time_with_broker_time(spark: SparkSession) -> None:
    streaming.ensure_tables(spark, CATALOG)
    rows = [
        (payload("prompt", created_at="2026-02-01T09:00:00Z"), 0, 20, "2026-02-01T09:30:00Z"),
        (payload("late", created_at="2026-01-20T09:00:00Z"), 0, 21, "2026-02-01T09:30:00Z"),
    ]
    valid, _ = streaming.validate(streaming.parse_events(raw_frame(spark, rows)))
    streaming.process_valid_batch(valid, batch_id=1, catalog=CATALOG)
    flags = {
        r.review_id: r.is_late
        for r in spark.table(f"{CATALOG}.silver.feedback")
        .filter("review_id in ('prompt', 'late')")
        .collect()
    }
    assert flags == {"prompt": False, "late": True}


def test_invalid_batches_are_quarantined_with_payload_and_reason(spark: SparkSession) -> None:
    streaming.ensure_tables(spark, CATALOG)
    rows = [(payload("bad", rating=0), 2, 30, KAFKA_TIME), ("garbage", 2, 31, KAFKA_TIME)]
    _, invalid = streaming.validate(streaming.parse_events(raw_frame(spark, rows)))
    streaming.process_invalid_batch(invalid, batch_id=2, catalog=CATALOG)
    quarantined = {
        r.payload_json: (r.error_reason, r.kafka_partition, r.kafka_offset)
        for r in spark.table(f"{CATALOG}.quarantine.feedback_events").collect()
    }
    assert quarantined[payload("bad", rating=0)] == ("rating must be between 1 and 5", 2, 30)
    assert quarantined["garbage"] == ("payload is not valid JSON", 2, 31)


def test_enrichment_explodes_aspects_and_keeps_the_evidence_clause(spark: SparkSession) -> None:
    source = spark.createDataFrame(
        [
            (
                "r1",
                "phone-42",
                ts("2026-01-05T10:00:00Z"),
                "The camera is great but the battery drains.",
            ),
            ("r2", "phone-42", ts("2026-01-05T11:00:00Z"), "Nothing relevant is discussed here."),
        ],
        "review_id string, product_id string, created_at timestamp, review_text string",
    )
    rows = enrichment.enrich(source).collect()
    by_aspect = {r.aspect: r for r in rows}
    assert {r.review_id for r in rows} == {"r1"}, "a review with no aspect yields no rows"
    assert by_aspect["camera"].sentiment_label == "positive"
    assert by_aspect["battery"].sentiment_label == "negative"
    assert "battery drains" in by_aspect["battery"].evidence
