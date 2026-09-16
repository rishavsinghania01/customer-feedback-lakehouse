"""Kafka to Iceberg Structured Streaming job.

The long-running stream is deployed independently from Airflow. Airflow handles
finite maintenance jobs such as enrichment, compaction and quality checks.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

EVENT_SCHEMA = StructType(
    [
        StructField("review_id", StringType()),
        StructField("product_id", StringType()),
        StructField("rating", IntegerType()),
        StructField("review_text", StringType()),
        StructField("source", StringType()),
        StructField("created_at", TimestampType()),
        StructField("updated_at", TimestampType()),
        StructField("schema_version", IntegerType()),
        StructField("customer_id_hash", StringType()),
    ]
)


# from_json in PERMISSIVE mode puts an unparseable payload's text in this field
# instead of failing the row, which is how malformed JSON is told apart from a
# well-formed object that is merely missing fields.
CORRUPT_RECORD = "_corrupt_record"
PARSE_SCHEMA = StructType(EVENT_SCHEMA.fields + [StructField(CORRUPT_RECORD, StringType())])
PARSE_OPTIONS = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT_RECORD}

LATE_AFTER_HOURS = 24


def spark_session() -> SparkSession:
    return SparkSession.builder.appName("customer-feedback-stream").getOrCreate()


def ensure_tables(spark: SparkSession, catalog: str) -> None:
    spark.sql(f"create namespace if not exists {catalog}.bronze")
    spark.sql(f"create namespace if not exists {catalog}.silver")
    spark.sql(f"create namespace if not exists {catalog}.quarantine")
    spark.sql(
        f"""
        create table if not exists {catalog}.bronze.feedback_events (
            event_hash string,
            payload_json string,
            kafka_partition int,
            kafka_offset long,
            ingested_at timestamp
        ) using iceberg
        partitioned by (days(ingested_at))
        """
    )
    spark.sql(
        f"""
        create table if not exists {catalog}.silver.feedback (
            review_id string,
            product_id string,
            rating int,
            review_text string,
            source string,
            created_at timestamp,
            updated_at timestamp,
            schema_version int,
            customer_id_hash string,
            event_hash string,
            is_late boolean,
            processed_at timestamp
        ) using iceberg
        partitioned by (days(created_at), product_id)
        """
    )
    spark.sql(
        f"""
        create table if not exists {catalog}.quarantine.feedback_events (
            event_hash string,
            payload_json string,
            error_reason string,
            kafka_partition int,
            kafka_offset long,
            quarantined_at timestamp
        ) using iceberg
        partitioned by (days(quarantined_at))
        """
    )


def parse_events(raw: DataFrame) -> DataFrame:
    """Turn Kafka records (value, partition, offset, timestamp) into contract rows.

    The raw JSON is kept as payload_json and hashed before parsing, so Bronze and
    quarantine always hold exactly what arrived, and an identical redelivery has
    an identical event_hash whatever the parser makes of it.
    """
    return (
        raw.select(
            F.col("value").cast("string").alias("payload_json"),
            "partition",
            "offset",
            "timestamp",
        )
        .withColumn("event_hash", F.sha2("payload_json", 256))
        .withColumn("event", F.from_json("payload_json", PARSE_SCHEMA, PARSE_OPTIONS))
        .withColumn("parse_error", F.col(f"event.{CORRUPT_RECORD}"))
        .withColumn("event", F.col("event").dropFields(CORRUPT_RECORD))
    )


def validation_error(parsed: DataFrame) -> DataFrame:
    """Add error_reason: null for a valid row, the first failed rule otherwise.

    Every rule tests for null explicitly. In Spark SQL a comparison with null is
    null, not false, and a filter drops null rows on both sides of a negation, so
    a row whose rating was missing used to vanish from both the valid and the
    invalid stream. Naming the rule also gives the runbook something to group
    quarantine by.
    """
    rating = F.col("event.rating")
    version = F.col("event.schema_version")
    reason = (
        F.when(
            F.col("event").isNull() | F.col("parse_error").isNotNull(), "payload is not valid JSON"
        )
        .when(F.col("event.review_id").isNull(), "review_id is required")
        .when(F.col("event.product_id").isNull(), "product_id is required")
        .when(F.col("event.review_text").isNull(), "review_text is required")
        .when(F.col("event.source").isNull(), "source is required")
        .when(F.col("event.created_at").isNull(), "created_at is required")
        .when(rating.isNull() | ~rating.between(1, 5), "rating must be between 1 and 5")
        .when(version.isNull() | (version != 1), "unsupported schema_version")
    )
    return parsed.withColumn("error_reason", reason)


def validate(parsed: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split parsed rows into the valid and the quarantine stream.

    Every input row lands in exactly one of the two outputs.
    """
    checked = validation_error(parsed)
    valid = checked.filter(F.col("error_reason").isNull()).drop("error_reason")
    invalid = checked.filter(F.col("error_reason").isNotNull())
    return valid, invalid


def process_valid_batch(batch: DataFrame, batch_id: int, catalog: str) -> None:
    if batch.isEmpty():
        return
    prepared = (
        batch.select("event.*", "event_hash", "payload_json", "partition", "offset", "timestamp")
        .withColumn("updated_at", F.coalesce("updated_at", "created_at"))
        # Late means the review happened long before the broker received it: the
        # event time is more than the watermark behind the Kafka ingestion time.
        # Kept as a flag, never dropped, so late arrivals stay queryable and countable.
        .withColumn(
            "is_late",
            F.col("created_at")
            < F.col("timestamp") - F.expr(f"interval {LATE_AFTER_HOURS} hours"),
        )
        .withColumn("processed_at", F.current_timestamp())
    )
    prepared.select(
        "event_hash",
        "payload_json",
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("processed_at").alias("ingested_at"),
    ).writeTo(f"{catalog}.bronze.feedback_events").append()

    window = Window.partitionBy("review_id").orderBy(
        F.col("updated_at").desc(), F.col("offset").desc()
    )
    latest = prepared.withColumn("row_number", F.row_number().over(window)).filter("row_number = 1")
    latest.select(
        "review_id",
        "product_id",
        "rating",
        "review_text",
        "source",
        "created_at",
        "updated_at",
        "schema_version",
        "customer_id_hash",
        "event_hash",
        "is_late",
        "processed_at",
    ).createOrReplaceTempView("feedback_microbatch")

    spark = batch.sparkSession
    spark.sql(
        f"""
        merge into {catalog}.silver.feedback target
        using feedback_microbatch source
        on target.review_id = source.review_id
        when matched and source.updated_at >= target.updated_at then update set *
        when not matched then insert *
        """
    )


def process_invalid_batch(batch: DataFrame, batch_id: int, catalog: str) -> None:
    if batch.isEmpty():
        return
    batch.select(
        "event_hash",
        "payload_json",
        "error_reason",
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.current_timestamp().alias("quarantined_at"),
    ).writeTo(f"{catalog}.quarantine.feedback_events").append()


def main() -> None:
    spark = spark_session()
    catalog = os.getenv("ICEBERG_CATALOG", "lakehouse")
    topic = os.getenv("KAFKA_TOPIC", "feedback-events")
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    checkpoint_root = os.getenv("CHECKPOINT_ROOT", "s3a://warehouse/checkpoints")
    ensure_tables(spark, catalog)

    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("startingOffsets", os.getenv("STARTING_OFFSETS", "earliest"))
        .option("failOnDataLoss", "false")
        .load()
    )
    valid, invalid = validate(parse_events(kafka))
    valid = valid.withWatermark("timestamp", f"{LATE_AFTER_HOURS} hours").dropDuplicates(
        ["event_hash"]
    )

    valid_query = (
        valid.writeStream.foreachBatch(
            lambda batch, batch_id: process_valid_batch(batch, batch_id, catalog)
        )
        .option("checkpointLocation", f"{checkpoint_root}/valid")
        .queryName("feedback-valid-to-iceberg")
        .start()
    )
    invalid_query = (
        invalid.writeStream.foreachBatch(
            lambda batch, batch_id: process_invalid_batch(batch, batch_id, catalog)
        )
        .option("checkpointLocation", f"{checkpoint_root}/quarantine")
        .queryName("feedback-quarantine-to-iceberg")
        .start()
    )
    spark.streams.awaitAnyTermination()
    valid_query.stop()
    invalid_query.stop()


if __name__ == "__main__":
    main()
