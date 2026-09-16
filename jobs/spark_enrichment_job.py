"""Finite Spark job that enriches Silver feedback with aspect sentiment."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType

from feedback_lakehouse.sentiment import score_aspects

RESULT_SCHEMA = ArrayType(
    StructType(
        [
            StructField("aspect", StringType(), False),
            StructField("sentiment_score", DoubleType(), False),
            StructField("sentiment_label", StringType(), False),
            StructField("evidence", StringType(), False),
        ]
    )
)


def main() -> None:
    spark = SparkSession.builder.appName("feedback-aspect-enrichment").getOrCreate()
    catalog = os.getenv("ICEBERG_CATALOG", "lakehouse")
    source = spark.table(f"{catalog}.silver.feedback")

    @F.udf(returnType=RESULT_SCHEMA)
    def extract(text: str) -> list[dict[str, str | float]]:
        return [result.as_dict() for result in score_aspects(text or "")]

    enriched = (
        source.select("review_id", "product_id", "created_at", "review_text")
        .withColumn("result", F.explode_outer(extract("review_text")))
        .filter("result is not null")
        .select(
            "review_id",
            "product_id",
            "created_at",
            F.col("result.aspect").alias("aspect"),
            F.col("result.sentiment_score").alias("sentiment_score"),
            F.col("result.sentiment_label").alias("sentiment_label"),
            F.col("result.evidence").alias("evidence"),
            F.current_timestamp().alias("processed_at"),
        )
    )
    spark.sql(f"create namespace if not exists {catalog}.gold")
    (
        enriched.writeTo(f"{catalog}.gold.aspect_sentiment")
        .using("iceberg")
        .partitionedBy(F.days("created_at"), "product_id")
        .createOrReplace()
    )
    spark.stop()


if __name__ == "__main__":
    main()
