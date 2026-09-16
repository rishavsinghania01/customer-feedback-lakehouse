# Architecture

## Data path

1. Historical CSV, JSON and JSONL files land in an immutable raw bucket.
2. Live feedback is validated at the API boundary and written to Kafka locally or Kinesis on AWS.
3. Spark Structured Streaming parses the versioned contract and separates valid and invalid events.
4. Valid events are appended to Bronze and merged into Silver by `review_id` and `updated_at`.
5. Invalid events keep their original payload and validation error in a quarantine table.
6. A finite Spark job extracts aspect-level sentiment from validated review text.
7. Airflow coordinates enrichment, Iceberg file compaction, dbt builds and quality gates.
8. dbt publishes review facts, aspect facts and daily product-health marts.

## Storage layers

| Layer | Purpose | Mutation policy |
|---|---|---|
| Raw | Original source objects | Immutable |
| Bronze | Kafka payload plus source offsets | Append only |
| Silver | Validated current review state | Merge by business key |
| Quarantine | Rejected payload plus reason | Append, then mark replayed |
| Gold | Consumer-facing facts and aggregates | Rebuilt or incrementally merged |

## Correctness invariants

- One current Silver row per `review_id`.
- An older update never replaces a newer review state.
- Reprocessing the same source file does not create new Bronze events.
- Every Bronze event reaches either an accepted, quarantined or stale-update outcome.
- Quarantine preserves the original payload so a corrected event can be replayed.
- Aspect rows cannot exist without their parent review.

## Local and AWS parity

The local deterministic pipeline uses DuckDB so CI can verify the complete data contract without cloud credentials. The streaming path uses Redpanda, Spark and Iceberg with MinIO. On AWS, the equivalent services are Kinesis or MSK, EMR Serverless, Iceberg on S3, Glue Catalog and Athena. The contract and layer boundaries stay the same across environments.
