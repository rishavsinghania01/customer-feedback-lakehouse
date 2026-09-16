# ADR 001: Use Apache Iceberg for analytical storage

## Status

Accepted.

## Context

The pipeline needs idempotent updates, schema evolution, partition evolution, snapshot inspection and safe backfills on object storage. Plain Parquet files do not provide table-level transactions or a reliable current-state view.

## Decision

Use Apache Iceberg tables on object storage, registered through a REST catalog locally and AWS Glue Catalog in the cloud. Partition Silver feedback by event day and product, while retaining the original event timestamp.

## Consequences

- Merge-based updates and snapshot rollback are available.
- Compaction and snapshot expiration become explicit maintenance tasks.
- Spark and query engines must use compatible Iceberg versions.
- Partitioning can evolve without rewriting consumer queries.
