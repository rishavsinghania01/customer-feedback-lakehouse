# Operations runbook

## A producer is returning HTTP 503

1. Check `/health` on the ingestion API.
2. Check broker connectivity and topic health.
3. Do not ask producers to retry without stable `review_id` values.
4. Restore the broker, then allow bounded exponential retries.

## Kafka lag is increasing

1. Compare incoming records with processed records.
2. Check Spark executor failures and checkpoint access.
3. Scale stream executors or Kafka partitions only after identifying the bottleneck.
4. Confirm that a schema failure is not sending the entire stream to quarantine.

## Quarantine count increased

1. Group failures by `schema_version` and validation error.
2. Identify the producer and first failing offset.
3. Fix the producer or create an explicit contract migration.
4. Replay corrected events with the original business keys.
5. Reconcile source, accepted and quarantined counts.

## An update appears to be missing

1. Locate all events for the `review_id` in Bronze.
2. Compare `updated_at`, Kafka partition and offset.
3. Confirm whether the event was intentionally classified as stale.
4. Check the Silver Iceberg snapshot history before replaying anything.

## Safe backfill procedure

1. Record the source range and expected event count.
2. Write the backfill under a unique run identifier.
3. Run contract validation and reconciliation before merging into Silver.
4. Merge by `review_id` and `updated_at`; never append directly to the current-state table.
5. Run dbt tests and compare Gold metrics before and after the backfill.
