# Demonstration script

## Deterministic pipeline

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,dashboard]"
make demo
make dbt
make test
```

The sample deliberately contains a late event and an invalid rating. The first run should create five Silver reviews, one quarantined event and one late-event marker. The immediate second run should recognise the identical source-file hash and insert nothing.

## Live event path

```bash
docker compose up -d redpanda redpanda-init minio minio-init iceberg-rest ingestion-api
docker compose --profile streaming up spark-stream
```

Publish a valid event:

```bash
curl -X POST http://localhost:8000/v1/feedback \
  -H 'content-type: application/json' \
  -d '{"review_id":"live-1","product_id":"phone-42","rating":2,"review_text":"The camera is great but the battery drains quickly.","source":"demo","created_at":"2026-09-16T10:00:00Z","schema_version":1}'
```

Then publish the same payload again, an out-of-range rating, and an event older than the watermark. Inspect the topic at `http://localhost:8080`, MinIO at `http://localhost:9001`, and the API contract at `http://localhost:8000/v1/contract`.
