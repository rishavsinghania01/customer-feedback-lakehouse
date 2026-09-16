# Feedback event contract

The current schema version is `1`. Producers must include:

| Field | Type | Rule |
|---|---|---|
| `review_id` | string | Stable business key, 1–100 characters |
| `product_id` | string | Stable product key, 1–100 characters |
| `rating` | integer | Inclusive range 1–5 |
| `review_text` | string | 3–20,000 characters |
| `source` | string | Producing channel |
| `created_at` | timestamp | ISO 8601 with timezone |
| `schema_version` | integer | Must equal `1` |

Optional fields are `updated_at` and `customer_id_hash`. `updated_at` cannot precede `created_at`. Unknown fields are rejected rather than silently ignored.

## Evolution policy

- Adding an optional field is backward compatible within version 1.
- Renaming, removing or changing the meaning of a field requires version 2.
- Consumers must route unsupported versions to quarantine.
- A migration job must be supplied before version 1 producers are retired.

The live JSON Schema is available from `GET /v1/contract` on the ingestion API.
