select
    review_id,
    product_id,
    rating,
    review_text,
    source,
    cast(created_at as date) as event_date,
    created_at,
    updated_at,
    schema_version,
    customer_id_hash,
    event_hash,
    is_late,
    processed_at
from {{ source('lakehouse', 'feedback') }}
