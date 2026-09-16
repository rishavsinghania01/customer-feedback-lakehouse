select
    a.review_id,
    f.product_id,
    f.event_date,
    a.aspect,
    a.sentiment_score,
    a.sentiment_label,
    a.evidence,
    a.processed_at
from {{ source('lakehouse', 'aspect_sentiment') }} a
inner join {{ ref('stg_feedback') }} f using (review_id)
