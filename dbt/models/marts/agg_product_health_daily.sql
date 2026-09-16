select
    event_date,
    product_id,
    source,
    count(*) as review_count,
    round(avg(rating), 2) as average_rating,
    round(100.0 * sum(case when rating <= 2 then 1 else 0 end) / count(*), 2)
        as negative_review_pct,
    sum(case when is_late then 1 else 0 end) as late_event_count
from {{ ref('fct_feedback') }}
group by 1, 2, 3
