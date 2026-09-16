select a.review_id
from {{ ref('fct_aspect_sentiment') }} a
left join {{ ref('fct_feedback') }} f using (review_id)
where f.review_id is null
