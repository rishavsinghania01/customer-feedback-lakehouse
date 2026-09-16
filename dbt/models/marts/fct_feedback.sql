{{ config(materialized='incremental', unique_key='review_id', incremental_strategy='delete+insert') }}

select *
from {{ ref('stg_feedback') }}
{% if is_incremental() %}
where updated_at >= (select coalesce(max(updated_at), timestamp '1900-01-01') from {{ this }})
{% endif %}
