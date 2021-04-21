{{ config(
    materialized='incremental',
    file_format='parquet',
    partition_by='dt',
    unique_key='principalId') }}

select /*+ COALESCE(1) */
    dt,
    eventSource,
    count(eventSource) as cnt
from {{ ref('cloudtrail_exploded') }}

{% if is_incremental() %}
and dt >= (select max(dt) from {{ this }})
{% endif %}

group by dt, eventSource
order by dt, eventSource desc