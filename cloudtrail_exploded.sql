/*
    Exploded the raw CloudTrail Event table
*/

{{ config(
    materialized='incremental',
    file_format='parquet',
    partition_by='region,dt',
    unique_key='eventId') }}

with exploded_view as (
    select explode(records) as arr, region, dt
    from cloudtrail_raw.cloudtrail_event
),
compact_view as (
    select
        arr.errorCode,
        arr.errorMessage,
        arr.eventName,
        arr.eventId,
        arr.eventSource,
        arr.eventType,
        arr.eventTime,
        arr.userIdentity.principalId,
        arr.sourceIPAddress,
        arr.requestParameters,
        arr.responseElements,
        region,
        dt
    from exploded_view

{% if is_incremental() %}
    where dt >= (select max(dt) from {{ this }})
{% endif %}

)
select /*+ COALESCE(4) */ * from compact_view