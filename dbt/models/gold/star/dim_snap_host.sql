{{ config(materialized='table', schema='gold') }}

with src as (
  select
    host_id, host_name, host_since, host_is_superhost, host_neighbourhood,
    scraped_date, dbt_valid_from, dbt_valid_to
  from {{ ref('snap_host') }}
  where host_id is not null
),
typed as (
  select
    *,
    case when dbt_valid_from is null or dbt_valid_from::text = '' then null else dbt_valid_from::timestamp end as valid_from_ts,
    case when dbt_valid_to   is null or dbt_valid_to::text   = '' then null else dbt_valid_to::timestamp   end as valid_to_ts
  from src
),
scd2 as (
  select
    md5(concat_ws('|', host_id::text, coalesce(host_neighbourhood,'UNKNOWN'), coalesce(dbt_valid_from::text,''))) as host_version_sk,
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    host_neighbourhood,
    dbt_valid_from,
    dbt_valid_to,
    (dbt_valid_to is null)::boolean as is_current,
    date_trunc('month', coalesce(valid_from_ts, timestamp '1970-01-01'))::date as valid_from_month,
    date_trunc('month', coalesce(valid_to_ts,   timestamp '9999-12-31') + interval '1 month')::date as valid_to_month,
    scraped_date
  from typed
)
select * from scd2