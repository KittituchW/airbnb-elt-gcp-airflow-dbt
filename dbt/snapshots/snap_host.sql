{% snapshot snap_host %}
{{
  config(
    target_schema='silver',
    unique_key='host_id',
    strategy='check',
    updated_at='scraped_date',
    check_cols=['host_name','host_is_superhost','host_neighbourhood'],
    invalidate_hard_deletes=true
  )
}}

with base as (
  select
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    -- 1) null/blank guard on host_neighbourhood
    nullif(trim(host_neighbourhood), '') as host_neighbourhood_raw,
    -- 2) fallback to listing_neighbourhood if host_neighbourhood is null/blank
    nullif(trim(listing_neighbourhood), '') as listing_neighbourhood_raw,
    scraped_date
  from {{ ref('stg_listing') }}
  where host_id is not null
    and scraped_date is not null
),
normalized as (
  select
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    -- 3) choose fallback then normalize: collapse spaces + uppercase
    upper(regexp_replace(
      coalesce(host_neighbourhood_raw, listing_neighbourhood_raw),
      '\s+', ' ', 'g'
    )) as host_neighbourhood,
    scraped_date
  from base
),
ranked as (
  select
    *,
    row_number() over (
      partition by host_id, scraped_date
      order by scraped_date desc
    ) as rn
  from normalized
)
select
  host_id,
  host_name,
  host_since,
  host_is_superhost,
  host_neighbourhood,   
  scraped_date
from ranked
where rn = 1
{% endsnapshot %}
