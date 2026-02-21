{{ config(materialized='table', schema='gold') }}

with src as (
  select
    listing_id, host_id, property_type, room_type, accommodates, listing_neighbourhood,
    price, has_availability, availability_30, number_of_reviews,
    review_scores_rating, review_scores_accuracy, review_scores_cleanliness,
    review_scores_checkin, review_scores_communication, review_scores_value,
    scraped_date, dbt_valid_from, dbt_valid_to
  from {{ ref('snap_listing') }}
  where listing_id is not null
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
    md5(concat_ws('|', listing_id::text, coalesce(dbt_valid_from::text,''))) as listing_version_sk,
    listing_id,
    host_id,
    property_type,
    room_type,
    accommodates,
    listing_neighbourhood,
    price,
    has_availability,
    availability_30,
    number_of_reviews,
    review_scores_rating,
    review_scores_accuracy,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_value,
    dbt_valid_from,
    dbt_valid_to,
    (dbt_valid_to is null)::boolean as is_current,
    date_trunc('month', coalesce(valid_from_ts, timestamp '1970-01-01'))::date as valid_from_month,
    date_trunc('month', coalesce(valid_to_ts,   timestamp '9999-12-31') + interval '1 month')::date as valid_to_month,
    scraped_date
  from typed
)
select * from scd2