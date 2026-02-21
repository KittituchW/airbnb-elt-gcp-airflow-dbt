{{ config(materialized='table', schema='silver') }}

with raw as (
  select * from {{ source('bronze','r_f_listing') }}
),
clean as (
  select
    -- ids
    "LISTING_ID"::bigint  as listing_id,
    "SCRAPE_ID"::bigint   as scrape_id,
    "HOST_ID"::bigint     as host_id,

    -- dates (TEXT -> DATE)
    case
      when "SCRAPED_DATE" ~ '^\d{4}-\d{2}-\d{2}$'
        then "SCRAPED_DATE"::date
      when "SCRAPED_DATE" ~ '^\d{1,2}[./-]\d{1,2}[./-]\d{4}$'
        then to_date(regexp_replace("SCRAPED_DATE",'[./]','-','g'),'DD-MM-YYYY')
      else null
    end as scraped_date,

    case
      when "HOST_SINCE" ~ '^\d{4}-\d{2}-\d{2}$'
        then "HOST_SINCE"::date
      when "HOST_SINCE" ~ '^\d{1,2}[./-]\d{1,2}[./-]\d{4}$'
        then to_date(regexp_replace("HOST_SINCE",'[./]','-','g'),'DD-MM-YYYY')
      else null
    end as host_since,

    -- booleans (t/f, true/false, 1/0, yes/no)
    case
      when lower(coalesce("HOST_IS_SUPERHOST", '')) in ('t','true','1','y','yes') then true
      when lower(coalesce("HOST_IS_SUPERHOST", '')) in ('f','false','0','n','no') then false
      else null
    end as host_is_superhost,

    case
      when lower(coalesce("HAS_AVAILABILITY", '')) in ('t','true','1','y','yes') then true
      when lower(coalesce("HAS_AVAILABILITY", '')) in ('f','false','0','n','no') then false
      else null
    end as has_availability,

    -- text normalized
    upper(trim("HOST_NAME"))               as host_name,
    upper(trim("HOST_NEIGHBOURHOOD"))      as host_neighbourhood,
    upper(trim("LISTING_NEIGHBOURHOOD"))   as listing_neighbourhood,
    upper(trim("PROPERTY_TYPE"))           as property_type,
    upper(trim("ROOM_TYPE"))               as room_type,

    -- numbers
    "ACCOMMODATES"::int                    as accommodates,
    nullif(regexp_replace(coalesce("PRICE"::text,''),'[^0-9\.\-]','','g'),'')::numeric as price,
    "AVAILABILITY_30"::int                 as availability_30,
    "NUMBER_OF_REVIEWS"::int               as number_of_reviews,
    "REVIEW_SCORES_RATING"::numeric        as review_scores_rating,
    "REVIEW_SCORES_ACCURACY"::numeric      as review_scores_accuracy,
    "REVIEW_SCORES_CLEANLINESS"::numeric   as review_scores_cleanliness,
    "REVIEW_SCORES_CHECKIN"::numeric       as review_scores_checkin,
    "REVIEW_SCORES_COMMUNICATION"::numeric as review_scores_communication,
    "REVIEW_SCORES_VALUE"::numeric         as review_scores_value,

    -- month date from "MM_YYYY"
    case
      when "MONTH_YEAR" ~ '^\d{2}_\d{4}$' then to_date('01_' || "MONTH_YEAR",'DD_MM_YYYY')
      else null
    end as month_date
  from raw
)
select * from clean
