{{ config(materialized='table', schema='gold') }}

select
  listing_id,
  host_id,
  month_date,
  has_availability,
  availability_30,
  nullif(price, 0) as price,
  case
    when has_availability then greatest(0, 30 - coalesce(availability_30, 0))
    else 0
  end as stays,
  number_of_reviews,
  review_scores_rating
from {{ ref('stg_listing') }}
where month_date is not null
  and listing_id is not null
