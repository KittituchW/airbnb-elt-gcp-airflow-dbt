{% snapshot snap_listing %}
{{
  config(
    target_schema = 'silver',
    unique_key    = 'listing_id',
    strategy      = 'timestamp',
    updated_at    = 'scraped_date'
  )
}}

-- Pull ALL attributes needed downstream (host_id, availability, price, reviews)
select
  listing_id,
  host_id,                                 
  property_type,
  room_type,
  accommodates,
  listing_neighbourhood,

  -- revenue/activeness inputs
  has_availability,
  availability_30,
  price,

  -- reviews
  number_of_reviews,
  review_scores_rating,
  review_scores_accuracy,
  review_scores_cleanliness,
  review_scores_checkin,
  review_scores_communication,
  review_scores_value,

  scraped_date
from {{ ref('stg_listing') }}
where listing_id is not null

{% endsnapshot %}
