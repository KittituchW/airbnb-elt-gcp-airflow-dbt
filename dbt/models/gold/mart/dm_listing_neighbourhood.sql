{{ config(
    materialized = 'view',
    schema       = 'gold',
    pre_hook     = ["set local work_mem='256MB'", "set local statement_timeout='0'"]
) }}

WITH listing_month AS (
  SELECT
    m.month_start::date AS month_start,
    l.listing_id,
    l.host_id,
    UPPER(TRIM(l.listing_neighbourhood))                 AS listing_neighbourhood,
    (l.has_availability IS TRUE)                         AS is_active,
    CASE WHEN l.has_availability
         THEN GREATEST(0, 30 - COALESCE(l.availability_30, 0))
         ELSE 0 END                                      AS stays,
    NULLIF(l.price, 0)::numeric                          AS price,
    NULLIF(l.review_scores_rating, 0)::numeric           AS review_scores_rating
  FROM {{ ref('dim_snap_listing') }} l
  CROSS JOIN LATERAL generate_series(
           GREATEST(date_trunc('month', l.valid_from_month)::date, DATE '2020-05-01'),
           LEAST(date_trunc('month', (l.valid_to_month - interval '1 day'))::date, DATE '2021-04-01'),
           interval '1 month'
       ) AS m(month_start)
),
host_month AS (
  SELECT
    m.month_start::date AS month_start,
    h.host_id,
    CASE
      WHEN h.host_is_superhost IS NULL THEN NULL
      ELSE LOWER(NULLIF(h.host_is_superhost::text, '')) IN ('t','true','yes','y','1')
    END AS is_superhost
  FROM {{ ref('dim_snap_host') }} h
  CROSS JOIN LATERAL generate_series(
           GREATEST(date_trunc('month', h.valid_from_month)::date, DATE '2020-05-01'),
           LEAST(date_trunc('month', (h.valid_to_month - interval '1 day'))::date, DATE '2021-04-01'),
           interval '1 month'
       ) AS m(month_start)
),
base AS (
  SELECT
    lm.month_start,
    lm.listing_neighbourhood,
    lm.listing_id,
    lm.host_id,
    lm.is_active,
    lm.stays,
    lm.price,
    lm.review_scores_rating,
    hm.is_superhost
  FROM listing_month lm
  LEFT JOIN host_month hm
    ON hm.host_id = lm.host_id
   AND hm.month_start = lm.month_start
),
-- Superhost rate by DISTINCT hosts (not listings)
host_flags AS (
  SELECT
    listing_neighbourhood,
    month_start,
    COUNT(DISTINCT host_id)                                              AS distinct_hosts,
    COUNT(DISTINCT host_id) FILTER (WHERE COALESCE(is_superhost, FALSE)) AS distinct_superhosts
  FROM base
  GROUP BY 1, 2
),
agg AS (
  SELECT
    b.listing_neighbourhood,
    b.month_start,

    COUNT(*)                                                    AS listings_total,
    COUNT(*) FILTER (WHERE b.is_active)                         AS listings_active,
    COUNT(*) FILTER (WHERE NOT b.is_active)                     AS listings_inactive,

    MAX(hf.distinct_hosts)                                      AS distinct_hosts,
    (MAX(hf.distinct_superhosts)::numeric
       / NULLIF(MAX(hf.distinct_hosts), 0))                     AS superhost_rate,

    MIN(b.price) FILTER (WHERE b.is_active AND b.price IS NOT NULL)   AS min_price_active,
    MAX(b.price) FILTER (WHERE b.is_active AND b.price IS NOT NULL)   AS max_price_active,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY b.price)
      FILTER (WHERE b.is_active AND b.price IS NOT NULL)              AS median_price_active,
    AVG(b.price) FILTER (WHERE b.is_active AND b.price IS NOT NULL)   AS avg_price_active,

    AVG(b.review_scores_rating) FILTER (WHERE b.is_active)            AS avg_review_rating_active,

    SUM(b.stays)                                                      AS total_stays,
    SUM(b.stays * COALESCE(b.price, 0))                               AS est_revenue,
    SUM(b.stays * COALESCE(b.price, 0))
      / NULLIF(COUNT(*) FILTER (WHERE b.is_active), 0)                AS avg_est_rev_per_active
  FROM base b
  JOIN host_flags hf
    ON hf.listing_neighbourhood = b.listing_neighbourhood
   AND hf.month_start           = b.month_start
  GROUP BY b.listing_neighbourhood, b.month_start
),
with_changes AS (
  SELECT
    a.*,
    (a.listings_active::numeric / NULLIF(a.listings_total, 0)) AS active_listings_rate,
    (a.listings_active - LAG(a.listings_active) OVER (PARTITION BY a.listing_neighbourhood ORDER BY a.month_start))
      / NULLIF(LAG(a.listings_active) OVER (PARTITION BY a.listing_neighbourhood ORDER BY a.month_start), 0)::numeric
                                                                  AS pct_change_active,
    (a.listings_inactive - LAG(a.listings_inactive) OVER (PARTITION BY a.listing_neighbourhood ORDER BY a.month_start))
      / NULLIF(LAG(a.listings_inactive) OVER (PARTITION BY a.listing_neighbourhood ORDER BY a.month_start), 0)::numeric
                                                                  AS pct_change_inactive
  FROM agg a
)

SELECT
  listing_neighbourhood,
  date_trunc('month', month_start)::date AS month_start,
  EXTRACT(YEAR  FROM month_start)::int   AS year,
  EXTRACT(MONTH FROM month_start)::int   AS month,
  active_listings_rate,
  min_price_active,
  max_price_active,
  median_price_active,
  avg_price_active,
  distinct_hosts,
  superhost_rate,
  avg_review_rating_active,
  total_stays,
  avg_est_rev_per_active,
  pct_change_active,
  pct_change_inactive
FROM with_changes
ORDER BY listing_neighbourhood, year, month
