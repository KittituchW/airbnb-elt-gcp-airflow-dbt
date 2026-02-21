--------------------------------------------------------------------------------------------------------------------------

/* Q1: Demographic differences between Top 3 and Bottom 3 LGAs
      by average estimated revenue per active listing (last 12 months)
*/

--------------------------------------------------------------------------------------------------------------------------
-- Step 1: Pull the latest available month from the Airbnb neighbourhood performance table
WITH last_window AS (
  SELECT MAX(month_start) AS max_month
  FROM analytics_gold.dm_listing_neighbourhood
),

-- Step 2: Take the last 12-month window of listing_neighbourhood performance data
last_12 AS (
  SELECT
    dln.listing_neighbourhood,
    dln.month_start,
    dln.avg_est_rev_per_active
  FROM analytics_gold.dm_listing_neighbourhood dln
  JOIN last_window lw ON TRUE
  WHERE dln.month_start BETWEEN (lw.max_month - INTERVAL '11 months') AND lw.max_month
),

-- Step 3: Map each listing_neighbourhood to an LGA using multiple lookup paths:
--          - dim_lga direct name match
--          - dim_suburb -> dim_lga indirect match
--          - staging table stg_lga_suburb for missing cases
mapped AS (
  SELECT
    l12.listing_neighbourhood,
    l12.month_start,
    l12.avg_est_rev_per_active,
    COALESCE(dl1.lga_code, dl2.lga_code, dl3.lga_code) AS "LGA_CODE_2016",
    COALESCE(dl1.lga_name, dl2.lga_name, dl3.lga_name) AS lga_name
  FROM last_12 l12
  LEFT JOIN analytics_gold.dim_lga dl1
    ON LOWER(TRIM(dl1.lga_name)) = LOWER(TRIM(l12.listing_neighbourhood))
  LEFT JOIN analytics_gold.dim_suburb ds
    ON LOWER(TRIM(ds.suburb_name)) = LOWER(TRIM(l12.listing_neighbourhood))
  LEFT JOIN analytics_gold.dim_lga dl2
    ON dl2.lga_name = ds.lga_name
  LEFT JOIN analytics_silver.stg_lga_suburb s
    ON LOWER(TRIM(s.suburb_name)) = LOWER(TRIM(l12.listing_neighbourhood))
  LEFT JOIN analytics_gold.dim_lga dl3
    ON dl3.lga_name = s.lga_name
),

-- Step 4: Aggregate to LGA level and compute average revenue per active listing as the ranking metric
lga_rev AS (
  SELECT
    "LGA_CODE_2016",
    AVG(avg_est_rev_per_active)::numeric AS avg_rev_per_active_listing
  FROM mapped
  WHERE "LGA_CODE_2016" IS NOT NULL
  GROUP BY 1
),

-- Step 5: Rank LGAs by average revenue per active listing (both descending and ascending)
ranked AS (
  SELECT
    "LGA_CODE_2016",
    avg_rev_per_active_listing,
    RANK() OVER (ORDER BY avg_rev_per_active_listing DESC) AS r_desc,
    RANK() OVER (ORDER BY avg_rev_per_active_listing ASC)  AS r_asc
  FROM lga_rev
),

-- Step 6: Select Top 3 and Bottom 3 LGAs based on revenue rankings
top_bottom AS (
  SELECT "LGA_CODE_2016", 'TOP3' AS band, avg_rev_per_active_listing
  FROM ranked WHERE r_desc <= 3
  UNION ALL
  SELECT "LGA_CODE_2016", 'BOT3' AS band, avg_rev_per_active_listing
  FROM ranked WHERE r_asc <= 3
)

-- Step 7: Join the selected LGAs with Census G01 (age distribution) and G02 (median age, household size)
--          to compute demographic characteristics
SELECT
  tb.band,
  COALESCE(dln.lga_name, 'UNKNOWN') AS lga_name,
  tb.avg_rev_per_active_listing,
  g02."Median_age_persons" AS median_age,
  g02."Average_household_size" AS avg_household_size,

  -- Step 8: Calculate percentage share of each major age band
  ((g01."Age_0_4_yr_P" + g01."Age_5_14_yr_P")::numeric) / NULLIF(g01."Tot_P_P", 0) AS pct_0_14,
  ((g01."Age_15_19_yr_P" + g01."Age_20_24_yr_P")::numeric) / NULLIF(g01."Tot_P_P", 0) AS pct_15_24,
  (g01."Age_25_34_yr_P"::numeric) / NULLIF(g01."Tot_P_P", 0) AS pct_25_34,
  (g01."Age_35_44_yr_P"::numeric) / NULLIF(g01."Tot_P_P", 0) AS pct_35_44,
  ((g01."Age_45_54_yr_P" + g01."Age_55_64_yr_P")::numeric) / NULLIF(g01."Tot_P_P", 0) AS pct_45_64,
  ((g01."Age_65_74_yr_P" + g01."Age_75_84_yr_P" + g01."Age_85ov_P")::numeric) / NULLIF(g01."Tot_P_P", 0) AS pct_65_plus

-- Step 9: Final join to dim_lga to attach readable names and order results for reporting
FROM top_bottom tb
LEFT JOIN analytics_silver.stg_census_g02 g02 ON g02."LGA_CODE_2016" = tb."LGA_CODE_2016"
LEFT JOIN analytics_silver.stg_census_g01 g01 ON g01."LGA_CODE_2016" = tb."LGA_CODE_2016"
LEFT JOIN (
  SELECT DISTINCT lga_code AS "LGA_CODE_2016", lga_name
  FROM analytics_gold.dim_lga
) dln ON dln."LGA_CODE_2016" = tb."LGA_CODE_2016"

-- Step 10: Order results - Top 3 first, then Bottom 3, sorted by revenue descending
ORDER BY
  CASE WHEN tb.band = 'TOP3' THEN 1 ELSE 2 END,
  tb.avg_rev_per_active_listing DESC;



--------------------------------------------------------------------------------------------------------------------------

/* Q2: Correlation between median age (Census) and revenue per active listing
      by neighbourhood (last 12 months)
*/

--------------------------------------------------------------------------------------------------------------------------

-- Step 1: Pull the latest available month from neighbourhood performance
WITH last_window AS (
  SELECT MAX(month_start) AS max_month
  FROM analytics_gold.dm_listing_neighbourhood
),

-- Step 2: Take the last 12-month window of neighbourhood performance
last_12 AS (
  SELECT
    dln.listing_neighbourhood,
    dln.month_start,
    dln.avg_est_rev_per_active
  FROM analytics_gold.dm_listing_neighbourhood dln
  JOIN last_window lw ON TRUE
  WHERE dln.month_start BETWEEN (lw.max_month - INTERVAL '11 months') AND lw.max_month
),

-- Step 3: Map each listing_neighbourhood to an LGA (multiple lookup paths)
neigh_lga AS (
  SELECT
    l12.listing_neighbourhood,
    COALESCE(dl1.lga_code, dl2.lga_code, dl3.lga_code) AS "LGA_CODE_2016"
  FROM last_12 l12
  LEFT JOIN analytics_gold.dim_lga dl1
    ON LOWER(TRIM(dl1.lga_name)) = LOWER(TRIM(l12.listing_neighbourhood))
  LEFT JOIN analytics_gold.dim_suburb ds
    ON LOWER(TRIM(ds.suburb_name)) = LOWER(TRIM(l12.listing_neighbourhood))
  LEFT JOIN analytics_gold.dim_lga dl2
    ON dl2.lga_name = ds.lga_name
  LEFT JOIN analytics_silver.stg_lga_suburb s
    ON LOWER(TRIM(s.suburb_name)) = LOWER(TRIM(l12.listing_neighbourhood))
  LEFT JOIN analytics_gold.dim_lga dl3
    ON dl3.lga_name = s.lga_name
  GROUP BY l12.listing_neighbourhood, dl1.lga_code, dl2.lga_code, dl3.lga_code
),

-- Step 4: Aggregate revenue per active listing to neighbourhood level over the last 12 months
neigh_rev AS (
  SELECT
    listing_neighbourhood,
    AVG(avg_est_rev_per_active)::numeric AS rev_active_avg_12m
  FROM last_12
  GROUP BY listing_neighbourhood
)

-- Step 5: Join neighbourhood revenue to Census G02 via LGA and compute correlation
SELECT
  COUNT(*) AS n_neighbourhoods,
  corr(g02."Median_age_persons", neigh_rev.rev_active_avg_12m) AS corr_median_age_vs_revenue
FROM neigh_rev
JOIN neigh_lga
  ON neigh_lga.listing_neighbourhood = neigh_rev.listing_neighbourhood
JOIN analytics_silver.stg_census_g02 g02
  ON g02."LGA_CODE_2016" = neigh_lga."LGA_CODE_2016"
WHERE neigh_lga."LGA_CODE_2016" IS NOT NULL
  AND neigh_rev.rev_active_avg_12m IS NOT NULL
  AND g02."Median_age_persons" IS NOT NULL;

--------------------------------------------------------------------------------------------------------------------------

/* Q3: Top 5 listing_neighbourhoods (by avg_est_rev_per_active over last 12 months),
       then, for each of those neighbourhoods, find the best (property_type, room_type, accommodates)
       combination by total stays over the same 12-month window.
*/

--------------------------------------------------------------------------------------------------------------------------

-- Step 1: Pull the latest available month
WITH last_window AS (
  SELECT MAX(month_start) AS max_month
  FROM analytics_gold.dm_listing_neighbourhood
),

-- Step 2: Restrict dm_listing_neighbourhood to the last 12 months
dm_last_12 AS (
  SELECT dln.listing_neighbourhood, dln.month_start, dln.avg_est_rev_per_active
  FROM analytics_gold.dm_listing_neighbourhood dln
  JOIN last_window lw ON TRUE
  WHERE dln.month_start BETWEEN (lw.max_month - INTERVAL '11 months') AND lw.max_month
),

-- Step 3: Compute 12-month average revenue per active listing by neighbourhood
neigh_rev AS (
  SELECT listing_neighbourhood,
         AVG(avg_est_rev_per_active)::numeric AS avg_rev_per_active_listing_12m
  FROM dm_last_12
  GROUP BY listing_neighbourhood
),

-- Step 4: Select the top 5 neighbourhoods by the above metric
top5 AS (
  SELECT listing_neighbourhood, avg_rev_per_active_listing_12m
  FROM (
    SELECT listing_neighbourhood,
           avg_rev_per_active_listing_12m,
           RANK() OVER (ORDER BY avg_rev_per_active_listing_12m DESC) AS rnk
    FROM neigh_rev
  ) r
  WHERE r.rnk <= 5
),

-- Step 5: Capture the distinct months in the last 12-month window for joins
last_12_dates AS (
  SELECT DISTINCT month_start FROM dm_last_12
),

-- Step 6: Build a monthly fact view for the selected months and neighbourhoods
--          Join the monthly fact to the SCD2 listing dimension to bring
--          listing_neighbourhood, property_type, room_type, and accommodates in the right month.
fa AS (
  SELECT
    f.listing_id,
    f.month_date,
    f.has_availability,
    f.stays,
    f.price,
    dl.listing_neighbourhood,
    dl.property_type,
    dl.room_type,
    dl.accommodates
  FROM analytics_gold.dim_listing_monthly f
  JOIN analytics_gold.dim_snap_listing dl
    ON dl.listing_id = f.listing_id
   AND (dl.valid_from_month IS NULL OR f.month_date >= dl.valid_from_month)
   AND (dl.valid_to_month   IS NULL OR f.month_date <  dl.valid_to_month)
  WHERE f.month_date IN (SELECT month_start FROM last_12_dates)
    AND dl.listing_neighbourhood IN (SELECT listing_neighbourhood FROM top5)
),

-- Step 7: Aggregate total stays (and revenue) over 12 months by type tuple
stays_by_type AS (
  SELECT
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    SUM(stays)::bigint AS total_stays_12m,
    SUM(stays * price)::numeric AS total_revenue_12m
  FROM fa
  GROUP BY 1,2,3,4
),

-- Step 8: Pick, within each top neighbourhood, the type tuple with the highest total stays
--          Tie-break by property_type, room_type, accommodates for determinism
best AS (
  SELECT
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    total_stays_12m,
    total_revenue_12m,
    ROW_NUMBER() OVER (
      PARTITION BY listing_neighbourhood
      ORDER BY total_stays_12m DESC, property_type, room_type, accommodates
    ) AS rn
  FROM stays_by_type
)

-- Step 9: Return the best tuple per neighbourhood, along with its neighbourhood 12-month average revenue
SELECT
  b.listing_neighbourhood,
  t.avg_rev_per_active_listing_12m,
  b.property_type,
  b.room_type,
  b.accommodates,
  b.total_stays_12m,
  b.total_revenue_12m
FROM best b
JOIN top5 t
  ON t.listing_neighbourhood = b.listing_neighbourhood
WHERE b.rn = 1
ORDER BY t.avg_rev_per_active_listing_12m DESC;

--------------------------------------------------------------------------------------------------------------------------

/* Q4: For hosts with multiple listings, are their properties concentrated within
        the same LGA, or distributed across different LGAs?
*/

--------------------------------------------------------------------------------------------------------------------------

-- Step 1: Pull the latest available month from the monthly listing fact
WITH last_window AS (
  SELECT MAX(month_date) AS max_month
  FROM analytics_gold.dim_listing_monthly
),

-- Step 2: Define the last 12-month window
w AS (
  SELECT (max_month - INTERVAL '11 months') AS min_month, max_month
  FROM last_window
),

-- Step 3: SCD2-align listing attributes to each month in the window
fa AS (
  SELECT
    f.host_id,
    f.listing_id,
    f.month_date,
    dl.listing_neighbourhood
  FROM analytics_gold.dim_listing_monthly f
  JOIN analytics_gold.dim_snap_listing dl
    ON dl.listing_id = f.listing_id
   AND (dl.valid_from_month IS NULL OR f.month_date >= dl.valid_from_month)
   AND (dl.valid_to_month   IS NULL OR f.month_date <  dl.valid_to_month)
  JOIN w ON TRUE
  WHERE f.month_date BETWEEN w.min_month AND w.max_month
),

-- Step 4: Map neighbourhood to LGA (direct LGA name match, else via suburb mappings)
mapped AS (
  SELECT
    fa.host_id,
    fa.listing_id,
    fa.month_date,
    COALESCE(dl1.lga_code, dl2.lga_code, dl3.lga_code) AS "LGA_CODE_2016"
  FROM fa
  LEFT JOIN analytics_gold.dim_lga dl1
    ON LOWER(TRIM(dl1.lga_name)) = LOWER(TRIM(fa.listing_neighbourhood))
  LEFT JOIN analytics_gold.dim_suburb ds
    ON LOWER(TRIM(ds.suburb_name)) = LOWER(TRIM(fa.listing_neighbourhood))
  LEFT JOIN analytics_gold.dim_lga dl2
    ON dl2.lga_name = ds.lga_name
  LEFT JOIN analytics_silver.stg_lga_suburb s
    ON LOWER(TRIM(s.suburb_name)) = LOWER(TRIM(fa.listing_neighbourhood))
  LEFT JOIN analytics_gold.dim_lga dl3
    ON dl3.lga_name = s.lga_name
),

-- Step 5: Deduplicate host-listing-LGA within the window
host_listing_lga AS (
  SELECT DISTINCT
    m.host_id,
    m.listing_id,
    m."LGA_CODE_2016"
  FROM mapped m
  WHERE m."LGA_CODE_2016" IS NOT NULL
),

-- Step 6: Per-host counts of distinct listings and distinct LGAs
host_agg AS (
  SELECT
    host_id,
    COUNT(DISTINCT listing_id)       AS n_listings,
    COUNT(DISTINCT "LGA_CODE_2016")  AS n_lgas
  FROM host_listing_lga
  GROUP BY host_id
),

-- Step 7: Keep only hosts with multiple listings
multi_hosts AS (
  SELECT host_id, n_listings, n_lgas
  FROM host_agg
  WHERE n_listings >= 2
)

-- Step 8: Classify spread and compute counts and percentages
SELECT
  CASE WHEN n_lgas = 1 THEN 'Concentrated' ELSE 'Distributed' END AS spread_type,
  COUNT(*)                                                        AS hosts,
  ROUND(100.0 * COUNT(*)::numeric / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS pct_hosts
FROM multi_hosts
GROUP BY spread_type
ORDER BY spread_type;


--------------------------------------------------------------------------------------------------------------------------

/* Q5: For hosts with a single Airbnb listing, does the estimated revenue over the last
        12 months cover the annualised median mortgage repayment in the corresponding LGA?
        Which LGA has the highest percentage of such hosts?
*/

--------------------------------------------------------------------------------------------------------------------------

-- Step 1: Pull the latest available month from the monthly listing fact
WITH last_window AS (
  SELECT MAX(month_date) AS max_month
  FROM analytics_gold.dim_listing_monthly
),

-- Step 2: Define the last 12-month window
w AS (
  SELECT (max_month - INTERVAL '11 months') AS min_month, max_month
  FROM last_window
),

-- Step 3: Slice the 12 months and SCD2-align listing attributes per month
fa AS (
  SELECT
    f.host_id,
    f.listing_id,
    f.month_date,
    f.has_availability,
    f.stays,
    f.price,
    dl.listing_neighbourhood
  FROM analytics_gold.dim_listing_monthly f
  JOIN analytics_gold.dim_snap_listing dl
    ON dl.listing_id = f.listing_id
   AND (dl.valid_from_month IS NULL OR f.month_date >= dl.valid_from_month)
   AND (dl.valid_to_month   IS NULL OR f.month_date <  dl.valid_to_month)
  JOIN w ON TRUE
  WHERE f.month_date BETWEEN w.min_month AND w.max_month
),

-- Step 4: Identify single-listing hosts within the 12-month window
single_hosts AS (
  SELECT host_id
  FROM fa
  GROUP BY host_id
  HAVING COUNT(DISTINCT listing_id) = 1
),

-- Step 5: Map neighbourhood to LGA using multiple lookup paths; keep LGA code
mapped AS (
  SELECT
    fa.host_id,
    fa.listing_id,
    fa.month_date,
    COALESCE(dl1.lga_code, dl2.lga_code, dl3.lga_code) AS "LGA_CODE_2016"
  FROM fa
  JOIN single_hosts sh ON sh.host_id = fa.host_id
  LEFT JOIN analytics_gold.dim_lga dl1
         ON LOWER(TRIM(dl1.lga_name)) = LOWER(TRIM(fa.listing_neighbourhood))
  LEFT JOIN analytics_gold.dim_suburb ds
         ON LOWER(TRIM(ds.suburb_name)) = LOWER(TRIM(fa.listing_neighbourhood))
  LEFT JOIN analytics_gold.dim_lga dl2
         ON dl2.lga_name = ds.lga_name
  LEFT JOIN analytics_silver.stg_lga_suburb s
         ON LOWER(TRIM(s.suburb_name)) = LOWER(TRIM(fa.listing_neighbourhood))
  LEFT JOIN analytics_gold.dim_lga dl3
         ON dl3.lga_name = s.lga_name
),

-- Step 6: Choose a single LGA per host by taking the modal LGA over the window
host_lga_mode AS (
  SELECT host_id, "LGA_CODE_2016"
  FROM (
    SELECT
      host_id,
      "LGA_CODE_2016",
      ROW_NUMBER() OVER (
        PARTITION BY host_id
        ORDER BY COUNT(*) DESC, MIN(COALESCE("LGA_CODE_2016",''))
      ) AS rn
    FROM mapped
    WHERE "LGA_CODE_2016" IS NOT NULL
    GROUP BY host_id, "LGA_CODE_2016"
  ) x
  WHERE rn = 1
),

-- Step 7: Sum revenue across the 12 months for each single-listing host
--         Only count revenue when has_availability is true
host_rev AS (
  SELECT
    f.host_id,
    SUM(CASE WHEN f.has_availability THEN (f.stays::numeric * f.price) ELSE 0 END) AS rev_12m
  FROM fa f
  JOIN single_hosts sh ON sh.host_id = f.host_id
  GROUP BY f.host_id
),

-- Step 8: Bring in LGA names from gold
lga_names AS (
  SELECT DISTINCT
         lga_code AS "LGA_CODE_2016",
         lga_name
  FROM analytics_gold.dim_lga
),

-- Step 9: Annualise the median monthly mortgage repayment from Census G02
g02 AS (
  SELECT
    "LGA_CODE_2016",
    ("Median_mortgage_repay_monthly"::numeric * 12) AS annual_median_mortgage
  FROM analytics_silver.stg_census_g02
)

-- Step 10: Evaluate coverage by LGA and compute counts and percentages
SELECT
  ln.lga_name,
  COUNT(*)                                AS single_hosts,
  SUM(CASE WHEN hr.rev_12m >= g02.annual_median_mortgage THEN 1 ELSE 0 END) AS hosts_covering,
  ROUND(
    100.0 * SUM(CASE WHEN hr.rev_12m >= g02.annual_median_mortgage THEN 1 ELSE 0 END)::numeric
    / NULLIF(COUNT(*), 0),
    2
  ) AS pct_covering
FROM host_rev hr
JOIN host_lga_mode hlm ON hlm.host_id = hr.host_id
JOIN g02              ON g02."LGA_CODE_2016" = hlm."LGA_CODE_2016"
JOIN lga_names ln     ON ln."LGA_CODE_2016"  = hlm."LGA_CODE_2016"
GROUP BY ln.lga_name
ORDER BY pct_covering DESC, ln.lga_name
LIMIT 30;


