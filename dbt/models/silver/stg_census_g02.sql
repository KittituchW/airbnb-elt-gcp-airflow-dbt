{{ config(materialized='table', schema='silver') }}

-- Normalized G02 with stable 5-digit LGA key (replace original column)
with src as (
  select *
  from {{ source('bronze','r_d_2016census_g02_nsw_lga') }}
)
select
  -- replace original column with normalized 5-digit numeric version
  lpad(regexp_replace("LGA_CODE_2016"::text, '\D', '', 'g'), 5, '0') as "LGA_CODE_2016",

  ("Median_age_persons")::numeric            as "Median_age_persons",
  ("Median_mortgage_repay_monthly")::numeric as "Median_mortgage_repay_monthly",
  ("Median_tot_prsnl_inc_weekly")::numeric   as "Median_tot_prsnl_inc_weekly",
  ("Median_rent_weekly")::numeric            as "Median_rent_weekly",
  ("Median_tot_fam_inc_weekly")::numeric     as "Median_tot_fam_inc_weekly",
  ("Average_num_psns_per_bedroom")::numeric  as "Average_num_psns_per_bedroom",
  ("Median_tot_hhd_inc_weekly")::numeric     as "Median_tot_hhd_inc_weekly",
  ("Average_household_size")::numeric        as "Average_household_size",

  -- add annual mortgage for convenience
  (("Median_mortgage_repay_monthly")::numeric * 12) as "annual_median_mortgage"
from src
