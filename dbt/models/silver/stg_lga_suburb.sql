{{ config(materialized='table', schema='silver') }}

select
  upper(trim("LGA_NAME"))    as lga_name,
  upper(trim("SUBURB_NAME")) as suburb_name
from {{ source('bronze','r_d_nsw_lga_suburb') }}
