{{ config(materialized='table', schema='silver') }}

select
  upper(trim("LGA_CODE")) as lga_code,
  upper(trim("LGA_NAME")) as lga_name
from {{ source('bronze','r_d_nsw_lga_code') }}
