{{ config(materialized='table', schema='gold') }}
select lga_code, lga_name
from {{ ref('stg_lga_code') }}
