{{ config(materialized='table', schema='gold') }}
select suburb_name, lga_name
from {{ ref('stg_lga_suburb') }}
