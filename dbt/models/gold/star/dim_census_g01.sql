{{ config(materialized='table', schema='gold') }}
select *
from {{ ref('stg_census_g01') }}
