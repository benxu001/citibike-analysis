{{
  config(
    materialized='view'
  )
}}

SELECT
    datetime,
    temperature_f,
    precipitation_mm,
    cloud_cover_pct,
    conditions
FROM {{ source('citibike_raw', 'weather') }}
