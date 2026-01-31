{{
  config(
    materialized='ephemeral'
  )
}}

/*
  Intermediate model that joins trips with weather data.
  This is an ephemeral model (not materialized) to avoid creating an extra table.
  The join is done on the hour-truncated timestamp to match trips with hourly weather.
*/

SELECT
    t.ride_id,
    t.rideable_type,
    t.started_at,
    t.ended_at,
    t.start_station_name,
    t.start_station_id,
    t.end_station_name,
    t.end_station_id,
    t.start_lat,
    t.start_lng,
    t.end_lat,
    t.end_lng,
    t.member_casual,
    w.temperature_f,
    w.precipitation_mm,
    w.conditions
FROM {{ ref('stg_trips') }} t
LEFT JOIN {{ ref('stg_weather') }} w
    ON DATETIME_TRUNC(t.started_at, HOUR) = w.datetime
