{{
  config(
    materialized='table',
    partition_by={
      "field": "trip_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["start_station_name", "member_casual"]
  )
}}

/*
  Cleaned and enriched trip data with weather information.

  Filters applied:
  - All required fields must be non-null
  - Duration must be positive
  - Only includes trips from the configured start_date onwards

  Enrichments:
  - Duration calculated in minutes
  - Hour of day extracted
  - Day of week extracted (1=Sunday, 7=Saturday)
  - Day name derived
  - Weather data joined from hourly weather table
*/

SELECT
    DATE(started_at) AS trip_date,
    TIMESTAMP_DIFF(ended_at, started_at, SECOND) / 60.0 AS duration_minutes,
    started_at,
    ended_at,
    start_station_name,
    end_station_name,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    rideable_type,
    member_casual,
    EXTRACT(HOUR FROM started_at) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM started_at) AS day_of_week,
    CASE EXTRACT(DAYOFWEEK FROM started_at)
        WHEN 1 THEN 'Sunday'
        WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'
        WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday'
        WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END AS day_name,
    temperature_f,
    precipitation_mm,
    conditions
FROM {{ ref('int_trips_with_weather') }}
WHERE
    ride_id IS NOT NULL
    AND rideable_type IS NOT NULL
    AND started_at IS NOT NULL
    AND ended_at IS NOT NULL
    AND start_station_name IS NOT NULL
    AND start_station_id IS NOT NULL
    AND end_station_name IS NOT NULL
    AND end_station_id IS NOT NULL
    AND start_lat IS NOT NULL
    AND start_lng IS NOT NULL
    AND end_lat IS NOT NULL
    AND end_lng IS NOT NULL
    AND member_casual IS NOT NULL
    AND TIMESTAMP_DIFF(ended_at, started_at, SECOND) > 0
