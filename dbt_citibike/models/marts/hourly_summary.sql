{{
  config(
    materialized='table',
    partition_by={
      "field": "trip_date",
      "data_type": "date",
      "granularity": "month"
    }
  )
}}

/*
  Hourly aggregated trip statistics with weather context.

  Groups trips by date, hour, and weather conditions to enable analysis of:
  - Hourly ridership patterns
  - Weather impact on ridership
  - Member vs casual usage patterns
  - Bike type preferences
*/

SELECT
    trip_date,
    hour_of_day,
    day_of_week,
    day_name,
    temperature_f,
    precipitation_mm,
    conditions,

    -- Trip counts
    COUNT(*) AS num_trips,
    COUNTIF(member_casual = 'member') AS member_trips,
    COUNTIF(member_casual = 'casual') AS casual_trips,
    COUNTIF(rideable_type = 'electric_bike') AS electric_trips,
    COUNTIF(rideable_type = 'classic_bike') AS classic_trips,

    -- Duration metrics (in minutes)
    SUM(duration_minutes) AS total_minutes,
    SUM(CASE WHEN member_casual = 'member' THEN duration_minutes ELSE 0 END) AS member_minutes,
    SUM(CASE WHEN member_casual = 'casual' THEN duration_minutes ELSE 0 END) AS casual_minutes,
    SUM(CASE WHEN rideable_type = 'electric_bike' THEN duration_minutes ELSE 0 END) AS electric_minutes,
    SUM(CASE WHEN rideable_type = 'classic_bike' THEN duration_minutes ELSE 0 END) AS classic_minutes

FROM {{ ref('trips_cleaned') }}
GROUP BY
    trip_date,
    hour_of_day,
    day_of_week,
    day_name,
    temperature_f,
    precipitation_mm,
    conditions
