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
  Daily aggregated trip statistics.

  Rolls up hourly data to daily totals for high-level trend analysis.
  Note: unique_start_stations and unique_end_stations are omitted
  as they cannot be accurately aggregated from hourly counts.
*/

SELECT
    trip_date,
    AVG(temperature_f) AS avg_temperature_f,

    -- Trip counts
    SUM(num_trips) AS num_trips,
    SUM(member_trips) AS member_trips,
    SUM(casual_trips) AS casual_trips,
    SUM(electric_trips) AS electric_trips,
    SUM(classic_trips) AS classic_trips,

    -- Duration metrics (in minutes)
    SUM(total_minutes) AS total_minutes,
    SUM(member_minutes) AS member_minutes,
    SUM(casual_minutes) AS casual_minutes,
    SUM(electric_minutes) AS electric_minutes,
    SUM(classic_minutes) AS classic_minutes

FROM {{ ref('hourly_summary') }}
GROUP BY trip_date
