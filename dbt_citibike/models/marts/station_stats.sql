{{
  config(
    materialized='table',
    cluster_by=["station_name"]
  )
}}

/*
  Station-level trip statistics.

  Aggregates all trips to calculate per-station metrics:
  - trips_started: Number of trips originating from this station
  - trips_ended: Number of trips ending at this station
  - total_activity: Sum of started + ended trips
  - net_flow: started - ended (positive = more pickups, negative = more dropoffs)

  This helps identify station imbalances and high-traffic stations.
*/

WITH start_stats AS (
    SELECT
        start_station_name AS station_name,
        AVG(start_lat) AS latitude,
        AVG(start_lng) AS longitude,
        COUNT(*) AS trips_started
    FROM {{ ref('trips_cleaned') }}
    GROUP BY start_station_name
),

end_stats AS (
    SELECT
        end_station_name AS station_name,
        COUNT(*) AS trips_ended
    FROM {{ ref('trips_cleaned') }}
    GROUP BY end_station_name
)

SELECT
    s.station_name,
    s.latitude,
    s.longitude,
    s.trips_started,
    COALESCE(e.trips_ended, 0) AS trips_ended,
    s.trips_started + COALESCE(e.trips_ended, 0) AS total_activity,
    s.trips_started - COALESCE(e.trips_ended, 0) AS net_flow
FROM start_stats s
LEFT JOIN end_stats e
    ON s.station_name = e.station_name
WHERE
    s.latitude IS NOT NULL
    AND s.longitude IS NOT NULL
