"""Configuration settings for the CitiBike data pipeline."""

import os

# BigQuery configuration
PROJECT_ID = "citibike-portfolio"
DATASET_ID = "citibike"
TRIPS_TABLE = "trips"
WEATHER_TABLE = "weather"

# Full table references
TRIPS_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TRIPS_TABLE}"
WEATHER_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{WEATHER_TABLE}"

# S3 Configuration
S3_BASE_URL = "https://s3.amazonaws.com/tripdata/"

# Date range for data fetching
START_YEAR = 2024
START_MONTH = 1
END_YEAR = 2025
END_MONTH = 12

# Local paths (relative to project root)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
WEATHER_CSV = os.path.join(PROJECT_ROOT, "nyc_weather_2024_2025.csv")

# Expected schema columns for CitiBike trip data
EXPECTED_COLUMNS = [
    "ride_id",
    "rideable_type",
    "started_at",
    "ended_at",
    "start_station_name",
    "start_station_id",
    "end_station_name",
    "end_station_id",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "member_casual",
]
