# CitiBike Usage Analytics and Automated Pipeline

An end-to-end data pipeline and analytics project for NYC Citi Bike trip data, with automated data ingestion, dbt transformation, an interactive dashboard, and orchestration with Github Actions.


## Motivation

Citi Bike releases trip history data monthly but lacks a systematic automated reporting workflow. This project builds an end-to-end data pipeline to ingest, model, and analyze CitiBike usage to answer questions like:
- How do usage patterns vary by hour/day?
- How do member vs casual riders differ?
- How does weather impact ridership?



## Architecture

```
                                 +----------------------+
                                 |      CitiBike S3     |
                                 |      (Trip Data)     |
                                 +-----------+----------+
                                             |
                                             v
+------------------+              +----------+----------+
|   Open-Meteo     |------------> +   Python Ingestion  |
|  (Weather API)   +              +----------+----------+
+------------------+                         |
                                             v
                                  +----------+----------+
                                  |   Google BigQuery   |
                                  |   (Data Warehouse)  |
                                  +----------+----------+
                                             |
                                             v
                                  +----------+----------+
                                  |     dbt Models      |
                                  |  (Transformations)  |
                                  +----------+----------+
                                             |
                                             v
                                  +----------+----------+
                                  |   Analytics Layer   |
                                  |   (Marts Tables)    |
                                  +---------------------+
                                             |
                                             v
                                  +----------+----------+
                                  |      Dashboard      |
                                  |   (Looker Studio)   |
                                  +---------------------+
                                             

                      Orchestrated by GitHub Actions (monthly schedule)
```

## Tech Stack

| Component | Technology |
|-----------|------------|
| Orchestration | GitHub Actions or Apache Airflow (Docker) |
| Data Warehouse | Google BigQuery |
| Transformations | dbt (Data Build Tool) |
| Ingestion | Python |
| Infrastructure | Docker, Docker Compose |
| Dashboard | Looker Studio |

## Project Structure

```
citibike/
├── .github/workflows/          # GitHub Actions
│   └── monthly_pipeline.yml    # Scheduled monthly pipeline
├── airflow/                    # Airflow orchestration (alternative)
│   ├── dags/                   # DAG definitions
│   ├── docker-compose.yaml     # Airflow Docker setup
│   └── Dockerfile              # Custom Airflow image
├── dbt_citibike/               # dbt project
│   └── models/
│       ├── staging/            # Raw data cleaning
│       ├── intermediate/       # Business logic
│       └── marts/              # Analytics tables
├── python/                     # Data ingestion scripts
│   ├── fetch_citibike_data.py  # Download from S3
│   ├── load_trips_to_bigquery.py
│   ├── load_weather_to_bigquery.py
│   ├── airflow_utils.py        # Incremental loading utilities
│   └── run_monthly_pipeline.py # Monthly pipeline script
└── data/                       # Local data (gitignored)
```

## Data Sources

- **CitiBike Trip Data**: [s3.amazonaws.com/tripdata](https://s3.amazonaws.com/tripdata/)
  - ~90 million trips (Jan 2024 - Dec 2025)
  - New monthly data beginning Jan 2026 will be ingested. 

- **Weather Data**: [Open-Meteo Archive API](https://open-meteo.com/)
  - Hourly temperature, precipitation, cloud cover for NYC


## Data Pipeline

### Monthly Pipeline (GitHub Actions)

Automated via GitHub Actions, runs on the 10th of every month at 6:00 AM UTC:

1. **Check Data Availability** - Verify CitiBike has published the month's data
2. **Download Trip Data** - Fetch from CitiBike's S3 bucket
3. **Delete Existing Data** - Remove old data for the month (idempotent reload)
4. **Load Trips** - Insert trip data into BigQuery
5. **Fetch Weather** - Get hourly weather from Open-Meteo API
6. **Load Weather** - Insert weather data into BigQuery
7. **Run dbt** - Execute transformations and tests

The pipeline can also be triggered manually via GitHub Actions with optional year/month parameters.

### Pipeline Failure Conditions

The GitHub Action fails (and alerts via GitHub notification) if:

- **Data unavailable** - CitiBike hasn't published the month's data yet
- **BigQuery errors** - Authentication, quota, or schema issues
- **dbt test failures** - Any `unique`, `not_null`, or `accepted_values` test fails
- **dbt model errors** - SQL compilation or execution errors

Failed runs can be manually retried from the Actions tab after fixing the issue.



### Alternative: Airflow (Docker)

An Airflow setup is also included in `airflow/` for local development or self-hosted orchestration.


### dbt Models

**Staging Layer** (views)
- `stg_trips` - Cleaned trip data with proper types
- `stg_weather` - Cleaned weather data

**Intermediate Layer** (ephemeral)
- `int_trips_with_weather` - Trips joined with weather conditions

**Marts Layer** (tables)
- `trips_cleaned` - Analysis-ready trip data with derived columns
- `daily_summary` - Daily aggregations (trip counts, durations, member splits)
- `hourly_summary` - Hourly patterns for time-of-day analysis
- `station_stats` - Station-level metrics (popularity, net flow)



## Data Warehouse Design

### Dataset Structure

| Layer | Tables | Materialization | Purpose |
|-------|--------|-----------------|---------|
| Raw | `trips`, `weather` | Tables | Source data loaded from Python |
| Staging | `stg_trips`, `stg_weather` | Views | Clean/cast types, no storage cost |
| Intermediate | `int_trips_with_weather` | Ephemeral | Business logic, compiled inline |
| Marts | `trips_cleaned`, `daily_summary`, `hourly_summary`, `station_stats` | Tables | Analytics-ready aggregations |


**Partitioning**: The `trips` and `trips_cleaned` tables (~90M rows, ~12GB) are partitioned by date, which allows for efficient, low cost querying. 



## Data Quality

### dbt Tests

50 tests run on every pipeline execution:

| Test Type | Examples |
|-----------|----------|
| `unique` | `ride_id` in trips, `datetime` in weather, `trip_date` in daily_summary |
| `not_null` | All primary keys, timestamps, required dimensions |
| `accepted_values` | `rideable_type` in (electric_bike, classic_bike, docked_bike), `member_casual` in (member, casual) |

### Known Data Quirks (Handled)

| Issue | How it's handled |
|-------|------------------|
| **Missing station names/IDs** | ~2% of trips have null stations (dockless rides). Filtered out in `trips_cleaned` mart. |
| **Negative/zero duration trips** | Some trips have `ended_at <= started_at`. Filtered out with `duration > 0` in marts. |



## Dashboard

**[View Live Dashboard on Looker Studio](https://lookerstudio.google.com/s/vHzwlner2zU)**

Interactive dashboard with five views:

- **Overview** - KPIs (total trips, daily average, member percentage) and time series trends
- **Trip Patterns** - Analysis by day of week and hour, segmented by member type and bike type
- **Weather Impact** - Correlation between ridership/trip duration and temperature, precipitation, cloud cover
- **Station Analysis** - Top 10 busiest stations, net flow patterns, and geographic heatmap of activity
- **Daily Summary Table** - Full daily summary table

Note that data and charts can be filtered by clicking into charts, such as clicking on bars or clicking and dragging between dates on a line chart.

<img width="1201" height="900" alt="image" src="https://github.com/user-attachments/assets/751c1684-e504-4485-b77a-61e56ce7b9ee" />
<img width="1200" height="898" alt="image" src="https://github.com/user-attachments/assets/4c7345c1-fa82-41f2-a878-d4b84d0623f8" />
<img width="1198" height="897" alt="image" src="https://github.com/user-attachments/assets/968e8b59-0905-4217-921f-f5f9028e625a" />
<img width="1197" height="896" alt="image" src="https://github.com/user-attachments/assets/4b172b28-5ff5-4203-ab15-cf8838cc2bdf" />




### Key Insights
- **~90M** trips analyzed across 2024-2025
- More trips and longer duration trips during during the summer.
- **Clear commuter patterns** for members with peaks at 8AM and 5-6PM. Significantly smaller commuter pattern for casual riders.
- Casual riders take significantly longer trips at all times. 
- Ridership correlates strongly with **temperature**. However, there is a dip in rides when the temperature is too high (over 75 degrees).
- There is surprisingly limited correlation between rain and trip length. 

### Business Recommendations

- Optimize bike **rebalancing** schedules based on weekday vs weekend, and daily commute usage patterns
- Target high-frequency casual riders with time-bound **membership conversion** offers
- Prioritize **dock expansion** at consistently high-utilization stations
- Incorporate weather forecasts into demand planning and rebalancing operations





