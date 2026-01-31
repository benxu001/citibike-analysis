# CitiBike Data Pipeline

An end-to-end data pipeline that ingests NYC CitiBike trip data and weather data, transforms it using dbt, and orchestrates monthly updates with Airflow.

## Architecture

```
                                 +----------------------+
                                 |      CitiBike S3     |
                                 |      (Trip Data)     |
                                 +-----------+----------+
                                             |
                                             v
+------------------+              +----------+----------+
|   Open-Meteo     |              |                     |
|  (Weather API)   +------------> +   Python Ingestion  |
+------------------+              |                     |
                                  +----------+----------+
                                             |
                                             v
                                  +----------+----------+
                                  |                     |
                                  |   Google BigQuery   |
                                  |   (Data Warehouse)  |
                                  |                     |
                                  +----------+----------+
                                             |
                                             v
                                  +----------+----------+
                                  |                     |
                                  |    dbt Models       |
                                  |  (Transformations)  |
                                  |                     |
                                  +----------+----------+
                                             |
                                             v
                                  +----------+----------+
                                  |                     |
                                  |   Analytics Layer   |
                                  |   (Marts Tables)    |
                                  |                     |
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

## Data Sources

- **CitiBike Trip Data**: [s3.amazonaws.com/tripdata](https://s3.amazonaws.com/tripdata/)
  - ~90 million trips (Jan 2024 - Dec 2025)
  - Fields: ride_id, timestamps, stations, coordinates, member type

- **Weather Data**: [Open-Meteo Archive API](https://open-meteo.com/)
  - Hourly temperature, precipitation, cloud cover for NYC

## Data Warehouse Design

### Dataset Structure

| Layer | Tables | Materialization | Purpose |
|-------|--------|-----------------|---------|
| Raw | `trips`, `weather` | Tables | Source data loaded from Python |
| Staging | `stg_trips`, `stg_weather` | Views | Clean/cast types, no storage cost |
| Intermediate | `int_trips_with_weather` | Ephemeral | Business logic, compiled inline |
| Marts | `daily_summary`, `hourly_summary`, `station_stats` | Tables | Analytics-ready aggregations |

### Partitioning & Clustering

The `trips` table (~90M rows, ~8GB) uses BigQuery optimizations:

```sql
-- Partition by day (most queries filter by date range)
PARTITION BY DATE(started_at)

-- Cluster by common filter/group columns
CLUSTER BY start_station_id, member_casual
```

**Why this matters:**
- Queries filtering by date only scan relevant partitions (e.g., 1 month = ~4M rows instead of 90M)
- Clustering improves performance for station-level and member-type analysis
- Reduces query costs by 10-50x for typical analytical queries

### Cost Controls

- **Staging models are views** - No storage cost, computed on read
- **Intermediate models are ephemeral** - Compiled inline, no table created
- **Partitioning** - Queries scan only relevant date ranges
- **BigQuery sandbox** - 1TB free queries/month, 10GB free storage
- **Monthly pipeline** - Only ~4M new rows/month, minimal incremental cost

Estimated monthly cost: **< $1** (well within free tier for typical usage)

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
| **Weather data gaps** | Weather joined on truncated hour. Nulls allowed in weather columns for trips without matching weather data. |

### Pipeline Failure Conditions

The GitHub Action fails (and alerts via GitHub notification) if:

- **Data unavailable** - CitiBike hasn't published the month's data yet
- **BigQuery errors** - Authentication, quota, or schema issues
- **dbt test failures** - Any `unique`, `not_null`, or `accepted_values` test fails
- **dbt model errors** - SQL compilation or execution errors

Failed runs can be manually retried from the Actions tab after fixing the issue.

## Infrastructure

**Google Cloud Platform**
- Created a GCP project with BigQuery enabled
- Service account with BigQuery Data Editor and Job User roles
- Raw data stored in `citibike.trips` (~90M rows) and `citibike.weather` tables

**GitHub Actions**
- Runs the monthly pipeline automatically on the 10th of each month
- Service account credentials stored securely as a repository secret
- Triggers: Python ingestion → BigQuery load → dbt transformations → dbt tests

**Local Development**
- Python scripts can be run locally with `python run_monthly_pipeline.py`
- An Airflow setup is included in `airflow/` for local orchestration with a web UI
- dbt models can be run independently with `dbt run && dbt test`

## Dashboard

**[View Live Dashboard on Looker Studio](https://lookerstudio.google.com/s/vHzwlner2zU)**

Interactive dashboard with four views:

- **Overview** - KPIs (total trips, daily average, member percentage) and time series trends
- **Trip Patterns** - Analysis by day of week and hour, segmented by member type and bike type
- **Weather Impact** - Correlation between ridership/trip duration and temperature, precipitation, cloud cover
- **Station Analysis** - Top 10 busiest stations, net flow patterns, and geographic heatmap of activity

Key insights from the data:
- 90M+ trips analyzed across 2024-2025
- Clear commuter patterns with peaks at 8AM and 5-6PM
- Members account for 82% of trips but casual riders take longer trips on weekends
- Ridership correlates strongly with temperature

## Sample Queries

```sql
-- Daily trip trends
SELECT trip_date, num_trips, avg_duration_minutes
FROM `your-project.citibike.daily_summary`
ORDER BY trip_date;

-- Busiest stations
SELECT station_name, total_activity, net_flow
FROM `your-project.citibike.station_stats`
ORDER BY total_activity DESC
LIMIT 10;

-- Weather impact on ridership
SELECT
  conditions,
  AVG(num_trips) as avg_daily_trips
FROM `your-project.citibike.daily_summary`
GROUP BY conditions;
```
