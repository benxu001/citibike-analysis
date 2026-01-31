# CitiBike Data Pipeline

An end-to-end data pipeline that ingests NYC CitiBike trip data and weather data, transforms it using dbt, and orchestrates monthly updates with Airflow.

## Architecture

```
                                    +------------------+
                                    |   CitiBike S3    |
                                    |   (Trip Data)    |
                                    +--------+---------+
                                             |
                                             v
+------------------+              +----------+----------+
|   Open-Meteo    |              |                     |
|  (Weather API)   +------------>+   Python Ingestion  |
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

         Orchestrated by Apache Airflow (monthly schedule)
```

## Tech Stack

| Component | Technology |
|-----------|------------|
| Orchestration | Apache Airflow (Docker) |
| Data Warehouse | Google BigQuery |
| Transformations | dbt (Data Build Tool) |
| Ingestion | Python |
| Infrastructure | Docker, Docker Compose |

## Project Structure

```
citibike/
├── airflow/                    # Airflow orchestration
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
│   └── airflow_utils.py        # Airflow helper functions
└── data/                       # Local data (gitignored)
```

## Data Pipeline

### Monthly Pipeline (Airflow DAG)

Runs on the 10th of every month to process the previous month's data:

1. **Check Data Availability** - Verify CitiBike has published the month's data
2. **Download Trip Data** - Fetch from CitiBike's S3 bucket
3. **Delete Existing Data** - Remove old data for the month (idempotent reload)
4. **Load Trips** - Insert trip data into BigQuery
5. **Fetch Weather** - Get hourly weather from Open-Meteo API
6. **Load Weather** - Insert weather data into BigQuery
7. **Run dbt** - Execute transformations and tests

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

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Google Cloud account with BigQuery enabled
- GCP service account with BigQuery Data Editor and Job User roles

### Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/citibike-pipeline.git
   cd citibike-pipeline
   ```

2. **Add GCP credentials**
   ```bash
   cp /path/to/your-service-account.json airflow/config/gcp-credentials.json
   ```

3. **Configure dbt profile** (`~/.dbt/profiles.yml`)
   ```yaml
   citibike:
     target: dev
     outputs:
       dev:
         type: bigquery
         method: service-account
         project: your-gcp-project
         dataset: citibike
         keyfile: /path/to/service-account.json
   ```

4. **Start Airflow**
   ```bash
   cd airflow
   docker compose up -d
   ```

5. **Access Airflow UI**
   - URL: http://localhost:8080
   - Username: `airflow`
   - Password: `airflow`

### Running the Pipeline

**Via Airflow UI:**
- Enable the `citibike_monthly_pipeline` DAG
- Trigger manually or wait for scheduled run

**Manual dbt run:**
```bash
cd dbt_citibike
dbt run
dbt test
```

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
