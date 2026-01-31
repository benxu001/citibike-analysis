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

         Orchestrated by GitHub Actions (monthly schedule)
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

## Getting Started

### Prerequisites

- Python 3.11+
- Google Cloud account with BigQuery enabled
- GCP service account with BigQuery Data Editor and Job User roles

### Setup (GitHub Actions - Recommended)

1. **Fork/clone the repository**
   ```bash
   git clone https://github.com/yourusername/citibike-pipeline.git
   cd citibike-pipeline
   ```

2. **Add GCP credentials to GitHub Secrets**
   - Go to your repo → Settings → Secrets and variables → Actions
   - Create a new secret named `GCP_CREDENTIALS`
   - Paste the entire contents of your service account JSON key

3. **Enable GitHub Actions**
   - Go to Actions tab and enable workflows
   - The pipeline will run automatically on the 10th of each month
   - Or trigger manually: Actions → Monthly CitiBike Pipeline → Run workflow

### Setup (Local Development)

1. **Install dependencies**
   ```bash
   pip install google-cloud-bigquery pandas pyarrow requests python-dateutil tqdm dbt-bigquery
   ```

2. **Configure dbt profile** (`~/.dbt/profiles.yml`)
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

3. **Set GCP credentials**
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
   ```

### Running the Pipeline

**Via GitHub Actions:**
- Runs automatically on schedule, or trigger manually from the Actions tab

**Manual run (local):**
```bash
cd python
python run_monthly_pipeline.py                     # Process previous month
python run_monthly_pipeline.py --year 2025 --month 12  # Specific month
```

**Manual dbt run:**
```bash
cd dbt_citibike
dbt run
dbt test
```

### Alternative: Airflow (Docker)

For local orchestration with a UI, use the Airflow setup:
```bash
cd airflow
docker compose up -d
# Access UI at http://localhost:8080 (airflow/airflow)
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
