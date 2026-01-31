"""
CitiBike Monthly Data Pipeline DAG

Schedule: 10th of every month at 6:00 AM UTC
Workflow: Process previous month's data
  - Check S3 for CitiBike trip data availability
  - Download and load trips to BigQuery (incremental)
  - Fetch and load weather data (incremental)
  - Run DBT models

Example: On Feb 10, 2026, this DAG processes January 2026 data.
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Add the python directory to the path for imports
# This allows Airflow to find our utility modules
# Use realpath to resolve symlinks (important when DAG is symlinked to ~/airflow/dags)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
PYTHON_DIR = os.path.join(PROJECT_ROOT, "python")
DBT_DIR = os.path.join(PROJECT_ROOT, "dbt_citibike")

sys.path.insert(0, PYTHON_DIR)


# Default arguments for all tasks
default_args = {
    "owner": "citibike-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(hours=2),
}


def _check_data_available(**context):
    """
    Check if CitiBike data is available for the target month.
    Raises an exception if data is not available.
    """
    from airflow_utils import get_target_month, check_citibike_data_available

    execution_date = context["logical_date"].date()
    year, month = get_target_month(execution_date)

    print(f"Execution date: {execution_date}")
    print(f"Target month: {year}-{month:02d}")

    is_available = check_citibike_data_available(year, month)

    if not is_available:
        raise ValueError(f"Data NOT available for {year}-{month:02d}")

    # Store target year/month in XCom for downstream tasks
    context["ti"].xcom_push(key="target_year", value=year)
    context["ti"].xcom_push(key="target_month", value=month)
    print(f"Data available for {year}-{month:02d} - continuing pipeline")


def _download_citibike_data(**context):
    """Download CitiBike data for the target month."""
    from airflow_utils import download_citibike_month

    ti = context["ti"]
    year = ti.xcom_pull(key="target_year", task_ids="check_s3_data_available")
    month = ti.xcom_pull(key="target_month", task_ids="check_s3_data_available")

    df = download_citibike_month(year, month)

    # Store row count in XCom for logging
    ti.xcom_push(key="trips_downloaded", value=len(df))
    print(f"Downloaded {len(df):,} trips for {year}-{month:02d}")

    return len(df)


def _delete_existing_trips(**context):
    """Delete existing trips for the target month to prevent duplicates."""
    from airflow_utils import delete_trips_for_month

    ti = context["ti"]
    year = ti.xcom_pull(key="target_year", task_ids="check_s3_data_available")
    month = ti.xcom_pull(key="target_month", task_ids="check_s3_data_available")

    rows_deleted = delete_trips_for_month(year, month)
    print(f"Deleted {rows_deleted:,} existing trips for {year}-{month:02d}")

    return rows_deleted


def _load_trips(**context):
    """Load downloaded trips to BigQuery."""
    import pandas as pd
    from airflow_utils import load_trips_incremental
    from config import DATA_DIR

    ti = context["ti"]
    year = ti.xcom_pull(key="target_year", task_ids="check_s3_data_available")
    month = ti.xcom_pull(key="target_month", task_ids="check_s3_data_available")

    # Read from disk (data was saved in download step)
    date_str = f"{year}{month:02d}"
    csv_path = os.path.join(DATA_DIR, f"{date_str}-citibike-tripdata.csv")

    df = pd.read_csv(csv_path, low_memory=False)
    rows_loaded = load_trips_incremental(df)

    print(f"Loaded {rows_loaded:,} trips to BigQuery")
    return rows_loaded


def _fetch_weather(**context):
    """Fetch weather data for the target month."""
    from airflow_utils import fetch_weather_for_month

    ti = context["ti"]
    year = ti.xcom_pull(key="target_year", task_ids="check_s3_data_available")
    month = ti.xcom_pull(key="target_month", task_ids="check_s3_data_available")

    df = fetch_weather_for_month(year, month)

    # Save to disk for the load step
    weather_dir = os.path.join(PROJECT_ROOT, "data")
    os.makedirs(weather_dir, exist_ok=True)
    weather_path = os.path.join(weather_dir, f"weather_{year}{month:02d}.csv")
    df.to_csv(weather_path, index=False)

    ti.xcom_push(key="weather_path", value=weather_path)
    ti.xcom_push(key="weather_rows", value=len(df))
    print(f"Fetched {len(df):,} weather records for {year}-{month:02d}")

    return len(df)


def _delete_existing_weather(**context):
    """Delete existing weather data for the target month."""
    from airflow_utils import delete_weather_for_month

    ti = context["ti"]
    year = ti.xcom_pull(key="target_year", task_ids="check_s3_data_available")
    month = ti.xcom_pull(key="target_month", task_ids="check_s3_data_available")

    rows_deleted = delete_weather_for_month(year, month)
    print(f"Deleted {rows_deleted:,} existing weather records for {year}-{month:02d}")

    return rows_deleted


def _load_weather(**context):
    """Load weather data to BigQuery."""
    import pandas as pd
    from airflow_utils import load_weather_incremental

    ti = context["ti"]
    weather_path = ti.xcom_pull(key="weather_path", task_ids="fetch_weather")

    df = pd.read_csv(weather_path)
    rows_loaded = load_weather_incremental(df)

    print(f"Loaded {rows_loaded:,} weather records to BigQuery")
    return rows_loaded


# DAG Definition
with DAG(
    dag_id="citibike_monthly_pipeline",
    default_args=default_args,
    description="Monthly CitiBike and weather data pipeline with DBT models",
    schedule="0 6 10 * *",  # 6:00 AM UTC on 10th of every month
    start_date=datetime(2026, 1, 10),  # Run for December 2025 data
    catchup=False,
    tags=["citibike", "bigquery", "dbt"],
    max_active_runs=1,
) as dag:

    # Task 1: Check if data is available
    check_s3_data_available = PythonOperator(
        task_id="check_s3_data_available",
        python_callable=_check_data_available,
    )

    # Task 2: Download CitiBike data
    download_citibike_data = PythonOperator(
        task_id="download_citibike_data",
        python_callable=_download_citibike_data,
    )

    # Task 3: Delete existing trips for the month
    delete_existing_trips = PythonOperator(
        task_id="delete_existing_trips",
        python_callable=_delete_existing_trips,
    )

    # Task 4: Load trips to BigQuery
    load_trips = PythonOperator(
        task_id="load_trips",
        python_callable=_load_trips,
    )

    # Task 5: Fetch weather data
    fetch_weather = PythonOperator(
        task_id="fetch_weather",
        python_callable=_fetch_weather,
    )

    # Task 6: Delete existing weather for the month
    delete_existing_weather = PythonOperator(
        task_id="delete_existing_weather",
        python_callable=_delete_existing_weather,
    )

    # Task 7: Load weather to BigQuery
    load_weather = PythonOperator(
        task_id="load_weather",
        python_callable=_load_weather,
    )

    # Task 8: Run DBT models
    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command=f"cd {DBT_DIR} && dbt run && dbt test",
    )

    # Task dependencies
    check_s3_data_available >> download_citibike_data >> delete_existing_trips >> load_trips
    load_trips >> fetch_weather >> delete_existing_weather >> load_weather
    load_weather >> run_dbt_models
