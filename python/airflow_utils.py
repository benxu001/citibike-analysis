"""Utility functions for Airflow DAG operations.

This module provides incremental loading functions for the monthly CitiBike
data pipeline. Unlike the bulk-load scripts, these functions:
- Don't recreate tables (append only)
- Support single-month operations
- Use delete-then-insert pattern to prevent duplicates
"""

import os
from calendar import monthrange
from datetime import date

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery

from config import (
    S3_BASE_URL,
    PROJECT_ID,
    TRIPS_TABLE_ID,
    WEATHER_TABLE_ID,
    DATA_DIR,
    EXPECTED_COLUMNS,
)
from fetch_citibike_data import download_file, extract_csv_from_zip, validate_schema
from load_trips_to_bigquery import prepare_dataframe, TRIPS_SCHEMA
from load_weather_to_bigquery import WEATHER_SCHEMA


def get_target_month(execution_date: date) -> tuple:
    """
    Given an execution date, return the target (year, month) to process.

    On Feb 10, 2026, we process January 2026 data.

    Args:
        execution_date: The Airflow execution date

    Returns:
        Tuple of (year, month) for the target data
    """
    target = execution_date - relativedelta(months=1)
    return target.year, target.month


def check_citibike_data_available(year: int, month: int) -> bool:
    """
    Check if CitiBike data is available for the given month.

    Since we can't query S3 bucket listings, we attempt HTTP HEAD requests
    to check if the file exists.

    Args:
        year: Target year
        month: Target month

    Returns:
        True if data exists, False otherwise
    """
    date_str = f"{year}{month:02d}"
    url_patterns = [
        f"{S3_BASE_URL}{date_str}-citibike-tripdata.csv.zip",
        f"{S3_BASE_URL}{date_str}-citibike-tripdata.zip",
    ]

    for url in url_patterns:
        try:
            response = requests.head(url, timeout=30)
            if response.status_code == 200:
                return True
        except requests.exceptions.RequestException:
            continue

    return False


def download_citibike_month(year: int, month: int) -> pd.DataFrame:
    """
    Download CitiBike data for a specific month.

    Args:
        year: Target year
        month: Target month

    Returns:
        DataFrame with validated schema

    Raises:
        FileNotFoundError: If no data available for the month
        ValueError: If schema validation fails
    """
    date_str = f"{year}{month:02d}"
    url_patterns = [
        f"{S3_BASE_URL}{date_str}-citibike-tripdata.csv.zip",
        f"{S3_BASE_URL}{date_str}-citibike-tripdata.zip",
    ]

    for url in url_patterns:
        try:
            print(f"Attempting download from: {url}")
            zip_bytes = download_file(url)
            df = extract_csv_from_zip(zip_bytes)

            is_valid, message = validate_schema(df, EXPECTED_COLUMNS)
            if not is_valid:
                raise ValueError(f"Schema validation failed: {message}")

            # Select only expected columns
            df = df[EXPECTED_COLUMNS]

            # Save to disk for debugging/recovery
            os.makedirs(DATA_DIR, exist_ok=True)
            output_path = os.path.join(DATA_DIR, f"{date_str}-citibike-tripdata.csv")
            df.to_csv(output_path, index=False)
            print(f"Saved {len(df):,} rows to {output_path}")

            return df

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                continue
            raise

    raise FileNotFoundError(f"No CitiBike data found for {date_str}")


def delete_trips_for_month(year: int, month: int) -> int:
    """
    Delete existing trips data for a specific month.

    Deletes based on ended_at to match how CitiBike organizes their files
    (files include trips that ended in that month, even if they started
    in the previous month).

    Args:
        year: Target year
        month: Target month

    Returns:
        Number of rows deleted
    """
    client = bigquery.Client(project=PROJECT_ID)

    last_day = monthrange(year, month)[1]
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{last_day:02d}"

    query = f"""
    DELETE FROM `{TRIPS_TABLE_ID}`
    WHERE DATE(ended_at) BETWEEN '{start_date}' AND '{end_date}'
    """

    print(f"Deleting trips where ended_at is between {start_date} and {end_date}...")
    job = client.query(query)
    job.result()  # Wait for completion

    rows_deleted = job.num_dml_affected_rows or 0
    print(f"Deleted {rows_deleted:,} existing rows")

    return rows_deleted


def load_trips_incremental(df: pd.DataFrame) -> int:
    """
    Load trips DataFrame to BigQuery using WRITE_APPEND.

    Unlike the bulk loader, this doesn't recreate the table.

    Args:
        df: DataFrame with trip data

    Returns:
        Number of rows loaded
    """
    client = bigquery.Client(project=PROJECT_ID)

    df = prepare_dataframe(df)

    job_config = bigquery.LoadJobConfig(
        schema=TRIPS_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    print(f"Loading {len(df):,} rows to {TRIPS_TABLE_ID}...")
    job = client.load_table_from_dataframe(df, TRIPS_TABLE_ID, job_config=job_config)
    job.result()

    print(f"Successfully loaded {len(df):,} rows")
    return len(df)


def fetch_weather_for_month(year: int, month: int) -> pd.DataFrame:
    """
    Fetch weather data from Open-Meteo API for a specific month.

    Args:
        year: Target year
        month: Target month

    Returns:
        DataFrame with weather data
    """
    last_day = monthrange(year, month)[1]
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{last_day:02d}"

    print(f"Fetching weather data for {start_date} to {end_date}...")

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 40.7128,
        "longitude": -74.0060,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,precipitation,cloudcover",
        "temperature_unit": "fahrenheit",
        "timezone": "America/New_York"
    }

    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    hourly = data["hourly"]
    df = pd.DataFrame({
        "datetime": pd.to_datetime(hourly["time"]),
        "temperature_f": hourly["temperature_2m"],
        "precipitation_mm": hourly["precipitation"],
        "cloud_cover_pct": hourly["cloudcover"]
    })

    def get_conditions(cloud_cover):
        if cloud_cover is None or pd.isna(cloud_cover):
            return "Unknown"
        elif cloud_cover <= 25:
            return "Sunny"
        elif cloud_cover <= 75:
            return "Partly Cloudy"
        else:
            return "Cloudy"

    df["conditions"] = df["cloud_cover_pct"].apply(get_conditions)

    print(f"Fetched {len(df):,} weather records")
    return df


def delete_weather_for_month(year: int, month: int) -> int:
    """
    Delete existing weather data for a specific month.

    Args:
        year: Target year
        month: Target month

    Returns:
        Number of rows deleted
    """
    client = bigquery.Client(project=PROJECT_ID)

    last_day = monthrange(year, month)[1]
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{last_day:02d}"

    query = f"""
    DELETE FROM `{WEATHER_TABLE_ID}`
    WHERE DATE(datetime) BETWEEN '{start_date}' AND '{end_date}'
    """

    print(f"Deleting weather for {start_date} to {end_date}...")
    job = client.query(query)
    job.result()  # Wait for completion

    rows_deleted = job.num_dml_affected_rows or 0
    print(f"Deleted {rows_deleted:,} existing weather rows")

    return rows_deleted


def load_weather_incremental(df: pd.DataFrame) -> int:
    """
    Load weather DataFrame to BigQuery using WRITE_APPEND.

    Args:
        df: DataFrame with weather data

    Returns:
        Number of rows loaded
    """
    client = bigquery.Client(project=PROJECT_ID)

    # Ensure proper types
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["temperature_f"] = pd.to_numeric(df["temperature_f"], errors="coerce")
    df["precipitation_mm"] = pd.to_numeric(df["precipitation_mm"], errors="coerce")
    df["cloud_cover_pct"] = pd.to_numeric(df["cloud_cover_pct"], errors="coerce")

    job_config = bigquery.LoadJobConfig(
        schema=WEATHER_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    print(f"Loading {len(df):,} weather rows to {WEATHER_TABLE_ID}...")
    job = client.load_table_from_dataframe(df, WEATHER_TABLE_ID, job_config=job_config)
    job.result()

    print(f"Successfully loaded {len(df):,} weather rows")
    return len(df)


def run_dbt_models(dbt_project_dir: str) -> bool:
    """
    Run DBT models.

    Args:
        dbt_project_dir: Path to DBT project directory

    Returns:
        True if successful, False otherwise
    """
    import subprocess

    print(f"Running DBT models from {dbt_project_dir}...")

    # Run dbt run
    result = subprocess.run(
        ["dbt", "run"],
        cwd=dbt_project_dir,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(f"DBT run failed: {result.stderr}")
        return False

    print(result.stdout)

    # Run dbt test
    result = subprocess.run(
        ["dbt", "test"],
        cwd=dbt_project_dir,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(f"DBT test failed: {result.stderr}")
        return False

    print(result.stdout)
    return True


if __name__ == "__main__":
    # Test functions with January 2025 data
    from datetime import date

    test_date = date(2025, 2, 10)  # Should process Jan 2025
    year, month = get_target_month(test_date)
    print(f"Target month for {test_date}: {year}-{month:02d}")

    # Check availability
    is_available = check_citibike_data_available(year, month)
    print(f"Data available: {is_available}")
