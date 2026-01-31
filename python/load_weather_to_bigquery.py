"""Load weather data to BigQuery."""

import pandas as pd
from google.cloud import bigquery

from config import (
    PROJECT_ID,
    DATASET_ID,
    WEATHER_TABLE_ID,
    WEATHER_CSV,
)


# BigQuery schema definition
WEATHER_SCHEMA = [
    bigquery.SchemaField("datetime", "TIMESTAMP"),
    bigquery.SchemaField("temperature_f", "FLOAT64"),
    bigquery.SchemaField("precipitation_mm", "FLOAT64"),
    bigquery.SchemaField("cloud_cover_pct", "FLOAT64"),
    bigquery.SchemaField("conditions", "STRING"),
]


def create_dataset_if_not_exists(client, dataset_id):
    """Create BigQuery dataset if it doesn't exist."""
    dataset_ref = f"{PROJECT_ID}.{dataset_id}"

    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_ref} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"Created dataset {dataset_ref}")


def create_weather_table(client, table_id):
    """Create the weather table."""
    table = bigquery.Table(table_id, schema=WEATHER_SCHEMA)

    try:
        client.delete_table(table_id)
        print(f"Deleted existing table {table_id}")
    except Exception:
        pass

    table = client.create_table(table)
    print(f"Created table {table_id}")

    return table


def load_weather_to_bigquery(csv_path=None):
    """
    Load weather data from CSV to BigQuery.

    Args:
        csv_path: Path to weather CSV file. Defaults to config WEATHER_CSV.
    """
    if csv_path is None:
        csv_path = WEATHER_CSV

    print(f"Loading weather data from {csv_path}")

    # Read CSV
    df = pd.read_csv(csv_path)
    print(f"Read {len(df):,} rows from CSV")

    # Convert datetime column
    df["datetime"] = pd.to_datetime(df["datetime"])

    # Ensure numeric columns are correct types
    df["temperature_f"] = pd.to_numeric(df["temperature_f"], errors="coerce")
    df["precipitation_mm"] = pd.to_numeric(df["precipitation_mm"], errors="coerce")
    df["cloud_cover_pct"] = pd.to_numeric(df["cloud_cover_pct"], errors="coerce")

    # Create BigQuery client
    client = bigquery.Client(project=PROJECT_ID)

    # Ensure dataset exists
    create_dataset_if_not_exists(client, DATASET_ID)

    # Create table
    create_weather_table(client, WEATHER_TABLE_ID)

    # Configure load job
    job_config = bigquery.LoadJobConfig(
        schema=WEATHER_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Load data
    print(f"Loading data to {WEATHER_TABLE_ID}...")
    job = client.load_table_from_dataframe(
        df,
        WEATHER_TABLE_ID,
        job_config=job_config,
    )
    job.result()  # Wait for completion

    # Verify row count
    query = f"SELECT COUNT(*) as count FROM `{WEATHER_TABLE_ID}`"
    result = client.query(query).result()
    actual_count = list(result)[0].count

    print(f"\nLoad complete!")
    print(f"Rows loaded: {actual_count:,}")

    # Print date range
    query = f"""
    SELECT
        MIN(datetime) as min_date,
        MAX(datetime) as max_date
    FROM `{WEATHER_TABLE_ID}`
    """
    result = client.query(query).result()
    row = list(result)[0]
    print(f"Date range: {row.min_date} to {row.max_date}")

    return actual_count


if __name__ == "__main__":
    load_weather_to_bigquery()
