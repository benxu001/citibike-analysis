"""Load CitiBike trip data to BigQuery."""

import pandas as pd
from google.cloud import bigquery
from tqdm import tqdm

from config import (
    PROJECT_ID,
    DATASET_ID,
    TRIPS_TABLE_ID,
    EXPECTED_COLUMNS,
)
from fetch_citibike_data import load_local_csvs


# BigQuery schema definition
TRIPS_SCHEMA = [
    bigquery.SchemaField("ride_id", "STRING"),
    bigquery.SchemaField("rideable_type", "STRING"),
    bigquery.SchemaField("started_at", "TIMESTAMP"),
    bigquery.SchemaField("ended_at", "TIMESTAMP"),
    bigquery.SchemaField("start_station_name", "STRING"),
    bigquery.SchemaField("start_station_id", "STRING"),
    bigquery.SchemaField("end_station_name", "STRING"),
    bigquery.SchemaField("end_station_id", "STRING"),
    bigquery.SchemaField("start_lat", "FLOAT64"),
    bigquery.SchemaField("start_lng", "FLOAT64"),
    bigquery.SchemaField("end_lat", "FLOAT64"),
    bigquery.SchemaField("end_lng", "FLOAT64"),
    bigquery.SchemaField("member_casual", "STRING"),
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


def create_trips_table(client, table_id):
    """Create the trips table with partitioning and clustering."""
    table = bigquery.Table(table_id, schema=TRIPS_SCHEMA)

    # Partition by started_at date
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="started_at",
    )

    # Cluster by commonly filtered columns
    table.clustering_fields = ["start_station_id", "member_casual"]

    try:
        client.delete_table(table_id)
        print(f"Deleted existing table {table_id}")
    except Exception:
        pass

    table = client.create_table(table)
    print(f"Created table {table_id}")
    print(f"  Partitioned by: started_at (DAY)")
    print(f"  Clustered by: start_station_id, member_casual")

    return table


def prepare_dataframe(df):
    """Prepare DataFrame for BigQuery loading."""
    df = df.copy()

    # Convert timestamp columns
    df["started_at"] = pd.to_datetime(df["started_at"])
    df["ended_at"] = pd.to_datetime(df["ended_at"])

    # Convert numeric columns
    df["start_lat"] = pd.to_numeric(df["start_lat"], errors="coerce")
    df["start_lng"] = pd.to_numeric(df["start_lng"], errors="coerce")
    df["end_lat"] = pd.to_numeric(df["end_lat"], errors="coerce")
    df["end_lng"] = pd.to_numeric(df["end_lng"], errors="coerce")

    # Ensure string columns are strings
    string_cols = ["ride_id", "rideable_type", "start_station_name",
                   "start_station_id", "end_station_name", "end_station_id",
                   "member_casual"]
    for col in string_cols:
        df[col] = df[col].astype(str).replace("nan", None)

    return df


def load_trips_to_bigquery(data_list=None, batch_size=5):
    """
    Load trip data to BigQuery.

    Args:
        data_list: List of dicts with 'df' keys containing DataFrames.
                   If None, loads from local CSV files.
        batch_size: Number of files to process before uploading to BigQuery.
    """
    client = bigquery.Client(project=PROJECT_ID)

    # Ensure dataset exists
    create_dataset_if_not_exists(client, DATASET_ID)

    # Create table
    create_trips_table(client, TRIPS_TABLE_ID)

    # Load data from disk if not provided
    if data_list is None:
        data_list = load_local_csvs()

    print(f"\nLoading {len(data_list)} files to BigQuery...")

    total_rows_loaded = 0
    errors = []

    # Process in batches
    for i in tqdm(range(0, len(data_list), batch_size), desc="Uploading batches"):
        batch = data_list[i:i + batch_size]

        # Combine DataFrames in batch
        dfs = []
        for item in batch:
            df = prepare_dataframe(item["df"])
            dfs.append(df)

        combined_df = pd.concat(dfs, ignore_index=True)

        # Configure load job
        job_config = bigquery.LoadJobConfig(
            schema=TRIPS_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        try:
            job = client.load_table_from_dataframe(
                combined_df,
                TRIPS_TABLE_ID,
                job_config=job_config,
            )
            job.result()  # Wait for completion

            total_rows_loaded += len(combined_df)

        except Exception as e:
            errors.append({
                "batch": i,
                "error": str(e),
            })
            print(f"\nError loading batch {i}: {e}")

    # Print summary
    print("\n" + "="*50)
    print("Load Summary")
    print("="*50)
    print(f"Total rows loaded: {total_rows_loaded:,}")

    if errors:
        print(f"\nErrors ({len(errors)}):")
        for error in errors:
            print(f"  Batch {error['batch']}: {error['error']}")

    # Verify row count
    query = f"SELECT COUNT(*) as count FROM `{TRIPS_TABLE_ID}`"
    result = client.query(query).result()
    actual_count = list(result)[0].count
    print(f"\nVerification - Rows in table: {actual_count:,}")

    return total_rows_loaded


if __name__ == "__main__":
    load_trips_to_bigquery()
