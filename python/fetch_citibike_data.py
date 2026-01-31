"""Fetch CitiBike trip data from S3 bucket."""

import os
import zipfile
import requests
import pandas as pd
from tqdm import tqdm
from io import BytesIO

from config import (
    S3_BASE_URL,
    START_YEAR,
    START_MONTH,
    END_YEAR,
    END_MONTH,
    DATA_DIR,
    EXPECTED_COLUMNS,
)


def generate_file_urls(start_year, start_month, end_year, end_month):
    """Generate list of URLs to download for the given date range."""
    urls = []

    year = start_year
    month = start_month

    while (year < end_year) or (year == end_year and month <= end_month):
        # Format: YYYYMM-citibike-tripdata
        date_str = f"{year}{month:02d}"

        # Try both file extensions - some months use .csv.zip, others use .zip
        url_patterns = [
            f"{S3_BASE_URL}{date_str}-citibike-tripdata.csv.zip",
            f"{S3_BASE_URL}{date_str}-citibike-tripdata.zip",
        ]

        urls.append({
            "date_str": date_str,
            "patterns": url_patterns,
        })

        # Move to next month
        month += 1
        if month > 12:
            month = 1
            year += 1

    return urls


def download_file(url, timeout=60):
    """Download a file from URL and return bytes."""
    response = requests.get(url, timeout=timeout, stream=True)
    response.raise_for_status()
    return response.content


def extract_csv_from_zip(zip_bytes):
    """Extract ALL CSV files from zip bytes and return as combined DataFrame.

    Each zip may contain multiple CSV files (max 1M rows each), e.g.:
    - 202401-citibike-tripdata_1.csv (1M rows)
    - 202401-citibike-tripdata_2.csv (remaining rows)
    """
    with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
        # Find all CSV files in the archive
        csv_files = sorted([f for f in zf.namelist() if f.endswith('.csv')])

        if not csv_files:
            raise ValueError("No CSV file found in zip archive")

        # Read and concatenate all CSV files
        dfs = []
        for csv_filename in csv_files:
            with zf.open(csv_filename) as csv_file:
                df = pd.read_csv(csv_file, low_memory=False)
                dfs.append(df)

        # Combine all DataFrames
        combined_df = pd.concat(dfs, ignore_index=True)

    return combined_df


def validate_schema(df, expected_columns):
    """Validate that DataFrame has expected columns."""
    actual_columns = set(df.columns)
    expected_set = set(expected_columns)

    missing = expected_set - actual_columns
    extra = actual_columns - expected_set

    if missing:
        return False, f"Missing columns: {missing}"

    if extra:
        # Extra columns are OK, we'll just select the expected ones
        pass

    return True, "Schema valid"


def fetch_citibike_data(save_to_disk=True):
    """
    Fetch CitiBike trip data for the configured date range.

    Args:
        save_to_disk: If True, save CSVs to data/raw directory

    Returns:
        List of DataFrames, one per month
    """
    urls = generate_file_urls(START_YEAR, START_MONTH, END_YEAR, END_MONTH)

    print(f"Fetching CitiBike data from {START_YEAR}-{START_MONTH:02d} to {END_YEAR}-{END_MONTH:02d}")
    print(f"Total months to download: {len(urls)}")
    print()

    if save_to_disk:
        os.makedirs(DATA_DIR, exist_ok=True)

    all_dfs = []
    failed_downloads = []
    schema_errors = []

    for url_info in tqdm(urls, desc="Downloading files"):
        date_str = url_info["date_str"]
        patterns = url_info["patterns"]

        downloaded = False

        for url in patterns:
            try:
                # Download the zip file
                zip_bytes = download_file(url)

                # Extract and read CSV
                df = extract_csv_from_zip(zip_bytes)

                # Validate schema
                is_valid, message = validate_schema(df, EXPECTED_COLUMNS)

                if not is_valid:
                    schema_errors.append({"date": date_str, "message": message})
                    print(f"\nWarning: {date_str} - {message}")
                    continue

                # Select only expected columns (in case there are extras)
                df = df[EXPECTED_COLUMNS]

                # Save to disk if requested
                if save_to_disk:
                    output_path = os.path.join(DATA_DIR, f"{date_str}-citibike-tripdata.csv")
                    df.to_csv(output_path, index=False)

                all_dfs.append({
                    "date_str": date_str,
                    "df": df,
                    "row_count": len(df),
                })

                downloaded = True
                break

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    # Try next URL pattern
                    continue
                else:
                    raise
            except Exception as e:
                # Try next URL pattern
                continue

        if not downloaded:
            failed_downloads.append(date_str)
            print(f"\nFailed to download: {date_str}")

    # Print summary
    print("\n" + "="*50)
    print("Download Summary")
    print("="*50)
    print(f"Successfully downloaded: {len(all_dfs)} files")

    total_rows = sum(d["row_count"] for d in all_dfs)
    print(f"Total rows: {total_rows:,}")

    if failed_downloads:
        print(f"\nFailed downloads ({len(failed_downloads)}):")
        for date in failed_downloads:
            print(f"  - {date}")

    if schema_errors:
        print(f"\nSchema errors ({len(schema_errors)}):")
        for error in schema_errors:
            print(f"  - {error['date']}: {error['message']}")

    return all_dfs


def load_local_csvs():
    """Load previously downloaded CSV files from data/raw directory."""
    if not os.path.exists(DATA_DIR):
        raise FileNotFoundError(f"Data directory not found: {DATA_DIR}")

    csv_files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith('.csv')])

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {DATA_DIR}")

    print(f"Loading {len(csv_files)} CSV files from {DATA_DIR}")

    all_dfs = []

    for filename in tqdm(csv_files, desc="Loading files"):
        filepath = os.path.join(DATA_DIR, filename)
        date_str = filename.replace("-citibike-tripdata.csv", "")

        df = pd.read_csv(filepath, low_memory=False)

        all_dfs.append({
            "date_str": date_str,
            "df": df,
            "row_count": len(df),
        })

    total_rows = sum(d["row_count"] for d in all_dfs)
    print(f"Loaded {len(all_dfs)} files with {total_rows:,} total rows")

    return all_dfs


if __name__ == "__main__":
    # Fetch and save all data
    data = fetch_citibike_data(save_to_disk=True)

    # Print sample from first file
    if data:
        print("\nSample data from first file:")
        print(data[0]["df"].head().to_string())
