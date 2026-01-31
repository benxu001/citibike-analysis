#!/usr/bin/env python3
"""
Monthly incremental pipeline for CitiBike data.

This script is designed to be run by GitHub Actions on a schedule.
It processes the previous month's data using incremental loading.

Usage:
    python run_monthly_pipeline.py                    # Process previous month
    python run_monthly_pipeline.py --year 2025 --month 12  # Process specific month
"""

import argparse
import subprocess
import sys
import os
from datetime import date
from dateutil.relativedelta import relativedelta

# Add the python directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from airflow_utils import (
    get_target_month,
    check_citibike_data_available,
    download_citibike_month,
    delete_trips_for_month,
    load_trips_incremental,
    fetch_weather_for_month,
    delete_weather_for_month,
    load_weather_incremental,
)


def run_monthly_pipeline(year: int = None, month: int = None, skip_dbt: bool = False):
    """
    Run the monthly incremental pipeline.

    Args:
        year: Target year (default: previous month's year)
        month: Target month (default: previous month)
        skip_dbt: If True, skip running dbt models
    """
    print("=" * 60)
    print("CitiBike Monthly Pipeline")
    print("=" * 60)
    print()

    # Determine target month
    if year is None or month is None:
        today = date.today()
        year, month = get_target_month(today)

    print(f"Target month: {year}-{month:02d}")
    print()

    # Step 1: Check data availability
    print("Step 1/7: Checking data availability...")
    print("-" * 40)
    if not check_citibike_data_available(year, month):
        print(f"ERROR: Data not available for {year}-{month:02d}")
        sys.exit(1)
    print(f"Data available for {year}-{month:02d}")
    print()

    # Step 2: Download CitiBike data
    print("Step 2/7: Downloading CitiBike data...")
    print("-" * 40)
    df_trips = download_citibike_month(year, month)
    print(f"Downloaded {len(df_trips):,} trips")
    print()

    # Step 3: Delete existing trips
    print("Step 3/7: Deleting existing trips for month...")
    print("-" * 40)
    rows_deleted = delete_trips_for_month(year, month)
    print(f"Deleted {rows_deleted:,} existing trips")
    print()

    # Step 4: Load trips to BigQuery
    print("Step 4/7: Loading trips to BigQuery...")
    print("-" * 40)
    rows_loaded = load_trips_incremental(df_trips)
    print(f"Loaded {rows_loaded:,} trips")
    print()

    # Step 5: Fetch weather data
    print("Step 5/7: Fetching weather data...")
    print("-" * 40)
    df_weather = fetch_weather_for_month(year, month)
    print(f"Fetched {len(df_weather):,} weather records")
    print()

    # Step 6: Delete existing weather
    print("Step 6/7: Deleting existing weather for month...")
    print("-" * 40)
    weather_deleted = delete_weather_for_month(year, month)
    print(f"Deleted {weather_deleted:,} existing weather records")
    print()

    # Step 7: Load weather to BigQuery
    print("Step 7/7: Loading weather to BigQuery...")
    print("-" * 40)
    weather_loaded = load_weather_incremental(df_weather)
    print(f"Loaded {weather_loaded:,} weather records")
    print()

    # Step 8: Run dbt (optional)
    if not skip_dbt:
        print("Step 8: Running dbt models...")
        print("-" * 40)
        dbt_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dbt_citibike")

        # Run dbt run
        result = subprocess.run(
            ["dbt", "run"],
            cwd=dbt_dir,
            capture_output=False,
        )
        if result.returncode != 0:
            print("ERROR: dbt run failed")
            sys.exit(1)

        # Run dbt test
        result = subprocess.run(
            ["dbt", "test"],
            cwd=dbt_dir,
            capture_output=False,
        )
        if result.returncode != 0:
            print("ERROR: dbt test failed")
            sys.exit(1)
        print()

    print("=" * 60)
    print("Pipeline Complete!")
    print("=" * 60)
    print()
    print(f"Summary for {year}-{month:02d}:")
    print(f"  - Trips loaded: {rows_loaded:,}")
    print(f"  - Weather records loaded: {weather_loaded:,}")


def main():
    parser = argparse.ArgumentParser(
        description="Run the CitiBike monthly incremental pipeline"
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Target year (default: previous month's year)"
    )
    parser.add_argument(
        "--month",
        type=int,
        help="Target month (default: previous month)"
    )
    parser.add_argument(
        "--skip-dbt",
        action="store_true",
        help="Skip running dbt models"
    )

    args = parser.parse_args()

    # Validate month if provided
    if args.month is not None and (args.month < 1 or args.month > 12):
        print("Error: Month must be between 1 and 12")
        sys.exit(1)

    # If one is provided, both must be provided
    if (args.year is None) != (args.month is None):
        print("Error: Must provide both --year and --month, or neither")
        sys.exit(1)

    run_monthly_pipeline(
        year=args.year,
        month=args.month,
        skip_dbt=args.skip_dbt,
    )


if __name__ == "__main__":
    main()
