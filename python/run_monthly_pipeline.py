#!/usr/bin/env python3
"""
Monthly incremental pipeline for CitiBike data.

This script is designed to be run by GitHub Actions on a schedule.

With no arguments it catches up: it finds the latest month already loaded in
BigQuery and processes every month after that, up to and including last
month, for which CitiBike has published a file. A month that was published
late is picked up automatically on the next run instead of being skipped.

Usage:
    python run_monthly_pipeline.py                    # Catch up on all missing months
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
    get_latest_loaded_month,
    check_citibike_data_available,
    download_citibike_month,
    delete_trips_for_month,
    load_trips_incremental,
    fetch_weather_for_month,
    delete_weather_for_month,
    load_weather_incremental,
)


def months_between(start: tuple, end: tuple) -> list:
    """Return [(year, month), ...] from start through end inclusive."""
    months = []
    current = date(start[0], start[1], 1)
    last = date(end[0], end[1], 1)
    while current <= last:
        months.append((current.year, current.month))
        current += relativedelta(months=1)
    return months


def process_month(year: int, month: int) -> tuple:
    """
    Load one month of trips and weather into BigQuery.

    Returns:
        (rows_loaded, weather_loaded)
    """
    print(f"Target month: {year}-{month:02d}")
    print()

    # Step 1: Download CitiBike data
    print("Step 1/6: Downloading CitiBike data...")
    print("-" * 40)
    df_trips = download_citibike_month(year, month)
    print(f"Downloaded {len(df_trips):,} trips")
    print()

    # Step 2: Delete existing trips
    print("Step 2/6: Deleting existing trips for month...")
    print("-" * 40)
    rows_deleted = delete_trips_for_month(year, month)
    print(f"Deleted {rows_deleted:,} existing trips")
    print()

    # Step 3: Load trips to BigQuery
    print("Step 3/6: Loading trips to BigQuery...")
    print("-" * 40)
    rows_loaded = load_trips_incremental(df_trips)
    print(f"Loaded {rows_loaded:,} trips")
    print()

    # Step 4: Fetch weather data
    print("Step 4/6: Fetching weather data...")
    print("-" * 40)
    df_weather = fetch_weather_for_month(year, month)
    print(f"Fetched {len(df_weather):,} weather records")
    print()

    # Step 5: Delete existing weather
    print("Step 5/6: Deleting existing weather for month...")
    print("-" * 40)
    weather_deleted = delete_weather_for_month(year, month)
    print(f"Deleted {weather_deleted:,} existing weather records")
    print()

    # Step 6: Load weather to BigQuery
    print("Step 6/6: Loading weather to BigQuery...")
    print("-" * 40)
    weather_loaded = load_weather_incremental(df_weather)
    print(f"Loaded {weather_loaded:,} weather records")
    print()

    return rows_loaded, weather_loaded


def run_dbt():
    """Run dbt models and tests, exiting on failure."""
    print("Running dbt models...")
    print("-" * 40)
    dbt_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dbt_citibike")

    result = subprocess.run(["dbt", "run"], cwd=dbt_dir, capture_output=False)
    if result.returncode != 0:
        print("ERROR: dbt run failed")
        sys.exit(1)

    result = subprocess.run(["dbt", "test"], cwd=dbt_dir, capture_output=False)
    if result.returncode != 0:
        print("ERROR: dbt test failed")
        sys.exit(1)
    print()


def run_monthly_pipeline(year: int = None, month: int = None, skip_dbt: bool = False):
    """
    Run the monthly incremental pipeline.

    Args:
        year: Target year (default: catch up from the latest loaded month)
        month: Target month (default: catch up from the latest loaded month)
        skip_dbt: If True, skip running dbt models
    """
    print("=" * 60)
    print("CitiBike Monthly Pipeline")
    print("=" * 60)
    print()

    # Determine which months to process
    last_month = get_target_month(date.today())
    if year is not None and month is not None:
        candidates = [(year, month)]
    else:
        latest_loaded = get_latest_loaded_month()
        if latest_loaded is None:
            candidates = [last_month]
        else:
            print(f"Latest month in BigQuery: {latest_loaded[0]}-{latest_loaded[1]:02d}")
            next_month = date(latest_loaded[0], latest_loaded[1], 1) + relativedelta(months=1)
            candidates = months_between((next_month.year, next_month.month), last_month)

        if not candidates:
            print("BigQuery is up to date - nothing to process")
            return

        print("Months to process: " + ", ".join(f"{y}-{m:02d}" for y, m in candidates))
        print()

    # Check availability. CitiBike publishes in order, so stop at the first
    # month that is missing rather than loading around a gap.
    print("Checking data availability...")
    print("-" * 40)
    available = []
    for y, m in candidates:
        if check_citibike_data_available(y, m):
            print(f"  {y}-{m:02d}: available")
            available.append((y, m))
        else:
            print(f"  {y}-{m:02d}: not published yet")
            break
    print()

    if not available:
        pending = ", ".join(f"{y}-{m:02d}" for y, m in candidates)
        print(f"ERROR: No data available. Pending months: {pending}")
        sys.exit(1)

    results = {}
    for y, m in available:
        results[(y, m)] = process_month(y, m)

    if not skip_dbt:
        run_dbt()

    print("=" * 60)
    print("Pipeline Complete!")
    print("=" * 60)
    print()
    for (y, m), (rows_loaded, weather_loaded) in results.items():
        print(f"Summary for {y}-{m:02d}:")
        print(f"  - Trips loaded: {rows_loaded:,}")
        print(f"  - Weather records loaded: {weather_loaded:,}")

    skipped = [ym for ym in candidates if ym not in results]
    if skipped:
        print()
        print("Not yet published (will be picked up on the next run): "
              + ", ".join(f"{y}-{m:02d}" for y, m in skipped))


def main():
    parser = argparse.ArgumentParser(
        description="Run the CitiBike monthly incremental pipeline"
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Target year (default: catch up from the latest loaded month)"
    )
    parser.add_argument(
        "--month",
        type=int,
        help="Target month (default: catch up from the latest loaded month)"
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
