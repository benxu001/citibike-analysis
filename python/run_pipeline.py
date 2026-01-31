"""
Run the full data pipeline:
1. Fetch CitiBike trip data from S3
2. Load trips to BigQuery
3. Load weather data to BigQuery
"""

import argparse
import sys

from fetch_citibike_data import fetch_citibike_data, load_local_csvs
from load_trips_to_bigquery import load_trips_to_bigquery
from load_weather_to_bigquery import load_weather_to_bigquery


def run_pipeline(skip_download=False, trips_only=False, weather_only=False):
    """
    Run the full data pipeline.

    Args:
        skip_download: If True, use existing CSV files instead of downloading
        trips_only: If True, only load trips data
        weather_only: If True, only load weather data
    """
    print("="*60)
    print("CitiBike Data Pipeline")
    print("="*60)
    print()

    if weather_only:
        # Only load weather
        print("Step 1/1: Loading weather data to BigQuery")
        print("-"*40)
        load_weather_to_bigquery()
        print("\nPipeline complete!")
        return

    if trips_only:
        # Only load trips
        if skip_download:
            print("Step 1/1: Loading existing trip data to BigQuery")
            print("-"*40)
            load_trips_to_bigquery()
        else:
            print("Step 1/2: Fetching trip data from S3")
            print("-"*40)
            data = fetch_citibike_data(save_to_disk=True)

            print("\nStep 2/2: Loading trip data to BigQuery")
            print("-"*40)
            load_trips_to_bigquery(data)

        print("\nPipeline complete!")
        return

    # Full pipeline
    if skip_download:
        print("Step 1/2: Loading existing trip data to BigQuery")
        print("-"*40)
        load_trips_to_bigquery()
    else:
        print("Step 1/3: Fetching trip data from S3")
        print("-"*40)
        data = fetch_citibike_data(save_to_disk=True)

        print("\nStep 2/3: Loading trip data to BigQuery")
        print("-"*40)
        load_trips_to_bigquery(data)

    print("\nStep 3/3: Loading weather data to BigQuery")
    print("-"*40)
    load_weather_to_bigquery()

    print("\n" + "="*60)
    print("Pipeline Complete!")
    print("="*60)
    print()
    print("Next steps:")
    print("1. Navigate to the dbt_citibike directory")
    print("2. Run: dbt debug (to verify connection)")
    print("3. Run: dbt run (to build all models)")
    print("4. Run: dbt test (to run tests)")


def main():
    parser = argparse.ArgumentParser(
        description="Run the CitiBike data pipeline"
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip downloading and use existing CSV files"
    )
    parser.add_argument(
        "--trips-only",
        action="store_true",
        help="Only process trips data (skip weather)"
    )
    parser.add_argument(
        "--weather-only",
        action="store_true",
        help="Only process weather data (skip trips)"
    )

    args = parser.parse_args()

    if args.trips_only and args.weather_only:
        print("Error: Cannot specify both --trips-only and --weather-only")
        sys.exit(1)

    run_pipeline(
        skip_download=args.skip_download,
        trips_only=args.trips_only,
        weather_only=args.weather_only,
    )


if __name__ == "__main__":
    main()
