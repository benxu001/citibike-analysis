import requests
import pandas as pd

def fetch_nyc_weather():
    """Fetch historical hourly weather data for NYC from Open-Meteo API."""

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 40.7128,
        "longitude": -74.0060,
        "start_date": "2024-01-01",
        "end_date": "2025-12-31",
        "hourly": "temperature_2m,precipitation,cloudcover",
        "temperature_unit": "fahrenheit",
        "timezone": "America/New_York"
    }

    print("Fetching weather data from Open-Meteo API...")
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    # Extract hourly data
    hourly = data["hourly"]

    # Create DataFrame
    df = pd.DataFrame({
        "datetime": pd.to_datetime(hourly["time"]),
        "temperature_f": hourly["temperature_2m"],
        "precipitation_mm": hourly["precipitation"],
        "cloud_cover_pct": hourly["cloudcover"]
    })

    # Derive conditions from cloud cover
    def get_conditions(cloud_cover):
        if cloud_cover is None:
            return "Unknown"
        elif cloud_cover <= 25:
            return "Sunny"
        elif cloud_cover <= 75:
            return "Partly Cloudy"
        else:
            return "Cloudy"

    df["conditions"] = df["cloud_cover_pct"].apply(get_conditions)

    # Save to CSV
    output_file = "nyc_weather_2024_2025.csv"
    df.to_csv(output_file, index=False)

    print(f"Saved {len(df)} rows to {output_file}")
    print(f"\nSample data:")
    print(df.head(10).to_string(index=False))

    return df

if __name__ == "__main__":
    fetch_nyc_weather()
