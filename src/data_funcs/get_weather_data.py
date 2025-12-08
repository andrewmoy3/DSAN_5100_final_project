import openmeteo_requests

import pandas as pd
import requests_cache
from retry_requests import retry

def get_weather_data(city, lat, lon):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": "2023-01-01",
        "end_date": "2023-12-31",
        "daily": ["cloud_cover_mean", "relative_humidity_2m_mean", "wind_gusts_10m_mean", "wind_speed_10m_mean", "precipitation_hours", "precipitation_sum", "temperature_2m_mean"],
        "timezone": "auto",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation: {response.Elevation()} m asl")
    print(f"Timezone: {response.Timezone()}{response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

    # Process daily data. The order of variables needs to be the same as requested.
    daily = response.Daily()
    daily_cloud_cover_mean = daily.Variables(0).ValuesAsNumpy()
    daily_relative_humidity_2m_mean = daily.Variables(1).ValuesAsNumpy()
    daily_wind_gusts_10m_mean = daily.Variables(2).ValuesAsNumpy()
    daily_wind_speed_10m_mean = daily.Variables(3).ValuesAsNumpy()
    daily_precipitation_hours = daily.Variables(4).ValuesAsNumpy()
    daily_precipitation_sum = daily.Variables(5).ValuesAsNumpy()
    daily_temperature_2m_mean = daily.Variables(6).ValuesAsNumpy()

    daily_data = {"date": pd.date_range(
        start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
        end =  pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = daily.Interval()),
        inclusive = "left"
    )}

    daily_data["cloud_cover_mean"] = daily_cloud_cover_mean
    daily_data["relative_humidity_2m_mean"] = daily_relative_humidity_2m_mean
    daily_data["wind_gusts_10m_mean"] = daily_wind_gusts_10m_mean
    daily_data["wind_speed_10m_mean"] = daily_wind_speed_10m_mean
    daily_data["precipitation_hours"] = daily_precipitation_hours
    daily_data["precipitation_sum"] = daily_precipitation_sum
    daily_data["temperature_2m_mean"] = daily_temperature_2m_mean

    df = pd.DataFrame(data = daily_data)

    save_path = f"data/weather_data/{city}.csv"
    df.to_csv(save_path, index = False)

cities = {
    "New York": (40.7128, -74.0060),
    "Los Angeles": (34.0522, -118.2437),
    "Chicago": (41.8781, -87.6298),
    "Houston": (29.7604, -95.3698),
    "Phoenix": (33.4484, -112.0740),
    "Philadelphia": (39.9526, -75.1652),
    "San Antonio": (29.4241, -98.4936),
    "San Diego": (32.7157, -117.1611),
    "Dallas": (32.7767, -96.7970),
    "San Jose": (37.3382, -121.8863),
}

for city, (lat, lon) in cities.items():
    print(f"Fetching weather data for {city}...")
    get_weather_data(city, lat, lon)
    print(f"Data for {city} saved.\n")


