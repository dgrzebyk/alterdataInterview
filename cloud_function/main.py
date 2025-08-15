# Data:  15.08.2025
# Autor: Daniel Grzebyk


import os
import functions_framework
import logging
import pandas as pd

from datetime import datetime

from openaq import OpenAQ
from typing import Dict, Tuple, List
from utils import upload_blob


# Constants
CITY_COORDINATES: Dict[str, Tuple[float, float]] = {
    "Warszawa": (52.2297, 21.0122),
    "Londyn": (51.5072, 0.1276)
}
RADIUS: int = 10000  # 10 km
BUCKET_NAME: str = 'openaq-weather-data'

# Configure logging
logging.basicConfig(level=logging.INFO)


def get_locations(client: OpenAQ, city: str, coordinates: Tuple[float, float]) -> pd.DataFrame:
    """Fetch measurement station locations for a city."""
    response = client.locations.list(coordinates=coordinates, radius=RADIUS, limit=1000)
    locations_dict = response.dict()
    locations_df = pd.json_normalize(locations_dict['results'])
    return locations_df


def validate_locations(locations_df: pd.DataFrame, city: str) -> List[int]:
    """Validate and extract station IDs for a city."""
    locations_ids = locations_df['id'].to_list()
    if len(locations_ids) < 3:
        logging.error(f"ERROR: Dla miasta {city} nie znaleziono co najmniej 3 stacji pomiarowych.")
        return []
    return locations_ids


def process_measurements(client: OpenAQ, locations_ids: List[int], locations_df: pd.DataFrame) -> pd.DataFrame:
    """Fetch and process measurement data."""
    measurements_list = []
    for location_id in locations_ids:
        measurements = client.locations.latest(locations_id=location_id)
        df = pd.DataFrame(measurements.dict()['results'])
        measurements_list.append(df)

    measurements_df = pd.concat(measurements_list, ignore_index=True)
    measurements_df = measurements_df.merge(
        locations_df[['sensors_id', 'parameter', 'unit']],
        how='left',
        on='sensors_id'
    )
    measurements_df['datetime_utc'] = measurements_df['datetime'].apply(lambda x: x['utc'])
    measurements_df['datetime_local'] = measurements_df['datetime'].apply(lambda x: x['local'])
    measurements_df['latitude'] = measurements_df['coordinates'].apply(lambda x: x['latitude'])
    measurements_df['longitude'] = measurements_df['coordinates'].apply(lambda x: x['longitude'])
    measurements_df.drop(columns=['datetime', 'coordinates'], inplace=True)

    measurements_df = measurements_df.loc[measurements_df['parameter'].isin(['no2', 'o3', 'pm10', 'pm25'])]

    return measurements_df


def save_to_csv_and_upload(results_df: pd.DataFrame, bucket_name: str) -> None:
    """Save results to a CSV file and upload to GCS."""
    dt_now = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    file_name = f"{dt_now}.csv"
    results_df[['city', 'latitude', 'longitude', 'parameter', 'value', 'unit', 'datetime_utc',
                'datetime_local']].to_csv(file_name, index=False)
    upload_blob(bucket_name=bucket_name, source_file_name=file_name, destination_blob_name=file_name)


@functions_framework.http
def openaq_data_download(request):
    if not os.environ.get("API_KEY"):
        raise ValueError("API_KEY environment variable not set")

    api_key = os.getenv('API_KEY')
    results = []

    with OpenAQ(api_key=api_key) as client:
        for city, coordinates in CITY_COORDINATES.items():

            locations_df = get_locations(client, city, coordinates)
            locations_ids = validate_locations(locations_df, city)
            if not locations_ids:
                continue

            # Enlarging DataFrame to contain one sensor per row
            locations_df = locations_df.explode('sensors', ignore_index=True)
            locations_df['sensors'].apply(lambda x: x['id'])
            locations_df['sensors_id'] = locations_df['sensors'].apply(lambda x: x['id'])
            locations_df['parameter'] = locations_df['sensors'].apply(lambda x: x['name'].split(' ')[0])
            locations_df['unit'] = locations_df['sensors'].apply(lambda x: x['name'].split(' ')[1])

            # Download measurements for each location
            measurements_df = process_measurements(client, locations_ids, locations_df)
            measurements_df['city'] = city
            results.append(measurements_df)

        # Save and upload results
        if results:
            results_df = pd.concat(results, ignore_index=True)
            save_to_csv_and_upload(results_df, BUCKET_NAME)

    return 'OK'
