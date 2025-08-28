# Data:  28.08.2025
# Autor: Daniel Grzebyk


import certifi
import functions_framework
import logging
import os
import ssl

import pandas as pd

from datetime import datetime
from geopy.geocoders import Nominatim
from openaq import OpenAQ
from typing import Tuple, List
from utils import upload_blob


# Configure logging
logging.basicConfig(level=logging.INFO)

def get_city_coordinates(city: str) -> Tuple[float, float]:
    logging.info("Obtaining city coordinates...")

    # Create an SSL context using certifi's certificate bundle
    ctx = ssl.create_default_context(cafile=certifi.where())

    # Pass the custom SSL context to the geolocator
    geolocator = Nominatim(
        user_agent='myapplication',
        ssl_context=ctx
    )

    location = geolocator.geocode(city)
    logging.info("City coordinates obtained.")
    return location.latitude, location.longitude


def get_locations(client: OpenAQ, city: str, RADIUS: int) -> pd.DataFrame:
    """Fetch measurement station locations for a city."""
    coordinates = get_city_coordinates(city)
    if coordinates is None:
        logging.error(f"{city} coordinates not found.")
        return pd.DataFrame()
    else:
        logging.info(f"Downloading list of stations in {city}...")
        response = client.locations.list(coordinates=coordinates, radius=RADIUS, limit=1000)
        locations_dict = response.dict()
        locations_df = pd.json_normalize(locations_dict['results'])
        logging.info(f"List of stations in {city} downloaded.")
        return locations_df


def validate_locations(locations_df: pd.DataFrame, city: str) -> List[int]:
    """Validate and extract station IDs for a city."""
    logging.info("Validating locations...")

    # Eliminate stations that are no longer in use
    locations_df['last_date'] = pd.to_datetime(locations_df['datetime_last.utc'], utc=True).dt.date
    locations_df = locations_df.loc[locations_df['last_date'] >= datetime.today().date()]
    if len(locations_df) == 0:
        logging.error(f"Stations available for {city} are no longer in use.")

    locations_ids = locations_df['id'].to_list()
    if len(locations_ids) < 3:
        logging.error(f"{city} does not have at least 3 weather measurement stations.")
        return []
    else:
        logging.info("Locations validated.")
        return locations_ids


def process_measurements(client: OpenAQ, locations_ids: List[int], locations_df: pd.DataFrame) -> pd.DataFrame:
    """Fetch and process measurement data."""
    logging.info("Downloading measurement data...")
    measurements_list = []
    for location_id in locations_ids:
        measurements = client.locations.latest(locations_id=location_id)
        df = pd.DataFrame(measurements.dict()['results'])
        measurements_list.append(df)

    measurements_df = pd.concat(measurements_list, ignore_index=True)
    measurements_df = measurements_df.merge(
        locations_df[['name', 'locality', 'country.name', 'timezone', 'sensors_id', 'parameter', 'unit']],
        how='left',
        on='sensors_id'
    )
    measurements_df['datetime_utc'] = measurements_df['datetime'].apply(lambda x: x['utc'])
    measurements_df['datetime_local'] = measurements_df['datetime'].apply(lambda x: x['local'])
    measurements_df['latitude'] = measurements_df['coordinates'].apply(lambda x: x['latitude'])
    measurements_df['longitude'] = measurements_df['coordinates'].apply(lambda x: x['longitude'])
    measurements_df.drop(columns=['datetime', 'coordinates'], inplace=True)

    measurements_df = measurements_df.loc[measurements_df['parameter'].isin(['no2', 'o3', 'pm10', 'pm25'])]
    logging.info("Measurements downloaded.")

    return measurements_df


def upload_to_gcs(results_df: pd.DataFrame, bucket_name: str) -> None:
    """Upload pandas DataFrame as a .CSV to GCS."""
    logging.info("Uploading results to GCS...")
    dt_now = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    upload_blob(bucket_name=bucket_name, df=results_df, destination_blob_name=f"{dt_now}.csv")
    logging.info("Results uploaded.")


@functions_framework.http
def openaq_data_download(request):

    # Constants
    CITIES: list = ["Warsaw", "London"]
    RADIUS: int = 10000  # 10 km
    BUCKET_NAME: str = 'dg_test_1'

    if RADIUS < 0 or RADIUS > 25000:
        logging.error("RADIUS must be greater than 0 and less than 25000 meters.")

    CITIES = [city for city in CITIES if city.strip().capitalize()]

    if not os.environ.get("API_KEY"):
        raise ValueError("API_KEY environment variable not set")

    api_key = os.getenv('API_KEY')
    results = []

    with OpenAQ(api_key=api_key) as client:
        for city in CITIES:
            locations_df = get_locations(client, city, RADIUS)
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
            upload_to_gcs(results_df, BUCKET_NAME)

    return 'OK'
