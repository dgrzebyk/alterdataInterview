# Data:  15.08.2025
# Autor: Daniel Grzebyk


import os
import functions_framework
import pandas as pd

from datetime import datetime

from cloudevents.http import CloudEvent
from openaq import OpenAQ
from utils import upload_blob


@functions_framework.http
def openaq_data_download(cloud_event: CloudEvent):
    data = cloud_event.data
    if not os.environ.get("API_KEY"):
        raise ValueError("API_KEY environment variable not set")

    # Dane wprowadzone przez użytkownika
    city_coordinates = {
        "Warszawa": (52.2297, 21.0122),
        "Londyn": (51.5072, 0.1276)
    }
    radius = 10000  # 10 km promień od centrum miasta - obszar w którym będą wyszukiwane stacje pomiarowe
    bucket_name = 'openaq-weather-data'

    api_key = os.getenv('API_KEY')
    client = OpenAQ(api_key=api_key)

    results = []
    for city, coordinates in city_coordinates.items():

        # Odnalezienie ID stacji pomiarowych w wybranym mieście
        response = client.locations.list(
            coordinates=coordinates,
            radius=radius,
            limit=1000
        )
        locations_dict = response.dict()
        locations_df = pd.json_normalize(locations_dict['results'])
        locations_ids = locations_df['id'].to_list()

        if len(locations_ids) < 3:
            print(f"ERROR: Dla miasta {city} nie znaleziono co najmniej 3 stacji pomiarowych.")
            continue

        # Używanie tylko stacji z pomiarami w bieżącym miesiącu
        # ...

        # Rozdzielenie kolumny 'sensors' na osobne wiersze
        locations_df = locations_df.explode('sensors', ignore_index=True)
        locations_df['sensors'].apply(lambda x: x['id'])
        locations_df['sensors_id'] = locations_df['sensors'].apply(lambda x: x['id'])
        locations_df['parameter'] = locations_df['sensors'].apply(lambda x: x['name'].split(' ')[0])
        locations_df['unit'] = locations_df['sensors'].apply(lambda x: x['name'].split(' ')[1])

        # Pobranie najnowszych pomiarów ze stacji meteorologicznych w wybranym mieście
        ls = []
        for location_id in locations_ids:
            measurements = client.locations.latest(locations_id=location_id)
            df = pd.DataFrame(measurements.dict()['results'])
            ls.append(df)

        measurements_df = pd.concat(ls, ignore_index=True)
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
        measurements_df['city'] = city
        results.append(measurements_df)

    # Save output
    results_df = pd.concat(results, ignore_index=True)
    dt_now = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    file_name = f"{dt_now}.csv"
    results_df[['city', 'latitude', 'longitude', 'parameter', 'value', 'unit',
                'datetime_utc', 'datetime_local']].to_csv(file_name, index=False)
    upload_blob(
        bucket_name=bucket_name,
        source_file_name=file_name,
        destination_blob_name=file_name
    )

    client.close()

    return 'OK'
