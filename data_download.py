# Data:  15.08.2025
# Autor: Daniel Grzebyk

# ZADANIE 1: Integracja Danych z Publicznego API i Wstępne Przetwarzanie
# Ten skrypt pobiera dane o jakości powietrza dla dwóch wybranych miast na świecie (np. Warszawa, Londyn) z publicznego
# API https://docs.openaq.org/ (klucz do API jest dostępny po założeniu darmowego konta).
#
# 1. Wymagania Funkcjonalne:
# a. Skrypt powinien pobierać najnowsze dostępne dane pomiarowe dla parametrów: PM2.5, PM10, O3, NO2 dla wybranych miast.
# b. Zintegruj dane z co najmniej 3 różnych stacji pomiarowych dla każdego miasta, jeśli są dostępne.
# c. Wyniki zapisz w wybranej formie (np. csv.), gdzie każdy element/wiersz reprezentuje jeden odczyt z konkretnej stacji, zawierający: nazwę miasta, lokalizację stacji, parametr, wartość, jednostkę oraz czas pomiaru.
# d. Dodaj prostą walidację danych i obsługę błędów.
# e. Weź pod uwagę możliwość zastosowania skryptu w zautomatyzowanym rozwiązaniu cyklicznie ładującym hurtownię danych.
#
# 2. Wymagania Techniczne i Środowisko Uruchomieniowe:
# a. Skrypt przetwarzający dane powinien zostać zaimplementowany jako funkcja Google Cloud (Cloud Function).
# b. Wynikowy plik, zawierający przetworzone dane, musi być ładowany do bucketu w usłudze Google Cloud Storage (GCS)


import os
import pandas as pd

from datetime import datetime
from openaq import OpenAQ

from utils import upload_blob


# USER INPUTS
city_coordinates = [
    (52.2297, 21.0122),  # Warsaw
    (51.5072, 0.1276)    # London
]
radius = 10000  # 10 km promień od centrum miasta - obszar w którym będą wyszukiwane stacje pomiarowe
bucket_name = 'openaq-weather-data'

api_key = os.getenv('API_KEY')
client = OpenAQ(api_key=api_key)

# Parameters: 00 - PM10 ug/m3, 01 - PM2.5 ug/m3, 02 - O3 ug/m3, 04 - NO2 ug/m3
# [0, 1, 2, 4]

for coordinates in city_coordinates:
    response = client.locations.list(
        coordinates=coordinates,
        radius=radius,
        limit=1000
    )
    locations_dict = response.dict()
    locations_df = pd.json_normalize(locations_dict['results'])

    # Używanie tylko stacji z pomiarami w bieżącym miesiącu
    # ...

    locations_df = locations_df.explode('sensors', ignore_index=True)
    locations_df['sensors'].apply(lambda x: x['id'])
    locations_df['sensors_id'] = locations_df['sensors'].apply(lambda x: x['id'])
    locations_df['parameter'] = locations_df['sensors'].apply(lambda x: x['name'].split(' ')[0])
    locations_df['unit'] = locations_df['sensors'].apply(lambda x: x['name'].split(' ')[1])

    locations_ids = locations_df['id'].to_list()

    # Pobieranie najnowszych pomiarów z wybranych stacji meteorologicznych w mieście
    ls = []
    for location_id in locations_ids:
        measurements = client.locations.latest(locations_id=location_id)
        df = pd.DataFrame(measurements.dict()['results'])
        ls.append(df)
        break  # TODO: To be removed (prevents reaching API limits)
    measurements_df = pd.concat(ls, ignore_index=True)
    measurements_df = measurements_df.merge(
        locations_df[['sensors_id', 'parameter', 'unit']],
        how='left',
        on='sensors_id'
    )
    measurements_df['datetime_utc'] = measurements_df['datetime'].apply(lambda x: x['utc'])
    measurements_df['datetime_local'] = measurements_df['datetime'].apply(lambda x: x['local'])
    measurements_df.drop(columns=['datetime'], inplace=True)

    # Wybranie tylko interesujących parametrów
    measurements_df.loc[measurements_df['parameter'].isin(['no2', 'o3', 'pm10', 'pm25'])]

    # Save output
    dt_now = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    file_name = f"{dt_now}.csv"
    measurements_df.to_csv(file_name)

    upload_blob(
        bucket_name=bucket_name,
        source_file_name=file_name,
        destination_blob_name=file_name
    )

client.close()
