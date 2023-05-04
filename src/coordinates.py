import os

import requests
from src.util import *


def create_coordinates_df():
    """reads location data for each municipality and merges it with the ranking data"""

    df_safety = read_csv('data', 'safety-ranking.csv')

    df_coordinates = read_csv('data', 'WGS84_koordinaten_2019.csv')

    df_coordinates = df_coordinates[df_coordinates['Kantonskürzel'] == 'LU']
    df_coordinates = df_coordinates.drop_duplicates(subset='Ortschaftsname', keep='first')

    df_coordinates['Ortschaftsname'] = df_coordinates['Ortschaftsname'].str.replace(' LU', '')
    df_coordinates['Ortschaftsname'] = df_coordinates['Ortschaftsname'].str.replace('Escholzmatt',
                                                                                    'Escholzmatt-Marbach')
    df_coordinates['Ortschaftsname'] = df_coordinates['Ortschaftsname'].str.replace(' b. Willisau', '')

    df_municipalities = pd.merge(df_safety, df_coordinates[['Ortschaftsname', 'E', 'N']], left_on='Gemeinde',
                                 right_on='Ortschaftsname', how='left')
    df_municipalities = df_municipalities.rename(columns={'E': 'Longitude', 'N': 'Latitude'})
    print(os.getcwd())
    return df_municipalities


def read_coop_data(filename: str):
    """adds a column with the search text to the coop data"""
    df_coop = pd.read_csv('../data/coop-pronto.csv', header=0, sep=',')
    df_coop['searchText'] = df_coop['Adresse'] + ' ' + df_coop['PLZ'].astype(str) + ' ' + df_coop['Ort']
    return df_coop


_API_URL = 'https://api3.geo.admin.ch/rest/services/api/SearchServer'


def search_api_to_json(searchtext: str, layer):
    url = f"{_API_URL}?type=locations&searchText={searchtext}&origins={layer}"
    return requests.get(url).json()


def get_lat_lon_from_api_json(json):
    return json['results'][0]['attrs']['lon'], json['results'][0]['attrs']['lat']


def add_lon_lat_to_df(df_coop: pd.DataFrame):
    df_coop['Longitude'] = None
    df_coop['Latitude'] = None

    for i, searchText in df_coop['searchText'].items():
        search_result = search_api_to_json(searchText, 'address')
        if search_result['results']:
            longitude, latitude = get_lat_lon_from_api_json(search_result)
            df_coop.loc[i, 'Longitude'] = longitude
            df_coop.loc[i, 'Latitude'] = latitude

    return df_coop


def compute_haversine_distance(lat1, lon1, lat2, lon2):
    """
    Berechnet die Entfernung zwischen zwei geographischen Koordinaten
    in Kilometern mit der Haversine-Formel.
    """
    from math import radians, cos, sin, asin, sqrt
    # Konvertieren in Radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # Haversine-Formel
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    # Radius der Erde (in Kilometern)
    r = 6371
    # Berechnung der Entfernung
    return c * r


def find_min_dist_to_next_pronto(df_municipality: pd.DataFrame, df_coop: pd.DataFrame):
    for i, row in df_municipality.iterrows():
        distances = []
    for j, coop_row in df_coop.iterrows():
        distance = compute_haversine_distance(row['Latitude'], row['Longitude'], coop_row['Latitude'], coop_row['Longitude'])
        distances.append(distance)
    df_municipality.at[i, 'Distanz zu nächsten Prontoshop in km'] = min(distances)
    cols = list(df_municipality.columns)
    score_index = cols.index('Score')
    cols = [cols[score_index]] + cols[:score_index] + cols[score_index+1:]

    df_municipality = df_municipality.reindex(columns=cols)

    # schreibe in ein csv names asn2.csv
    # df_municipality.to_csv('asn2.csv')

    return df_municipality

create_coordinates_df()