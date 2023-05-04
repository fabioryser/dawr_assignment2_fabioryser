from pathlib import Path
import os
import pandas as pd
import requests

def getPathDynamically(folder: str, file: str, check_exists=True) -> Path:

    current_directory = os.getcwd()
    parent_directory = os.path.dirname(current_directory)
    current_folder = os.path.basename(current_directory)
    parent_folder = os.path.basename(parent_directory)

    path: Path
    # Check in what dir the code is executed
    if (current_folder == 'Assignment2'):
        path = Path(os.path.join('.', folder, file))
    elif (current_folder == 'src' and parent_folder == 'Assignment2'):
        path = Path(os.path.join('..', folder, file))
    else:
        # code uses dynamic paths so only these two directories work
        raise Exception(f'Please execute .py files while your working directory is either /Assignment2 or /Assignment2/src. Currently it is {current_folder}, which will not work.')

    # checks if file exists unless you specifically declare check_exists=False
    if (check_exists and not path.exists()):
        raise FileNotFoundError(f'The file {path} could not be found.')
    return path


def create_csv(folder: str, file:str, df: pd.DataFrame, index=False, overwrite=False) -> None:
    path = getPathDynamically(folder, file, check_exists=False)
    if (not overwrite and not path.exists()):
        df.to_csv(path, index=index, na_rep='0')
    elif (overwrite):
        df.to_csv(path, mode='w', index=index, na_rep='0')


def create_csv_path(path: Path, df: pd.DataFrame, index=False, overwrite=False) -> None:
    if (not overwrite and not path.exists()):
        df.to_csv(path, index=index, na_rep='0')
    if (overwrite):
        df.to_csv(path, mode='w', index=index, na_rep='0')


def read_csv(folder: str, file: str) -> pd.DataFrame:
    path = getPathDynamically(folder, file)
    if(file == 'WGS84_koordinaten_2019.csv'):
        df = pd.read_csv(path, encoding='ISO-8859-1', header=0, sep=';')
    else:
        df = pd.read_csv(path, na_values=['---', ''])
    df.replace('---', pd.np.nan, inplace=True)
    df.fillna(value=0, inplace=True)
    return df


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