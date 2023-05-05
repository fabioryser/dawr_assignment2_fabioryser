import requests
import pandas as pd
from pathlib import Path


def read_data():
    """reads location data for each municipality and merges it with the ranking data"""
    path = get_path_dynamically('data', 'safety-processed.csv')
    df_safety = pd.read_csv(path)

    path = get_path_dynamically('data', 'WGS84_koordinaten_2019.csv')
    df_coordinates = pd.read_csv(path, encoding='ISO-8859-1', header=0, sep=';')
    df_coordinates = df_coordinates[df_coordinates['Kantonskürzel'] == 'LU']
    df_coordinates = df_coordinates.drop_duplicates(subset='Ortschaftsname', keep='first')

    df_coordinates['Ortschaftsname'] = df_coordinates['Ortschaftsname'].str.replace(' LU', '')
    df_coordinates['Ortschaftsname'] = df_coordinates['Ortschaftsname'].str.replace('Escholzmatt',
                                                                                    'Escholzmatt-Marbach')
    df_coordinates['Ortschaftsname'] = df_coordinates['Ortschaftsname'].str.replace(' b. Willisau', '')

    df_municipalities = pd.merge(df_safety, df_coordinates[['Ortschaftsname', 'E', 'N']], left_on='Gemeindename',
                                 right_on='Ortschaftsname', how='left')
    df_municipalities = df_municipalities.rename(columns={'E': 'Longitude', 'N': 'Latitude'})
    return df_municipalities


def coop_data(filename: str):
    """adds a column with the search text to the coop data"""
    path = get_path_dynamically('data', filename)
    df_coop = pd.read_csv(path, header=0, sep=',')
    df_coop['searchText'] = df_coop['Adresse'] + ' ' + df_coop['PLZ'].astype(str) + ' ' + df_coop['Ort']
    return df_coop


def get_path_dynamically(folder: str, file: str, check_exists=True) -> Path:
    """returns a path object to the file in the folder. If check_exists is True, it will raise an error if the file does not exist"""
    current_directory = Path.cwd()
    parent_directory = current_directory.parent
    current_folder = current_directory.name
    parent_folder = parent_directory.name

    path: Path
    # Check in what dir the code is executed
    if (current_folder == 'dawr_assignment2_fabioryser'):
        path = Path(folder, file)
    elif (current_folder == 'src' and parent_folder == 'dawr_assignment2_fabioryser'):
        path = Path('..', folder, file)
    else:
        # code uses dynamic paths so only these two directories work
        raise Exception(
            f'Please execute .py files while your working directory is either /dawr_assignment2_fabioryser or /dawr_assignment2_fabioryser/src. Currently it is {current_folder}, which will not work.')

    # checks if file exists unless you specifically declare check_exists=False
    if (check_exists and not path.exists()):
        raise FileNotFoundError(f'The file {path} could not be found.')
    return path


def get_coordinates_for_prontos(df: pd.DataFrame):
    """adds two column with the coordinates to the coop data"""
    df['Longitude'] = None
    df['Latitude'] = None

    for i, searchText in df['searchText'].items():
        search_result = search_api_to_json(searchText, 'address')
        if search_result['results']:
            longitude, latitude = get_lat_lon_from_api_json(search_result)
            df.loc[i, 'Longitude'] = longitude
            df.loc[i, 'Latitude'] = latitude
    return df


def find_min_dist_to_next_pronto(df_municipality: pd.DataFrame, df_coop: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the distance from each municipality to the nearest Coop Pronto Shop and adds it to the input DataFrame.
    """
    df_municipality['Distanz zu nächsten Prontoshop in km'] = None
    for i, row in df_municipality.iterrows():
        distances = []
        for j, coop_row in df_coop.iterrows():
            distance = haversine_distance(row['Latitude'], row['Longitude'], coop_row['Latitude'],
                                          coop_row['Longitude'])
            distances.append(distance)
        if distances:
            df_municipality.at[i, 'Distanz zu nächsten Prontoshop in km'] = min(distances)
    # Reorder columns so that Score is the first column
    cols = list(df_municipality.columns)
    score_index = cols.index('Score')
    cols = [cols[score_index]] + cols[:score_index] + cols[score_index + 1:]
    df_municipality = df_municipality.reindex(columns=cols)
    return df_municipality


def search_api_to_json(searchText, layer):
    _API_URL = 'https://api3.geo.admin.ch/rest/services/api/SearchServer'
    url = f"{_API_URL}?type=locations&searchText={searchText}&origins={layer}"
    return requests.get(url).json()


def get_lat_lon_from_api_json(json):
    return json['results'][0]['attrs']['lon'], json['results'][0]['attrs']['lat']


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculates the distance between two geographical coordinates
    in kilometers using the Haversine formula.
    """
    from math import radians, cos, sin, asin, sqrt
    # Convert to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    # Earth radius (in kilometers)
    r = 6371
    # Calculate distance
    return c * r


def read_csv(folder: str, file: str) -> pd.DataFrame:
    """reads a csv file and returns a pandas dataframe"""
    path = get_path_dynamically(folder, file)
    if (file == 'WGS84_koordinaten_2019.csv'):
        df = pd.read_csv(path, encoding='ISO-8859-1', header=0, sep=';')
    else:
        df = pd.read_csv(path, na_values=['---', ''])
    df.replace('---', pd.np.nan, inplace=True)
    df.fillna(value=0, inplace=True)
    return df


def create_csv(folder: str, file: str, df: pd.DataFrame, index=False, overwrite=False) -> None:
    """creates a csv file in the folder with the given name and writes the dataframe to it"""
    path = get_path_dynamically(folder, file, check_exists=False)
    if (not overwrite and not path.exists()):
        df.to_csv(path, index=index, na_rep='0')
    elif (overwrite):
        df.to_csv(path, mode='w', index=index, na_rep='0')


def preprocess_finance_df(df: pd.DataFrame) -> pd.DataFrame:
    """preprocesses the finance data"""
    features = ['Gemeindename',
                'Median Einkommen',
                'Ergänzungsleistungen in % zu Median',
                'Einkommen aus Vermögenserträgen',
                'Frei verfügbares Einkommen',
                '20000',
                '100000',
                '1000000',
                'Total Aktiven',
                'Finanzvermögen¹',
                'Verwaltungsvermögen',
                'Total Passiven',
                'Fremdkapital',
                'Eigenkapital',
                'Spezielfinanzierung EK',
                'Fond im EK',
                'Aufwertungsreserven',
                'überiges Eigentkapital',
                'Bilanzüberschuss']
    return df[features]


def merge_dataframes(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """merges two dataframes on Gemeinde and Gemeindename"""
    df_merged = pd.merge(df1, df2, left_on='Gemeinde', right_on='Gemeindename', how='left')
    return df_merged
