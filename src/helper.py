import pandas as pd

from src.coordinates import find_min_dist_to_next_pronto
from src.main import *


def prepare_dataframe(ranking: pd.DataFrame) -> pd.DataFrame:
    df_municipalities = read_data()
    df_coop = coop_data('coop-pronto.csv')
    df_coop = get_coordinates_for_prontos(df_coop)
    df_municipalities = find_min_dist_to_next_pronto(df_municipalities, df_coop)

    df_municipalities = df_municipalities.set_index('Gemeindename')

    df_finance = read_csv('data', 'finanzen.csv')
    df_finance = preprocess_finance_df(df_finance)

    df_final = pd.merge(df_municipalities, df_finance, left_index=True, right_on='Gemeindename')
    df_final = df_final.drop(['Total Anzahl Personen Sozialhilfe'],axis=1)
    df_final = df_final.drop(['Total Verunfallte Personen'],axis=1)
    df_final = df_final.drop(['Total Anzahl Unfälle 2021'],axis=1)
    df_final['Gemeindename'] = df_final['Ortschaftsname']
    df_final = df_final.drop(['Ortschaftsname'],axis=1)

    df_final = df_final.set_index('Gemeindename')
    df_final['Score'] = ranking['Score']
    #unset index
    df_final.reset_index(inplace=True)

    create_csv('data','engineering_final.csv',df_final)

    #create_csv('data', 'engineering.csv', df_final, index=False,overwrite=True)
    return df_final