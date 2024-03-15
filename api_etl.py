import pandas as pd
import json
from datetime import datetime
import s3fs
import requests

def run_jakpos_etl():
    api_url = 'https://jakpost.vercel.app/api/podcast'

    data = requests.get(api_url)

    def extract_api(to_df):
        if isinstance(to_df, list):
            return pd.DataFrame(to_df)
        else:
            return None

    if data.status_code == 200:
        data = data.json()['podcast']
        df = extract_api(data)

        if df is not None:
            df.to_csv('s3://reyhan-airflow-development-bucket/jakpos_data_2024.csv', index=False)
        else:
            print("Data tidak valid")
    else:
        print("Gagal mendapatkan data dari API")

