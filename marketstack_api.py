import os

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv('MARKETSTACK_API')

if not API_KEY:
    raise ValueError("API KEY NOT FOUND")

API_URL = 'http://api.marketstack.com/v1/eod'
symbols = ['MSTR', 'COIN']
params = {
    'access_key':API_KEY,
    'symbols': ','.join(symbols),
    'date_from':'2024-10-01'

}

def fetch_data():
    try:
        response = requests.get(API_URL, params)
        response.raise_for_status()
        data = response.json()

        df = pd.DataFrame(data['data'])

        df = df[['symbol', 'exchange', 'date', 'open', 'close', 'volume']]
        print('Data fetechedn succesfully')
        print(df.head())
        return df

    except Exception as err:
        print(f"An error occured: {err}")

if __name__ == "__main__":
    df = fetch_data()
    df.to_csv('./raw_data/raw_data.csv', index=False)