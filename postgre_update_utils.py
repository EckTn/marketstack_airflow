import psycopg2
import os
import csv
from dotenv import load_dotenv
import pandas as pd
from io import BytesIO
from azure.storage.blob import BlobServiceClient

load_dotenv()

query = """
INSERT INTO stock_prices(symbol, exchange, date, open, close, volume)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol, date) DO NOTHING;
"""

try:

    # df = pd.read_csv('./raw_data/raw_data.csv')
    # df['date'] = pd.to_datetime(df['date']).dt.date
    # records = df.to_records(index=False)
    # tuple_records = list(records)

    connector = psycopg2.connect(
        host='mktstockdb1.postgres.database.azure.com',
        dbname='postgres',
        user=os.getenv('POSTGRE_USER'),
        password=os.getenv('POSTGRE_PASSWORD'),
        port=5432
    )
    cur = connector.cursor()

    with open('./raw_data/raw_data.csv', 'r') as file:
        data_reader = csv.reader(file)
        next(data_reader)

        for row in data_reader:
            cur.execute(query, row)

    # cur.executemany(query, tuple_records)
    connector.commit()
    connector.close()
    print('Data successfully inserted')

except Exception as e:
    print("An error occured: ", e)


# df = pd.read_csv('./raw_data/raw_data.csv')
# df['date'] = pd.to_datetime(df['date']).dt.date
# records = df.to_records(index=False)
# tuple_records = list(records)
# print(records)