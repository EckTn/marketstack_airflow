import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

query = """
CREATE TABLE  IF NOT EXISTS stock_prices
(
    symbol TEXT,
    exchange TEXT,
    date DATE,
    open FLOAT,
    close FLOAT,
    volume FLOAT,
    PRIMARY KEY(symbol, date)
);
"""

try:
    connector = psycopg2.connect(
        host='mktstockdb1.postgres.database.azure.com',
        dbname='postgres',
        user=os.getenv('POSTGRE_USER'),
        password=os.getenv('POSTGRE_PASSWORD'),
        port=5432
    )
    cur = connector.cursor()
    cur.execute(query)
    connector.commit()
    connector.close()

    print("Table succesfully created")
except Exception as e:
    print("Error occured:", e)