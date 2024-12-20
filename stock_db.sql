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