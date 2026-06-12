"""
Supabase PostgreSQL ETL Loader - Stock Portfolio
------------------------------------------------
Loads stock price data from Alpha Vantage into Supabase and computes metrics.

Data Flow:
- Fetch stock data from Alpha Vantage API
- Store in stock_prices table
- Compute and upsert portfolio metrics

Required packages:
    pip install pandas sqlalchemy psycopg2-binary python-dotenv requests
"""

from __future__ import annotations

import os
from datetime import datetime
import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# =============================
# CONFIG
# =============================

TARGET_DATE_2009 = "2009-06-01"

# =============================
# CONNECTION SETUP
# =============================

def get_database_url() -> str:
    load_dotenv()
    db_url = os.getenv("SUPABASE_DB_URL")

    if not db_url:
        raise RuntimeError("Missing SUPABASE_DB_URL in .env")

    return db_url

def get_api_key() -> str:
    load_dotenv()
    key = os.getenv("ALPHA_VANTAGE_API_KEY")

    if not key:
        raise RuntimeError("Missing ALPHA_VANTAGE_API_KEY")

    return key

# =============================
# SCHEMA CREATION
# =============================

def create_schema(engine):
    sql = """
    CREATE TABLE IF NOT EXISTS stock_prices (
        id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
        ticker TEXT NOT NULL,
        date DATE NOT NULL,
        close_price NUMERIC NOT NULL,
        UNIQUE (ticker, date)
    );

    CREATE TABLE IF NOT EXISTS portfolio_metrics (
        id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
        holding_id UUID UNIQUE,
        ticker TEXT,
        shares NUMERIC,
        price_2009 NUMERIC,
        price_current NUMERIC,
        value_2009 NUMERIC,
        value_current NUMERIC,
        calculated_at TIMESTAMP DEFAULT NOW()
    );
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

# =============================
# FETCH STOCK DATA
# =============================

def fetch_stock_data(ticker: str) -> pd.DataFrame:
    api_key = get_api_key()

    url = (
        f"https://www.alphavantage.co/query?"
        f"function=TIME_SERIES_DAILY_ADJUSTED"
        f"&symbol={ticker}&outputsize=full&apikey={api_key}"
    )

    response = requests.get(url)
    data = response.json()

    time_series = data.get("Time Series (Daily)", {})

    records = []
    for date, values in time_series.items():
        records.append({
            "ticker": ticker,
            "date": date,
            "close_price": float(values["4. close"])
        })

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    return df

# =============================
# LOAD STOCK PRICES
# =============================

def load_stock_prices(df: pd.DataFrame, engine):
    print("Loading stock_prices...")

    df.to_sql(
        "stock_prices",
        engine,
        schema="public",
        if_exists="append",
        index=False,
        method="multi"
    )

# =============================
# METRICS COMPUTATION
# =============================

def update_metrics(engine):
    print("Updating portfolio_metrics...")

    sql = f"""
    INSERT INTO portfolio_metrics (
        holding_id,
        ticker,
        shares,
        price_2009,
        price_current,
        value_2009,
        value_current
    )
    SELECT
        h.id,
        h.ticker,
        h.shares,
        sp_2009.close_price,
        sp_today.close_price,
        (h.shares * sp_2009.close_price),
        (h.shares * sp_today.close_price)

    FROM holdings h

    LEFT JOIN stock_prices sp_2009
        ON sp_2009.ticker = h.ticker
        AND sp_2009.date = '{TARGET_DATE_2009}'

    LEFT JOIN stock_prices sp_today
        ON sp_today.ticker = h.ticker
        AND sp_today.date = CURRENT_DATE

    ON CONFLICT (holding_id)
    DO UPDATE SET
        price_2009 = EXCLUDED.price_2009,
        price_current = EXCLUDED.price_current,
        value_2009 = EXCLUDED.value_2009,
        value_current = EXCLUDED.value_current,
        calculated_at = NOW();
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

# =============================
# MAIN WORKFLOW
# =============================

def main():
    engine = create_engine(get_database_url())

    print("Creating schema...")
    create_schema(engine)

    # Example tickers (replace with query from holdings later)
    tickers = ["AAPL", "MSFT", "GOOG"]

    for ticker in tickers:
        print(f"Fetching data for {ticker}...")
        df = fetch_stock_data(ticker)
        load_stock_prices(df, engine)

    update_metrics(engine)

    print("===================================")
    print("STOCK DATA LOAD COMPLETE")
    print("===================================")

if __name__ == "__main__":
    main()

