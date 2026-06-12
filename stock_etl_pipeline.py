"""
Stock ETL Pipeline
"""

import pandas as pd
import yfinance as yf
from datetime import datetime
from sqlalchemy import create_engine, text

# =============================
# CONFIG
# =============================

DB_URL = "postgresql+psycopg2://postgres:#0340Df6034@db.qcvwqyoxvvpdsnzrnjio.supabase.co:5432/postgres"

SYMBOL = "AAPL"
DEFAULT_SHARES = 1000
START_DATE = pd.Timestamp("2007-12-01")

engine = create_engine(DB_URL)

# =============================
# LOGGING
# =============================

def log(msg):
    print(f"[{datetime.now()}] {msg}")

# =============================
# EXTRACT
# =============================

def fetch_stock_data(symbol):
    log(f"Fetching {symbol}")

    df = yf.download(symbol, period="max", interval="1d")

    if df.empty:
        return pd.DataFrame()

    df = df.reset_index()

    
    df = df[["Date", "Open", "High", "Low", "Volume"]]

    df.columns = ["price_date", "open", "high", "low", "volume"]

    df["price_date"] = pd.to_datetime(df["price_date"])
    df = df[df["price_date"] >= START_DATE]

    # Convert once — no datetime bugs later
    df["price_date"] = df["price_date"].dt.date

    return df.reset_index(drop=True)

# =============================
# DB SETUP
# =============================

def create_tables_if_not_exist():
    sql = """
    CREATE TABLE IF NOT EXISTS stock (
        stock_id SERIAL PRIMARY KEY,
        symbol TEXT UNIQUE
    );

    CREATE TABLE IF NOT EXISTS stock_price (
        stock_id INT,
        price_date DATE,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        volume BIGINT,
        PRIMARY KEY (stock_id, price_date)
    );

    CREATE TABLE IF NOT EXISTS stock_indicator (
        stock_id INT,
        record_date DATE,
        sma FLOAT,
        rsi FLOAT,
        signal TEXT
    );

    CREATE TABLE IF NOT EXISTS portfolio_metrics (
        stock_id INT,
        record_date DATE,
        shares INT,
        position_value FLOAT,
        daily_return_pct FLOAT
    );

    CREATE TABLE IF NOT EXISTS performance_summary (
        symbol TEXT,
        bottom_date DATE,
        bottom_price FLOAT,
        current_price_date DATE,
        current_price FLOAT
    );
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

# =============================
# STOCK ID
# =============================

def get_stock_id(symbol):
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                INSERT INTO stock (symbol)
                VALUES (:symbol)
                ON CONFLICT DO NOTHING
                RETURNING stock_id
            """),
            {"symbol": symbol}
        ).fetchone()

        if result:
            return result[0]

        return conn.execute(
            text("SELECT stock_id FROM stock WHERE symbol=:symbol"),
            {"symbol": symbol}
        ).scalar()

# =============================
# INCREMENTAL FILTER
# =============================

def filter_new_rows(df, stock_id):
    query = """
        SELECT MAX(price_date)
        FROM stock_price
        WHERE stock_id = :stock_id
    """

    with engine.begin() as conn:
        last_date = conn.execute(text(query), {"stock_id": stock_id}).scalar()

    if last_date:
        df = df[df["price_date"] > last_date]
        log(f"Last stored date: {last_date}")
    else:
        log("Full load")

    return df.reset_index(drop=True)

# =============================
# METRICS (USING OPEN)
# =============================

def add_metrics(df):
    df = df.copy()

    price = df["open"]  

    df["sma"] = price.rolling(5).mean()

    delta = price.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    rs = gain.rolling(5).mean() / loss.rolling(5).mean()
    df["rsi"] = 100 - (100 / (1 + rs))

    df["signal"] = df["rsi"].apply(
        lambda x: "BUY" if x < 30 else "SELL" if x > 70 else "HOLD"
    )

    df["shares"] = DEFAULT_SHARES
    df["position_value"] = price * DEFAULT_SHARES
    df["daily_return_pct"] = price.pct_change() * 100

    return df

# =============================
# SUMMARY (USING OPEN)
# =============================

def compute_summary(df):
    log("Computing summary")

    df = df.sort_values("price_date").reset_index(drop=True)

    price = df["open"]

    bottom_idx = price.idxmin()
    latest_idx = len(price) - 1

    return {
        "symbol": SYMBOL,
        "bottom_date": df["price_date"].iloc[bottom_idx],
        "bottom_price": float(price.iloc[bottom_idx]),
        "current_price_date": df["price_date"].iloc[latest_idx],
        "current_price": float(price.iloc[latest_idx])
    }

# =============================
# LOAD
# =============================

def load_data(df, stock_id, summary):
    if df.empty:
        log("No new data")
        return

    df = df.copy()
    df["stock_id"] = stock_id

    df[["stock_id", "price_date", "open", "high", "low", "volume"]] \
        .to_sql("stock_price", engine, if_exists="append", index=False)

    df[["stock_id", "price_date", "sma", "rsi", "signal"]] \
        .rename(columns={"price_date": "record_date"}) \
        .to_sql("stock_indicator", engine, if_exists="append", index=False)

    df[["stock_id", "price_date", "shares", "position_value", "daily_return_pct"]] \
        .rename(columns={"price_date": "record_date"}) \
        .to_sql("portfolio_metrics", engine, if_exists="append", index=False)

    pd.DataFrame([summary]).to_sql(
        "performance_summary", engine, if_exists="replace", index=False
    )

    log("Load complete")

# =============================
# MAIN
# =============================

def main():
    log("START")

    create_tables_if_not_exist()
    stock_id = get_stock_id(SYMBOL)

    df = fetch_stock_data(SYMBOL)
    if df.empty:
        return

    df = filter_new_rows(df, stock_id)

    if df.empty:
        log("No new data")
        return

    df = add_metrics(df)
    summary = compute_summary(df)

    load_data(df, stock_id, summary)

    log("DONE")

# =============================
# RUN
# =============================

if __name__ == "__main__":
    main()