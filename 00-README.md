# Stock ETL Pipeline

ETL pipeline for extracting, transforming, and loading historical stock market data into a PostgreSQL database for analysis and reporting.

---

## Project Structure

- **src/** *(recommended if you split later)*: Core ETL logic (extract, transform, load functions)
- **pipelines/**
  - `main.py`: Main pipeline script (runs full ETL process)
- **data/** *(optional)*: Sample or test data outputs
- **config/** *(optional)*: Configuration files

---

## Pipeline Script

### main.py

**Purpose**: End-to-end ETL pipeline for stock market data using Yahoo Finance.

---

### Workflow

1. **Extract**
   - Fetches historical stock data using `yfinance`
   - Pulls daily data (`Open`, `High`, `Low`, `Volume`)
   - Filters data starting from December 1, 2007

2. **Transform**
   - Cleans and formats column names
   - Converts timestamps to date format
   - Adds financial metrics:
     - 5-day Simple Moving Average (SMA)
     - Relative Strength Index (RSI)
     - Buy/Sell/Hold trading signals
     - Portfolio metrics (position value, returns)

3. **Load**
   - Inserts data into PostgreSQL database (Supabase)
   - Writes to structured tables:
     - `stock`
     - `stock_price`
     - `stock_indicator`
     - `portfolio_metrics`
     - `performance_summary`

4. **Incremental Updates**
   - Only loads new records based on latest stored date
   - Prevents duplicate data loads

---

## Database Schema

### stock
Stores unique stock symbols

- `stock_id` (PK)
- `symbol`

### stock_price
Stores daily stock price data

- `stock_id`
- `price_date`
- `open`, `high`, `low`
- `volume`

---

### stock_indicator
Stores calculated indicators

- `stock_id`
- `record_date`
- `sma`
- `rsi`
- `signal`

---

### portfolio_metrics
Tracks position performance

- `stock_id`
- `record_date`
- `shares`
- `position_value`
- `daily_return_pct`

---

### performance_summary
Summary of stock performance

- `symbol`
- `bottom_date`
- `bottom_price`
- `current_price_date`
- `current_price`

---

## Key Features

- Fully automated ETL pipeline
- Incremental data loading (no duplicates)
- Technical indicators (SMA, RSI)
- Portfolio performance tracking
- PostgreSQL integration (Supabase)
- Clean modular functions (extract, transform, load)

---

## Requirements

Install dependencies using:

```bash
pip install -r requirements.txt
