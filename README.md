# Stock Market Data Pipeline

## Overview
This project implements an **end-to-end data pipeline** for stock market data.  
It ingests **daily OHLCV data**, cleans and normalizes it,  
enriches with **technical indicators**, and stores it for  
downstream consumers (e.g., backtesting engines, dashboards, live trading systems).

**Key Features**
- Batch ingestion (daily OHLCV)
- Real-time aggregation into 1d bars
- Technical indicators (EMA, RSI, MACD)
- Query-ready serving layer in PostgreSQL
- Orchestration & data quality checks
- Visualisation in grafana
- Persistant storage in self-hosted postgresql database
- Correlation Matrix to show stocks with similar 20-day movements

## Setup
Add the following variables to a `.env` file:
```
DB_USER=admin
DB_PASSWORD=postgres_password
DB_HOST=127.0.0.0
DB_PORT=5432
DB_NAME=postgres_database

DB_TABLE_TICKERS=tickers
DB_TABLE_RAW_DATA=raw_tickers
DB_TABLE_INDICATORS=indicators
DB_TABLE_CORRELATION=correlations
DB_OPTIONS_TABLE=option_snapshots

ALPACA_KEY=alpaca-key
ALPACA_SECRET=alpaca-secret-key
```

There are several services needed for this system
- PostgreSQL
- Grafana
- Alpaca Broker (for stock ingest)

To stand up **PostgreSQL** and **Grafana** follow the setup [here](https://github.com/keegan-28/monitoring_stack) using Ansible deployment.

To run the Alpaca Broker and API run this
```bash 
uv venv
source .venv/bin/activate
uv sync

uvicorn src.api.main:app --reload
```

To run as a docker container run detached
```bash
docker compose up --build bankvault -d
```

Then navigate to the [SwaggerUI](http://0.0.0.0:8000/api/v1/docs)