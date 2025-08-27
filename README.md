# Stock Market Data Pipeline

## Overview
This project implements an **end-to-end data pipeline** for stock market data.  
It ingests **live streaming prices** and **daily OHLCV data**, cleans and normalizes it,  
enriches with **technical indicators + simple buy/sell/hold signals**, and stores it for  
downstream consumers (e.g., backtesting engines, dashboards, live trading systems).

**Key Features**
- Live ingestion (ticks) via Kafka
- Batch ingestion (daily OHLCV)
- Real-time aggregation into 1d bars
- Technical indicators (EMA, RSI, MACD)
- Rule-based buy/sell/hold signal generation
- Query-ready serving layer in PostgreSQL
- Orchestration & data quality checks

---

## Architecture

```mermaid
flowchart TD
    A[Live Price Feed] -->|ticks| B[Kafka: ticks.raw]
    B --> C[Stream Processor: Clean + Normalize]
    C -->|ticks.clean| D[Data Lake Bronze/Silver]
    C --> E[Bar Aggregator: 1d OHLCV]
    E -->|bars.1d| F[Lake: Silver]
    E --> G[Indicator & Signal Enricher]
    G -->|signals| H[Lake: Gold]
    H --> I[PostgreSQL: Signals + Bars]
    J[Batch Daily OHLCV API] --> K[Dagster Batch Jobs]
    K --> F
    K --> G
```
---


