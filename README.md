# Stock Market Data Pipeline

## 📖 Overview
This project implements an **end-to-end data pipeline** for stock market data.  
It ingests **live streaming prices** and **daily OHLCV data**, cleans and normalizes it,  
enriches with **technical indicators + simple buy/sell/hold signals**, and stores it for  
downstream consumers (e.g., backtesting engines, dashboards, live trading systems).

**Key Features**
- Live ingestion (ticks) via Kafka
- Batch ingestion (daily OHLCV)
- Real-time aggregation into 1m bars
- Technical indicators (EMA, RSI, MACD)
- Rule-based buy/sell/hold signal generation
- ACID storage using Delta/Iceberg
- Query-ready serving layer in PostgreSQL
- Orchestration & data quality checks

---

## 🏗️ Architecture

```mermaid
flowchart TD
    A[Live Price Feed] -->|ticks| B[Kafka: ticks.raw]
    B --> C[Stream Processor: Clean + Normalize]
    C -->|ticks.clean| D[Data Lake Bronze/Silver]
    C --> E[Bar Aggregator: 1m OHLCV]
    E -->|bars.1m| F[Lake: Silver]
    E --> G[Indicator & Signal Enricher]
    G -->|signals| H[Lake: Gold]
    H --> I[PostgreSQL: Signals + Bars]
    J[Batch Daily OHLCV API] --> K[Airflow Batch Jobs]
    K --> F
    K --> G
```
---

# 🕒 Time Estimate for MVP  

**MVP Goal**: Get a working pipeline that can:  
1. Ingest live data (to Kafka)  
2. Process into clean ticks + 1m bars  
3. Enrich with 1–2 indicators + signals  
4. Store in PostgreSQL  
5. Query with SQL  

**Rough Breakdown (solo dev, 2–3 hrs/day effort):**

| Task | Tools | Time |
|------|-------|------|
| Setup Kafka + Schema Registry (Docker Compose) | Kafka, Docker | 1 day |
| Write live data collector → Kafka | Python | 1–2 days |
| Spark Streaming job for ticks → 1m bars | Spark | 2–3 days |
| Add basic indicators (EMA, RSI) + signals | Spark/Pandas UDFs | 2 days |
| PostgreSQL integration + schema | Postgres | 1 day |
| Batch OHLCV ingestion (daily API pull) | Airflow/Python | 1–2 days |
| Basic monitoring (Prometheus/Grafana) | Docker, Prometheus | 1 day |
| Documentation + repo polish | Markdown, diagrams | 1 day |

