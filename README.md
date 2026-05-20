# Real-Time Global Crypto Trade Analytics Pipeline Using Kafka, Spark, and Streamlit

---

# 1. Project Overview

## Project Title

**Real-Time Global Crypto Trade Analytics Pipeline Using Kafka, Spark, and Streamlit**

## Project Idea

This project implements a fully automated, end-to-end data engineering streaming pipeline. Instead of using mock data generators, this production-grade pipeline ingests **100% real-world, high-velocity financial market events** directly from the global cryptocurrency markets.

The pipeline establishes a live connection to a streaming data source, ingests raw unstructured records, balances sudden data traffic spikes using a distributed message broker, runs micro-batch state transformations using a cluster processing framework, and renders real-time business intelligence metrics on an interactive analytical dashboard.

---

# 2. System Architecture

```plaintext
[Binance Live WebSocket API] (Real-time BTC/USDT Trades)
                 │
                 ▼ (Asynchronous Ingestion)
    [Kafka Producer (producer.py)]
                 │
                 ▼ (Topic: crypto_trades)
   [Apache Kafka Broker (Docker Container)]
                 │
                 ▼ (PySpark Structured Streaming Consumer)
   [Spark Streaming (spark_processor.py)]
                 │
                 ▼ (Writes Atomic Mini-Batch JSON Partitions)
     [Local File System (./spark_output)]
                 │
                 ▼ (Reactive Plotly Engine)
   [Streamlit Dashboard (dashboard.py)]
```

---

# 3. Technology Stack & Core Roles

| Technology | Role inside Pipeline | Justification |
|---|---|---|
| Python | System Core & Interface Glue | Handles asynchronous data collection, payload structures, and analytics visualization logic. |
| Binance Live WebSocket API | Live Data Source Stream | Supplies continuous, un-cached global trade operations data without requiring API keys. |
| Apache Kafka (Confluent) | Distributed Message Queue | Acts as a shock absorber to prevent pipeline crashes by queuing high-velocity event spikes. |
| Apache Spark (PySpark) | Stream Processing Engine | Handles high-throughput record parsing, structural schema casting, and continuous disk IO writing. |
| Docker & Docker Compose | Infrastructure Isolation | Packs Zookeeper and Kafka brokers cleanly inside lightweight virtual runtime networks. |
| Streamlit & Plotly | Dynamic Business Intelligence | Pulls atomic query batches to render interactive figures and live data insights frames. |

---

# 4. Real Data Source Specifications

The streaming pipeline taps into the Binance WebSocket Server Engine using an active connection.

## Live Connection String

```plaintext
wss://stream.binance.com:9443/ws/btcusdt@trade
```

## Throughput

- ~5 to 30 transactions per second
- Depends on global market volatility

## Raw JSON Schema Captured

```json
{
  "e": "trade",
  "E": 1715612345678,
  "s": "BTCUSDT",
  "t": 123456789,
  "p": "64150.50",
  "q": "0.00452",
  "b": 987654321,
  "a": 123456781,
  "T": 1715612345123,
  "m": true
}
```

## Field Descriptions

| Field | Description |
|---|---|
| `e` | Event Type |
| `E` | Epoch Timestamp in milliseconds |
| `s` | Trading Ticker Symbol |
| `t` | Unique Aggregate Trade ID |
| `p` | Execution Price per unit |
| `q` | Transaction Quantity |
| `b` | Buyer Order ID |
| `a` | Seller Order ID |
| `T` | Trade Time |
| `m` | Indicates whether buyer is the market maker |

---

# 5. Analytical Feature Engineering

When raw strings hit Apache Spark, the pipeline transforms the data by parsing string decimals into active numerics and calculating the true financial capital value on the fly:

```math
Total Capital Inflow (USD) = Price (USD) × Trade Quantity (BTC)
```

---

# 6. Dashboard Visualization & Insights Grid

The UI layer switches from static plots to a high-speed reactive grid, providing four real-time analytical vectors:

## Real-Time Price Trend (Plotly Line Chart)

Plots price variations chronologically across the batch processing window to capture market trends.

## Cumulative Capital Inflow (Plotly Area Chart)

Tracks the running total volume of fiat capital entering the order book over the streaming timeline.

## Whale Tracker / Participant Profile (Plotly Donut Chart)

Automatically categorizes transactions into distinct financial tiers using advanced data classification logic:

- **Retail Profile:** Transactions under `$2,000` USD
- **Professional Profile:** Transactions between `$2,000` and `$15,000` USD
- **Institutional Profile:** Transactions between `$15,000` and `$50,000` USD
- **Whale Profile:** Transactions exceeding `$50,000` USD

## Order Density Analysis (Plotly Histogram Chart)

Aggregates order frequency across micro price brackets to dynamically reveal immediate support and resistance thresholds.

---

# 7. Setup & Execution Blueprint

Follow this step-by-step guide to run the pipeline locally on your machine.

## Prerequisites

- Python 3.10+
- Docker Desktop running in the background
- Java Development Kit (JDK 11 or higher)

---

## Step 1: Clone & Install Dependencies

```bash
# Setup clean isolated environment
python3 -m venv venv
source venv/bin/activate

# Install requirements
pip install confluent-kafka streamlit pandas plotly websockets streamlit-autorefresh numpy pyspark==3.5.0
```

---

## Step 2: Launch the Infrastructure Cluster

```bash
# Start Kafka and Zookeeper background instances
docker-compose up -d

# Verify containers are healthy
docker ps
```

---

## Step 3: Run the Ingestion Engine (Terminal 1)

```bash
source venv/bin/activate
python3 producer.py
```

---

## Step 4: Start the Stream Processor (Terminal 2)

```bash
source venv/bin/activate
python3 spark_processor.py
```

---

## Step 5: Boot Up the Presentation Interface (Terminal 3)

```bash
source venv/bin/activate
streamlit run dashboard.py
```

---

# 8. Core Data Engineering Principles Proved

## Asynchronous Concurrency

Utilizes Python's `asyncio` and non-blocking network loops to decouple rapid external API collection from the internal pipeline.

## Stream Buffer Decoupling

Employs Kafka brokers as distributed log buffers to guarantee data durability under extreme load spikes.

## Structured Stream Processing

Leverages PySpark to cast schemas, perform atomic data cleansing, and manage streaming checkpoint boundaries.

## Disk IO Performance Safety

Optimizes the dashboard by sorting partition logs and using a fixed sliding window to read data, preventing memory issues during long-running presentations.
