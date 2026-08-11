# 🌍 India Global Trade Analyzer

A real-time data engineering pipeline that ingests India's import/export trade statistics from the World Bank WITS API, streams them through Apache Kafka, processes them in Databricks using PySpark, and visualizes insights in Power BI.

---

## 📌 Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Pipeline Stages](#pipeline-stages)
  - [1. Data Ingestion](#1-data-ingestion)
  - [2. Kafka Streaming](#2-kafka-streaming)
  - [3. Databricks ETL (Bronze → Silver → Gold)](#3-databricks-etl-bronze--silver--gold)
  - [4. Power BI Dashboard](#4-power-bi-dashboard)
- [Setup & Installation](#setup--installation)
- [How to Run](#how-to-run)
- [Automation & Scheduling](#automation--scheduling)
- [Dashboard Insights](#dashboard-insights)

---

## Project Overview

The **India Global Trade Analyzer** is an end-to-end data engineering project that:

- Fetches India's import and export trade data (2015–2023) from the **World Bank WITS API**
- Streams records in real-time to **Apache Kafka** running on **AWS EC2**
- Consumes and processes the data in **Databricks** using **PySpark**
- Applies a **Medallion Architecture** (Bronze → Silver → Gold) to build clean analytics tables
- Visualizes trade trends, top partners, top products, and KPIs in **Power BI**

---

## Architecture

```
┌──────────────────────────────────────────────┐
│           WITS World Bank API                │
│  Import / Export Trade Statistics Data       │
└──────────────────────────────────────────────┘
                     │
                     │ HTTP GET Requests
                     ▼
┌──────────────────────────────────────────────┐
│                wits_test.py                  │
│               Kafka Producer                 │
│                                              │
│ • Calls WITS API                             │
│ • Extracts Trade Records                     │
│ • Converts to JSON                           │
│ • Streams Messages                           │
└──────────────────────────────────────────────┘
                     │
                     │ JSON Messages
                     ▼
┌──────────────────────────────────────────────┐
│          Apache Kafka on AWS EC2             │
│                                              │
│  Broker : localhost:9092                     │
│  Topic  : trade_topic                        │
│                                              │
│  Stores Trade Events                         │
└──────────────────────────────────────────────┘
                     │
                     │ Kafka Consumer
                     ▼
┌──────────────────────────────────────────────┐
│              Databricks Spark                │
│                                              │
│ • Reads Kafka Stream                         │
│ • Creates Spark DataFrame                    │
│ • Performs ETL Processing                    │
└──────────────────────────────────────────────┘
                     │
          ┌──────────┼──────────┐
          ▼          ▼          ▼
       Bronze      Silver      Gold
       Layer        Layer      Layer
                     │
                     ▼
┌──────────────────────────────────────────────┐
│          Databricks Delta Lake               │
│  Parquet + _delta_log + ACID + Versioning    │
└──────────────────────────────────────────────┘
                     │
                     │ Databricks Token
                     ▼
┌──────────────────────────────────────────────┐
│                 Power BI                     │
│  KPI Dashboard · Trade Trends · Top Products │
└──────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Source | World Bank WITS API |
| Message Broker | Apache Kafka (on AWS EC2) |
| Cloud Infra | AWS EC2 (Ubuntu) |
| Processing | Databricks (PySpark) |
| Storage | Databricks Delta Lake |
| Visualization | Microsoft Power BI |
| Language | Python 3 |



---

## Pipeline Stages

### 1. Data Ingestion

**File:** `ingestion/wits_test.py`

The producer script fetches India's trade data from the [World Bank WITS API](https://wits.worldbank.org/) for two indicators:

| Indicator Code | Description |
|---|---|
| `MPRT-TRD-VL` | Import Trade Value |
| `XPRT-TRD-VL` | Export Trade Value |

**Years covered:** 2015 to 2023

For each API response, the script:
- Decodes dimension indexes (frequency, reporter country, partner country, product code, indicator)
- Extracts the trade value from observations
- Produces a JSON message to Kafka

**Sample Kafka Message:**
```json
{
  "year": 2022,
  "frequency": "Annual",
  "reporter_country": "India",
  "partner_country": "United States",
  "product_code": "TOTAL",
  "product_name": "All Products",
  "indicator": "Import Trade Value in 1000 USD",
  "trade_value": 45823621.5
}
```

**Kafka Producer Config:**
```python
producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'trade-producer',
    'acks': '1',
    'batch.num.messages': 1000
})
```

---

### 2. Kafka Streaming

Apache Kafka runs on an **AWS EC2** instance.

- **Broker:** `13.62.175.247:9092`
- **Topic:** `trade_topic`

Databricks connects to Kafka to consume messages:

```python
kafka_df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "13.62.175.247:9092")
    .option("subscribe", "trade_topic")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)
```

---

### 3. Databricks ETL (Bronze → Silver → Gold)

#### 🥉 Bronze Layer — Raw Ingestion

Parses the raw Kafka JSON string into a typed Spark DataFrame using a defined schema and saves it as a Delta table.


#### 🥈 Silver Layer — Cleaned Data

Applies data quality rules:
- Removes duplicate records
- Drops rows with null values in key dimensions
- Fills missing `trade_value` with `0`


#### 🥇 Gold Layer — Business Analytics

Eight aggregated analytics tables are created for Power BI consumption:

| Table | Description |
|---|---|
| `top_partner` | Top trading partners of India by total trade value |
| `top_product` | Top traded products by total trade value |
| `yearly_trade` | Year-wise total trade growth (2015–2023) |
| `country_product` | Number of unique products traded per country |
| `category_trade` | Trade distribution by product category |
| `trade_route` | Most valuable bilateral trade routes |
| `avg_product` | Average trade value per product |
| `trade_kpi` | KPI metrics: total transactions, total value, partner count, product count |

---

### 4. Power BI Dashboard

Power BI connects to Databricks Delta Lake using a **Databricks Token** and visualizes:

- 📊 KPI Cards — Total Transactions, Total Trade Value, Partner Countries, Products
- 📈 Year-wise Trade Growth Line Chart
- 🗺️ Top Trading Partners Bar Chart
- 📦 Top Products Treemap
- 🌐 Trade Routes Map
- 🏷️ Product Category Distribution

---

## Setup & Installation

### Prerequisites

- Python 3.8+
- Apache Kafka running on AWS EC2
- Databricks workspace
- Power BI Desktop

### 1. Clone the Repository

```bash
git clone https://github.com/SujalGodse/india-global-trade-analyzer.git
cd india-global-trade-analyzer
```

### 2. Create Python Virtual Environment

```bash
python3 -m venv trade_venv
source trade_venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install requests confluent-kafka
```

### 4. Start Kafka on EC2

```bash
# Create Kafka Topic
bin/kafka-topics.sh --create --topic trade_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

---

## How to Run

### Step 1 — Start EC2 Instance

Simply start the EC2 instance. Zookeeper and Kafka will auto-start via systemd — no manual commands needed.

### Step 2 — Run the Kafka Producer

```bash
cd ingestion
python wits_test.py
```

This will:
- Fetch data from WITS API for 2015–2023
- Stream trade records to `trade_topic` on Kafka

### Step 3 — Run Databricks Notebooks

Execute the notebooks in order in your Databricks workspace:

```
01_kafka_reader.py    →  Load Kafka data
02_bronze_layer.py    →  Create bronze_trade table
03_silver_layer.py    →  Create silver_trade table
04_gold_layer.py      →  Build Gold analytics tables
05_save_gold.py       →  Save all Gold tables to Delta Lake
```

### Step 4 — Connect Power BI

1. Open Power BI Desktop
2. Click **Get Data → Databricks**
3. Enter your Databricks workspace URL
4. Authenticate using a Databricks Personal Access Token
5. Select the Gold Layer tables and build your dashboards

---

## ⚙️ Automation & Scheduling

### EC2 Auto-Start (systemctl)

Kafka and Zookeeper are configured as **systemd services** on the EC2 instance. They automatically start on every EC2 boot — no manual commands required.

```bash
sudo systemctl enable zookeeper
sudo systemctl enable kafka
```

A startup script then automatically runs `wits_test.py` to begin streaming trade data to Kafka as soon as the instance is live.

### Databricks Scheduled Job

A Databricks job is scheduled to run **every 5 minutes**, which:
- Re-reads the latest Kafka messages
- Refreshes Bronze → Silver → Gold Delta tables automatically

### Power BI Live Refresh

Power BI connects to Databricks Delta Lake via a Databricks Token. Clicking **Refresh** in Power BI pulls the latest Gold Layer data, giving near real-time trade analytics.


## Dashboard Insights

The Power BI dashboard answers key business questions:

- Which countries are India's top import/export partners?
- Which product categories dominate India's trade?
- How has India's total trade value changed year over year?
- Which bilateral trade routes are the most valuable?
- How many unique products does India trade with each country?

---

## 🚀 Future Scope

- **Real-time streaming** — Replace batch Kafka reads with Spark Structured Streaming for true real-time pipeline processing
- **Multi-country support** — Extend the pipeline beyond India to analyze trade data for multiple countries simultaneously
- **ML forecasting** — Integrate machine learning models in Databricks to predict future trade values and trends
- **Automated alerts** — Set up Databricks alerts to notify when trade values cross defined thresholds
- **Data quality checks** — Add Great Expectations or Databricks data quality rules for automated validation at each layer
- **CI/CD pipeline** — Automate code deployments to Databricks using GitHub Actions
- **Cloud storage** — Move Delta Lake storage to AWS S3 or Azure Data Lake for scalable, persistent storage
- **Additional indicators** — Include more WITS indicators such as tariff rates, trade balance, and GDP correlation
