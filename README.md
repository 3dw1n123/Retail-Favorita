
# 🛒 Corporación Favorita - Retail Analytics & Data Pipeline

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13.0+-blue.svg)
![Docker](https://img.shields.io/badge/Docker-Enabled-blue.svg)
![Polars](https://img.shields.io/badge/Polars-Fast-orange.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-Interactive-red.svg)

## 📌 Project Overview
This project is an end-to-end Data Engineering and Business Intelligence (BI) solution based on the "Corporación Favorita Grocery Sales" Kaggle dataset.

The primary objective is to build a robust data pipeline capable of ingesting, cleaning, and processing over 129 million historical sales records. The architecture transforms raw data into a highly optimized analytical format (Parquet) to power an interactive, ultra-low latency local Dashboard.

## 🏗️ Architecture & Tech Stack
The project implements a modular ETL (Extract, Transform, Load) workflow, leveraging high-performance tools for large-scale data handling:
* **1. Efficient Ingestion (Data Loading):** `Pandas` + `psycopg2`. Raw data from large CSV files is processed in chunks and injected into a `PostgreSQL` instance (containerized via `Docker`). By utilizing the `cursor.copy_from()` method with memory buffers (`StringIO`), the pipeline achieves high-speed bulk loads without exhausting RAM.

* **2. Processing & Extraction (ETL):** `Polars` + `ConnectorX`. SQL Views are used to join sales data with oil prices, holidays, and store metadata. `Polars` then performs high-speed database reads and exports the data into Snappy-compressed `.parquet` files, partitioned by year for analytical optimization.

* **3. Visualization & Analytics (BI):** `Streamlit` & `Plotly`. The dashboard consumes Parquet files directly using Lazy Evaluation (`pl.scan_parquet()`), allowing users to explore millions of records with second response times.

## 🚀 Key Features
1. **Out-of-Core Ingestion:** Optimized loading script capable of populating PostgreSQL from a ~5GB CSV on local hardware with limited resources.

2. **Scalable Extraction Pipeline:** Automated workflow that exports data from PostgreSQL into a local Data Lake (Parquet format), optimizing storage and read speeds.

3. **Executive Dashboard (Overview):** Historical KPIs, monthly sales trends, and day-of-the-week pattern analysis.

4. **Granular Deep Dive:** Interactive analysis by store and product family, including the impact of promotions on sales volume.

## 📂 Project Structure
```text
retail-Favorita/
├── data/
│   ├── raw/               # Original Kaggle CSV files (git-ignored)
│   └── processed/         # Optimized Parquet files partitioned by year
├── notebooks/             # Exploratory Data Analysis (EDA) & Prototyping
├── src/
│   ├── data/              # Ingestion scripts (Pandas chunking + copy_from)
│   ├── features/          # ETL to Parquet (Polars + ConnectorX) & SQL queries
│   └── dashboard/         # Streamlit analytical application
├── docker-compose.yml     # Container orchestration (PostgreSQL + pgAdmin)
├── requirements.txt       # Python dependencies
└── .env.example           # Environment variables template
```
## ⚙️ Local Setup

### 1. Prerequisites
* **Docker & Docker Compose** installed.
* **Python 3.9+** installed.
* **Kaggle Data:** Download [the dataset](https://www.kaggle.com/c/favorita-grocery-sales-forecasting/data) and place the CSV files in `data/raw/`.

### 2. Environment Configuration
Clone the repository and set up a virtual environment:

```bash
git clone [https://github.com/3dw1n123/Retail-Favorita.git](https://github.com/3dw1n123/Retail-Favorita.git)
cd retail-Favorita
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Create a .env file in the root directory based on .env.example:
```bash
# PostgreSQL Configuration
POSTGRES_USER=favorita_user
POSTGRES_PASSWORD=favorita_pass
POSTGRES_DB=favorita_sales
POSTGRES_HOST=localhost
POSTGRES_PORT=5432

# pgAdmin Configuration
PGADMIN_EMAIL=admin@favorita.com
PGADMIN_PASSWORD=admin123
PGADMIN_PORT=5050
```

### 3. Database Deployment
Start the PostgreSQL and pgAdmin containers:
```bash
docker-compose up -d
```
### 4. ETL Pipeline Execution**

Note: Ensure CSV files are in data/raw/ before proceeding.
First, ingest raw data into PostgreSQL:
```bash
python src/data/ingestion.py
```
Then, extract and optimize data into Parquet format:
```bash
python src/features/export_to_parquet.py
```
### 5. Launch the Dashboard**
Once the Parquet files are ready, run the application:
```bash
streamlit run src/dashboard/app.py
```
The dashboard will be available at http://localhost:8501.
