# Smart Sales Analyzer

A modern data engineering project demonstrating **Azure Databricks** best practices with end-to-end ETL/ELT pipelines, medallion architecture, and analytics-ready data warehousing.

## Tech Stack

**Azure Cloud Platform:**
- **Azure Databricks** - Managed Apache Spark for distributed data processing
- **Azure Data Lake Storage Gen2 (ADLS)** - Scalable data lake for raw and processed data
- **Delta Lake** - ACID transactions, time travel, and schema evolution
- **Databricks SQL Warehouse** - Serverless analytics engine
- **Unity Catalog** - Data governance and access control

**Local Development:**
- **Python 3.12** with Polars/Pandas for prototyping
- **DuckDB** - Local analytics database for testing
- **Great Expectations** - Data quality validation
- **Streamlit** - Interactive dashboard

## What You Get
- **Medallion Architecture**: Bronze → Silver → Gold data layers with Delta Lake
- **Modern ETL**: Production-ready Databricks notebooks with PySpark
- **Flexible sourcing**: Kaggle dataset + synthetic data augmentation
- **Data Quality**: Built-in validation with Delta Live Tables expectations
- **Ready-to-query warehouse**: DuckDB for local dev, Databricks SQL for production
- **Interactive reporting**: Streamlit dashboard for demos

## Quickstart

```bash
# 1) Clone and enter the project
cd smart_sales_analyzer

# 2) Create/activate a virtual environment (optional but recommended)
python3 -m venv .venv
source .venv/bin/activate

# 3) Install dependencies
pip install -r requirements.txt

# 4) (Optional) Export Kaggle credentials if you want to re-download the dataset
export KAGGLE_USERNAME=your_username
export KAGGLE_KEY=your_key
```

## Run the ETL

```bash
source .venv/bin/activate
python src/etl.py
```

Outputs land in `data/`:
- `train.csv` – cleansed Kaggle source (cached copy if Kaggle API is unavailable)
- `sales_enriched.parquet` – blended dataset with synthetic augmentation
- `regional_revenue.csv`, `segment_yearly.csv`, `top_products.csv`, `yearly.csv`
- `quality_report.json` – Great Expectations results
- `sales_analytics.duckdb` – DuckDB file with analytics tables
- `etl_pipeline.log` – run history

## Launch the Streamlit Dashboard

```bash
source .venv/bin/activate
streamlit run src/dashboard.py
```

Use the URL printed in the terminal to open the interactive report (filters, charts, KPIs).

## Explore DuckDB Tables via CLI

```bash
source .venv/bin/activate
python - <<'PY'
import duckdb

with duckdb.connect("data/sales_analytics.duckdb", read_only=True) as con:
    con.sql("SHOW TABLES FROM analytics").show()
    con.sql("""
        SELECT order_year, SUM(sales) AS revenue
        FROM analytics.sales
        GROUP BY 1
        ORDER BY 1
    """).show()
PY
```

More handy snippets for screenshots or demos:

```bash
source .venv/bin/activate
python - <<'PY'
import duckdb

with duckdb.connect("data/sales_analytics.duckdb", read_only=True) as con:
    con.sql("""
        SELECT COUNT(*) AS rows, SUM(sales) AS total_revenue
        FROM analytics.sales
    """).show()
    con.sql("""
        SELECT MIN(order_date) AS min_date, MAX(order_date) AS max_date
        FROM analytics.sales
    """).show()
    con.sql("""
        SELECT order_year, segment, revenue
        FROM analytics.segment_yearly_summary
        ORDER BY order_year, revenue DESC
    """).show()
    con.sql("""
        SELECT *
        FROM analytics.regional_revenue_summary
        ORDER BY revenue DESC
    """).show()
    con.sql("""
        SELECT *
        FROM analytics.top_products_summary
        ORDER BY revenue DESC
        LIMIT 15
    """).show()
PY
```

## Repository Layout

```
├── data/                     # Local development data
│   ├── sales_analytics.duckdb
│   ├── sales_enriched.parquet
│   ├── *.csv (summary tables)
│   └── quality_report.json
├── src/                      # Local ETL scripts
│   ├── etl.py
│   ├── dashboard.py
│   └── synthetic_data_generator.py
├── databricks/               # Azure Databricks deployment
│   ├── notebooks/
│   │   ├── bronze/           # Raw data ingestion
│   │   ├── silver/           # Cleaned & enriched data
│   │   └── gold/             # Business aggregations
│   ├── workflows/            # Job orchestration configs
│   └── config/               # Databricks settings
├── docs/                     # Architecture documentation
│   └── ARCHITECTURE.md       # Azure Databricks stack design
├── requirements.txt
└── README.md
```

## Azure Databricks Architecture

This project demonstrates a production-ready **Medallion Architecture** on Azure Databricks:

**Bronze Layer** (Raw Data)
- Ingest from Kaggle + synthetic generation
- Store as Delta Lake tables in ADLS
- No transformations, full audit trail

**Silver Layer** (Cleaned Data)
- Data quality validation with DLT expectations
- Column standardization and type casting
- Derived columns (order_year, order_month)
- Partitioned by date for performance

**Gold Layer** (Analytics Ready)
- Pre-aggregated business metrics
- Optimized for BI queries
- Yearly/segment/regional summaries

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed architecture diagrams and design decisions.

## Demo Shots
Drop your screenshots in `assets/` and reference them here. Examples from my run:

<p align="center">
  <img src="assets/streamline_dashboard_1.png" alt="Streamlit dashboard overview" width="640">
</p>

<p align="center">
  <img src="assets/streamline_dashboard_2.png" alt="Streamlit dashboard product drilldown" width="640">
</p>

<p align="center">
  <img src="assets/streamline_dashboard_3.png" alt="Streamlit dashboard regional view" width="640">
</p>

<p align="center">
  <img src="assets/pipeline_run_snippet.png" alt="ETL execution with data quality success" width="640">
</p>

<p align="center">
  <img src="assets/analytics.yearly_summary.png" alt="DuckDB yearly revenue query results" width="640">
</p>

<p align="center">
  <img src="assets/analytics.top_products_summary.png" alt="DuckDB top products query results" width="640">
</p>

<p align="center">
  <img src="assets/analytics.regional_revenue_summary.png" alt="DuckDB regional revenue query results" width="640">
</p>
