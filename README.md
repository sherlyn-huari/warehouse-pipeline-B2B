# Smart Sales Analyzer
ETL pipeline that blends Kaggle sales data with synthetic records, cleans and validates the dataset with Great Expectations, and stores curated outputs (CSV/Parquet + DuckDB) for analytics

## What You Get
- `src/etl.py` – full pipeline: download → clean → enrich → summaries → DuckDB.
- `src/synthetic_data_generator.py` – deterministic faker-based data generator.
- `src/dashboard.py` – Streamlit view over the gold tables.
- `data/input`, `data/output` – landing spots for raw and processed files.

## Requirements
- Python 3.10+ with `pip`.
- Kaggle API credentials (optional: only needed to pull the base dataset).
- 2‑3 GB of free disk space for DuckDB and CSV exports.

## Setup
```bash
git clone <repo-url>
cd smart_sales_analyzer
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
If you want the Kaggle dataset automatically:
```bash
export KAGGLE_USERNAME=<your_user>
export KAGGLE_KEY=<your_api_key>
```

## Run the Pipeline
```bash
source .venv/bin/activate
python src/etl.py
```
What happens:
1. Looks for `data/input/train.csv`. If missing, tries to download it from Kaggle (falls back to synthetic-only if download is unavailable).
2. Cleans the CSV, generates synthetic rows, and merges both sources.
3. Runs transformations, summary aggregates, and Great Expectations checks.
4. Writes outputs under `data/output/` and loads everything into `data/output/sales_analytics.duckdb`.

> **Offline tip:** drop your own `train.csv` into `data/input/` before running and the pipeline will skip the Kaggle step.

## Outputs
- `data/output/sales_enriched.parquet` – full cleaned dataset.
- `data/output/yearly.csv`, `segment_yearly.csv`, `regional_revenue.csv`, `top_products.csv` – ready-to-chart aggregates.
- `data/output/quality_report.json` – Great Expectations results.
- `data/output/sales_analytics.duckdb` – warehouse-style tables mirroring the gold layer.
- `etl_pipeline.log` – run history (info/warnings/errors).

## Warehouse Dimension
The DuckDB warehouse implements a **star schema** optimized for analytical queries, with `fact_sales` at the center surrounded by four dimension tables.

```
        dim_customer
              |
dim_date ── fact_sales ── dim_product
              |
        dim_location
```

### Dimension Tables
- **dim_customer** – Unique customers with attributes (customer_id, name, segment). Indexed on customer_id.
- **dim_product** – Product catalog hierarchy (product_id, name, category, sub_category). Indexed on product_id.
- **dim_location** – Geographic hierarchy (city, state, postal_code, region, country). Indexed on region and state.
- **dim_date** – Date dimension with calendar attributes (year, month, week, month_name). Auto-generated for all dates in sales range.

### Fact Table
- **fact_sales** – Grain: one row per order line. Contains foreign keys to all dimensions plus measures (sales_amount, quantity, unit_price, ship_latency_days). Enforces referential integrity with foreign key constraints and includes indexes on all dimension keys for fast joins.

Run `python src/build_dimensional_model.py` after the ETL to create the star schema.

## Streamlit Dashboard
```bash
source .venv/bin/activate
streamlit run src/dashboard.py
```