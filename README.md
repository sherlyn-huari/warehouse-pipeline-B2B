# Smart Sales Analyzer

An AWS-focused **data engineering pipeline** that ingests raw sales records, applies a Medallion-style refinement flow, and produces curated analytics outputs without racking up large infrastructure costs. Develop locally with DuckDB/Streamlit, then deploy the same steps inside AWS managed services when desired.

## Project scope
- This repository is **purely a data engineering + pipeline build**: ingestion, cleaning, enrichment, quality checks, and curated outputs.
- There is **no production app or SaaS** component—just an end-to-end pipeline you can point at AWS Glue/S3/Athena.
- Streamlit is included only to **inspect the gold tables produced by the pipeline**.

## Why it matters
- **Real AWS story** – S3-based lakehouse, AWS Glue ETL, Athena SQL layer, and QuickSight-ready outputs.
- **Modern patterns** – medallion layers, data quality checks, DuckDB development workflow, and an interactive Streamlit dashboard.

## Architecture at a glance
```
Kaggle + Synthetic Data
          │
          ▼
 ┌─────────────────┐
 │  S3 Bronze Lake │  (raw parquet/csv, versioned)
 └─────────────────┘
          │  AWS Glue Jobs / Ray on Glue
          ▼
 ┌─────────────────┐
 │ S3 Silver Layer │  (clean + enriched, schema enforced)
 └─────────────────┘
          │  Quality checks → AWS Glue Data Quality / Great Expectations
          ▼
 ┌─────────────────┐
 │ S3 Gold Layer   │  (aggregations for BI)
 └─────────────────┘
          │
     ┌────┴───────────────┐
     │                     │
 Amazon Athena       Amazon QuickSight
 (SQL + DuckDB      (dashboards, or keep
  parity)            Streamlit for demos)
```

Orchestration options:
- **AWS Step Functions + Glue Jobs** for fully managed pipelines.
- **Amazon MWAA (Airflow)** or **Dagster on ECS Fargate** if you need Airflow/Dagster compatibility.
- **EventBridge Scheduler + Lambda** for the absolute lowest-cost cron style trigger.

## AWS components & cost controls
- **Amazon S3** – single bucket with `bronze/`, `silver/`, `gold/` prefixes; enable Intelligent-Tiering and lifecycle rules to stay in the free tier.
- **AWS Glue** – use **Glue Studio Notebooks** (pay only when you run) for dev, then convert to Glue Jobs; 0.0625 DPU sessions keep per-run cost pennies.
- **AWS Glue Data Catalog + Lake Formation** – central schema registry and governance; free unless you exceed one million requests.
- **AWS Glue Data Quality or Great Expectations** – start with the local GE checks already in `src/etl.py`, then port expectations into Glue.
- **Amazon Athena** – query the gold tables directly on S3; costs ~$5 per TB scanned, so keep data columnar (Parquet) and partition on `order_year`.
- **Amazon QuickSight SPICE** – free for 1 user for 90 days; after that either export CSVs or connect the Streamlit app via `streamlit run src/dashboard.py`.
- **Amazon CloudWatch Logs** – ingest Glue/Step Functions logs with log retention set to 1–3 days to avoid surprises.

> **Pro tip:** stay entirely in local mode (DuckDB + Streamlit) while building; once you’re happy, deploy a single Glue job + Athena view run. One monthly execution at the smallest scale keeps costs well under $1.

## Local development workflow (free)
```bash
# 1) Clone and enter the project
git clone <your-fork-url>
cd smart_sales_analyzer

# 2) Create & activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3) Install dependencies
pip install -r requirements.txt

# 4) (Optional) pull fresh data
export KAGGLE_USERNAME=your_username
export KAGGLE_KEY=your_key
python src/etl.py --download
```

## Run the ETL locally
```bash
source .venv/bin/activate
python src/etl.py
```

Key outputs inside `data/`:
- `train.csv` – cached Kaggle dataset
- `sales_enriched.parquet` – silver-layer equivalent
- `regional_revenue.csv`, `segment_yearly.csv`, `top_products.csv`, `yearly.csv` – gold-layer summaries
- `sales_analytics.duckdb` – DuckDB database mirroring what Athena/Glue tables will hold
- `quality_report.json` – Great Expectations validation log
- `etl_pipeline.log` – run history

## Streamlit dashboard (pipeline validation)
```bash
source .venv/bin/activate
streamlit run src/dashboard.py
```
This reads the curated tables so you can validate dimensions, measures, and filters before exposing them through Athena or QuickSight.

## Deploying to AWS on a budget
1. **Create an S3 bucket** (`smart-sales-analyzer-<alias>`) with `bronze/`, `silver/`, `gold/` prefixes.
2. **Upload** `data/train.csv` to `bronze/raw/`.
3. **Package the ETL**: zip `src/etl.py`, dependencies from `requirements.txt`, and upload to S3 (or use a container image stored in ECR) for Glue.
4. **Spin up a Glue Studio Notebook**, point it at the `etl.py` logic, and test with a `1 DPU` session (costs cents).
5. **Convert notebook → Glue Job**, schedule it via **EventBridge Scheduler** or **Step Functions** (both free for low volume).
6. **Catalog the tables** with Glue Crawlers (first million objects per month are free) so Athena can discover the schemas.
7. **Build Athena views** over `gold/` data to expose the metrics layer.
8. **Optional**: Publish a QuickSight dashboard or host the Streamlit app if stakeholders need visuals.

## Repository layout
```
├── data/                     # Local development data + DuckDB lakehouse mirror
├── src/
│   ├── etl.py                # Spark/Polars ETL (drop-in for Glue)
│   ├── synthetic_data_generator.py
│   └── dashboard.py          # Streamlit front-end
├── assets/                   # Reference charts generated from gold tables
├── requirements.txt
└── README.md
```

When you add AWS-specific assets (IaC, Glue scripts, Step Functions), drop them under `infra/aws/` or `aws/` for quick discovery.

Have fun building, and enjoy running an AWS pipeline that proves you can design scalable data systems while keeping costs low.
