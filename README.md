# Smart Sales Analyzer

End-to-end B2B retail analytics platform featuring an ETL pipeline, dimensional data warehouse, and interactive dashboard. Combines real Kaggle sales data with synthetic records, validates with Great Expectations, and delivers insights through Streamlit.

## Features

- **ETL Pipeline** – Automated data extraction, transformation, and loading with data quality checks
- **Dimensional Modeling** – Star schema warehouse with fact and dimension tables in DuckDB
- **Synthetic Data** – Deterministic faker-based generator for testing and development
- **Interactive Dashboard** – Real-time filtering and 8+ visualizations for sales insights
- **Data Validation** – Great Expectations integration for quality assurance

## Project Structure

```
smart_sales_analyzer/
├── src/
│   ├── etl.py                          # Main ETL pipeline
│   ├── synthetic_data_generator.py     # Synthetic data creation
│   ├── build_dimensional_model.py      # Star schema builder
│   └── dashboard.py                    # Streamlit dashboard
├── data/
│   ├── input/                          # Raw data files
│   └── output/                         # Processed outputs & DuckDB
└── requirements.txt                    # Python dependencies
```

## Requirements

- Python 3.10+
- 2-3 GB free disk space
- (Optional) Kaggle API credentials for dataset download

## Quick Start

### 1. Installation

```bash
git clone <repo-url>
cd smart_sales_analyzer
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Kaggle (Optional)

To automatically download the dataset:
```bash
export KAGGLE_USERNAME=<your_username>
export KAGGLE_KEY=<your_api_key>
```

**Offline mode:** Place your own `train.csv` in `data/input/` to skip Kaggle download.

### 3. Run ETL Pipeline

```bash
python src/etl.py
```

**Pipeline Steps:**
1. **Extract** – Downloads Kaggle dataset or uses local `train.csv`
2. **Clean** – Removes duplicates, handles nulls, validates data types
3. **Enrich** – Generates and merges synthetic records
4. **Validate** – Runs Great Expectations quality checks
5. **Load** – Saves to Parquet, CSV, and DuckDB

### 4. Build Data Warehouse

```bash
python src/build_dimensional_model.py
```

Creates star schema with dimension and fact tables in DuckDB.

### 5. Launch Dashboard

```bash
streamlit run src/dashboard.py
```

Opens interactive dashboard at `http://localhost:8501`

## Dashboard Features

The Streamlit dashboard provides:

- **KPI Metrics** – Revenue, Orders, Quantity, Customers, Products
- **Filters** – Year and month selection
- **Visualizations:**
  - Revenue & Orders by Month (line charts)
  - Revenue by Segment (multi-line chart)
  - Top 10 Customers (table with revenue, orders, quantity)
  - Cities by Revenue (sortable table)
  - Revenue by Region (horizontal bar chart)
  - Revenue by Category (table)
  - Top 5 Products (table with category, revenue, quantity)

All charts respond dynamically to filter selections!

## Outputs

| File | Description |
|------|-------------|
| `data/output/synthetic_data.parquet` | Full cleaned dataset |
| `data/output/yearly.csv` | Yearly revenue aggregates |
| `data/output/segment_yearly.csv` | Segment performance by year |
| `data/output/regional_revenue.csv` | Revenue by region |
| `data/output/top_products.csv` | Best-selling products |
| `data/output/quality_report.json` | Data validation results |
| `data/output/sales_analytics.duckdb` | Star schema warehouse |
| `etl_pipeline.log` | Pipeline execution log |

## Data Warehouse Schema

Star schema implementation in DuckDB:

```
        dim_customer
              |
dim_date ── fact_sales ── dim_product
              |
        dim_location
```

### Dimension Tables

| Table | Description | Key Attributes |
|-------|-------------|----------------|
| **dim_customer** | Customer master data | customer_id, name, segment |
| **dim_product** | Product catalog | product_id, name, category, sub_category |
| **dim_location** | Geographic hierarchy | city, state, postal_code, region, country |
| **dim_date** | Calendar dimension | date, year, month, week, month_name |

### Fact Table

**fact_sales** – Grain: One row per order line item
- Foreign keys: customer_id, product_id, location_id, order_date
- Measures: sales_amount, quantity, unit_price, ship_latency_days