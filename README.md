# Smart Sales Analyzer

End-to-end B2B retail analytics platform featuring an ETL pipeline, dimensional data warehouse, and interactive dashboard. Uses synthetic sales data for testing and demonstration, with data quality validation via Great Expectations and interactive insights through Streamlit.

<div align="center">
<img src="images/dashboard_1.png" width=700>
<div><i>Interactive sales analytics dashboard with real-time filtering</i></div>
</div>


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
│   ├── etl.py                         
│   ├── synthetic_data_generator.py     
│   ├── build_dimensional_model.py  
│   └── dashboard.py                   
├── data/
│   ├── input/                 
│   └── output/                         
└── requirements.txt                 
```

## Requirements

- Python 3.10+
- 2-3 GB free disk space

## Quick Start

### 1. Installation

```bash
git clone <repo-url>
cd smart_sales_analyzer
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Run ETL Pipeline

```bash
python src/etl.py
```

**Pipeline Steps:**
1. **Generate** – Creates synthetic B2B sales data using [product_catalog.json](data/input/product_catalog.json)
2. **Clean** – Removes duplicates, handles nulls, validates data types
3. **Validate** – Runs Great Expectations quality checks
4. **Load** – Saves to Parquet, exports CSV summaries, loads into DuckDB

**Customization parts inside the pipeline:**

- To adjust the Data Generation like:

  1. Number of rows to generate
  2. Start Date of the dataset
  3. End Date of the dataset
  4. Rebuild (True: if you want to regenerate the whole dataset / False: keep the last dataset you generate )

modify in this line [etl.py:303](src/etl.py#L303).

- To generate different synthetic datasets, change the seed in [synthetic_data_generator.py:44](src/synthetic_data_generator.py#L44).

- To rebuild your data modeling - Star schema, put True on each run, otherwise False to reuse existing data modeling [etl.py:281](src/etl.py#L281).

### 3. Launch Dashboard

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
  - Top Performing Cities (Revenue - Orders - Quantity) (sortable table)
  - Revenue by Region (horizontal bar chart)
  - Revenue by Category (table)
  - Top 10 Products (Revenue - Orders - Quantity)

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

### Access the data warehouse in DuckDB:**

```bash

duckdb data/output/warehouse/sales_analytics.duckdb

# Query dimension tables
SELECT * FROM dim_customer LIMIT 10;
SELECT * FROM dim_product WHERE category = 'Technology';
SELECT * FROM dim_location WHERE region = 'West';
SELECT * FROM dim_date WHERE year = 2023;

# Query fact table
SELECT * FROM fact_sales LIMIT 10;

# Example of a query
SELECT
  d.year,
  d.month_name,
  SUM(f.sales_amount) as revenue
FROM fact_sales f
JOIN dim_date d ON f.order_date = d.date
GROUP BY d.year, d.month_name
ORDER BY d.year, d.month;
```