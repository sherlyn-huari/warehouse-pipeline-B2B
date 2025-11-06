# Architecture: Azure Databricks Data Engineering Stack

## Overview

The Smart Sales Analyzer is a modern data engineering pipeline built for **Azure Databricks**, demonstrating end-to-end ETL/ELT workflows with data quality validation and analytics.

## Technology Stack

### Azure Cloud Services
- **Azure Data Lake Storage Gen2 (ADLS)** - Scalable data lake for raw and processed data
- **Azure Databricks** - Managed Apache Spark platform for data processing
- **Databricks SQL Warehouse** - Serverless compute for SQL analytics
- **Delta Lake** - Open-source storage layer with ACID transactions

### Data Processing
- **PySpark** - Distributed data processing
- **Delta Lake Format** - Time travel, schema evolution, ACID guarantees
- **Python** - Data generation and utilities

### Data Quality & Governance
- **Delta Live Tables (DLT)** - Declarative pipelines with expectations
- **Unity Catalog** - Data governance and access control

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
│  ┌──────────────────┐          ┌─────────────────────┐         │
│  │  Kaggle Dataset  │          │  Synthetic Data     │         │
│  │   (Sales Data)   │          │   Generator         │         │
│  └──────────────────┘          └─────────────────────┘         │
└────────────────┬────────────────────────┬─────────────────────┘
                 │                        │
                 └────────────┬───────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  AZURE DATA LAKE STORAGE (ADLS Gen2)            │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐   │
│  │  BRONZE LAYER (Raw Data)                               │   │
│  │  - raw_sales_data/                                     │   │
│  │  - Format: Delta Lake                                  │   │
│  │  - Schema: Original columns from sources               │   │
│  └─────────────────────┬──────────────────────────────────┘   │
│                        │                                        │
│                        ▼                                        │
│  ┌────────────────────────────────────────────────────────┐   │
│  │  SILVER LAYER (Cleaned & Enriched)                     │   │
│  │  - sales_enriched/                                     │   │
│  │  - Format: Delta Lake                                  │   │
│  │  - Transformations:                                    │   │
│  │    • Data type standardization                         │   │
│  │    • Null handling                                     │   │
│  │    • Derived columns (order_year, order_month)         │   │
│  │    • Data quality validation                           │   │
│  └─────────────────────┬──────────────────────────────────┘   │
│                        │                                        │
│                        ▼                                        │
│  ┌────────────────────────────────────────────────────────┐   │
│  │  GOLD LAYER (Analytics Ready)                          │   │
│  │  - yearly_summary/                                     │   │
│  │  - segment_yearly_summary/                             │   │
│  │  - regional_revenue_summary/                           │   │
│  │  - top_products_summary/                               │   │
│  │  - Format: Delta Lake                                  │   │
│  └────────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              DATABRICKS SQL WAREHOUSE                            │
│  - Serverless SQL compute                                        │
│  - Query gold layer tables                                       │
│  - Power BI / Tableau connectivity                               │
└─────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ANALYTICS & BI                                │
│  ┌──────────────────┐          ┌─────────────────────┐         │
│  │ Databricks SQL   │          │     Power BI        │         │
│  │   Dashboards     │          │    Dashboards       │         │
│  └──────────────────┘          └─────────────────────┘         │
└─────────────────────────────────────────────────────────────────┘
```

## Medallion Architecture

### Bronze Layer (Raw)
**Purpose**: Store raw, unprocessed data exactly as received from sources

- **Location**: `adls://bronze/raw_sales_data/`
- **Format**: Delta Lake
- **Schema**: Original structure from Kaggle + synthetic generator
- **Partitioning**: By `order_date` (date-based partitions)
- **Operations**:
  - Append-only writes
  - No transformations
  - Full audit trail

### Silver Layer (Cleaned)
**Purpose**: Cleaned, validated, and enriched data ready for analytics

- **Location**: `adls://silver/sales_enriched/`
- **Format**: Delta Lake with schema enforcement
- **Transformations**:
  - Column standardization (snake_case)
  - Type casting (dates, numerics)
  - Null handling
  - Derived columns (`order_year`, `order_month`)
  - Data quality checks (Great Expectations / DLT)
- **Partitioning**: By `order_year` and `order_month`
- **Features**:
  - Schema evolution enabled
  - Change data feed (CDC) for downstream tracking
  - Optimized with Z-ordering on `customer_id`

### Gold Layer (Aggregated)
**Purpose**: Business-ready aggregations for analytics and reporting

- **Location**: `adls://gold/<table_name>/`
- **Tables**:
  - `yearly_summary` - Annual sales metrics
  - `segment_yearly_summary` - Segment × year breakdown
  - `regional_revenue_summary` - Geographic performance
  - `top_products_summary` - Top 15 products by revenue
- **Format**: Delta Lake (optimized for queries)
- **Update Frequency**: Incremental updates on Silver changes
- **Optimization**: Pre-aggregated for fast BI queries

## Data Pipeline Flow

### Orchestration: Databricks Workflows

```
Job: Sales Analytics ETL Pipeline
├── Task 1: Bronze Ingestion
│   ├── Download Kaggle dataset → ADLS
│   ├── Generate synthetic data
│   └── Write to bronze layer (Delta)
│
├── Task 2: Silver Transformation
│   ├── Read from bronze layer
│   ├── Clean & standardize data
│   ├── Add derived columns
│   ├── Run data quality checks (DLT expectations)
│   └── Write to silver layer (Delta)
│
└── Task 3: Gold Aggregation
    ├── Read from silver layer
    ├── Build summary tables:
    │   ├── Yearly aggregations
    │   ├── Segment breakdowns
    │   ├── Regional revenue
    │   └── Top products
    └── Write to gold layer (Delta)
```

### Execution Schedule
- **Trigger**: Scheduled (daily) or event-driven (file arrival)
- **Cluster**: Job cluster (auto-scaling, spot instances)
- **Notifications**: Email/Slack on success/failure

## Data Quality Framework

### Delta Live Tables Expectations

```python
# Example expectations in DLT pipeline
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_sales", "sales > 0")
@dlt.expect("valid_date_range", "order_year BETWEEN 2015 AND 2025")
```

### Quality Checks
- **Completeness**: No null values in critical columns
- **Validity**: Sales > 0, valid date ranges
- **Consistency**: Order date ≤ Ship date
- **Uniqueness**: Unique `row_id` per record

## Warehouse Layer

### Databricks SQL Warehouse Configuration
- **Type**: Serverless (recommended) or Pro
- **Scaling**: Auto-scale based on query load
- **Catalog**: Unity Catalog for governance
- **Schema**: `analytics` (contains gold tables)

### Sample Queries

```sql
-- Total sales by year
SELECT order_year, SUM(sales) as total_sales
FROM analytics.yearly_summary
GROUP BY order_year
ORDER BY order_year;

-- Top performing regions
SELECT region, total_revenue
FROM analytics.regional_revenue_summary
ORDER BY total_revenue DESC
LIMIT 5;
```

## Scalability & Performance

### Optimization Techniques
1. **Partitioning**: Date-based partitions for time-series data
2. **Z-Ordering**: Multi-dimensional clustering on `customer_id`, `product_id`
3. **Caching**: Delta cache for frequently accessed tables
4. **Auto-optimize**: Automatic compaction of small files
5. **Liquid Clustering**: (Future) Automatic data organization

### Cost Optimization
- **Spot instances** for non-critical jobs
- **Job clusters** terminate after completion
- **Serverless SQL** for unpredictable workloads
- **Photon engine** for 3x faster queries

## Security & Governance

### Unity Catalog
- **Metastore**: Centralized metadata management
- **Access Control**: Row/column-level security
- **Lineage**: Automatic data lineage tracking
- **Auditing**: Query history and access logs

### Authentication
- **Service Principal**: For automated workflows
- **Azure AD Integration**: For user access
- **Managed Identity**: For ADLS access

## Monitoring & Observability

### Metrics
- **Pipeline success rate**: Job completion status
- **Data freshness**: Time since last update
- **Query performance**: SQL warehouse response times
- **Data quality**: Expectation pass/fail rates

### Logging
- **Databricks Logs**: Cluster logs, job run history
- **Application Logs**: Custom ETL logging
- **Azure Monitor**: Centralized logging & alerts

## Future Enhancements

- [ ] Real-time streaming with Event Hubs → Delta Live Tables
- [ ] ML model training for sales forecasting (MLflow)
- [ ] CI/CD with Databricks Asset Bundles (DABs)
- [ ] Power BI embedded dashboards
- [ ] Incremental processing with watermark checkpoints
