# Databricks Deployment

This folder contains notebooks and configurations for deploying the Smart Sales Analyzer on **Azure Databricks**.

## Folder Structure

```
databricks/
├── notebooks/          # Databricks notebooks (Python/SQL)
│   ├── bronze/        # Raw data ingestion
│   ├── silver/        # Cleaned & transformed data
│   └── gold/          # Business-ready aggregations
├── workflows/         # Databricks job definitions
└── config/            # Configuration files
```

## Azure Databricks Stack

This project demonstrates modern Data Engineering practices using:

- **Azure Data Lake Storage Gen2 (ADLS)** - Data lake storage
- **Databricks Workspace** - Collaborative notebooks & Spark clusters
- **Delta Lake** - ACID transactions & versioning
- **Databricks SQL Warehouse** - Analytics & BI queries
- **Databricks Workflows** - Job orchestration

## Medallion Architecture

The pipeline follows the medallion architecture pattern:

1. **Bronze Layer** - Raw data from sources (Kaggle + Synthetic)
2. **Silver Layer** - Cleaned, validated, enriched data
3. **Gold Layer** - Business aggregations & analytics tables

## Setup (Coming Soon)

Instructions for deploying to Azure Databricks will be added in future updates.
