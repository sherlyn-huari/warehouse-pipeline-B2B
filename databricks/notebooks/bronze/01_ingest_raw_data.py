# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Raw Data Ingestion
# MAGIC
# MAGIC **Purpose**: Ingest raw sales data from Kaggle and synthetic generator into Bronze layer
# MAGIC
# MAGIC **Input**:
# MAGIC - Kaggle dataset (downloaded)
# MAGIC - Synthetic data generator
# MAGIC
# MAGIC **Output**:
# MAGIC - Bronze Delta table: `bronze.raw_sales_data`
# MAGIC - Location: `/mnt/adls/bronze/raw_sales_data/`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# DBTITLE 1,Import Libraries
import pandas as pd
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Configuration Parameters
# Storage paths (update these for your Azure environment)
BRONZE_PATH = "/mnt/adls/bronze/raw_sales_data/"
CATALOG_NAME = "sales_analytics"
SCHEMA_NAME = "bronze"
TABLE_NAME = "raw_sales_data"

# Data generation settings
SYNTHETIC_ROWS = 5000
RANDOM_SEED = 42

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Download Kaggle Dataset
# MAGIC
# MAGIC In production, this would download from Kaggle API to ADLS.
# MAGIC For demo purposes, we assume the file is already in DBFS or mounted storage.

# COMMAND ----------

# DBTITLE 1,Download from Kaggle (Simulated)
# In production Azure Databricks:
# 1. Use Kaggle API with credentials in Azure Key Vault
# 2. Download to ADLS Gen2 mounted storage
# 3. Store in bronze layer without transformations

# Example code structure (requires Kaggle setup):
# import kagglehub
# dataset_path = kagglehub.dataset_download("rohitsahoo/sales-forecasting")
# dbutils.fs.cp(f"file://{dataset_path}", BRONZE_PATH)

print("✓ Kaggle dataset download simulated")
print(f"  Location: {BRONZE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generate Synthetic Data
# MAGIC
# MAGIC Generate realistic synthetic sales records to augment the dataset.

# COMMAND ----------

# DBTITLE 1,Synthetic Data Generator
from faker import Faker
import random

class SyntheticDataGenerator:
    """Generates realistic synthetic sales data"""

    def __init__(self, seed=42):
        Faker.seed(seed)
        random.seed(seed)
        self.fake = Faker('en_US')

        # Configuration
        self.cities = [
            ("New York", "NY", "10001", "East"),
            ("Los Angeles", "CA", "90001", "West"),
            ("Chicago", "IL", "60601", "Central"),
            ("Houston", "TX", "77001", "Central"),
            ("Phoenix", "AZ", "85001", "West"),
            ("Philadelphia", "PA", "19101", "East"),
            ("San Antonio", "TX", "78201", "Central"),
            ("San Diego", "CA", "92101", "West"),
            ("Dallas", "TX", "75201", "Central"),
            ("San Jose", "CA", "95101", "West"),
        ]

        self.categories = {
            "Furniture": ["Bookcases", "Chairs", "Furnishings", "Tables"],
            "Office Supplies": ["Appliances", "Art", "Binders", "Envelopes", "Fasteners", "Labels", "Paper", "Storage", "Supplies"],
            "Technology": ["Accessories", "Copiers", "Machines", "Phones"]
        }

        self.segments = ["Consumer", "Corporate", "Home Office"]
        self.ship_modes = ["Standard Class", "Second Class", "First Class", "Same Day"]

        self.product_names = [
            "Office Chair", "Desk Lamp", "Wireless Mouse", "USB Cable",
            "Paper Box", "Stapler", "Printer", "Monitor Stand"
        ]

    def generate_record(self, row_id):
        """Generate a single synthetic sales record"""
        city, state, postal_code, region = random.choice(self.cities)
        category = random.choice(list(self.categories.keys()))
        sub_category = random.choice(self.categories[category])

        # Generate dates
        order_date = self.fake.date_between(start_date='-3y', end_date='today')
        ship_date = self.fake.date_between(start_date=order_date, end_date='+10d')

        return {
            "Row ID": row_id,
            "Order ID": f"SYN-{self.fake.bothify(text='??-####-####')}",
            "Order Date": order_date.strftime("%Y-%m-%d"),
            "Ship Date": ship_date.strftime("%Y-%m-%d"),
            "Ship Mode": random.choice(self.ship_modes),
            "Customer ID": f"CUS-{self.fake.bothify(text='####-#####')}",
            "Customer Name": self.fake.name(),
            "Segment": random.choice(self.segments),
            "Country": "United States",
            "City": city,
            "State": state,
            "Postal Code": postal_code,
            "Region": region,
            "Product ID": f"PROD-{self.fake.bothify(text='###-####')}",
            "Category": category,
            "Sub-Category": sub_category,
            "Product Name": random.choice(self.product_names),
            "Sales": round(random.uniform(10.0, 5000.0), 2),
        }

    def generate_dataset(self, num_records):
        """Generate multiple synthetic records"""
        return [self.generate_record(i + 10000) for i in range(num_records)]

# COMMAND ----------

# DBTITLE 1,Generate Synthetic Data
generator = SyntheticDataGenerator(seed=RANDOM_SEED)
synthetic_data = generator.generate_dataset(SYNTHETIC_ROWS)

# Convert to Spark DataFrame
synthetic_df = spark.createDataFrame(pd.DataFrame(synthetic_data))

print(f"✓ Generated {SYNTHETIC_ROWS} synthetic records")
print(f"  Schema: {len(synthetic_df.columns)} columns")
display(synthetic_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Load Real Kaggle Data (Simulated)
# MAGIC
# MAGIC In production, read from ADLS mounted storage.

# COMMAND ----------

# DBTITLE 1,Load Kaggle Dataset
# In production, this would read from ADLS:
# kaggle_df = spark.read.csv(f"{BRONZE_PATH}/train.csv", header=True, inferSchema=True)

# For demo purposes, create a sample schema
print("✓ Kaggle dataset load simulated")
print("  In production: Read from /mnt/adls/bronze/kaggle_raw/train.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write to Bronze Delta Table
# MAGIC
# MAGIC Store raw data in Delta Lake format with ACID guarantees.

# COMMAND ----------

# DBTITLE 1,Write to Delta Lake (Bronze Layer)
# Write to Delta table
synthetic_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("Order Date") \
    .save(BRONZE_PATH)

# Create catalog table (Unity Catalog)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}
    USING DELTA
    LOCATION '{BRONZE_PATH}'
""")

print(f"✓ Data written to Bronze layer")
print(f"  Path: {BRONZE_PATH}")
print(f"  Table: {CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}")
print(f"  Format: Delta Lake")
print(f"  Partitioning: Order Date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Data Quality

# COMMAND ----------

# DBTITLE 1,Basic Data Quality Checks
bronze_df = spark.read.format("delta").load(BRONZE_PATH)

print("=== Bronze Layer Statistics ===")
print(f"Total Records: {bronze_df.count():,}")
print(f"Total Columns: {len(bronze_df.columns)}")
print(f"\nSchema:")
bronze_df.printSchema()

print(f"\nSample Data:")
display(bronze_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Data Completeness Check
from pyspark.sql.functions import col, count, when

completeness_check = bronze_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in bronze_df.columns
])

print("=== Null Value Counts ===")
display(completeness_check)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ✅ **Bronze Layer Ingestion Complete**
# MAGIC
# MAGIC - Raw data stored in Delta Lake format
# MAGIC - ACID transactions enabled
# MAGIC - Partitioned by Order Date for efficient queries
# MAGIC - Ready for Silver layer transformation
# MAGIC
# MAGIC **Next Step**: Run `02_silver_transformation.py` to clean and enrich data
