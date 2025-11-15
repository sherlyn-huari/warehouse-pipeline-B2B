"""Sales Analyzer - ETL pipeline"""
from __future__ import annotations
import logging
import os
import warnings
import json
import polars as pl
import numpy as np
import shutil
from pathlib import Path
from typing import Dict, Optional
import duckdb
import pandas as pd
from datetime import date
import kagglehub
import great_expectations as ge
from dotenv import load_dotenv
from great_expectations.core.batch import Batch
from great_expectations.core.batch_spec import RuntimeDataBatchSpec
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.validator.validator import Validator
from synthetic_data_generator import SyntheticDataGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etl_pipeline.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

class SalesETL:

    def __init__(self, input_dir: Path | str = "data/input" , output_dir: str | Path = "data/output") -> None:
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir) 
        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok= True)
        self.synthetic_generator = SyntheticDataGenerator()
        self.df: Optional[pd.DataFrame] = None
        self.warehouse_path = self.output_dir / "sales_analytics.duckdb"
        self.dataset_slug = "rohitsahoo/sales-forecasting"
        self._setup_kaggle_credentials()
    
    def _setup_kaggle_credentials(self) -> None:
        """Load kaggle credentials from .env"""
        load_dotenv()
        missing: list[str] = []

        for var in ("KAGGLE_USERNAME", "KAGGLE_KEY"):
            value = os.getenv(var)
            if value:
                os.environ[var] = value
            else:
                missing.append(var)

        if missing:
            logger.warning(
                "Missing Kaggle credential(s): %s; dataset download may fail",
                ", ".join(missing),
            )
        else:
            logger.debug("Kaggle credentials loaded from environment")

    def download_kaggle_dataset(self,force: bool = False ) -> Optional[Path]:
        """Download dataset into the data directory"""
        try:
            logger.info("Downloading dataset %s ", self.dataset_slug)
            download_path = Path(kagglehub.dataset_download(self.dataset_slug))

            train_csv = download_path / "train.csv"
            if train_csv.exists():
                target = self.input_dir / "train.csv"
                shutil.copy2(train_csv, target)
                logger.info("Dataset copied to %s", target)
                return target
            else:
                logger.warning("Dataset download failed")
                return None
        except Exception as exc: 
            logger.error("Dataset download failed for %s", exc)
            return None

    def _fix_format_csv(self, csv_path: Path) -> None:
        """Check CSV issues before loading (extra commas)"""
        if not csv_path.exists():
            return None

        replacements = {
            "Flash Drive, 16GB": "Flash Drive 16GB",
            "Flash Drive, 32GB": "Flash Drive 32GB",
        }

        content = csv_path.read_text(encoding="utf-8")
        cleaned = content
        for needle, fix in replacements.items():
            cleaned = cleaned.replace(needle, fix)

        if cleaned != content:
            csv_path.write_text(cleaned, encoding="utf-8")
            logger.debug("Patched Kaggle dataset CSV to fix malformed product names.")

    def _clean_csv(self, csv_path: Path) -> Optional[pd.DataFrame]:
        """Clean a CSV by removing unnamed columns and any rows with null values"""
        if not csv_path.exists():
            logger.warning("Missing csv at %s", csv_path)
            return None

        try:
            df = pd.read_csv(csv_path, engine="python")
        except Exception as exc:
            logger.error("Failed to read csv %s: %s", csv_path, exc)
            return None

        df = df.loc[:, ~df.columns.str.startswith("Unnamed")]
        initial_rows = len(df)
        df = df.dropna(axis=0, how="any")
        rows_removed = initial_rows - len(df)
        logger.info("Removed %s rows containing null values", rows_removed)

        removed_columns = ["Row ID","Order ID","Order Date", "Ship Date"]
        df = df.drop(removed_columns, axis =1)
        logger.info("Removed columns: %s  ", removed_columns)
        return df
    
    def load_base_dataset(self, csv_path: Path) -> Optional[pd.DataFrame]:
        """Load and clean the base dataset from CSV."""
        if csv_path is None or not csv_path.exists():
            logger.warning("CSV path does not exist: %s", csv_path)
            return None

        self._fix_format_csv(csv_path)
        return self._clean_csv(csv_path)

    def build_dataset(
        self,
        base_df: Optional[pd.DataFrame],
        num_synthetic_rows: int = 10_000,
        start_date = date(2024,1,1),
        end_date = date(2025,1,1)
    ) -> pd.DataFrame:
        """Combine kaggle data where order_date it is in the year of 2017 or major 
          with synthetic data"""
        
        update_df  = self.synthetic_generator.update_data( kaggle_df= base_df,start_date = start_date, end_date=end_date)
        synthetic_df = self.synthetic_generator.generate_synthetic_data( 
                num_rows=num_synthetic_rows,start_date = start_date, end_date=end_date ) # Here  add the updated date function and add row_id coumn 
        
        updated_rows = update_df.height
        synthetic_rows = synthetic_df.height

        logger.info("Combining %s Kaggle rows with %s synthetic rows",
                    updated_rows, synthetic_rows)
        
        combined = pl.concat([update_df, synthetic_df], how="vertical")

        # add the Row ID
        combined = combined.with_columns( pl.arange(1, combined.height() + 1).alias("row_id"))
        return combined
    
    def add_quantity(self, df: pd.DataFrame,  min_amount: int = 1 , max_amount: int = 999, seed: int | None = None) -> pd.DataFrame:
        """Add a quantity column"""
        if seed is not None:
            np.random.seed(seed)
        df["quantity"] = np.random.randint(min_amount, max_amount +1, size =len(df))
        return df 

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw data: standardize columns, extract dates, calculate metrics"""
        rename_map: Dict[str, str] = {
            col: col.strip().lower().replace(" ", "_").replace("-", "_")
            for col in df.columns
        }
        transformed = df.rename(columns=rename_map).copy()

        if "order_date" in transformed.columns:
            transformed["order_date"] = pd.to_datetime(transformed["order_date"], errors="coerce", dayfirst=True)
            null_mask = transformed["order_date"].isna()
            if null_mask.any():
                logger.warning("Found %s rows with invalid order_date values", null_mask.sum())

            transformed["order_year"] = transformed["order_date"].dt.year
            transformed["order_month"] = transformed["order_date"].dt.month

        if "ship_date" in transformed.columns:
            transformed["ship_date"] = pd.to_datetime(transformed["ship_date"], errors="coerce", dayfirst=True)

        if "order_date" in transformed.columns and "ship_date" in transformed.columns:
            transformed["ship_latency_days"] = (transformed["ship_date"] - transformed["order_date"]).dt.days

        if "sales" in transformed.columns:
            transformed["sales"] = pd.to_numeric(transformed["sales"], errors="coerce")

        if "postal_code" in transformed.columns:
            transformed["postal_code"] = transformed["postal_code"].astype(str)

        self.df = transformed
        logger.debug("Transformed dataset with %s rows and %s columns", len(transformed), len(transformed.columns))
        return transformed

    def build_summaries(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Create summary tables for analytics"""
        summaries: Dict[str, pd.DataFrame] = {}

        if "order_year" in df.columns:
            summaries["yearly"] = (
                df.groupby("order_year", as_index=False)
                .agg(
                    rows=("order_year", "count"),
                    revenue=("sales", lambda x: round(x.sum(), 2)),
                    unique_customers=("customer_id", "nunique"),
                )
                .sort_values("order_year")
            )

        if {"order_year", "segment"}.issubset(df.columns):
            summaries["segment_yearly"] = (
                df.groupby(["order_year", "segment"], as_index=False)
                .agg(revenue=("sales", lambda x: round(x.sum(), 2)))
                .sort_values(["order_year", "revenue"], ascending=[True, False])
            )

        if "region" in df.columns:
            summaries["regional_revenue"] = (
                df.groupby("region", as_index=False)
                .agg(revenue=("sales", lambda x: round(x.sum(), 2)))
                .sort_values("revenue", ascending=False)
            )

        if "product_name" in df.columns:
            summaries["top_products"] = (
                df.groupby("product_name", as_index=False)
                .agg(
                    revenue=("sales", lambda x: round(x.sum(), 2)),
                    orders=("product_name", "count"),
                )
                .sort_values("revenue", ascending=False)
                .head(15)
            )

        logger.debug("Built %s summary tables", len(summaries))
        return summaries

    def run_quality_checks(self, df: pd.DataFrame) -> Dict[str, bool]:
        """Run Great Expectations checks on critical columns"""
        pandas_df = df
        context = ge.get_context()
        execution_engine = PandasExecutionEngine()
        execution_engine.data_context = context

        batch_data = execution_engine.get_batch_data( 
            RuntimeDataBatchSpec(batch_data=pandas_df)
        )
        batch = Batch(data=batch_data, data_context=context)
        suite = ExpectationSuite("sales_quality_checks")
        validator = Validator(
            execution_engine=execution_engine,
            data_context=context,
            expectation_suite=suite,
            batches=[batch],
        )
        expectations: Dict[str, bool] = {}

        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="`result_format` configured at the Validator-level will not be persisted",
                category=UserWarning,
            )
            warnings.filterwarnings(
                "ignore",
                message="`result_format` configured at the Expectation-level will not be persisted",
                category=UserWarning,
            )

            for column in ["order_id", "customer_id", "sales", "order_date"]:
                if column in pandas_df.columns:
                    result = validator.expect_column_values_to_not_be_null(column)
                    expectations[f"{column}_not_null"] = bool(result.success)

            if "sales" in pandas_df.columns:
                result = validator.expect_column_values_to_be_between(
                    "sales", min_value=0, strict_min=True
                )
                expectations["sales_positive"] = bool(result.success)

            if "order_year" in pandas_df.columns:
                result = validator.expect_column_values_to_be_between(
                    "order_year",
                    min_value=int(df["order_year"].min()),
                    max_value=int(df["order_year"].max()),
                )
                expectations["order_year_valid_range"] = bool(result.success)

        expectations["overall_success"] = all(expectations.values())
        logger.info("Data quality checks success=%s", expectations["overall_success"])
        return expectations

    def persist_outputs(
        self,
        df: pd.DataFrame,
        summaries: Dict[str, pd.DataFrame],
        quality_results: Dict[str, bool],
    ) -> None:
        """Persist the dataset, summary tables, and quality report under the data/output directory"""
        dataset_path = self.output_dir / "sales_enriched.parquet"
        df.to_parquet(dataset_path, index=False)
        logger.info("Saved enriched dataset to %s", dataset_path)

        for name, table in summaries.items():
            output_path = self.output_dir / f"{name}.csv"
            table.to_csv(output_path, index=False)
            logger.info("Saved %s summary to %s", name, output_path)

        quality_path = self.output_dir / "quality_report.json"
        quality_path.write_text(json.dumps(quality_results, indent=2))
        logger.info("Saved data quality report to %s", quality_path)

    def load_into_warehouse(
        self,
        df: pd.DataFrame,
        summaries: Dict[str, pd.DataFrame],
    ) -> None:
        """Load datasets into DuckDB"""
        with duckdb.connect(str(self.warehouse_path)) as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
            import pyarrow as pa
            arrow_table = pa.Table.from_pandas(df)
            conn.register("sales_dataset", arrow_table)
            conn.execute(
                "CREATE OR REPLACE TABLE analytics.sales AS SELECT * FROM sales_dataset"
            )
            conn.unregister("sales_dataset")

            for name, table in summaries.items():
                view_name = f"{name}_summary"
                arrow_summary = pa.Table.from_pandas(table)
                conn.register(view_name, arrow_summary)
                conn.execute(
                    f"CREATE OR REPLACE TABLE analytics.{view_name} AS SELECT * FROM {view_name}"
                )
                conn.unregister(view_name)

        logger.info("Persisted analytics tables into %s", self.warehouse_path)

    def run(
        self,
        num_synthetic_rows: int = 10_000,
        **kwargs,
    ) -> pd.DataFrame:
        """Execute the pipeline"""
        train_csv = self.input_dir / "train.csv"

        if train_csv.exists():
            csv = Path(train_csv)
        else:
            self._setup_kaggle_credentials()
            csv = self.download_kaggle_dataset()
        
        if csv is None:
            raise ValueError("Failed to obtain dataset from Kaggle")

        base_df = self.load_base_dataset(csv)
        combined_df = self.build_dataset(base_df, num_synthetic_rows=num_synthetic_rows, **kwargs)
        completed_df = self.add_quantity(combined_df,seed=43)
        transformed_df = self.transform(completed_df)
        summaries = self.build_summaries(transformed_df)
        quality_results = self.run_quality_checks(transformed_df)
        self.persist_outputs(transformed_df, summaries, quality_results)
        self.load_into_warehouse(transformed_df, summaries)

        return transformed_df

if __name__ == "__main__":
    etl = SalesETL()
    try:
        etl.run()
    except ValueError as exc:
        logger.error("Pipeline aborted: %s", exc)
