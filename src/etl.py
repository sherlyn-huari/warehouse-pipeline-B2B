"""Smart Sales Analyzer - Local ETL pipeline"""

from __future__ import annotations
import logging
import os
import warnings
import json
import shutil
from pathlib import Path
from typing import Dict, Optional
import duckdb
import pandas as pd
import polars as pl
import kagglehub
import great_expectations as ge
from dotenv import load_dotenv
from great_expectations.core.batch import Batch
from great_expectations.core.batch_spec import RuntimeDataBatchSpec
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.validator.validator import Validator

from synthetic_data_generator import SyntheticDataGenerator

# Configure logging once at module import
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etl_pipeline.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class SalesETL:
    """Local ETL pipeline that enriches data and produces analytical summaries."""

    def __init__(self, data_dir: str | Path = "data") -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.synthetic_generator = SyntheticDataGenerator()
        self.df: Optional[pd.DataFrame] = None
        self.warehouse_path = self.data_dir / "sales_analytics.duckdb"
        self.dataset_slug = "rohitsahoo/sales-forecasting"
        self._setup_kaggle_credentials()
        logger.debug("SalesETL initialized with data directory: %s", self.data_dir.resolve())

    # Data sourcing
    def check_dataset(self) -> Optional[pd.DataFrame]:
        """Check if we have the dataset if yes return the df if not return a message saying that we are going to donwload the info"""
        if self.target_csv.exists():
            logger.debug("Existing dataset found at %s; skipping download.", self.target_csv)
            df = pd.read_csv(self.target_csv, engine= "python")
            return df
        logger.info("Dataset not found; proceeding download with Kaggle credentials")
        return None
    
    def _setup_kaggle_credentials(self) -> None:
        """Load Kaggle credentials from .env (if present)."""
        load_dotenv()
        username = os.getenv("KAGGLE_USERNAME")
        key = os.getenv("KAGGLE_KEY")
        if username and key:
            os.environ["KAGGLE_USERNAME"] = username
            os.environ["KAGGLE_KEY"] = key
            logger.debug("Kaggle credentials loaded from environment.")
        else:
            logger.warning("Kaggle credentials not found; dataset download may fail.")

    def download_kaggle_dataset(self,force: bool = False ) -> Optional[Path]:
        """Download dataset from Kaggle into the data directory."""
        try:
            logger.info("Downloading dataset %s ", self.dataset_slug)
            download_path = Path(kagglehub.dataset_download(self.dataset_slug))

            train_csv = download_path / "train.csv"
            if train_csv.exists():
                target = self.data_dir / "train.csv"
                shutil.copy2(train_csv, target)
                logger.info("Dataset copied to %s", target)
                return target
            else:
                logger.warning("train.csv not found in %s", download_path)
                return None
        except Exception as exc: 
            logger.error("Dataset download failed for %s", exc)
            return None

    def _fix_format_csv(self, csv_path: Path) -> None:
        """Patch known CSV issues (e.g., stray commas in product names) before loading."""
        if not csv_path.exists():
            return

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

        return df
    
    def build_dataset(
        self,
        base_df: Optional[pd.DataFrame],
        num_synthetic_rows: int = 5000,
        **kwargs,
    ) -> pd.DataFrame:
        """Combine base data with synthetic rows (or generate synthetic from scratch)."""
        if base_df is None or len(base_df) == 0:
            logger.info("Generating synthetic dataset with %s rows", num_synthetic_rows)
            return self.synthetic_generator.generate_synthetic_data(
                num_rows=num_synthetic_rows, **kwargs
            )

        logger.info(
            "Augmenting base dataset (%s rows) with %s synthetic rows",
            len(base_df),
            num_synthetic_rows,
        )
        return self.synthetic_generator.augment_dataframe(
            base_df, num_synthetic_rows=num_synthetic_rows, **kwargs
        )


    # Transformations

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Clean column names, enforce types, and derive analytical columns."""
        rename_map: Dict[str, str] = {
            col: col.strip().lower().replace(" ", "_").replace("-", "_")
            for col in df.columns
        }
        transformed = df.rename(rename_map)

        with_columns = []

        if "order_date" in transformed.columns:
            with_columns.append(pl.col("order_date").cast(pl.Date))
            with_columns.append(pl.col("order_date").dt.year().alias("order_year"))
            with_columns.append(pl.col("order_date").dt.month().alias("order_month"))

        if "ship_date" in transformed.columns:
            with_columns.append(pl.col("ship_date").cast(pl.Date))

        if "sales" in transformed.columns:
            with_columns.append(pl.col("sales").cast(pl.Float64))

        if "postal_code" in transformed.columns:
            with_columns.append(pl.col("postal_code").cast(pl.Utf8))

        if with_columns:
            transformed = transformed.with_columns(with_columns)

        self.df = transformed
        logger.debug("Transformed dataset with %s rows and %s columns", len(transformed), len(transformed.columns))
        return transformed

    # Reporting

    def build_summaries(self, df: pl.DataFrame) -> Dict[str, pl.DataFrame]:
        """Create summary tables for analytics and reporting."""
        summaries: Dict[str, pl.DataFrame] = {}

        if "order_year" in df.columns:
            summaries["yearly"] = (
                df.group_by("order_year")
                .agg(
                    pl.len().alias("rows"),
                    pl.col("sales").sum().round(2).alias("revenue"),
                    pl.col("customer_id").n_unique().alias("unique_customers"),
                )
                .sort("order_year")
            )

        if {"order_year", "segment"}.issubset(df.columns):
            summaries["segment_yearly"] = (
                df.group_by(["order_year", "segment"])
                .agg(pl.col("sales").sum().round(2).alias("revenue"))
                .sort(["order_year", "revenue"], descending=[False, True])
            )

        if "region" in df.columns:
            summaries["regional_revenue"] = (
                df.group_by("region")
                .agg(pl.col("sales").sum().round(2).alias("revenue"))
                .sort("revenue", descending=True)
            )

        if "product_name" in df.columns:
            summaries["top_products"] = (
                df.group_by("product_name")
                .agg(
                    pl.col("sales").sum().round(2).alias("revenue"),
                    pl.len().alias("orders"),
                )
                .sort("revenue", descending=True)
                .head(15)
            )

        logger.debug("Built %s summary tables", len(summaries))
        return summaries

    def run_quality_checks(self, df: pl.DataFrame) -> Dict[str, bool]:
        """Run lightweight Great Expectations checks on critical columns."""
        pandas_df = df.to_pandas()
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

    # Persistence
    def persist_outputs(
        self,
        df: pl.DataFrame,
        summaries: Dict[str, pl.DataFrame],
        quality_results: Dict[str, bool],
    ) -> None:
        """Persist the dataset, summary tables, and quality report under the data directory."""
        dataset_path = self.data_dir / "sales_enriched.parquet"
        df.write_parquet(dataset_path)
        logger.info("Saved enriched dataset to %s", dataset_path)

        for name, table in summaries.items():
            output_path = self.data_dir / f"{name}.csv"
            table.write_csv(output_path)
            logger.info("Saved %s summary to %s", name, output_path)

        quality_path = self.data_dir / "quality_report.json"
        quality_path.write_text(json.dumps(quality_results, indent=2))
        logger.info("Saved data quality report to %s", quality_path)

    def load_into_warehouse(
        self,
        df: pl.DataFrame,
        summaries: Dict[str, pl.DataFrame],
    ) -> None:
        """Load datasets into DuckDB for interactive analytics."""
        with duckdb.connect(str(self.warehouse_path)) as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
            conn.register("sales_dataset", df.to_arrow())
            conn.execute(
                "CREATE OR REPLACE TABLE analytics.sales AS SELECT * FROM sales_dataset"
            )
            conn.unregister("sales_dataset")

            for name, table in summaries.items():
                view_name = f"{name}_summary"
                conn.register(view_name, table.to_arrow())
                conn.execute(
                    f"CREATE OR REPLACE TABLE analytics.{view_name} AS SELECT * FROM {view_name}"
                )
                conn.unregister(view_name)

        logger.info("Persisted analytics tables into %s", self.warehouse_path)

    # Entry point
    def run(
        self,
        csv_path: Optional[str | Path] = None,
        num_synthetic_rows: int = 5_000,
        **kwargs,
    ) -> pl.DataFrame:
        """Execute the full pipeline."""
        if csv_path is None:
            csv_candidate = self.download_kaggle_dataset()
        else:
            csv_candidate = Path(csv_path)

        base_df = self.load_base_dataset(csv_candidate)
        combined_df = self.build_dataset(base_df, num_synthetic_rows=num_synthetic_rows, **kwargs)
        transformed_df = self.transform(combined_df)
        summaries = self.build_summaries(transformed_df)
        quality_results = self.run_quality_checks(transformed_df)
        self.persist_outputs(transformed_df, summaries, quality_results)
        self.load_into_warehouse(transformed_df, summaries)

        return transformed_df


if __name__ == "__main__":
    etl = SalesETL(data_dir="data")
    etl.run()
