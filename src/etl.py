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
    def check_dataset(self) -> Optional[Path]:
        """Check if we have the dataset if yes return the df if not return a message saying that we are going to donwload the info"""
        train_csv = self.data_dir / "train.csv"
        if train_csv.exists():
            logger.debug("Existing dataset found at %s; skipping download.", self.target_csv)
            return train_csv
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
        num_synthetic_rows: int = 10000,
        **kwargs,
    ) -> pd.DataFrame:
        """Combine Kaggle data with synthetic augmentation."""
        logger.info("Generating %s synthetic rows", num_synthetic_rows)
        synthetic_df = self.synthetic_generator.generate_synthetic_data(
                num_rows=num_synthetic_rows, **kwargs )

        if base_df is not None:
            logger.info("Combining %s Kaggle rows with %s synthetic rows", len(base_df), len(synthetic_df))
            return pd.concat([base_df, synthetic_df], ignore_index=True)

        return synthetic_df

    # Transformations

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw data: standardize columns, extract dates, calculate metrics."""
        rename_map: Dict[str, str] = {
            col: col.strip().lower().replace(" ", "_").replace("-", "_")
            for col in df.columns
        }
        transformed = df.rename(columns=rename_map).copy()

        if "order_date" in transformed.columns:
            transformed["order_date"] = pd.to_datetime(transformed["order_date"], errors="coerce")
            transformed["order_year"] = transformed["order_date"].dt.year
            transformed["order_month"] = transformed["order_date"].dt.month

        if "ship_date" in transformed.columns:
            transformed["ship_date"] = pd.to_datetime(transformed["ship_date"], errors="coerce")

        if "order_date" in transformed.columns and "ship_date" in transformed.columns:
            transformed["ship_latency_days"] = (transformed["ship_date"] - transformed["order_date"]).dt.days

        if "sales" in transformed.columns:
            transformed["sales"] = pd.to_numeric(transformed["sales"], errors="coerce")

        if "postal_code" in transformed.columns:
            transformed["postal_code"] = transformed["postal_code"].astype(str)

        self.df = transformed
        logger.debug("Transformed dataset with %s rows and %s columns", len(transformed), len(transformed.columns))
        return transformed

    # Reporting

    def build_summaries(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Create summary tables for analytics and reporting."""
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
        """Run lightweight Great Expectations checks on critical columns."""
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

    # Persistence
    def persist_outputs(
        self,
        df: pd.DataFrame,
        summaries: Dict[str, pd.DataFrame],
        quality_results: Dict[str, bool],
    ) -> None:
        """Persist the dataset, summary tables, and quality report under the data directory."""
        dataset_path = self.data_dir / "sales_enriched.parquet"
        df.to_parquet(dataset_path, index=False)
        logger.info("Saved enriched dataset to %s", dataset_path)

        for name, table in summaries.items():
            output_path = self.data_dir / f"{name}.csv"
            table.to_csv(output_path, index=False)
            logger.info("Saved %s summary to %s", name, output_path)

        quality_path = self.data_dir / "quality_report.json"
        quality_path.write_text(json.dumps(quality_results, indent=2))
        logger.info("Saved data quality report to %s", quality_path)

    def load_into_warehouse(
        self,
        df: pd.DataFrame,
        summaries: Dict[str, pd.DataFrame],
    ) -> None:
        """Load datasets into DuckDB for interactive analytics."""
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

    # Entry point
    def run(
        self,
        csv_path: Optional[str | Path] = None,
        num_synthetic_rows: int = 5_000,
        **kwargs,
    ) -> pd.DataFrame:
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
