"""Sales Analyzer - ETL pipeline"""
from __future__ import annotations
import argparse
import logging
import warnings
import pyarrow as pa
import json
from pathlib import Path
from typing import Dict, Optional
import duckdb
import pandas as pd
from datetime import date
import great_expectations as ge
from great_expectations.core.batch import Batch
from great_expectations.core.batch_spec import RuntimeDataBatchSpec
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.validator.validator import Validator

try:
    from synthetic_data_generator import SyntheticDataGenerator
    from build_dimensional_model import DimensionalModelBuilder
except ImportError:
    from src.synthetic_data_generator import SyntheticDataGenerator
    from src.build_dimensional_model import DimensionalModelBuilder

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etl_pipeline.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def parse_date(value: str) -> date:
    """Parse ISO date string."""
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'. Use YYYY-MM-DD."
        ) from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run sales ETL pipeline")
    parser.add_argument(
        "--rows",
        type=int,
        default=100_000,
        help="Number of synthetic rows to generate (default: 100000).",
    )
    parser.add_argument(
        "--start-date",
        type=parse_date,
        default=date(2024, 1, 1),
        help="Start date in YYYY-MM-DD format (default: 2024-01-01).",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        default=date(2024, 12, 31),
        help="End date in YYYY-MM-DD format (default: 2024-12-31).",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Always regenerate synthetic data.",
    )
    parser.add_argument(
        "--reuse",
        action="store_true",
        help="Reuse existing parquet dataset if present.",
    )
    parser.add_argument(
        "--build-star-schema",
        action="store_true",
        help="Build dimensional model in DuckDB after load.",
    )
    parser.add_argument(
        "--skip-star-schema",
        action="store_true",
        help="Skip dimensional model build and only load analytics.sales.",
    )

    args = parser.parse_args()
    if args.rebuild and args.reuse:
        parser.error("Use only one of --rebuild or --reuse.")
    if args.build_star_schema and args.skip_star_schema:
        parser.error("Use only one of --build-star-schema or --skip-star-schema.")
    return args


class SalesETL:
    def __init__(
        self,
        input_dir: str | Path = "data/input",
        output_dir: str | Path = "data/output"
    ) -> None:
        
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.warehouse_path = self.output_dir / "warehouse" / "sales_analytics.duckdb"

        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.warehouse_path.parent.mkdir(parents=True, exist_ok=True)

        self.synthetic_generator = SyntheticDataGenerator(input_dir=self.input_dir)
        self.df: Optional[pd.DataFrame] = None

    def build_dataset(
        self,
        num_synthetic_rows: int,
        start_date: date,
        end_date: date,
        rebuild: bool,
    ) -> pd.DataFrame:
        """Create a dataset of B2B retail sales for the company 
        Args:
            rebuild: If True, always generate new data. If False, load from file if exists.
        """

        dataset_file = self.output_dir / "synthetic_data.parquet"

        if not rebuild and dataset_file.exists():
            logger.info("Loading existing dataset from %s", dataset_file)
            self.df = pd.read_parquet(dataset_file)
            logger.info("Loaded %s rows and %s columns", len(self.df), len(self.df.columns))
            return self.df

        logger.info("Generating new synthetic data (rebuild=%s)", rebuild)
        synthetic_df = self.synthetic_generator.generate_synthetic_data(
                num_rows=num_synthetic_rows,start_date = start_date, end_date=end_date )

        logger.info("Creating a total of  %s  synthetic rows and %s columns ",
                    len(synthetic_df), len(synthetic_df.columns))

        synthetic_df.to_parquet(dataset_file)
        logger.info("Saved dataset to %s", dataset_file)

        self.df = synthetic_df
        return synthetic_df

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate data
        Handles nulls in critical columns 
        """
        numeric_cols = ["price", "quantity", "discount"]
        for col in numeric_cols:
            if col in df.columns:
                null_count = df[col].isna().sum()
                if null_count > 0:
                    logger.warning("Found %s nulls in '%s', filling with 0", null_count, col)
                    df[col] = df[col].fillna(0)


        if "discount" in df.columns:
            df["discount"] = df["discount"].clip(0, 1)

        total_nulls = df.isna().sum().sum()
        if total_nulls > 0:
            logger.info("Remaining nulls in dataset: %s", total_nulls)

        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Get calculate metrics"""

        if "order_date" in df:
            df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
            df["order_year"] = df["order_date"].dt.year
            df["order_month"] = df["order_date"].dt.month

        if "price" in df.columns and "quantity" in df.columns and "discount" in df.columns:
            df["revenue"] = (df["price"] * df["quantity"] * (1 - df["discount"])).round(2)
            df["discount_amount"] = (df["price"] * df["quantity"] * df["discount"]).round(2)
            df["gross_sales"] = (df["price"] * df["quantity"]).round(2)

        self.df = df
        logger.debug("df dataset with %s rows and %s columns", len(df), len(df.columns))
        return df

    def build_summaries(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Create summary tables for analytics"""
        summaries: Dict[str, pd.DataFrame] = {}

        if "order_year" in df.columns and "revenue" in df.columns:
            summaries["yearly"] = (
                df.groupby("order_year", as_index=False)
                .agg(
                    total_orders=("order_year", "count"),
                    total_revenue=("revenue", lambda x: round(x.sum(), 2)),
                    unique_customers=("customer_id", "nunique"),
                )
                .sort_values("order_year")
            )

        if {"region", "revenue"}.issubset(df.columns):
            summaries["regional_revenue"] = (
                df.groupby("region", as_index=False)
                .agg(
                    total_revenue=("revenue", lambda x: round(x.sum(), 2)),
                    total_orders=("region", "count"),
                    unique_customers =("customer_id", "nunique"),
                )
                .sort_values("total_revenue", ascending=False)
            )

        if {"category", "product_name", "revenue"}.issubset(df.columns):
            summaries["top_products"] = (
                df.groupby(["category", "product_name"], as_index=False)
                .agg(
                    total_revenue=("revenue", lambda x: round(x.sum(), 2))
                )
                .sort_values(["category","total_revenue"], ascending=[True, False])
                .groupby("category")
                .head(3)
                .reset_index(drop= True)
            )

        logger.info("Built %s summary tables", len(summaries))
        return summaries

    def run_quality_checks(self, df: pd.DataFrame) -> Dict[str, bool]:
        """Run Great Expectations checks on critical columns"""
        context = ge.get_context()
        execution_engine = PandasExecutionEngine()
        execution_engine.data_context = context

        batch_data = execution_engine.get_batch_data(
            RuntimeDataBatchSpec(batch_data=df)
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

            for column in ["order_id", "customer_id", "price", "order_date"]:
                if column in df.columns:
                    result = validator.expect_column_values_to_not_be_null(column)
                    expectations[f"{column}_not_null"] = bool(result.success)

            if "price" in df.columns:
                result = validator.expect_column_values_to_be_between(
                    "price", min_value=0, strict_min=True)
                expectations["sales_positive"] = bool(result.success)

            if "discount" in df.columns:
                result = validator.expect_column_values_to_be_between(
                    "discount", min_value = 0, max_value = 0.5)
                expectations["discount_in_range"] = bool(result.success)

            if "quantity" in df.columns:
                result = validator.expect_column_values_to_be_between(
                    "quantity", min_value = 1, max_value = 100)
                expectations["quantity_in_range"] = bool(result.success)

            if "order_year" in df.columns:
                result = validator.expect_column_values_to_be_between( "order_year",
                        min_value=int(df["order_year"].min()),
                        max_value=int(df["order_year"].max()))
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
        """ Put inside the data/output directory the following
         - Full dataset in parquet format (this file will not be in the github)
         - Sample of te dataset: First 50 rows  
         - CSV files: Summary tables
         - Quality report of Great Expectations """
        
        df_path = self.output_dir / "synthetic_data.parquet"
        df.to_parquet(df_path)
        logger.info("Saved full dataset to %s", df_path)
        
        sample_df = df.head(50)
        sample_path = self.output_dir / "sample_data.csv"
        sample_df.to_csv(sample_path, index=False)
        logger.info("Saved sample of the dataset to %s", sample_path)

        for name, table in summaries.items():
            output_path = self.output_dir / f"{name}.csv"
            table.to_csv(output_path, index=False)
            logger.info("Saved %s summary to %s", name, output_path)

        quality_path = self.output_dir / "quality_report.json"
        quality_path.write_text(json.dumps(quality_results, indent=2))
        logger.info("Saved data quality report to %s", quality_path)

    def load_into_warehouse(
        self,
        transformed_df: pd.DataFrame,
        build_star_schema: bool = True ) -> None:
        """Load transformed data and dimensional model (start model) into DuckDB"""

        with duckdb.connect(str(self.warehouse_path)) as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
            arrow_table = pa.Table.from_pandas(transformed_df)
            conn.register("sales_data", arrow_table)
            conn.execute(
                "CREATE OR REPLACE TABLE analytics.sales AS SELECT * FROM sales_data"
            )
            conn.unregister("sales_data")

        logger.info("Loaded analytics.sales into %s", self.warehouse_path)

        if build_star_schema:
            try:
                with DimensionalModelBuilder(warehouse_path=self.warehouse_path) as builder:
                    builder.build_all()
                logger.info("Dimensional model (star schema) built successfully")
            except Exception as exc:
                logger.error("Failed to build dimensional model: %s", exc)
                logger.warning("Continuing without dimensional model")

    def run(
        self,
        num_synthetic_rows: int = 100_000,
        start_date: date = date(2024, 1, 1),
        end_date: date = date(2024, 12, 31),
        rebuild: bool = True,
        build_star_schema: bool = True,
    ) -> pd.DataFrame:
        """Execute the pipeline"""
        if num_synthetic_rows < 1:
            raise ValueError("num_synthetic_rows must be >= 1")
        if start_date > end_date:
            raise ValueError("start_date must be <= end_date")

        combined_df = self.build_dataset(
            num_synthetic_rows=num_synthetic_rows,
            start_date=start_date,
            end_date=end_date,
            rebuild=rebuild,
        )
        cleaned_df = self.clean_data(combined_df)
        transformed_df = self.transform(cleaned_df)

        summaries = self.build_summaries(transformed_df)
        quality_results = self.run_quality_checks(transformed_df)
        self.persist_outputs(transformed_df, summaries, quality_results)
        self.load_into_warehouse(transformed_df, build_star_schema=build_star_schema)

        return transformed_df

if __name__ == "__main__":
    args = parse_args()
    rebuild = True
    if args.reuse:
        rebuild = False
    elif args.rebuild:
        rebuild = True

    build_star_schema = True
    if args.skip_star_schema:
        build_star_schema = False
    elif args.build_star_schema:
        build_star_schema = True

    etl = SalesETL()
    try:
        etl.run(
            num_synthetic_rows=args.rows,
            start_date=args.start_date,
            end_date=args.end_date,
            rebuild=rebuild,
            build_star_schema=build_star_schema,
        )
    except ValueError as exc:
        logger.error("Pipeline aborted: %s", exc)
