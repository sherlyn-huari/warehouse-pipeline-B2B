from datetime import date
import unittest

import pandas as pd

from src.etl import SalesETL


class TestSalesETL(unittest.TestCase):
    def setUp(self) -> None:
        self.etl = SalesETL()

    def test_transform_adds_expected_metrics(self) -> None:
        raw = pd.DataFrame(
            {
                "order_date": ["2024-01-10", "2024-01-11"],
                "price": [100.0, 50.0],
                "quantity": [2, 3],
                "discount": [0.1, 0.0],
            }
        )

        transformed = self.etl.transform(raw)

        self.assertEqual(transformed["order_year"].tolist(), [2024, 2024])
        self.assertEqual(transformed["order_month"].tolist(), [1, 1])
        self.assertEqual(transformed["revenue"].tolist(), [180.0, 150.0])
        self.assertEqual(transformed["discount_amount"].tolist(), [20.0, 0.0])
        self.assertEqual(transformed["gross_sales"].tolist(), [200.0, 150.0])

    def test_build_summaries_returns_expected_tables(self) -> None:
        df = pd.DataFrame(
            {
                "order_year": [2024, 2024, 2025],
                "customer_id": ["c1", "c2", "c1"],
                "region": ["East", "East", "West"],
                "category": ["Tech", "Tech", "Office"],
                "product_name": ["P1", "P2", "P3"],
                "revenue": [100.0, 200.0, 150.0],
            }
        )

        summaries = self.etl.build_summaries(df)

        self.assertEqual(
            set(summaries.keys()), {"yearly", "regional_revenue", "top_products"}
        )
        self.assertEqual(len(summaries["yearly"]), 2)
        self.assertEqual(summaries["regional_revenue"]["total_revenue"].iloc[0], 300.0)

    def test_quality_checks_track_discount_and_overall_failure(self) -> None:
        df = pd.DataFrame(
            {
                "order_id": ["o1", "o2"],
                "customer_id": ["c1", "c2"],
                "price": [10.0, 20.0],
                "order_date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                "discount": [0.2, 0.9],
                "quantity": [1, 2],
                "order_year": [2024, 2024],
            }
        )

        results = self.etl.run_quality_checks(df)

        self.assertFalse(results["discount_in_range"])
        self.assertFalse(results["overall_success"])

    def test_run_rejects_invalid_inputs(self) -> None:
        with self.assertRaisesRegex(ValueError, "num_synthetic_rows must be >= 1"):
            self.etl.run(num_synthetic_rows=0)

        with self.assertRaisesRegex(ValueError, "start_date must be <= end_date"):
            self.etl.run(start_date=date(2024, 2, 1), end_date=date(2024, 1, 1))
