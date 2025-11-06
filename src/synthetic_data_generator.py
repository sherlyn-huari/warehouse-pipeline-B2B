import logging
import random
from datetime import date, datetime, timedelta
from typing import Optional, Sequence

import numpy as np
import polars as pl
from faker import Faker

logger = logging.getLogger(__name__)


class SyntheticDataGenerator:
    """Generate realistic synthetic sales data."""

    SHIP_MODES = ["First Class", "Same Day", "Second Class", "Standard Class"]
    SEGMENTS = ["Consumer", "Corporate", "Home Office"]
    US_CITIES = {
        "New York": {"state": "New York", "zip_prefix": "100", "region": "East"},
        "Los Angeles": {"state": "California", "zip_prefix": "900", "region": "West"},
        "Chicago": {"state": "Illinois", "zip_prefix": "606", "region": "Central"},
        "Houston": {"state": "Texas", "zip_prefix": "770", "region": "Central"},
        "Phoenix": {"state": "Arizona", "zip_prefix": "850", "region": "West"},
        "Philadelphia": {"state": "Pennsylvania", "zip_prefix": "191", "region": "East"},
        "San Antonio": {"state": "Texas", "zip_prefix": "782", "region": "Central"},
        "San Diego": {"state": "California", "zip_prefix": "921", "region": "West"},
        "Dallas": {"state": "Texas", "zip_prefix": "752", "region": "Central"},
        "San Jose": {"state": "California", "zip_prefix": "951", "region": "West"},
        "Austin": {"state": "Texas", "zip_prefix": "787", "region": "Central"},
        "Jacksonville": {"state": "Florida", "zip_prefix": "322", "region": "South"},
        "Fort Worth": {"state": "Texas", "zip_prefix": "761", "region": "Central"},
        "Columbus": {"state": "Ohio", "zip_prefix": "432", "region": "East"},
        "Charlotte": {"state": "North Carolina", "zip_prefix": "282", "region": "South"},
        "Indianapolis": {"state": "Indiana", "zip_prefix": "462", "region": "Central"},
        "Seattle": {"state": "Washington", "zip_prefix": "981", "region": "West"},
        "Denver": {"state": "Colorado", "zip_prefix": "802", "region": "West"},
        "Boston": {"state": "Massachusetts", "zip_prefix": "021", "region": "East"},
        "Nashville": {"state": "Tennessee", "zip_prefix": "372", "region": "South"},
        "Detroit": {"state": "Michigan", "zip_prefix": "482", "region": "Central"},
        "Portland": {"state": "Oregon", "zip_prefix": "972", "region": "West"},
        "Las Vegas": {"state": "Nevada", "zip_prefix": "891", "region": "West"},
        "Miami": {"state": "Florida", "zip_prefix": "331", "region": "South"},
        "Atlanta": {"state": "Georgia", "zip_prefix": "303", "region": "South"},
    }

    CATEGORIES = {
        "Furniture": {"Bookcases", "Chairs", "Furnishings", "Tables"},
        "Office Supplies": {
            "Appliances",
            "Art",
            "Binders",
            "Envelopes",
            "Fasteners",
            "Labels",
            "Paper",
            "Storage",
            "Supplies",
        },
        "Technology": {"Accessories", "Copiers", "Machines", "Phones"},
    }

    def __init__(self, seed: int = 42, locale: str = "en_US") -> None:
        """Initialize the synthetic data generator."""

        self.fake = Faker(locale)
        Faker.seed(seed)
        random.seed(seed)
        np.random.seed(seed)
        logger.info("Initialized SyntheticDataGenerator with seed=%s", seed)

    def generate_customer_name(self) -> str:
        """Generate a customer name for the United States locale."""
        return self.fake.name()

    def generate_customer_id(self, customer_name: str) -> str:
        """Generate a customer identifier (e.g. CG-12456)."""
        parts = customer_name.split()
        if len(parts) < 2:
            initials = (parts[0][0] if parts else "X") + "X"
        else:
            initials = parts[0][0] + parts[1][0]
        number = random.randint(10000, 99999)
        return f"{initials.upper()}-{number}"

    def generate_order_dates(
        self,
        start_date: date,
        end_date: date,
        min_ship_days: int = 1,
        max_ship_days: int = 14,
    ) -> tuple[date, date]:
        """Generate order and ship dates (ship date after order date)."""
        order_date = self.fake.date_between(start_date=start_date, end_date=end_date)
        ship_date = self.fake.date_between(
            start_date=order_date + timedelta(days=min_ship_days),
            end_date=order_date + timedelta(days=max_ship_days),
        )
        return order_date, ship_date

    def generate_location_data(self) -> dict[str, str]:
        """Generate city, state, postal code and region."""
        city = random.choice(list(self.US_CITIES))
        city_info = self.US_CITIES[city]
        postal_code = f"{city_info['zip_prefix']}{random.randint(10, 99)}"
        return {
            "City": city,
            "State": city_info["state"],
            "Postal Code": postal_code,
            "Region": city_info["region"],
        }

    def generate_categories(self) -> dict[str, str]:
        """Generate category and sub-category by random choice."""
        category = random.choice(list(self.CATEGORIES))
        sub_category = random.choice(list(self.CATEGORIES[category]))
        return {"Category": category, "Sub-Category": sub_category}

    def generate_order_id(self, order_date: datetime) -> str:
        """Generate an order identifier (e.g. US-2016-118983)."""
        return f"US-{order_date.year}-{random.randint(100000, 999999)}"

    def generate_sales_amount(self, min_amount: int = 20, max_amount: int = 2000) -> float:
        """Generate a sales amount within the provided range."""
        return float(random.randint(min_amount, max_amount))

    def generate_product_id(self, category: str, subcategory: str) -> str:
        """Generate a product identifier based on category and sub-category."""
        cat_abbr = category[:3].upper()
        sub_abbr = subcategory[:2].upper()
        return f"{cat_abbr}-{sub_abbr}-100{random.randint(10000, 99999)}"

    def _fallback_products(self) -> Sequence[str]:
        """Fallback list of synthetic product names."""
        products = []
        for category, subcategories in self.CATEGORIES.items():
            for sub in subcategories:
                products.append(f"{category} - {sub}")
        return products

    def generate_synthetic_data(
        self,
        original_df: Optional[pl.DataFrame] = None,
        num_rows: int = 10_000,
        start_date: date = date(2015, 1, 1),
        end_date: date = date(2018, 12, 31),
    ) -> pl.DataFrame:
        """Generate synthetic sales records."""

        logger.info("Generating %s synthetic rows...", num_rows)

        product_names: Sequence[str] = []
        start_row_id = 1

        if original_df is not None and len(original_df) > 0:
            if "Product Name" in original_df.columns:
                try:
                    product_names = (
                        original_df.select("Product Name")
                        .unique()
                        ["Product Name"]
                        .to_list()
                    )
                except Exception as exc:  # pragma: no cover - defensive
                    logger.warning("Could not extract product names: %s", exc)
            if "Row ID" in original_df.columns:
                try:
                    start_row_id = int(
                        original_df.select(pl.col("Row ID").max()).item()
                    ) + 1
                except Exception as exc:  # pragma: no cover - defensive
                    logger.warning("Could not determine starting Row ID: %s", exc)

        if not product_names:
            product_names = self._fallback_products()

        synthetic_rows = []

        for i in range(num_rows):
            customer_name = self.generate_customer_name()
            customer_id = self.generate_customer_id(customer_name)
            order_date, ship_date = self.generate_order_dates(
                start_date=start_date, end_date=end_date
            )
            order_id = self.generate_order_id(order_date)
            location = self.generate_location_data()
            categories = self.generate_categories()
            product_id = self.generate_product_id(
                categories["Category"], categories["Sub-Category"]
            )
            product_name = random.choice(product_names)
            sales = self.generate_sales_amount(min_amount=10, max_amount=2900)

            row = {
                "Row ID": start_row_id + i,
                "Order ID": order_id,
                "Order Date": order_date,
                "Ship Date": ship_date,
                "Ship Mode": random.choice(self.SHIP_MODES),
                "Customer ID": customer_id,
                "Customer Name": customer_name,
                "Segment": random.choice(self.SEGMENTS),
                "Country": "United States",
                "City": location["City"],
                "State": location["State"],
                "Postal Code": location["Postal Code"],
                "Region": location["Region"],
                "Category": categories["Category"],
                "Sub-Category": categories["Sub-Category"],
                "Product ID": product_id,
                "Product Name": product_name,
                "Sales": sales,
            }
            synthetic_rows.append(row)

        synthetic_df = pl.DataFrame(synthetic_rows)
        logger.info("Successfully generated %s synthetic rows", len(synthetic_df))
        return synthetic_df

    def augment_dataframe(
        self,
        original_df: pl.DataFrame,
        num_synthetic_rows: int = 10_000,
        **kwargs,
    ) -> pl.DataFrame:
        """Augment an existing sales dataframe with synthetic rows."""

        synthetic_df = self.generate_synthetic_data(
            original_df, num_rows=num_synthetic_rows, **kwargs
        )
        if original_df is not None and len(original_df) > 0:
            schema = original_df.schema
            aligned_columns = []
            for column_name, dtype in schema.items():
                if column_name in synthetic_df.columns:
                    aligned_columns.append(
                        pl.col(column_name).cast(dtype, strict=False).alias(column_name)
                    )
                else:
                    aligned_columns.append(
                        pl.lit(None).cast(dtype).alias(column_name)
                    )
            synthetic_df = synthetic_df.select(aligned_columns)

        combined_df = pl.concat([original_df, synthetic_df])
        logger.info(
            "Augmented data: %s original + %s synthetic = %s total rows",
            len(original_df),
            len(synthetic_df),
            len(combined_df),
        )
        return combined_df
