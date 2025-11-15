"""
Build Dimensional Model (Star Schema) from Sales data
- Fact table: fact_sales
- Dimension tables: dim_customer, dim_product, dim_location, dim_date
"""
from __future__ import annotations
import logging
from pathlib import Path
import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class DimensionalModelBuilder:

    def __init__(self, warehouse_path: str | Path = "data/output/sales_analytics.duckdb"):
        self.warehouse_path = Path(warehouse_path)
        if not self.warehouse_path.exists():
            raise FileNotFoundError(f"Warehouse not found: {self.warehouse_path}")

        self.conn = duckdb.connect(str(self.warehouse_path))
        logger.info("Connected to warehouse: %s", self.warehouse_path)

    def create_dim_customer(self) -> None:
        """
        Create customer dimension table
        """
        logger.info("Creating dim_customer")

        sql = """
        DROP TABLE IF EXISTS analytics.dim_customer;

        CREATE TABLE analytics.dim_customer AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key,
            customer_id,                                              
            customer_name,
            segment,
            CURRENT_TIMESTAMP AS created_at,
            CURRENT_TIMESTAMP AS updated_at
        FROM (

            SELECT DISTINCT
                customer_id,
                customer_name,
                segment
            FROM analytics.sales
            WHERE customer_id IS NOT NULL
        ) unique_customers
        ORDER BY customer_key;

        ALTER TABLE analytics.dim_customer
        ADD CONSTRAINT pk_dim_customer PRIMARY KEY (customer_key);

        CREATE INDEX idx_dim_customer_id ON analytics.dim_customer(customer_id);
        """

        self.conn.execute(sql)
        count = self.conn.execute("SELECT COUNT(*) FROM analytics.dim_customer").fetchone()[0]
        logger.info(" Created dim_customer with %d unique customers", count)

    def create_dim_product(self) -> None:
        """
        Create product dimension table: 
        This table stores unique products by Category → Sub-Category → Product
        """
        logger.info("Creating dim_product")

        sql = """

        DROP TABLE IF EXISTS analytics.dim_product;

        CREATE TABLE analytics.dim_product AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY product_id) AS product_key,
            product_id,                                  
            product_name,
            category,
            sub_category,
            CURRENT_TIMESTAMP AS created_at,
            CURRENT_TIMESTAMP AS updated_at
        FROM (
            SELECT DISTINCT
                product_id,
                product_name,
                category,
                sub_category
            FROM analytics.sales
            WHERE product_id IS NOT NULL
        ) unique_products
        ORDER BY product_key;

        ALTER TABLE analytics.dim_product
        ADD CONSTRAINT pk_dim_product PRIMARY KEY (product_key);

        CREATE INDEX idx_dim_product_id ON analytics.dim_product(product_id);
        """

        self.conn.execute(sql)
        count = self.conn.execute("SELECT COUNT(*) FROM analytics.dim_product").fetchone()[0]
        logger.info("Created dim_product with %d unique products", count)


    def create_dim_location(self) -> None:
        """
        Create location dimension table: This table stores unique geographic locations
        """
        logger.info("Creating dim_location")

        sql = """

        DROP TABLE IF EXISTS analytics.dim_location;

        CREATE TABLE analytics.dim_location AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY country, region, state, city, postal_code) AS location_key,
            city,
            state,
            postal_code,
            region,
            country,
            CURRENT_TIMESTAMP AS created_at
        FROM (
            SELECT DISTINCT
                city,
                state,
                postal_code,
                region,
                country
            FROM analytics.sales
            WHERE city IS NOT NULL
        ) unique_locations
        ORDER BY location_key;

        ALTER TABLE analytics.dim_location
        ADD CONSTRAINT pk_dim_location PRIMARY KEY (location_key);


        CREATE INDEX idx_dim_location_region ON analytics.dim_location(region);
        CREATE INDEX idx_dim_location_state ON analytics.dim_location(state);
        """

        self.conn.execute(sql)

        count = self.conn.execute("SELECT COUNT(*) FROM analytics.dim_location").fetchone()[0]
        logger.info("Created dim_location with %d unique locations", count)


    def create_dim_date(self) -> None:
        """
        Create date dimension table generates  all dates 
        """
        logger.info("Creating dim_date")

        sql = """

        DROP TABLE IF EXISTS analytics.dim_date;

        CREATE TABLE analytics.dim_date AS
        WITH date_range AS (
            -- Get min and max dates from sales data
            SELECT
                MIN(DATE_TRUNC('day', order_date)) AS min_date,
                MAX(DATE_TRUNC('day', order_date)) AS max_date
            FROM analytics.sales
        ),
        all_dates AS (

            SELECT
                UNNEST(
                    GENERATE_SERIES(
                        (SELECT min_date FROM date_range),
                        (SELECT max_date FROM date_range),
                        INTERVAL '1 day'
                    )
                ) AS date_value
        )
        SELECT
            -- Surrogate key -> 20170101
            CAST(STRFTIME(date_value, '%Y%m%d') AS INTEGER) AS date_key,

            CAST(date_value AS DATE) AS date,

            -- Year attributes
            EXTRACT(YEAR FROM date_value) AS year,

            -- Month attributes
            EXTRACT(MONTH FROM date_value) AS month,
            STRFTIME(date_value, '%B') AS month_name,

            -- Week attributes
            EXTRACT(WEEK FROM date_value) AS week_of_year,

            -- Metadata
            CURRENT_TIMESTAMP AS created_at

        FROM all_dates
        ORDER BY date_key;

        ALTER TABLE analytics.dim_date
        ADD CONSTRAINT pk_dim_date PRIMARY KEY (date_key);

        CREATE INDEX idx_dim_date_date ON analytics.dim_date(date);
        """

        self.conn.execute(sql)

        count = self.conn.execute("SELECT COUNT(*) FROM analytics.dim_date").fetchone()[0]
        logger.info("Created dim_date with %d dates", count)

    def create_fact_sales(self) -> None:
        """
        Create fact table for sales 
        """
        logger.info("Creating fact_sales")

        sql = """

        DROP TABLE IF EXISTS analytics.fact_sales;

        CREATE TABLE analytics.fact_sales AS
        SELECT
            -- Surrogate key for this fact
            s.row_id AS sales_key,

            -- Business keys
            s.order_id,

            -- Foreign keys to dimension tables
            dc.customer_key,
            dp.product_key,
            dl.location_key,
            CAST(STRFTIME(s.order_date, '%Y%m%d') AS INTEGER) AS order_date_key,
            CAST(STRFTIME(s.ship_date, '%Y%m%d') AS INTEGER) AS ship_date_key,

            s.ship_mode,

            -- Measures
            s.sales AS sales_amount,
            s.quantity,
            s.ship_latency_days,

            -- Calculated measures
            s.sales / NULLIF(s.quantity, 0) AS unit_price,

            -- Metadata
            CURRENT_TIMESTAMP AS created_at

        FROM analytics.sales s

        -- Join to get customer dimension key
        LEFT JOIN analytics.dim_customer dc
            ON s.customer_id = dc.customer_id

        -- Join to get product dimension key
        LEFT JOIN analytics.dim_product dp
            ON s.product_id = dp.product_id

        -- Join to get location dimension key
        LEFT JOIN analytics.dim_location dl
            ON s.city = dl.city
            AND s.state = dl.state
            AND s.postal_code = dl.postal_code

        ORDER BY s.row_id;

        ALTER TABLE analytics.fact_sales
        ADD CONSTRAINT pk_fact_sales PRIMARY KEY (sales_key);

        ALTER TABLE analytics.fact_sales
        ADD CONSTRAINT fk_fact_customer
        FOREIGN KEY (customer_key) REFERENCES analytics.dim_customer(customer_key);

        ALTER TABLE analytics.fact_sales
        ADD CONSTRAINT fk_fact_product
        FOREIGN KEY (product_key) REFERENCES analytics.dim_product(product_key);

        ALTER TABLE analytics.fact_sales
        ADD CONSTRAINT fk_fact_location
        FOREIGN KEY (location_key) REFERENCES analytics.dim_location(location_key);

        ALTER TABLE analytics.fact_sales
        ADD CONSTRAINT fk_fact_order_date
        FOREIGN KEY (order_date_key) REFERENCES analytics.dim_date(date_key);

        ALTER TABLE analytics.fact_sales
        ADD CONSTRAINT fk_fact_ship_date
        FOREIGN KEY (ship_date_key) REFERENCES analytics.dim_date(date_key);

        CREATE INDEX idx_fact_customer ON analytics.fact_sales(customer_key);
        CREATE INDEX idx_fact_product ON analytics.fact_sales(product_key);
        CREATE INDEX idx_fact_location ON analytics.fact_sales(location_key);
        CREATE INDEX idx_fact_order_date ON analytics.fact_sales(order_date_key);
        CREATE INDEX idx_fact_ship_date ON analytics.fact_sales(ship_date_key);
        """

        self.conn.execute(sql)
        count = self.conn.execute("SELECT COUNT(*) FROM analytics.fact_sales").fetchone()[0]
        logger.info("Created fact_sales with %d transactions", count)

    def validate_model(self) -> None:
        """
        Validate the dimensional model
        """

        logger.info("VALIDATING DIMENSIONAL MODEL")

        # Check if table exists
        tables = self.conn.execute("""
            SELECT table_name, estimated_size
            FROM duckdb_tables()
            WHERE schema_name = 'analytics'
            AND table_name IN ('dim_customer', 'dim_product', 'dim_location', 'dim_date', 'fact_sales')
        """).df()

        print("\n Tables Created:")
        print(tables.to_string(index=False))

        # Check row counts
        counts = self.conn.execute("""
            SELECT
                (SELECT COUNT(*) FROM analytics.fact_sales) as fact_sales,
                (SELECT COUNT(*) FROM analytics.dim_customer) as dim_customer,
                (SELECT COUNT(*) FROM analytics.dim_product) as dim_product,
                (SELECT COUNT(*) FROM analytics.dim_location) as dim_location,
                (SELECT COUNT(*) FROM analytics.dim_date) as dim_date,
                (SELECT COUNT(*) FROM analytics.sales) as original_sales
        """).df()

        print("\n Row Counts:")
        print(counts.T.to_string())

        # Validate no orphans in fact table
        orphan_check = self.conn.execute("""
            SELECT
                COUNT(*) as total_rows,
                COUNT(customer_key) as has_customer,
                COUNT(product_key) as has_product,
                COUNT(location_key) as has_location,
                COUNT(order_date_key) as has_order_date
            FROM analytics.fact_sales
        """).df()

        print("\n✓ Foreign Key Coverage:")
        print(orphan_check.to_string(index=False))

        # analytical query
        print("\n Sample Analytical Query - Sales by Region and Category:")
        result = self.conn.execute("""
            SELECT
                l.region,
                p.category,
                COUNT(*) as order_count,
                SUM(f.sales_amount) as total_revenue,
                AVG(f.sales_amount) as avg_sale,
                SUM(f.quantity) as total_quantity
            FROM analytics.fact_sales f
            JOIN analytics.dim_location l ON f.location_key = l.location_key
            JOIN analytics.dim_product p ON f.product_key = p.product_key
            GROUP BY l.region, p.category
            ORDER BY total_revenue DESC
            LIMIT 10
        """).df()
        print(result.to_string(index=False))

        logger.info("Dimensional model validation complete")

    def build_all(self) -> None:
        """Build dimensional model"""
        logger.info("\n Build dimensional model")

        self.create_dim_customer()
        self.create_dim_date()
        self.create_dim_product()
        self.create_dim_location()

        self.create_fact_sales()

        self.validate_model()

        logger.info("\nDimensional model complete")
        logger.info(f"Warehouse location: {self.warehouse_path}")

    def close(self) -> None:
        self.conn.close()
        logger.info("Database connection closed")


def main():
    builder = DimensionalModelBuilder()
    try:
        builder.build_all()
    finally:
        builder.close()


if __name__ == "__main__":
    main()
