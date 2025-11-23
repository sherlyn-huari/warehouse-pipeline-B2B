"""Streamlit dashboard """

from __future__ import annotations

from pathlib import Path

import altair as alt
import polars as pl
import streamlit as st

DATA_DIR = Path(__file__).parent.parent / "data" / "output"
PARQUET_PATH = DATA_DIR / "synthetic_data.parquet"
YEARLY_CSV = DATA_DIR / "yearly.csv"
SEGMENT_CSV = DATA_DIR / "segment_yearly.csv"
REGION_CSV = DATA_DIR / "regional_revenue.csv"
TOP_PRODUCTS_CSV = DATA_DIR / "top_products.csv"


@st.cache_data(show_spinner=False)
def load_dataset() -> pl.DataFrame:
    if not PARQUET_PATH.exists():
        raise FileNotFoundError(
            f"Missing {PARQUET_PATH}. Run `python3 src/etl.py` first."
        )
    return pl.read_parquet(PARQUET_PATH)


@st.cache_data(show_spinner=False)
def load_summary(path: Path) -> pl.DataFrame | None:
    if path.exists():
        return pl.read_csv(path)
    return None


def main() -> None:
    st.set_page_config(page_title="Sales Analyzer", layout="wide")
    st.title("📊 B2B Retail sales Dashboard")

    try:
        dataset = load_dataset()
    except FileNotFoundError as exc:
        st.error(str(exc))
        st.stop()
        return

    total_rows = len(dataset)
    total_revenue = dataset["revenue"].sum()
    unique_customers = dataset["customer_id"].n_unique()
    unique_products = dataset["product_id"].n_unique()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Rows", f"{total_rows:,}")
    col2.metric("Total Revenue", f"${total_revenue:,.0f}")
    col3.metric("Unique Customers", f"{unique_customers:,}")
    col4.metric("Unique Products", f"{unique_products:,}")

    # Filters
    st.sidebar.header("Filters")

    # Year filter
    all_years = sorted(dataset["order_year"].unique().to_list())
    selected_years = st.sidebar.multiselect(
        "Select Years",
        options=all_years,
        default=all_years
    )

    # Month filter
    all_months = list(range(1, 13))
    month_names = {
        1: "January", 2: "February", 3: "March", 4: "April",
        5: "May", 6: "June", 7: "July", 8: "August",
        9: "September", 10: "October", 11: "November", 12: "December"
    }
    selected_months = st.sidebar.multiselect(
        "Select Months",
        options=all_months,
        default=all_months,
        format_func=lambda x: month_names[x]
    )

    # Apply filters
    filtered_dataset = dataset.filter(
        (pl.col("order_year").is_in(selected_years)) &
        (pl.col("order_month").is_in(selected_months))
    )

    # Update metrics with filtered data
    if len(filtered_dataset) > 0:
        st.sidebar.metric("Filtered Rows", f"{len(filtered_dataset):,}")
        st.sidebar.metric("Filtered Revenue", f"${filtered_dataset['revenue'].sum():,.0f}")
        
    with st.container():
        st.subheader("Revenue by Month")
        if filtered_dataset is not None and len(filtered_dataset) > 0:
            # Group by year and month
            monthly_revenue = (
                filtered_dataset.group_by(["order_year", "order_month"])
                .agg(pl.col("revenue").sum().alias("total_revenue"))
                .sort(["order_year", "order_month"])
            )
            # Create a date column for better display
            monthly_revenue = monthly_revenue.with_columns(
                (pl.col("order_year").cast(str) + "-" + pl.col("order_month").cast(str).str.zfill(2)).alias("year_month")
            )

            chart = (
                alt.Chart(monthly_revenue.to_pandas())
                .mark_line(point=True)
                .encode(
                    x=alt.X("year_month:O", title="Month", axis=alt.Axis(labelAngle=-45)),
                    y=alt.Y("total_revenue:Q", title="Revenue", axis=alt.Axis(format="$,.0f")),
                    tooltip=[
                        alt.Tooltip("year_month:O", title="Month"),
                        alt.Tooltip("total_revenue:Q", title="Revenue", format="$,.0f")
                    ]
                )
                .properties(height=350)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Run the ETL pipeline to populate this chart.")

    left, right = st.columns(2)

    with left:
        st.subheader("Revenue by Category and Month")
        if filtered_dataset is not None and len(filtered_dataset) > 0:
            category_monthly = (
                filtered_dataset.group_by(["order_year", "order_month", "category"])
                .agg(pl.col("revenue").sum().alias("total_revenue"))
                .sort(["order_year", "order_month"])
            )
            category_monthly = category_monthly.with_columns(
                (pl.col("order_year").cast(str) + "-" + pl.col("order_month").cast(str).str.zfill(2)).alias("year_month")
            )

            chart = (
                alt.Chart(category_monthly.to_pandas())
                .mark_line(point=True)
                .encode(
                    x=alt.X("year_month:O", title="Month", axis=alt.Axis(labelAngle=-45)),
                    y=alt.Y("total_revenue:Q", title="Revenue", axis=alt.Axis(format="$,.0f")),
                    color=alt.Color("category:N", title="Category"),
                    tooltip=[
                        alt.Tooltip("year_month:O", title="Month"),
                        alt.Tooltip("category:N", title="Category"),
                        alt.Tooltip("total_revenue:Q", title="Revenue", format="$,.0f")
                    ]
                )
                .properties(height=300)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Category data unavailable.")

    with right:
        st.subheader("Quantity Sold by Ship Mode and Year")
        if filtered_dataset is not None and len(filtered_dataset) > 0:
            shipmode_yearly = (
                filtered_dataset.group_by(["ship_mode", "order_year"])
                .agg(pl.col("quantity").sum().alias("total_quantity"))
                .sort(["order_year", "ship_mode"])
            )
            # Create pivot table
            shipmode_pivot = shipmode_yearly.to_pandas().pivot(
                index='ship_mode',
                columns='order_year',
                values='total_quantity'
            ).fillna(0)
            # Format values
            st.dataframe(
                shipmode_pivot.style.format("{:,.0f}"),
                use_container_width=True
            )
        else:
            st.info("Ship mode data unavailable.")

    st.subheader("Revenue by Segment and Year")
    if filtered_dataset is not None and len(filtered_dataset) > 0:
        # Calculate from dataset directly
        segment_data = (
            filtered_dataset.group_by(["segment", "order_year"])
            .agg(pl.col("revenue").sum().alias("total_revenue"))
        )
        # Create pivot table
        segment_pivot = segment_data.to_pandas().pivot(
            index='segment',
            columns='order_year',
            values='total_revenue'
        ).fillna(0)
        st.dataframe(
            segment_pivot.style.format("${:,.0f}"),
            use_container_width=True
        )
    else:
        st.info("Segment-level data unavailable.")

    # New row with two columns
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Top 3 Cities by Revenue")
        if filtered_dataset is not None and len(filtered_dataset) > 0:
            top_cities = (
                filtered_dataset.group_by("city")
                .agg(pl.col("revenue").sum().alias("total_revenue"))
                .sort("total_revenue", descending=True)
                .head(3)
                .to_pandas()
            )
            top_cities['total_revenue'] = top_cities['total_revenue'].apply(lambda x: f"${x:,.0f}")
            top_cities = top_cities.rename(columns={
                'city': 'City',
                'total_revenue': 'Revenue'
            })
            st.dataframe(top_cities, use_container_width=True)
        else:
            st.info("City data unavailable.")

    with col_right:
        st.subheader("Quantity Sold by Segment and Month")
        if filtered_dataset is not None and len(filtered_dataset) > 0:
            segment_quantity = (
                filtered_dataset.group_by(["segment", "order_year", "order_month"])
                .agg(pl.col("quantity").sum().alias("total_quantity"))
                .sort(["order_year", "order_month"])
            )
            # Create year-month column
            segment_quantity = segment_quantity.with_columns(
                (pl.col("order_year").cast(str) + "-" + pl.col("order_month").cast(str).str.zfill(2)).alias("year_month")
            )

            segment_df = segment_quantity.to_pandas()
            segment_df['quantity_k'] = segment_df['total_quantity'] / 1000

            chart = (
                alt.Chart(segment_df)
                .mark_line(point=True)
                .encode(
                    x=alt.X("year_month:O", title="Month", axis=alt.Axis(labelAngle=-45)),
                    y=alt.Y("quantity_k:Q", title="Quantity Sold (K)", axis=alt.Axis(format=".1f")),
                    color=alt.Color("segment:N", title="Segment"),
                    tooltip=[
                        alt.Tooltip("year_month:O", title="Month"),
                        alt.Tooltip("segment:N", title="Segment"),
                        alt.Tooltip("total_quantity:Q", title="Quantity", format=",")
                    ]
                )
                .properties(height=300)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Segment data unavailable.")

    st.subheader("Top 5 Products")
    if filtered_dataset is not None and len(filtered_dataset) > 0:
        # Calculate top products with quantity
        top5_products = (
            filtered_dataset.group_by(["category", "product_name"])
            .agg([
                pl.col("revenue").sum().alias("total_revenue"),
                pl.col("quantity").sum().alias("total_quantity")
            ])
            .sort("total_revenue", descending=True)
            .head(5)
            .to_pandas()
        )
        # Format the revenue column
        top5_products['total_revenue'] = top5_products['total_revenue'].apply(lambda x: f"${x:,.0f}")
        top5_products['total_quantity'] = top5_products['total_quantity'].apply(lambda x: f"{x:,.0f}")
        # Rename columns for display
        top5_products = top5_products.rename(columns={
            'category': 'Category',
            'product_name': 'Product',
            'total_revenue': 'Revenue',
            'total_quantity': 'Quantity Sold'
        })
        st.dataframe(top5_products, use_container_width=True)
    else:
        st.info("Product-level summary unavailable.")


if __name__ == "__main__":
    main()
