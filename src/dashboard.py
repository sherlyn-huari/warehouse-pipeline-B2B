from __future__ import annotations
from pathlib import Path
import altair as alt
import polars as pl
import streamlit as st

DATA_DIR = Path(__file__).parent.parent / "data" / "output"
PARQUET_PATH = DATA_DIR / "synthetic_data.parquet"

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

    st.sidebar.header("Filters")

    all_years = sorted(dataset["order_year"].unique().to_list())
    selected_years = st.sidebar.multiselect(
        "Select Years",
        options=all_years,
        default=all_years
    )


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

    filtered_dataset = dataset.filter(
        (pl.col("order_year").is_in(selected_years)) &
        (pl.col("order_month").is_in(selected_months))
    )

    if len(filtered_dataset) > 0:
        st.sidebar.metric("Filtered Rows", f"{len(filtered_dataset):,}")
        st.sidebar.metric("Filtered Revenue", f"${filtered_dataset['revenue'].sum():,.0f}")

    total_revenue = filtered_dataset["revenue"].sum()
    total_orders = len(filtered_dataset)
    total_quantity = filtered_dataset["quantity"].sum()
    unique_customers = filtered_dataset["customer_id"].n_unique()
    unique_products = filtered_dataset["product_id"].n_unique()

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Revenue", f"${format_number(total_revenue)}")
    col2.metric("Total Orders", f"{format_number(total_orders)}")
    col3.metric("Total Quantity", f"{format_number(total_quantity)}")
    col4.metric("Unique Customers", f"{format_number(unique_customers)}")
    col5.metric("Unique Products", f"{format_number(unique_products)}")

    left, right = st.columns(2)
        
    with left:
        st.subheader("Revenue by Month")
        if filtered_dataset is not None and len(filtered_dataset) > 0:
            monthly_revenue = (
                filtered_dataset.group_by(["order_year", "order_month"])
                .agg(pl.col("revenue").sum().alias("total_revenue"))
                .sort(["order_year", "order_month"])
            )
            monthly_revenue = monthly_revenue.with_columns(
                (pl.col("order_year").cast(str) + "-" + pl.col("order_month").cast(str).str.zfill(2)).alias("year_month")
            )

            chart = (
                alt.Chart(monthly_revenue.to_pandas())
                .mark_line(point=True)
                .encode(
                    x=alt.X("year_month:O", title="Month", axis=alt.Axis(labelAngle=-45)),
                    y=alt.Y("total_revenue:Q", title="Revenue", axis=alt.Axis(format="$.2s")),
                    tooltip=[
                        alt.Tooltip("year_month:O", title="Month"),
                        alt.Tooltip("total_revenue:Q", title="Revenue", format="$,.0f")
                    ]
                )
                .properties(height=300)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Run the ETL pipeline to populate this chart")

    with right:
        st.subheader("Orders by Month")
        if filtered_dataset is not None and len(filtered_dataset) > 0:
            monthly_orders = ( filtered_dataset.group_by(["order_year", "order_month"])
                              .agg(pl.col("order_id").count().alias("total_orders"))
                              .sort(["order_year", "order_month"])
                            )
            monthly_orders = monthly_orders.with_columns(
            (pl.col("order_year").cast(str) + "-" + pl.col("order_month").cast(str).str.zfill(2)).alias("year_month")
            )
            chart = (
                alt.Chart(monthly_orders.to_pandas())
                .mark_line(point=True)
                .encode(
                    x=alt.X("year_month:O", title="Month", axis=alt.Axis(labelAngle=-45)),
                    y=alt.Y("total_orders:Q", title="Orders"),
                    tooltip=[
                        alt.Tooltip("year_month:O", title="Month"),
                        alt.Tooltip("total_orders:Q", title="Orders", format=",d")
                    ]
                )
                .properties(height=300)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Run the ETL pipeline to populate this chart")


    with left:
        st.subheader("Revenue by Segment")
        if filtered_dataset is not None and len(filtered_dataset) > 0:
            segment_revenue = (
                filtered_dataset.group_by(["order_year","order_month","segment"])
                .agg(pl.col("revenue").sum().alias("total_revenue"))
                .sort(["order_year", "order_month"])
            )

            segment_revenue = segment_revenue.with_columns(
                (pl.col("order_year").cast(str) + "-" + pl.col("order_month").cast(str).str.zfill(2)).alias("year_month")
            )

            chart = (
                alt.Chart(segment_revenue.to_pandas())
                .mark_line(point = True)
                .encode(
                    x=alt.X("year_month:O", title="Month", axis=alt.Axis(labelAngle=-45)),
                    y=alt.Y("total_revenue:Q", title="Revenue", axis=alt.Axis(format="$,.0f")),
                    color=alt.Color("segment:N", title="Segment"),
                    tooltip=[
                        alt.Tooltip("year_month:O", title="Month"),
                        alt.Tooltip("total_revenue:Q", title="Revenue", format="$,.0f"),
                        alt.Tooltip("segment:N", title="Segment")
                    ]
                )
                .properties(height=300)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Segment-level data unavailable")

    with right:
        st.subheader("Top 10 customer by Revenue")
        if filtered_dataset is not None and len(filtered_dataset) > 0:
            top_customers = (
                filtered_dataset.group_by("customer_name")
                .agg([
                    pl.col("revenue").sum().alias("total_revenue")
                ])
                .sort("total_revenue", descending=True)
                .head(10)
                .to_pandas()
            )
            top_customers['total_revenue'] = top_customers['total_revenue'].apply(lambda x: f"${x:,.0f}")
            top_customers = top_customers.rename(columns={
                'customer_name': 'Customer',
                'total_revenue': 'Revenue'
            })
            st.dataframe(top_customers, use_container_width=True, height=300, hide_index=True)
        else:
            st.info("Customer data unavailable")

    with left:
        st.subheader("Top Performing Cities (Revenue - Orders - Quantity)")
        if filtered_dataset is not None and len(filtered_dataset) > 0:
            cities = (
                filtered_dataset.group_by("city")
                .agg([
                    pl.col("revenue").sum().alias("total_revenue"),
                    pl.col("order_id").n_unique().alias("total_orders"),
                    pl.col("quantity").sum().alias("total_quantity")
                ])
                .sort("total_revenue", descending=True)
                .to_pandas()
            )
            cities['total_revenue'] = cities['total_revenue'].apply(lambda x: f"${x:,.0f}")
            cities['total_orders'] = cities['total_orders'].apply(lambda x: f"{x:,}")
            cities['total_quantity'] = cities['total_quantity'].apply(lambda x: f"{x:,}")
            cities = cities.rename(columns={
                'city': 'City',
                'total_revenue': 'Revenue',
                'total_orders': 'Orders',
                'total_quantity': 'Quantity'
            })
            st.dataframe(cities, use_container_width=True, height=300, hide_index=True)
        else:
            st.info("City data unavailable")

    with right:
        st.subheader("Revenue by Region")
        if filtered_dataset is not None and len(filtered_dataset) > 0:
            region_revenue = (
                filtered_dataset.group_by("region")
                .agg(pl.col("revenue").sum().alias("total_revenue"))
                .sort("total_revenue", descending=True)
                .to_pandas()
            )

            chart = (
                alt.Chart(region_revenue)
                .mark_bar()
                .encode(
                    x=alt.X("total_revenue:Q", title="Revenue", axis=alt.Axis(format="$,.0f")),
                    y=alt.Y("region:N", title="Region", sort="-x"),
                    tooltip=[
                        alt.Tooltip("region:N", title="Region"),
                        alt.Tooltip("total_revenue:Q", title="Revenue", format="$,.0f")
                    ]
                )
                .properties(height=300)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Region data unavailable")

    with left:
        st.subheader("Revenue by Category")
        if filtered_dataset is not None and len(filtered_dataset) > 0:
            category_revenue = (
                filtered_dataset.group_by(["order_year","order_month","category"])
                .agg(pl.col("revenue").sum().alias("total_revenue"))
                .sort(["order_year", "order_month"])
            )

            category_revenue = category_revenue.with_columns(
                (pl.col("order_year").cast(str) + "-" + pl.col("order_month").cast(str).str.zfill(2)).alias("year_month")
            )

            chart = (
                alt.Chart(category_revenue.to_pandas())
                .mark_line(point = True)
                .encode(
                    x=alt.X("year_month:O", title="Month", axis=alt.Axis(labelAngle=-45)),
                    y=alt.Y("total_revenue:Q", title="Revenue", axis=alt.Axis(format="$,.0f")),
                    color=alt.Color("category:N", title="Category"),
                    tooltip=[
                        alt.Tooltip("year_month:O", title="Month"),
                        alt.Tooltip("total_revenue:Q", title="Revenue", format="$,.0f"),
                        alt.Tooltip("category:N", title="Category")
                    ]
                )
                .properties(height=300)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Category data unavailable")

    with right:
        st.subheader("Top 10 Products (Revenue - Orders - Quantity)")
        if filtered_dataset is not None and len(filtered_dataset) > 0:
            top_products = (
                filtered_dataset.group_by(["category", "product_name"])
                .agg([
                    pl.col("revenue").sum().alias("total_revenue"),
                    pl.col("order_id").n_unique().alias("total_orders"),
                    pl.col("quantity").sum().alias("total_quantity")
                ])
                .sort("total_revenue", descending=True)
                .head(10)
                .to_pandas()
            )
            top_products['total_revenue'] = top_products['total_revenue'].apply(lambda x: f"${x:,.0f}")
            top_products['total_orders'] = top_products['total_orders'].apply(lambda x: f"{x:,}")
            top_products['total_quantity'] = top_products['total_quantity'].apply(lambda x: f"{x:,}")
            top_products = top_products.rename(columns={
                'category': 'Category',
                'product_name': 'Product',
                'total_revenue': 'Revenue',
                'total_orders': 'Orders',
                'total_quantity': 'Quantity'
            })
            st.dataframe(top_products, use_container_width=True, height=300, hide_index=True)
        else:
            st.info("Product data unavailable")


def format_number(num):
    if num >= 1_000_000_000:
        return f"{num / 1_000_000_000:.1f}B"
    elif num >= 1_000_000:
        return f"{num / 1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num / 1_000:.1f}K"
    else:
        return f"{num:.0f}"

if __name__ == "__main__":
    main()
