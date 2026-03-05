import sys
from pathlib import Path
from typing import Tuple

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

import streamlit as st
import polars as pl
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Page configuration
st.set_page_config(
    page_title="Favorita Sales Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_data(ttl=3600)
def load_master_data_lazy(data_dir: str = "data/processed") -> pl.LazyFrame:
    """
    Load all master_data partitions using lazy evaluation.
    
    Args:
        data_dir: Directory containing parquet files
        
    Returns:
        Lazy DataFrame with all historical data
    """
    try:
        data_path = Path(data_dir)
        pattern = str(data_path / "master_data_*.parquet")
        
        # Scan all parquet files with glob pattern
        lf = pl.scan_parquet(pattern)
        
        return lf
        
    except Exception as e:
        st.error(f"Error loading master data: {e}")
        st.stop()

@st.cache_data(ttl=3600)
def load_submission_data() -> pd.DataFrame:
    """
    Load submission predictions CSV.
    
    Returns:
        DataFrame with predictions
    """
    try:
        submission_path = Path("data/processed/submission.csv")
        df = pd.read_csv(submission_path)
        return df
        
    except Exception as e:
        st.error(f"Error loading submission data: {e}")
        st.stop()


@st.cache_data(ttl=3600)
def load_stores_data()->pd.DataFrame:

    try:
        stores_path = Path("data/raw/stores.csv")
        df = pd.read_csv(stores_path)
        return df
    except Exception as e:
        st.error(f"Error loading stores data: {e}")
        st.stop()
        

@st.cache_data(ttl=3600)
def load_items_data()->pd.DataFrame:

    try:
        items_path = Path("data/raw/items.csv")
        df = pd.read_csv(items_path)
        return df
    except Exception as e:
        st.error(f"Error loading items data: {e}")
        st.stop()


# Data processing functions

def get_kpi_metrics(lf: pl.LazyFrame, stores_data: pd.DataFrame, items_data: pd.DataFrame) -> Tuple[float, float, int, int, int]:
    """
    Calculate KPI metrics from historical data.
    
    Args:
        lf: Lazy DataFrame with sales data
        
    Returns:
        Tuple of (total_sales, avg_daily_sales, total_days)
    """
    metrics = (
        lf
        .filter(pl.col("unit_sales").is_not_null())  # Only actual sales
        .select([
            pl.col("unit_sales").sum().alias("total_sales"),
            pl.col("date").n_unique().alias("total_days"),
        ])
        .collect()
        .to_pandas()
    )
    
    total_sales = float(metrics["total_sales"].iloc[0])
    total_days = int(metrics["total_days"].iloc[0])
    avg_daily_sales = total_sales / total_days if total_days > 0 else 0
    total_stores = len(stores_data)
    total_items = len(items_data)

    return total_sales, avg_daily_sales, total_days, total_stores, total_items


def get_monthly_sales(lf: pl.LazyFrame) -> pd.DataFrame:
    """
    Aggregate sales by year and month.
    
    Args:
        lf: Lazy DataFrame with sales data
        
    Returns:
        DataFrame with monthly sales
    """
    monthly = (
        lf
        .filter(pl.col("unit_sales").is_not_null())
        .group_by([pl.col("year"), pl.col("month")])
        .agg([
            pl.col("unit_sales").sum().alias("total_sales")
        ])
        .sort(["year", "month"])
        .with_columns([
            (pl.col("year").cast(pl.Int32).cast(pl.String) + "-" + 
             pl.col("month").cast(pl.Int32).cast(pl.String).str.zfill(2))
            .alias("year_month")
        ])
        .collect()
        .to_pandas()
    )
    
    return monthly

def get_sales_by_dimension(
    lf: pl.LazyFrame,
    dimension: str,
    top_n: int = 10
) -> pd.DataFrame:
    """
    Get top N sales by a specific dimension (city, store_type, etc.).
    
    Args:
        lf: Lazy DataFrame with sales data
        dimension: Column name to group by
        top_n: Number of top items to return
        
    Returns:
        DataFrame with aggregated sales
    """
    result = (
        lf
        .filter(pl.col("unit_sales").is_not_null())
        .group_by(dimension)
        .agg([
            pl.col("unit_sales").sum().alias("total_sales")
        ])
        .sort("total_sales", descending=True)
        .limit(top_n)
        .collect()
        .to_pandas()
    )
    
    return result

def get_sales_by_day_of_week(lf: pl.LazyFrame) -> pd.DataFrame:
    """
    Get average total daily sales by day of week.
    Requires a two-step aggregation to avoid granularity mismatch.
    """
    dow_map = {
        0: "Sunday",
        1: "Monday", 
        2: "Tuesday",
        3: "Wednesday",
        4: "Thursday",
        5: "Friday",
        6: "Saturday"
    }
    
    result = (
        lf
        .filter(pl.col("unit_sales").is_not_null())
        .group_by(["date", "day_of_week"])
        .agg(pl.col("unit_sales").sum().alias("daily_total"))

        .group_by("day_of_week")
        .agg(pl.col("daily_total").mean().alias("avg_sales"))
        .sort("day_of_week")
        .collect()
        .to_pandas()
    )
    
    result["day_name"] = result["day_of_week"].map(dow_map)
    
    return result

def get_filtered_time_series(
    lf: pl.LazyFrame,
    store_nbr: int | None = None,
    item_family: str | None = None,
    year: int | None = None
) -> pd.DataFrame:
    """
    Get daily sales time series with filters.
    
    Args:
        lf: Lazy DataFrame with sales data
        store_nbr: Filter by store number
        item_family: Filter by item family
        year: Filter by year
        
    Returns:
        DataFrame with daily sales
    """
    query = lf.filter(pl.col("unit_sales").is_not_null())
    
    if store_nbr is not None:
        query = query.filter(pl.col("store_nbr") == store_nbr)
    
    if item_family is not None:
        query = query.filter(pl.col("item_family") == item_family)
    
    if year is not None:
        query = query.filter(pl.col("year") == year)
    
    result = (
        query
        .group_by("date")
        .agg([
            pl.col("unit_sales").sum().alias("total_sales")
        ])
        .sort("date")
        .collect()
        .to_pandas()
    )
    
    return result


def get_promotion_impact(
    lf: pl.LazyFrame,
    store_nbr: int | None = None,
    item_family: str | None = None,
    year: int | None = None
) -> pd.DataFrame:
    """
    Compare sales with and without promotions.
    
    Args:
        lf: Lazy DataFrame with sales data
        store_nbr: Filter by store number
        item_family: Filter by item family
        year: Filter by year
        
    Returns:
        DataFrame with promotion comparison
    """
    query = lf.filter(pl.col("unit_sales").is_not_null())
    
    if store_nbr is not None:
        query = query.filter(pl.col("store_nbr") == store_nbr)
    
    if item_family is not None:
        query = query.filter(pl.col("item_family") == item_family)
    
    if year is not None:
        query = query.filter(pl.col("year") == year)
    
    result = (
        query
        .group_by("onpromotion")
        .agg([
            pl.col("unit_sales").sum().alias("total_sales"),
        ])
        .collect()
        .to_pandas()
    )
    
    result["promotion_status"] = result["onpromotion"].map({
        True: "With Promotion",
        False: "Without Promotion"
    })
    
    return result


#Plot functions

def plot_monthly_trend(df: pd.DataFrame) -> go.Figure:
    """Create monthly sales trend line chart."""
    fig = px.line(
        df,
        x="year_month",
        y="total_sales",
        title="Monthly Sales Trend (2013-2017)",
        labels={"year_month": "Year-Month", "total_sales": "Total Sales"},
        markers=True,
    )
    
    fig.update_layout(
        hovermode="x unified",
        height=400,
        showlegend=False
    )

    
    return fig


def plot_top_dimension(df: pd.DataFrame, dimension: str) -> go.Figure:
    """Create horizontal bar chart for top performers."""
    fig = px.bar(
        df,
        x="total_sales",
        y=dimension,
        orientation="h",
        title=f"Top 10 by {dimension.replace('_', ' ').title()}",
        labels={"total_sales": "Total Sales", dimension: dimension.replace('_', ' ').title()}
    )
    
    fig.update_layout(
        yaxis={'categoryorder': 'total ascending'},
        height=400
    )
    
    return fig

def plot_day_of_week(df: pd.DataFrame) -> go.Figure:
    """Create bar chart for day-of-week patterns."""
    fig = px.bar(
        df,
        x="day_name",
        y="avg_sales",
        title="Average Sales by Day of Week",
        labels={"day_name": "Day", "avg_sales": "Average Sales"},
        color="avg_sales",
        color_continuous_scale="Blues"
    )
    
    fig.update_layout(height=400, showlegend=True)
    
    return fig

def plot_time_series(df: pd.DataFrame, title: str = "Daily Sales") -> go.Figure:
    """Create time series line chart."""
    fig = px.line(
        df,
        x="date",
        y="total_sales",
        title=f"📊 {title}",
        labels={"date": "Date", "total_sales": "Total Sales"}
    )
    
    fig.update_layout(
        hovermode="x unified",
        height=400
    )
    
    return fig

def plot_promotion_comparison(df: pd.DataFrame) -> go.Figure:
    """Create comparison chart for promotion impact."""
    fig = px.bar(
        df,
        x="promotion_status",
        y="total_sales",
        title="Sales Impact: Promotion vs No Promotion",
        labels={"promotion_status": "Promotion Status", "total_sales": "Total Sales"},
        color="promotion_status",
        color_discrete_map={
            "With Promotion": "#FF6B6B",
            "Without Promotion": "#4ECDC4"
        }
    )
    
    fig.update_layout(height=400, showlegend=False)
    
    return fig

#Main Dashboard

def main():
    """Main dashboard application."""
    
    # Header
    st.title("Corporación Favorita - Sales Analytics Dashboard")
    st.markdown("""
    **End-to-End Machine Learning Project**: Demand forecasting for retail inventory optimization  
    **Dataset**: 129M+ transactions | **Period**: 2013-2017 | **Competition**: Kaggle  
    """)
    
    st.divider()
    
    # Load data
    with st.spinner("Loading data..."):
        master_lf = load_master_data_lazy()
        submission_df = load_submission_data()
        stores_df = load_stores_data()
        items_df = load_items_data()
    
    # Create tabs
    tab1, tab2 = st.tabs([
        "📊 Overview (2013-2017)",
        "🔍 Deep Dive (Store/Product)",
    ])
    

    # TAB 1: Overview

    with tab1:
        st.header("Historical Sales Overview")
        
        # KPIs
        with st.spinner("Calculating KPIs..."):
            total_sales, avg_daily_sales, total_days, total_stores, total_items = get_kpi_metrics(master_lf, stores_df, items_df)
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric(
                "💰 Total Sales (Historical)",
                f"{total_sales:,.0f}",
                help="Sum of all unit sales from 2013-2017"
            )
        
        with col2:
            st.metric(
                "📈 Average Daily Sales",
                f"{avg_daily_sales:,.0f}",
                help="Average sales per day across all stores"
            )
        
        with col3:
            st.metric(
                "📅 Days of Data",
                f"{total_days:,}",
                help="Number of unique dates in dataset"
            )
        
        with col4:
            st.metric(
                "🏪 Total stores",
                f"{total_stores:,}",
                help="Number of unique stores in dataset"
            )

        with col5:
            st.metric(
                "🛒 Total items",
                f"{total_items:,}",
                help="Number of unique items in dataset"
            )
        
        st.divider()

        # Monthly trend
        with st.spinner("Loading monthly trend..."):
            monthly_df = get_monthly_sales(master_lf)
        
        st.plotly_chart(
            plot_monthly_trend(monthly_df),
            width = 'stretch'
        )

        # Columns for comparisons
        col1, col2, col3 = st.columns(3)
        
        with col1:
            with st.spinner("Loading top cities..."):
                city_df = get_sales_by_dimension(master_lf, "city", top_n=10)
            
            st.plotly_chart(
                plot_top_dimension(city_df, "city"),
                width='stretch'
            )
        
        with col2:
            with st.spinner("Loading day-of-week pattern..."):
                dow_df = get_sales_by_day_of_week(master_lf)
            
            st.plotly_chart(
                plot_day_of_week(dow_df),
                width='stretch'
            )
        
        with col3:
            with st.spinner("Loading top items family..."):
                families_df = get_sales_by_dimension(master_lf, "item_family", top_n=10)
            
            st.plotly_chart(
                plot_top_dimension(families_df,"item_family")
            )

    # Tab 2: Filter
    with tab2:
        st.header("Deep Dive: Store & Product Analysis")
        
        # Get filter options
        @st.cache_data
        def get_filter_options():
            stores = (
                master_lf
                .select("store_nbr")
                .unique()
                .sort("store_nbr")
                .collect()
                .to_pandas()["store_nbr"]
                .tolist()
            )
            
            families = (
                master_lf
                .select("item_family")
                .unique()
                .sort("item_family")
                .collect()
                .to_pandas()["item_family"]
                .dropna()
                .tolist()
            )
            
            years = list(range(2013, 2018))
            
            return stores, families, years
        
        stores, families, years = get_filter_options()

        # Filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            selected_store = st.selectbox(
                "🏪 Select Store",
                options=[None] + stores,
                format_func=lambda x: "All Stores" if x is None else f"Store {x}"
            )
        
        with col2:
            selected_family = st.selectbox(
                "🛒 Select Product Family",
                options=[None] + families,
                format_func=lambda x: "All Families" if x is None else x
            )
        
        with col3:
            selected_year = st.selectbox(
                "📅 Select Year",
                options=years,
                index=4  # Default to 2017
            )
        
        st.divider()
        
        # Time series
        with st.spinner("Loading time series..."):
            ts_df = get_filtered_time_series(
                master_lf,
                store_nbr=selected_store,
                item_family=selected_family,
                year=selected_year
            )
        
        if len(ts_df) > 0:
            st.plotly_chart(
                plot_time_series(
                    ts_df,
                    title=f"Daily Sales - Store {selected_store or 'All'} - {selected_family or 'All Families'} - {selected_year}"
                ),
                width='stretch'
            )
        else:
            st.warning("No data available for the selected filters.")
        
        # Promotion impact
        with st.spinner("Analyzing promotion impact..."):
            promo_df = get_promotion_impact(
                master_lf,
                store_nbr=selected_store,
                item_family=selected_family,
                year=selected_year
            )
        
        if len(promo_df) > 0:
            st.plotly_chart(
                plot_promotion_comparison(promo_df),
                width='stretch'
            )
        else:
            st.info("No promotion data available for this selection.")


if __name__ == "__main__":
    main()