# dashboard.py
import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Real-Time Ride-Sharing Dashboard", layout="wide")
st.title("🚕 Real-Time Ride-Sharing Trips Dashboard")
st.caption(
    "Streaming synthetic ride-sharing trips via Kafka → PostgreSQL → Streamlit."
)

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5432/kafka_db"

@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)

engine = get_engine(DATABASE_URL)

def load_data(status_filter: str | None = None, limit: int = 500) -> pd.DataFrame:
    """
    Load recent trips from the database with optional status filter.
    """
    base_query = "SELECT * FROM trips"
    params: dict = {}

    if status_filter and status_filter != "All":
        base_query += " WHERE status = :status"
        params["status"] = status_filter

    base_query += " ORDER BY timestamp DESC LIMIT :limit"
    params["limit"] = limit

    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(text(base_query), con=conn, params=params)
        return df
    except Exception as e:
        st.error(f"Error loading data from database: {e}")
        return pd.DataFrame()

# Sidebar controls
status_options = ["All", "requested", "ongoing", "completed", "cancelled"]
selected_status = st.sidebar.selectbox("Filter by Trip Status", status_options)
update_interval = st.sidebar.slider(
    "Update Interval (seconds)", min_value=2, max_value=20, value=5
)
limit_records = st.sidebar.number_input(
    "Number of recent trips to load",
    min_value=50,
    max_value=5000,
    value=500,
    step=50,
)

if st.sidebar.button("Refresh now"):
    st.rerun()

placeholder = st.empty()

# Simple auto-refresh loop
while True:
    df_trips = load_data(selected_status, limit=int(limit_records))

    with placeholder.container():
        if df_trips.empty:
            st.warning("No trip records found yet. Waiting for data...")
            st.caption(f"Last checked: {datetime.now().isoformat()}")
            time.sleep(update_interval)
            continue

        # Ensure timestamp is datetime
        if "timestamp" in df_trips.columns:
            df_trips["timestamp"] = pd.to_datetime(df_trips["timestamp"])

        # KPIs
        total_trips = len(df_trips)
        total_revenue = df_trips["fare_amount"].sum()
        avg_fare = total_revenue / total_trips if total_trips > 0 else 0.0
        avg_distance = df_trips["distance_km"].mean() if total_trips > 0 else 0.0

        completed = len(df_trips[df_trips["status"] == "completed"])
        completion_rate = (
            completed / total_trips * 100 if total_trips > 0 else 0.0
        )

        st.subheader(
            f"Displaying {total_trips} trips "
            f"(Status filter: {selected_status})"
        )

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total Trips (recent)", f"{total_trips:,}")
        k2.metric("Total Revenue (recent)", f"${total_revenue:,.2f}")
        k3.metric("Avg Fare", f"${avg_fare:,.2f}")
        k4.metric("Avg Distance (km)", f"{avg_distance:,.2f}")

        k5, k6, _, _ = st.columns(4)
        k5.metric("Completed Trips", completed)
        k6.metric("Completion Rate", f"{completion_rate:,.2f}%")

        # Raw data preview
        st.markdown("### Most Recent Trips (Top 10)")
        st.dataframe(
            df_trips[
                [
                    "timestamp",
                    "trip_id",
                    "pickup_city",
                    "dropoff_city",
                    "distance_km",
                    "fare_amount",
                    "status",
                    "payment_method",
                    "driver_rating",
                ]
            ].head(10),
            use_container_width=True,
        )

        # Charts
        chart_col1, chart_col2 = st.columns(2)

        # Revenue by pickup city
        with chart_col1:
            st.markdown("### Revenue by Pickup City")
            grouped_city = (
                df_trips.groupby("pickup_city")["fare_amount"]
                .sum()
                .reset_index()
                .sort_values("fare_amount", ascending=False)
            )
            if not grouped_city.empty:
                fig_city = px.bar(
                    grouped_city,
                    x="pickup_city",
                    y="fare_amount",
                    labels={"pickup_city": "Pickup City", "fare_amount": "Total Fare"},
                    title="Total Revenue by Pickup City",
                )
                st.plotly_chart(fig_city, use_container_width=True)
            else:
                st.info("Not enough data for city chart yet.")

        # Trips by status
        with chart_col2:
            st.markdown("### Trips by Status")
            with chart_col2:
                st.markdown("### Trips by Status")
                status_counts = (
                    df_trips["status"]
                    .value_counts()
                    .reset_index(name="count")  # first col is 'status', second is 'count'
                    .rename(columns={"index": "status"})  # rename the index column to 'status'
                )

                if not status_counts.empty:
                    fig_status = px.bar(
                        status_counts,
                        x="status",
                        y="count",
                        labels={"status": "Status", "count": "Number of Trips"},
                        title="Trip Count by Status",
                    )
                    st.plotly_chart(fig_status, use_container_width=True)
                else:
                    st.info("Not enough data for status chart yet.")


        st.markdown("---")
        st.caption(
            f"Last updated: {datetime.now().isoformat()} • "
            f"Auto-refresh: every {update_interval} seconds"
        )

    time.sleep(update_interval)
