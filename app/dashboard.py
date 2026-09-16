"""Streamlit dashboard for the local DuckDB demonstration."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import streamlit as st

st.set_page_config(page_title="Customer Feedback Lakehouse", layout="wide")
st.title("Customer Feedback Lakehouse")
st.caption("Operational view of validated and enriched customer feedback")

database = Path(os.getenv("LAKEHOUSE_DB", "build/lakehouse.duckdb"))
if not database.exists():
    st.info("Run `make demo` to build the local lakehouse first.")
    st.stop()

connection = duckdb.connect(str(database), read_only=True)
counts = connection.execute(
    """
    select
      (select count(*) from bronze.feedback_events) as bronze,
      (select count(*) from silver.feedback) as silver,
      (select count(*) from quarantine.feedback_events) as quarantined,
      (select count(*) from silver.feedback where is_late) as late
    """
).fetchone()

for column, label, value in zip(
    st.columns(4),
    ["Bronze events", "Silver reviews", "Quarantined", "Late"],
    counts,
    strict=True,
):
    column.metric(label, value)

st.subheader("Product health")
st.dataframe(
    connection.execute(
        "select * from gold.product_health_daily order by event_date desc, product_id"
    ).df(),
    use_container_width=True,
    hide_index=True,
)

st.subheader("Aspect health")
aspects = connection.execute(
    "select * from gold.aspect_health_daily order by event_date desc, product_id, aspect"
).df()
st.dataframe(aspects, use_container_width=True, hide_index=True)
if not aspects.empty:
    chart = aspects.groupby("aspect", as_index=False)["average_sentiment"].mean()
    st.bar_chart(chart, x="aspect", y="average_sentiment")

st.subheader("Quarantine")
st.dataframe(
    connection.execute(
        "select event_hash, error_message, quarantined_at from quarantine.feedback_events"
    ).df(),
    use_container_width=True,
    hide_index=True,
)
