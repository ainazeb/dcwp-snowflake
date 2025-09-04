# app/streamlit_app.py

import os
import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

try:
    session = get_active_session()
except Exception:
    from snowflake.snowpark import Session
    conn = {
        "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
        "user":      os.getenv("SNOWFLAKE_USER"),
        "password":  os.getenv("SNOWFLAKE_PASSWORD"),
        "role":      os.getenv("SNOWFLAKE_ROLE", "DEVELOPER"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "WH_DEV"),
        "database":  os.getenv("SNOWFLAKE_DATABASE", "DCWP_ANALYTICS"),
        "schema":    os.getenv("SNOWFLAKE_SCHEMA", "MART"),
    }
    session = Session.builder.configs(conn).create()


st.set_page_config(page_title="DCWP Explorer", layout="wide")
st.title("📊 DCWP Complaints – Interactive Explorer")

session = get_active_session()

st.write(session.sql("select current_role(), current_warehouse(), current_database(), current_schema()").to_pandas())


@st.cache_data(ttl=600)
def load_summary():
    df = session.table("DCWP_ANALYTICS.MART.DCWP_SUMMARY").to_pandas()
    return df


@st.cache_data(ttl=600)
def load_l1_sample(max_rows=10000):
    df = (session.table("DCWP_ANALYTICS.STAGING.DCWP_L1")
          .select("INDUSTRY", "COMPLAINT_TYPE", "MEDIATION_START_DATE",
                  "RESTITUTION", "BUSINESS_CITY", "BUSINESS_STATE",
                  "BUSINESS_ZIP", "LONGITUDE", "LATITUDE")
          .limit(max_rows)
          .to_pandas())
    return df


summary = load_summary()
l1 = load_l1_sample()

with st.sidebar:
    st.header("Filters")
    industries = sorted(summary["INDUSTRY"].dropna().unique().tolist())
    selected_ind = st.multiselect("Industry", industries, default=industries[:5])

    c_types = sorted(summary["COMPLAINT_TYPE"].dropna().unique().tolist())
    selected_ct = st.multiselect("Complaint Type", c_types, default=c_types[:5])

    months = pd.to_datetime(summary["MONTH"]).sort_values().unique()
    m_from, m_to = st.select_slider(
        "Month range",
        options=months,
        value=(months[0], months[-1]),
        format_func=lambda x: pd.to_datetime(x).strftime("%Y-%m")
    )

summary["MONTH"] = pd.to_datetime(summary["MONTH"])
mask = (
        summary["INDUSTRY"].isin(selected_ind) &
        summary["COMPLAINT_TYPE"].isin(selected_ct) &
        (summary["MONTH"] >= m_from) &
        (summary["MONTH"] <= m_to)
)
f = summary.loc[mask].copy()

c1, c2, c3 = st.columns(3)
c1.metric("Total rows", f["CNT"].sum())
c2.metric("Avg Restitution", f["AVG_RESTITUTION"].mean().round(2) if not f.empty else 0)
c3.metric("Distinct industries", f["INDUSTRY"].nunique())

st.subheader("Trend by Month")
trend = (f.groupby("MONTH", as_index=False)
         .agg(CNT=("CNT", "sum"), AVG_REST=("AVG_RESTITUTION", "mean")))
chart = (alt.Chart(trend).mark_line(point=True)
         .encode(x="MONTH:T", y="CNT:Q", tooltip=["MONTH", "CNT", "AVG_REST"]))
st.altair_chart(chart, use_container_width=True)

st.subheader("Heatmap – Industry × Month")
pivot = (f.pivot_table(index="INDUSTRY", columns="MONTH", values="CNT", aggfunc="sum")
         .fillna(0).reset_index())
st.dataframe(pivot, use_container_width=True)

st.subheader("Map (sample points)")

map_df = l1.dropna(subset=["LONGITUDE", "LATITUDE"]).copy()
map_df = map_df[
    (map_df["INDUSTRY"].isin(selected_ind)) &
    (map_df["COMPLAINT_TYPE"].isin(selected_ct))
    ]
st.map(map_df.rename(columns={"LATITUDE": "lat", "LONGITUDE": "lon"})[["lat", "lon"]], zoom=10)

st.subheader("Top Complaint Types (filtered)")
top = (f.groupby("COMPLAINT_TYPE", as_index=False)
       .agg(TOTAL=("CNT", "sum"))
       .sort_values("TOTAL", ascending=False)
       .head(20))
st.dataframe(top, use_container_width=True)


st.download_button(
    "Download filtered summary (CSV)",
    data=f.to_csv(index=False).encode("utf-8"),
    file_name="dcwp_filtered_summary.csv",
    mime="text/csv"
)


