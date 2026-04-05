"""
dashboard.py — Streamlit dashboard for ScottishPower review insights.

Run with:
    streamlit run dashboard.py
"""

import json
import sqlite3
from collections import Counter
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from config import DB_PATH, TOPICS

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="ScottishPower App Reviews",
    page_icon="⚡",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


@st.cache_data(ttl=60)
def load_data():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    reviews = pd.read_sql_query(
        """
        SELECT r.review_id, r.source, r.author, r.rating, r.body,
               r.app_version, r.date_posted,
               i.topics, i.sentiment, i.insight
        FROM raw_reviews r
        LEFT JOIN insights i USING (review_id, source)
        WHERE r.is_processed = 1
        ORDER BY r.date_posted DESC
        """,
        conn,
    )
    conn.close()

    reviews["date_posted"] = pd.to_datetime(reviews["date_posted"])
    reviews["date"] = reviews["date_posted"].dt.date
    reviews["week"] = reviews["date_posted"].dt.to_period("W").apply(lambda p: p.start_time)
    reviews["topics_list"] = reviews["topics"].apply(
        lambda t: json.loads(t) if t else ["other"]
    )
    return reviews


df = load_data()

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------

st.sidebar.title("⚡ Filters")

sources = ["All"] + sorted(df["source"].unique().tolist())
selected_source = st.sidebar.selectbox("Store", sources)

sentiments = ["All", "positive", "neutral", "negative"]
selected_sentiment = st.sidebar.selectbox("Sentiment", sentiments)

ratings = ["All"] + [str(r) for r in sorted(df["rating"].dropna().unique().astype(int).tolist())]
selected_rating = st.sidebar.selectbox("Rating", ratings)

topic_options = TOPICS
selected_topics = st.sidebar.multiselect("Topics (any match)", topic_options)

min_date = df["date_posted"].min().date()
max_date = df["date_posted"].max().date()
date_range = st.sidebar.date_input("Date range", value=(min_date, max_date), min_value=min_date, max_value=max_date)

# Apply filters
filtered = df.copy()
if selected_source != "All":
    filtered = filtered[filtered["source"] == selected_source]
if selected_sentiment != "All":
    filtered = filtered[filtered["sentiment"] == selected_sentiment]
if selected_rating != "All":
    filtered = filtered[filtered["rating"] == int(selected_rating)]
if selected_topics:
    filtered = filtered[filtered["topics_list"].apply(lambda t: any(topic in t for topic in selected_topics))]
if len(date_range) == 2:
    start, end = date_range
    filtered = filtered[(filtered["date"].astype(str) >= str(start)) & (filtered["date"].astype(str) <= str(end))]

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("⚡ ScottishPower App — Review Intelligence")
st.caption(f"Data from {min_date} to {max_date} · {len(df)} total reviews analysed")

# ---------------------------------------------------------------------------
# KPI row
# ---------------------------------------------------------------------------

total = len(filtered)
positive = (filtered["sentiment"] == "positive").sum()
negative = (filtered["sentiment"] == "negative").sum()
avg_rating = filtered["rating"].mean()
pos_pct = (positive / total * 100) if total else 0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Reviews", f"{total:,}")
k2.metric("Positive", f"{positive:,}", f"{pos_pct:.0f}%")
k3.metric("Negative", f"{negative:,}", f"{(negative/total*100):.0f}%" if total else "0%")
k4.metric("Avg Rating", f"{avg_rating:.2f} ★" if not pd.isna(avg_rating) else "—")
k5.metric("Unanalysed", f"{len(df) - len(df.dropna(subset=['sentiment'])):,}")

st.divider()

# ---------------------------------------------------------------------------
# Row 1: Sentiment over time | Rating distribution
# ---------------------------------------------------------------------------

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Sentiment over time")
    weekly = (
        filtered.groupby(["week", "sentiment"])
        .size()
        .reset_index(name="count")
    )
    if not weekly.empty:
        fig = px.bar(
            weekly,
            x="week",
            y="count",
            color="sentiment",
            color_discrete_map={"positive": "#22c55e", "neutral": "#94a3b8", "negative": "#ef4444"},
            labels={"week": "", "count": "Reviews", "sentiment": "Sentiment"},
            barmode="stack",
        )
        fig.update_layout(margin=dict(t=10, b=0), height=300, legend=dict(orientation="h", y=1.1))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data for selected filters.")

with col2:
    st.subheader("Rating distribution")
    rating_counts = filtered["rating"].dropna().astype(int).value_counts().sort_index()
    if not rating_counts.empty:
        colours = ["#ef4444", "#f97316", "#eab308", "#84cc16", "#22c55e"]
        fig = go.Figure(go.Bar(
            x=[f"{r}★" for r in rating_counts.index],
            y=rating_counts.values,
            marker_color=[colours[r - 1] for r in rating_counts.index],
            text=rating_counts.values,
            textposition="outside",
        ))
        fig.update_layout(margin=dict(t=10, b=0), height=300, showlegend=False, yaxis_title="Reviews")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data.")

# ---------------------------------------------------------------------------
# Row 2: Topic breakdown | Source split
# ---------------------------------------------------------------------------

col3, col4 = st.columns([2, 1])

with col3:
    st.subheader("Topics breakdown")
    topic_rows = []
    for _, row in filtered.iterrows():
        for topic in row["topics_list"]:
            topic_rows.append({"topic": topic, "sentiment": row["sentiment"]})
    if topic_rows:
        topic_df = pd.DataFrame(topic_rows)
        topic_sentiment = (
            topic_df.groupby(["topic", "sentiment"])
            .size()
            .reset_index(name="count")
        )
        topic_totals = topic_df.groupby("topic").size().reset_index(name="total")
        topic_sentiment = topic_sentiment.merge(topic_totals, on="topic")
        topic_sentiment = topic_sentiment.sort_values("total", ascending=True)
        fig = px.bar(
            topic_sentiment,
            y="topic",
            x="count",
            color="sentiment",
            color_discrete_map={"positive": "#22c55e", "neutral": "#94a3b8", "negative": "#ef4444"},
            orientation="h",
            labels={"topic": "", "count": "Reviews", "sentiment": "Sentiment"},
            barmode="stack",
        )
        fig.update_layout(margin=dict(t=10, b=0), height=350, legend=dict(orientation="h", y=1.1))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data.")

with col4:
    st.subheader("Store split")
    source_counts = filtered["source"].value_counts()
    if not source_counts.empty:
        fig = px.pie(
            values=source_counts.values,
            names=source_counts.index,
            color_discrete_sequence=["#3b82f6", "#8b5cf6"],
            hole=0.45,
        )
        fig.update_layout(margin=dict(t=10, b=0), height=350, legend=dict(orientation="h", y=-0.1))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data.")

st.divider()

# ---------------------------------------------------------------------------
# Row 3: Review table
# ---------------------------------------------------------------------------

st.subheader(f"Reviews ({total:,})")

# Display columns
display_cols = ["date_posted", "source", "rating", "sentiment", "topics_list", "insight", "body"]
table = filtered[display_cols].rename(columns={
    "date_posted": "Date",
    "source": "Store",
    "rating": "★",
    "sentiment": "Sentiment",
    "topics_list": "Topics",
    "insight": "AI Insight",
    "body": "Review",
})
table["Date"] = table["Date"].dt.strftime("%Y-%m-%d")
table["Topics"] = table["Topics"].apply(lambda t: ", ".join(t))

st.dataframe(
    table,
    use_container_width=True,
    height=400,
    column_config={
        "★": st.column_config.NumberColumn(format="%d ★"),
        "Review": st.column_config.TextColumn(width="large"),
        "AI Insight": st.column_config.TextColumn(width="large"),
    },
)
