"""
dashboard.py — Streamlit dashboard for ScottishPower review insights.

Run with:
    streamlit run dashboard.py
"""

import json
import os
import sqlite3
from collections import Counter
from datetime import timedelta

import anthropic
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

DB_PATH = "reviews.db"
CSV_PATH = "data/reviews_analysed.csv"
TOPICS = [
    "login", "billing", "app_crash", "smart_meter",
    "customer_service", "outage", "account", "other",
]

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="ScottishPower App Reviews",
    page_icon="⚡",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Brand styling — extracted from ScottishPower index.css
# ---------------------------------------------------------------------------
# Colour tokens
#   --primary-1: #334b0e  (darkest green)
#   --primary-2: #486a14  (main brand green — used for active states, headers)
#   --primary-3: #5c881a  (mid green — hover / accent)
#   --bg-light:  #eff3e8  (very light green tint — sidebar / card backgrounds)
#   --text:      #2d2d2d  (default body text)
#   --subtle:    #5f6971  (secondary / caption text)
#   --border:    #d7d9db  (dividers, table borders)
#   --error:     #9d131f  (error / negative states)
# Typography: Lato (400 / 700) loaded from Google Fonts, fallback Roboto → Helvetica → Arial
# ---------------------------------------------------------------------------

st.markdown(
    """
    <link
      href="https://fonts.googleapis.com/css2?family=Lato:ital,wght@0,400;0,700;1,400;1,700&display=swap"
      rel="stylesheet"
    />
    <style>
      /* ── Global reset & font ─────────────────────────────────── */
      html, body, [class*="css"] {
        font-family: Lato, Roboto, Helvetica, Arial, sans-serif !important;
        color: #2d2d2d;
      }

      /* ── App background ──────────────────────────────────────── */
      .stApp {
        background-color: #ffffff;
      }

      /* ── Sidebar ─────────────────────────────────────────────── */
      [data-testid="stSidebar"] {
        background-color: #eff3e8;
        border-right: 1px solid #d7d9db;
      }
      [data-testid="stSidebar"] .stMarkdown p,
      [data-testid="stSidebar"] label,
      [data-testid="stSidebar"] span {
        color: #2d2d2d;
      }

      /* ── Page title (h1) ─────────────────────────────────────── */
      h1, .stTitle {
        font-size: 2.44140625rem !important;
        line-height: 3rem !important;
        font-weight: 700 !important;
        color: #334b0e !important;
      }

      /* ── Section subheadings (h2 / st.subheader) ─────────────── */
      h2, [data-testid="stHeadingWithActionElements"] h2 {
        font-size: 1.25rem !important;
        line-height: 2rem !important;
        font-weight: 700 !important;
        color: #486a14 !important;
      }

      /* ── h3 ──────────────────────────────────────────────────── */
      h3 {
        font-size: 1rem !important;
        line-height: 1.5rem !important;
        font-weight: 700 !important;
        color: #486a14 !important;
      }

      /* ── Caption / subtle text ───────────────────────────────── */
      .stCaption, [data-testid="stCaptionContainer"] p {
        color: #5f6971 !important;
        font-size: 0.8rem !important;
        line-height: 1rem !important;
      }

      /* ── Metric cards ────────────────────────────────────────── */
      [data-testid="stMetric"] {
        background-color: #eff3e8;
        border: 1px solid #d7d9db;
        border-radius: 4px;
        padding: 0.75rem 1rem;
      }
      [data-testid="stMetricLabel"] {
        color: #5f6971 !important;
        font-size: 0.8rem !important;
        font-weight: 400 !important;
      }
      [data-testid="stMetricValue"] {
        color: #334b0e !important;
        font-size: 1.953125rem !important;
        font-weight: 700 !important;
        line-height: 2.5rem !important;
      }
      [data-testid="stMetricDelta"] {
        color: #5c881a !important;
        font-size: 0.8rem !important;
      }

      /* ── Primary button ──────────────────────────────────────── */
      button[kind="primary"], .stButton > button[kind="primary"] {
        background-color: #486a14 !important;
        color: #ffffff !important;
        border: none !important;
        border-radius: 4px !important;
        font-family: Lato, Roboto, Helvetica, Arial, sans-serif !important;
        font-weight: 700 !important;
        padding: 0.25rem 1rem !important;
        min-height: 32px;
      }
      button[kind="primary"]:hover, .stButton > button[kind="primary"]:hover {
        background-color: #5c881a !important;
      }

      /* ── Secondary / default button ──────────────────────────── */
      .stButton > button {
        border-radius: 4px !important;
        font-family: Lato, Roboto, Helvetica, Arial, sans-serif !important;
        min-height: 32px;
      }

      /* ── Tabs ────────────────────────────────────────────────── */
      [data-testid="stTabs"] button {
        border-radius: 4px !important;
        font-family: Lato, Roboto, Helvetica, Arial, sans-serif !important;
        white-space: nowrap;
        min-height: 32px;
        padding: 0.25rem 1rem 0.25rem 0.75rem !important;
        gap: 0.5rem;
      }
      [data-testid="stTabs"] button:hover {
        background-color: #d7d9db !important;
      }
      [data-testid="stTabs"] button[aria-selected="true"] {
        background-color: #486a14 !important;
        color: #ffffff !important;
      }

      /* ── Dataframe / table ───────────────────────────────────── */
      [data-testid="stDataFrame"] thead th {
        background-color: #eff3e8 !important;
        color: #2d2d2d !important;
        font-weight: 700 !important;
        border-bottom: 1px solid #d7d9db !important;
      }
      [data-testid="stDataFrame"] td {
        color: #2d2d2d !important;
        font-size: 1rem !important;
        line-height: 1.5rem !important;
        padding: 0.75rem !important;
        border-bottom: 1px solid #d7d9db !important;
      }

      /* ── Divider ─────────────────────────────────────────────── */
      hr {
        border-color: #d7d9db !important;
      }

      /* ── Info / alert boxes ──────────────────────────────────── */
      [data-testid="stAlert"] {
        border-radius: 4px !important;
        border-left: 4px solid #486a14 !important;
        background-color: #eff3e8 !important;
        color: #2d2d2d !important;
      }

      /* ── Select / multiselect ────────────────────────────────── */
      [data-testid="stSelectbox"] > div > div,
      [data-testid="stMultiSelect"] > div > div {
        border-radius: 4px !important;
        border-color: #d7d9db !important;
      }
      [data-testid="stSelectbox"] > div > div:focus-within,
      [data-testid="stMultiSelect"] > div > div:focus-within {
        border-color: #486a14 !important;
        box-shadow: 0 0 0 2px rgba(72, 106, 20, 0.2) !important;
      }

      /* ── Spinner ─────────────────────────────────────────────── */
      [data-testid="stSpinner"] svg {
        stroke: #486a14 !important;
      }

      /* ── Tooltip shadow (matches design system) ──────────────── */
      [data-testid="stTooltipIcon"] {
        color: #5f6971;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


@st.cache_data(ttl=60)
def load_data():
    if os.path.exists(DB_PATH):
        conn = sqlite3.connect(DB_PATH)
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
    else:
        reviews = pd.read_csv(CSV_PATH)

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
# Executive Summary
# ---------------------------------------------------------------------------


def period_stats(frame: pd.DataFrame) -> dict:
    """Compute aggregated stats for a slice of reviews."""
    if frame.empty:
        return {}

    topic_counts: Counter = Counter()
    topic_sentiment: dict[str, Counter] = {t: Counter() for t in TOPICS}
    for _, row in frame.iterrows():
        for t in row["topics_list"]:
            topic_counts[t] += 1
            if row["sentiment"]:
                topic_sentiment.get(t, Counter())[row["sentiment"]] += 1

    sentiment_counts = frame["sentiment"].value_counts().to_dict()
    avg_r = frame["rating"].mean()

    # Sample up to 30 insights — prioritise negative ones for richer signal
    neg = frame[frame["sentiment"] == "negative"]["insight"].dropna().tolist()
    pos = frame[frame["sentiment"] == "positive"]["insight"].dropna().tolist()
    sample_insights = (neg[:20] + pos[:10])[:30]

    return {
        "total": len(frame),
        "avg_rating": round(float(avg_r), 2) if not pd.isna(avg_r) else None,
        "sentiment": sentiment_counts,
        "topics": dict(topic_counts.most_common()),
        "sample_insights": sample_insights,
    }


def build_summary_prompt(current: dict, prior: dict, current_range: str, prior_range: str) -> str:
    def fmt(stats: dict) -> str:
        if not stats:
            return "  No data available."
        lines = [
            f"  Reviews: {stats['total']}",
            f"  Avg rating: {stats['avg_rating'] or 'N/A'}",
            f"  Sentiment: {stats['sentiment']}",
            f"  Topics (by mention count): {stats['topics']}",
            f"  Sample insights (up to 30):",
        ]
        for ins in stats.get("sample_insights", []):
            lines.append(f"    - {ins}")
        return "\n".join(lines)

    return f"""You are an analyst for the ScottishPower digital team. Write a concise executive summary of app review data.

CURRENT PERIOD ({current_range}):
{fmt(current)}

PRIOR PERIOD ({prior_range}):
{fmt(prior)}

Write the summary in this structure:
1. **Overview** — 2–3 sentences on overall volume, sentiment, and average rating vs prior period.
2. **Top topics** — for the 3–4 most-mentioned topics, describe what users are saying and whether sentiment is positive or negative.
3. **Key issues** — bullet list of the most significant problems users raised, with specific detail from the insights.
4. **Notable changes** — what has improved or worsened compared to the prior period. Be specific about which topics shifted.
5. **Recommendation** — one or two actionable priorities for the product/support team.

Be direct and specific. Use concrete numbers from the data. Do not pad with generic statements."""


def get_api_key() -> str:
    # Streamlit Cloud secrets take priority, then environment variable
    try:
        return st.secrets["ANTHROPIC_API_KEY"]
    except (KeyError, FileNotFoundError):
        return os.environ.get("ANTHROPIC_API_KEY", "")


st.subheader("Executive Summary")

# Derive the prior period (same length, immediately before the selected range)
if len(date_range) == 2:
    sel_start, sel_end = date_range
    period_days = (sel_end - sel_start).days or 1
    prior_end = sel_start - timedelta(days=1)
    prior_start = prior_end - timedelta(days=period_days - 1)
    current_range_str = f"{sel_start} to {sel_end}"
    prior_range_str = f"{prior_start} to {prior_end}"

    prior_slice = df[
        (df["date"].astype(str) >= str(prior_start)) &
        (df["date"].astype(str) <= str(prior_end))
    ]
    # Apply same non-date filters to prior slice
    if selected_source != "All":
        prior_slice = prior_slice[prior_slice["source"] == selected_source]
    if selected_topics:
        prior_slice = prior_slice[prior_slice["topics_list"].apply(
            lambda t: any(topic in t for topic in selected_topics)
        )]
else:
    current_range_str = "selected period"
    prior_range_str = "prior period"
    prior_slice = pd.DataFrame()

api_key = get_api_key()

if not api_key:
    st.info("Set `ANTHROPIC_API_KEY` in your environment or Streamlit secrets to enable AI summaries.")
elif total == 0:
    st.info("No reviews in the selected date range.")
else:
    st.caption(
        f"Comparing **{current_range_str}** ({total} reviews) "
        f"against prior period **{prior_range_str}** ({len(prior_slice)} reviews)"
    )
    if st.button("Generate Executive Summary", type="primary"):
        current_stats = period_stats(filtered)
        prior_stats = period_stats(prior_slice)
        prompt = build_summary_prompt(current_stats, prior_stats, current_range_str, prior_range_str)

        client = anthropic.Anthropic(api_key=api_key)
        with st.spinner("Generating summary…"):
            summary_placeholder = st.empty()
            full_text = ""
            with client.messages.stream(
                model="claude-haiku-4-5-20251001",
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}],
            ) as stream:
                for text in stream.text_stream:
                    full_text += text
                    summary_placeholder.markdown(full_text + "▌")
            summary_placeholder.markdown(full_text)

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
