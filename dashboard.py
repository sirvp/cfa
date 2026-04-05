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

# Brand colours and font are set via .streamlit/config.toml
# primaryColor=#486a14  backgroundColor=#ffffff  secondaryBackgroundColor=#eff3e8  textColor=#2d2d2d

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
default_start = max(min_date, max_date - timedelta(days=6))  # last 7 days by default
date_range = st.sidebar.date_input("Date range", value=(default_start, max_date), min_value=min_date, max_value=max_date)

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

st.title("Acorn — Customer Insights")
st.caption(f"Data from {min_date} to {max_date} · {len(df)} total reviews analysed")

# ---------------------------------------------------------------------------
# KPI row
# ---------------------------------------------------------------------------

total = len(filtered)
positive = (filtered["sentiment"] == "positive").sum()
negative = (filtered["sentiment"] == "negative").sum()
avg_rating = filtered["rating"].mean()
pos_pct = (positive / total * 100) if total else 0

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Reviews", f"{total:,}")

neg_pct = (negative / total * 100) if total else 0
k2.markdown(
    f"<p style='font-size:0.875rem;color:#5f6971;margin:0'>Positive</p>"
    f"<p style='font-size:1.95rem;font-weight:700;color:#16a34a;margin:0;line-height:1.2'>{positive:,}</p>"
    f"<p style='font-size:0.8rem;color:#16a34a;margin:0'>↑ {pos_pct:.0f}%</p>",
    unsafe_allow_html=True,
)
k3.markdown(
    f"<p style='font-size:0.875rem;color:#5f6971;margin:0'>Negative</p>"
    f"<p style='font-size:1.95rem;font-weight:700;color:#dc2626;margin:0;line-height:1.2'>{negative:,}</p>"
    f"<p style='font-size:0.8rem;color:#dc2626;margin:0'>↑ {neg_pct:.0f}%</p>",
    unsafe_allow_html=True,
)
k4.metric("Avg Rating", f"{avg_rating:.2f} ★" if not pd.isna(avg_rating) else "—")

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
    """Build the prompt for the AI executive summary, requesting structured JSON output."""
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

    return f"""You are an analyst for the ScottishPower digital team. Return ONLY a JSON object — no prose, no markdown fences.

CURRENT PERIOD ({current_range}):
{fmt(current)}

PRIOR PERIOD ({prior_range}):
{fmt(prior)}

Return exactly this JSON shape:
{{
  "snapshot": "X reviews · Y★ avg · Z% negative",
  "vs_prior": "one short phrase on volume/sentiment change vs prior, e.g. +145% volume, negative up 24%→63%",
  "top_issues": [
    {{"topic": "Login", "count": 110, "detail": "max 8 words on what users say"}},
    {{"topic": "Billing", "count": 85, "detail": "max 8 words on what users say"}},
    {{"topic": "App Crash", "count": 81, "detail": "max 8 words on what users say"}}
  ],
  "key_change": "one sentence on the single most significant shift vs prior period",
  "recommendation": "one actionable sentence for the product team"
}}

Include the top 3 topics by mention count. Keep all strings terse."""


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

# Cache key: regenerate only when the filtered dataset or period changes
_summary_cache_key = f"{current_range_str}|{total}|{len(prior_slice)}"

def render_summary_card(data: dict) -> None:
    """Render the structured JSON summary as an infographic card."""
    snap_col, change_col = st.columns([3, 2])
    with snap_col:
        st.markdown(
            f"<div style='font-size:1.1rem;font-weight:700;color:#2d2d2d'>{data.get('snapshot','')}</div>",
            unsafe_allow_html=True,
        )
    with change_col:
        st.markdown(
            f"<div style='font-size:0.85rem;color:#5f6971;text-align:right;padding-top:4px'>{data.get('vs_prior','')}</div>",
            unsafe_allow_html=True,
        )

    PILL_COLOURS = ["#9d131f", "#c0392b", "#e67e22"]
    issues = data.get("top_issues", [])
    pills_html = "<div style='display:flex;gap:8px;flex-wrap:wrap;margin:10px 0'>"
    for i, issue in enumerate(issues):
        bg = PILL_COLOURS[i % len(PILL_COLOURS)]
        pills_html += (
            f"<div style='background:{bg};color:#fff;border-radius:4px;"
            f"padding:6px 12px;font-size:0.82rem;line-height:1.3'>"
            f"<strong>{issue.get('topic','')} ({issue.get('count','')})</strong>"
            f"<br>{issue.get('detail','')}</div>"
        )
    pills_html += "</div>"
    st.markdown(pills_html, unsafe_allow_html=True)

    left, right = st.columns(2)
    with left:
        st.markdown(
            f"<div style='background:#fff3cd;border-left:4px solid #e6a817;"
            f"border-radius:4px;padding:10px 14px;font-size:0.85rem;color:#2d2d2d'>"
            f"⚠️ <strong>Key change</strong><br>{data.get('key_change','')}</div>",
            unsafe_allow_html=True,
        )
    with right:
        st.markdown(
            f"<div style='background:#eff3e8;border-left:4px solid #486a14;"
            f"border-radius:4px;padding:10px 14px;font-size:0.85rem;color:#2d2d2d'>"
            f"💡 <strong>Recommendation</strong><br>{data.get('recommendation','')}</div>",
            unsafe_allow_html=True,
        )


if not api_key:
    st.info("Set `ANTHROPIC_API_KEY` in your environment or Streamlit secrets to enable AI summaries.")
elif total == 0:
    st.info("No reviews in the selected date range.")
else:
    st.caption(
        f"Comparing **{current_range_str}** ({total} reviews) "
        f"against prior period **{prior_range_str}** ({len(prior_slice)} reviews)"
    )

    # Auto-generate on first load or when the period/data changes; button forces refresh
    if (
        "summary_data" not in st.session_state
        or st.session_state.get("summary_cache_key") != _summary_cache_key
    ):
        current_stats = period_stats(filtered)
        prior_stats = period_stats(prior_slice)
        prompt = build_summary_prompt(current_stats, prior_stats, current_range_str, prior_range_str)
        client = anthropic.Anthropic(api_key=api_key)
        with st.spinner("Generating summary…"):
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=400,
                messages=[{"role": "user", "content": prompt}],
            )
        raw = msg.content[0].text.strip()
        import re as _re
        _match = _re.search(r"\{.*\}", raw, _re.DOTALL)
        try:
            st.session_state["summary_data"] = json.loads(_match.group(0) if _match else raw)
            st.session_state["summary_cache_key"] = _summary_cache_key
        except Exception:
            st.markdown(raw)
            st.session_state.pop("summary_data", None)

    if st.button("↺ Refresh", type="secondary"):
        st.session_state.pop("summary_data", None)
        st.rerun()

    if "summary_data" in st.session_state:
        render_summary_card(st.session_state["summary_data"])

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

st.divider()
st.markdown("<p style='text-align:center; color:gray; font-size:0.85rem;'>Made by Vishnu Prasad</p>", unsafe_allow_html=True)
