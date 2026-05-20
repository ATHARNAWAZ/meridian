"""
Meridian Streamlit Dashboard — 5 pages.

Pages:
  1. Live Overview       — auto-refresh every 5s, revenue + orders + fraud metrics
  2. Fraud Alerts        — CEP-detected fraud events with review workflow
  3. Quarantine Console  — quarantine-dq integration, replay button
  4. Iceberg Time Travel — snapshot browser and diff viewer
  5. Pipeline Health     — Flink job status, Kafka lag, dbt test results
"""

import sys
import time
from pathlib import Path

import streamlit as st

# Allow imports from repo root
sys.path.insert(0, str(Path(__file__).parent.parent))

from dashboard.style import inject_custom_css
from dashboard.pages import (
    page_live_overview,
    page_fraud_alerts,
    page_quarantine_console,
    page_iceberg_time_travel,
    page_pipeline_health,
)
from duckdb.query_layer import MeridianQueryLayer

# ─── Page config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Meridian Analytics",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

inject_custom_css()

# ─── Sidebar navigation ───────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## ⚡ Meridian")
    st.markdown("*Real-time e-commerce analytics*")
    st.divider()

    page = st.radio(
        "Navigate",
        options=[
            "📊 Live Overview",
            "🚨 Fraud Alerts",
            "🗑 Quarantine Console",
            "🕰 Iceberg Time Travel",
            "🔧 Pipeline Health",
        ],
        label_visibility="collapsed",
    )

    st.divider()
    st.markdown(
        "**Stack:** Kafka · Flink · Iceberg · dbt · DuckDB",
        help="Full real-time pipeline running locally via Docker Compose",
    )
    st.caption("github.com/ATHARNAWAZ/meridian")

# ─── Query layer (shared across pages) ────────────────────────────────────────

@st.cache_resource
def get_query_layer() -> MeridianQueryLayer:
    return MeridianQueryLayer()


ql = get_query_layer()

# ─── Route to page ────────────────────────────────────────────────────────────

if page == "📊 Live Overview":
    page_live_overview(ql)
elif page == "🚨 Fraud Alerts":
    page_fraud_alerts(ql)
elif page == "🗑 Quarantine Console":
    page_quarantine_console(ql)
elif page == "🕰 Iceberg Time Travel":
    page_iceberg_time_travel(ql)
elif page == "🔧 Pipeline Health":
    page_pipeline_health(ql)
