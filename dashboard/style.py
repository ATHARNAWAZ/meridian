"""Custom CSS for the Meridian dashboard."""

import streamlit as st


def inject_custom_css() -> None:
    st.markdown(
        """
        <style>
        /* Dark sidebar */
        [data-testid="stSidebar"] {
            background-color: #0f1117;
            border-right: 1px solid #262730;
        }
        [data-testid="stSidebar"] .stMarkdown,
        [data-testid="stSidebar"] label,
        [data-testid="stSidebar"] .stCaption {
            color: #c9d1d9 !important;
        }

        /* Metric cards */
        [data-testid="stMetric"] {
            background: #1a1d27;
            border: 1px solid #30363d;
            border-radius: 8px;
            padding: 1rem 1.25rem;
        }
        [data-testid="stMetricLabel"] {
            color: #8b949e;
            font-size: 0.8rem;
            font-weight: 500;
            letter-spacing: 0.05em;
            text-transform: uppercase;
        }
        [data-testid="stMetricValue"] {
            color: #f0f6fc;
            font-size: 1.75rem;
            font-weight: 700;
        }
        [data-testid="stMetricDelta"] {
            font-size: 0.85rem;
        }

        /* Page title */
        h1 {
            color: #f0f6fc;
            font-size: 1.6rem;
            font-weight: 700;
            border-bottom: 2px solid #21262d;
            padding-bottom: 0.5rem;
            margin-bottom: 1.5rem;
        }
        h2, h3 {
            color: #e6edf3;
        }

        /* Dataframe */
        [data-testid="stDataFrame"] {
            border: 1px solid #30363d;
            border-radius: 6px;
        }

        /* Divider */
        hr {
            border-color: #21262d;
        }

        /* Alert / info boxes */
        [data-testid="stAlert"] {
            border-radius: 6px;
        }

        /* Main background */
        .main .block-container {
            background-color: #0d1117;
            padding-top: 1.5rem;
        }

        /* Tabs */
        [data-testid="stTab"] {
            background: #161b22;
            border-radius: 4px;
        }

        /* Button styling */
        .stButton > button {
            background-color: #238636;
            color: #ffffff;
            border: none;
            border-radius: 6px;
            font-weight: 500;
        }
        .stButton > button:hover {
            background-color: #2ea043;
        }

        /* Streamlit default theme override */
        body {
            background-color: #0d1117;
            color: #e6edf3;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
