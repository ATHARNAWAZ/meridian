"""
All 5 dashboard pages for Meridian.
Each page function receives the MeridianQueryLayer instance.
"""

import subprocess
import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

from duckdb_layer.query_layer import MeridianQueryLayer

# ─── Page 1: Live Overview ────────────────────────────────────────────────────

def page_live_overview(ql: MeridianQueryLayer) -> None:
    st.title("📊 Live Overview")

    # Auto-refresh control
    col_refresh, col_interval = st.columns([3, 1])
    with col_refresh:
        auto_refresh = st.checkbox("Auto-refresh (5s)", value=True)
    with col_interval:
        refresh_interval = st.selectbox("Interval", [5, 10, 30], index=0, label_visibility="collapsed")

    # ── Metric cards ──────────────────────────────────────────────────────────
    metrics = ql.get_hourly_metrics()
    quarantine_df = ql.get_quarantine_stats()
    q_count_today = 0
    if not quarantine_df.empty:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        q_count_today = len(quarantine_df[quarantine_df.get("created_date", pd.Series()) == today]) if "created_date" in quarantine_df.columns else len(quarantine_df)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Revenue (last hour)", f"€{metrics['revenue']:,.0f}")
    with c2:
        st.metric("Orders (last hour)", f"{metrics['order_count']:,}")
    with c3:
        st.metric("Fraud Alerts Today", f"{metrics['fraud_count']:,}", delta=None)
    with c4:
        st.metric("Quarantined Events", f"{q_count_today:,}", delta=None)

    st.divider()

    # ── Revenue per minute chart ──────────────────────────────────────────────
    col_chart1, col_chart2 = st.columns([2, 1])

    with col_chart1:
        st.subheader("Revenue per minute (last 30m)")
        rpm_df = ql.get_revenue_per_minute(last_n_minutes=30)
        if not rpm_df.empty:
            fig = px.line(
                rpm_df,
                x="minute",
                y="revenue",
                title=None,
                labels={"minute": "", "revenue": "Revenue (€)"},
                color_discrete_sequence=["#58a6ff"],
            )
            fig.update_layout(
                plot_bgcolor="#0d1117",
                paper_bgcolor="#1a1d27",
                font_color="#e6edf3",
                xaxis=dict(gridcolor="#21262d"),
                yaxis=dict(gridcolor="#21262d"),
                margin=dict(l=0, r=0, t=10, b=0),
                height=300,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No revenue data yet. Start the producer: `make producer`")

    with col_chart2:
        st.subheader("Orders by category (last hour)")
        live_df = ql.get_live_revenue(window_minutes=60)
        if not live_df.empty and "category" in live_df.columns:
            cat_df = live_df.groupby("category").size().reset_index(name="count")
            fig2 = px.bar(
                cat_df.sort_values("count", ascending=True),
                x="count",
                y="category",
                orientation="h",
                color_discrete_sequence=["#3fb950"],
                labels={"count": "Orders", "category": ""},
            )
            fig2.update_layout(
                plot_bgcolor="#0d1117",
                paper_bgcolor="#1a1d27",
                font_color="#e6edf3",
                margin=dict(l=0, r=0, t=10, b=0),
                height=300,
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Waiting for data...")

    # ── Live events table ─────────────────────────────────────────────────────
    st.subheader("Live events (last 20)")
    events_df = ql.get_recent_events(limit=20)
    if not events_df.empty:
        st.dataframe(
            events_df,
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No events yet. Run `make producer` to start generating data.")

    # Auto-refresh
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()


# ─── Page 2: Fraud Alerts ─────────────────────────────────────────────────────

def page_fraud_alerts(ql: MeridianQueryLayer) -> None:
    st.title("🚨 Fraud Alerts")

    col_filter, col_toggle = st.columns([2, 1])
    with col_toggle:
        show_reviewed = st.toggle("Show reviewed", value=False)

    fraud_df = ql.get_fraud_alerts(limit=200)

    if fraud_df.empty:
        st.info("No fraud alerts yet. The fraud detector will populate this as orders arrive.")
        return

    # ── Scatter: fraud amount vs time of day ──────────────────────────────────
    if "detected_at" in fraud_df.columns and "amount" in fraud_df.columns:
        fraud_df["hour_of_day"] = pd.to_datetime(fraud_df["detected_at"]).dt.hour
        fig = px.scatter(
            fraud_df,
            x="hour_of_day",
            y="amount",
            color="category" if "category" in fraud_df.columns else None,
            title="Fraud amount by hour of day",
            labels={"hour_of_day": "Hour (UTC)", "amount": "Order Amount (€)"},
        )
        fig.update_layout(
            plot_bgcolor="#0d1117",
            paper_bgcolor="#1a1d27",
            font_color="#e6edf3",
            xaxis=dict(gridcolor="#21262d", tickmode="linear", dtick=1),
            yaxis=dict(gridcolor="#21262d"),
            height=300,
        )
        st.plotly_chart(fig, use_container_width=True)

    st.subheader(f"Fraud events ({len(fraud_df)} total)")

    # ── Review state (stored in session_state) ────────────────────────────────
    if "reviewed_orders" not in st.session_state:
        st.session_state.reviewed_orders = set()

    display_df = fraud_df.copy()
    display_df["reviewed"] = display_df["order_id"].isin(st.session_state.reviewed_orders)

    if not show_reviewed:
        display_df = display_df[~display_df["reviewed"]]

    if display_df.empty:
        st.success("All fraud alerts have been reviewed.")
        return

    for _, row in display_df.head(50).iterrows():
        with st.expander(f"🚨 {row.get('order_id', 'N/A')[:16]}… | {row.get('user_id', '')} | €{row.get('amount', 0):.2f}"):
            cols = st.columns(4)
            cols[0].metric("Amount", f"€{row.get('amount', 0):.2f}")
            cols[1].metric("Category", row.get("category", "—"))
            cols[2].metric("Country", row.get("country", "—"))
            cols[3].metric("User", row.get("user_id", "—"))

            st.caption(f"Detected at: {row.get('detected_at', 'N/A')}")

            reviewed = row.get("order_id") in st.session_state.reviewed_orders
            if not reviewed:
                if st.button("✓ Mark reviewed", key=f"review_{row.get('order_id')}"):
                    st.session_state.reviewed_orders.add(row.get("order_id"))
                    st.rerun()
            else:
                st.success("Reviewed")


# ─── Page 3: Quarantine Console ───────────────────────────────────────────────

def page_quarantine_console(ql: MeridianQueryLayer) -> None:
    st.title("🗑 Quarantine Console")
    st.caption("quarantine-dq — bad events caught, stored, and replayed")

    qdf = ql.get_quarantine_stats()

    if qdf.empty:
        st.info(
            "No quarantined events yet.\n\n"
            "Start the producer with `make producer` — bad events (null IDs, "
            "negative amounts, invalid currencies) will appear here automatically."
        )
        return

    # ── Counts bar chart ──────────────────────────────────────────────────────
    if "failure_type" in qdf.columns:
        counts = qdf["failure_type"].value_counts().reset_index()
        counts.columns = ["failure_type", "count"]
        fig = px.bar(
            counts,
            x="failure_type",
            y="count",
            color="failure_type",
            title="Quarantined events by failure type",
            labels={"failure_type": "Failure Type", "count": "Count"},
            color_discrete_sequence=px.colors.qualitative.Pastel,
        )
        fig.update_layout(
            plot_bgcolor="#0d1117",
            paper_bgcolor="#1a1d27",
            font_color="#e6edf3",
            showlegend=False,
            height=280,
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Filters ───────────────────────────────────────────────────────────────
    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        pipelines = ["All"] + sorted(qdf["pipeline"].unique().tolist()) if "pipeline" in qdf.columns else ["All"]
        sel_pipeline = st.selectbox("Pipeline", pipelines)
    with col_f2:
        failure_types = ["All"] + sorted(qdf["failure_type"].unique().tolist()) if "failure_type" in qdf.columns else ["All"]
        sel_type = st.selectbox("Failure Type", failure_types)
    with col_f3:
        dates = ["All"] + sorted(qdf["created_date"].unique().tolist(), reverse=True) if "created_date" in qdf.columns else ["All"]
        sel_date = st.selectbox("Date", dates)

    filtered = qdf.copy()
    if sel_pipeline != "All" and "pipeline" in filtered.columns:
        filtered = filtered[filtered["pipeline"] == sel_pipeline]
    if sel_type != "All" and "failure_type" in filtered.columns:
        filtered = filtered[filtered["failure_type"] == sel_type]
    if sel_date != "All" and "created_date" in filtered.columns:
        filtered = filtered[filtered["created_date"] == sel_date]

    st.subheader(f"Quarantined records ({len(filtered)})")
    if not filtered.empty:
        st.dataframe(filtered, use_container_width=True, hide_index=True)

    st.divider()

    # ── Replay button ─────────────────────────────────────────────────────────
    replayable_count = len(qdf[qdf["replayable"] == True]) if "replayable" in qdf.columns else 0
    col_btn, col_info = st.columns([1, 3])
    with col_btn:
        if st.button(f"▶ Replay all replayable ({replayable_count})", type="primary"):
            with st.spinner("Running replay job..."):
                try:
                    result = subprocess.run(
                        ["python", "quarantine_int/replay_job.py", "--pipeline", "meridian"],
                        capture_output=True,
                        text=True,
                        timeout=60,
                    )
                    if result.returncode == 0:
                        st.success("Replay complete!")
                        st.code(result.stdout[-2000:] if result.stdout else "(no output)")
                    else:
                        st.error(f"Replay failed:\n{result.stderr[-1000:]}")
                except Exception as e:
                    st.error(f"Could not run replay job: {e}")
    with col_info:
        st.caption(
            "Replay re-validates SCHEMA_VIOLATION records against the current Pydantic "
            "schema and re-produces them to Kafka if they're now valid. "
            "LATE_ARRIVAL records are written directly to Iceberg."
        )


# ─── Page 4: Iceberg Time Travel ──────────────────────────────────────────────

def page_iceberg_time_travel(ql: MeridianQueryLayer) -> None:
    st.title("🕰 Iceberg Time Travel")
    st.caption(
        "Every write creates an immutable snapshot. Query any historical state. "
        "This is Iceberg's killer feature."
    )

    table_names = [
        "meridian.orders",
        "meridian.clicks",
        "meridian.payments",
        "meridian.inventory",
        "meridian.returns",
    ]

    col_table, col_action = st.columns([2, 2])
    with col_table:
        selected_table = st.selectbox("Select table", table_names)

    snapshots_df = ql.get_iceberg_snapshots(selected_table)

    if snapshots_df.empty:
        st.info(
            f"No snapshots found for `{selected_table}`. "
            "This means no data has been written to this table yet. "
            "Start the full pipeline with `make demo` to populate it."
        )
        return

    # ── Snapshot list ─────────────────────────────────────────────────────────
    st.subheader(f"Snapshots — {selected_table}")
    st.dataframe(snapshots_df, use_container_width=True, hide_index=True)

    # ── Query at snapshot ─────────────────────────────────────────────────────
    snap_ids = snapshots_df["snapshot_id"].tolist() if "snapshot_id" in snapshots_df.columns else []

    if snap_ids:
        col_snap, col_btn = st.columns([2, 1])
        with col_snap:
            selected_snap = st.selectbox(
                "Query at snapshot",
                snap_ids,
                format_func=lambda sid: f"Snapshot {sid}"
                + (f" — {snapshots_df[snapshots_df['snapshot_id']==sid]['timestamp_iso'].iloc[0]}"
                   if 'timestamp_iso' in snapshots_df.columns else ""),
            )
        with col_btn:
            if st.button("⏱ Query snapshot"):
                with st.spinner("Reading historical snapshot from Iceberg..."):
                    try:
                        from iceberg.time_travel import query_at
                        arrow_table = query_at(selected_table, selected_snap)
                        if arrow_table is not None:
                            df = arrow_table.to_pandas()
                            st.success(f"Snapshot {selected_snap}: {len(df):,} rows")
                            st.dataframe(df.head(100), use_container_width=True, hide_index=True)
                        else:
                            st.warning("No data returned for this snapshot.")
                    except Exception as e:
                        st.error(f"Time travel query failed: {e}")

    # ── Snapshot diff ─────────────────────────────────────────────────────────
    if len(snap_ids) >= 2:
        st.divider()
        st.subheader("Snapshot diff")
        col_s1, col_s2, col_diff = st.columns([2, 2, 1])
        with col_s1:
            snap_before = st.selectbox("Before", snap_ids[:-1], key="snap_before")
        with col_s2:
            snap_after = st.selectbox("After", snap_ids[1:], key="snap_after")
        with col_diff:
            st.write("")
            if st.button("Compare"):
                with st.spinner("Computing diff..."):
                    try:
                        from iceberg.time_travel import diff_snapshots
                        diff = diff_snapshots(selected_table, snap_before, snap_after)
                        if "error" not in diff:
                            cols = st.columns(4)
                            cols[0].metric("Rows before", f"{diff['rows_before']:,}")
                            cols[1].metric("Rows after", f"{diff['rows_after']:,}")
                            cols[2].metric("Added", f"+{diff['rows_added']:,}", delta=diff['rows_added'])
                            cols[3].metric("Removed", f"-{diff['rows_removed']:,}", delta=-diff['rows_removed'])
                        else:
                            st.error(diff["error"])
                    except Exception as e:
                        st.error(f"Diff failed: {e}")


# ─── Page 5: Pipeline Health ──────────────────────────────────────────────────

def page_pipeline_health(ql: MeridianQueryLayer) -> None:
    st.title("🔧 Pipeline Health")

    flink_url = "http://localhost:8082"

    # ── Flink job status ──────────────────────────────────────────────────────
    st.subheader("Flink Jobs")
    try:
        resp = requests.get(f"{flink_url}/jobs", timeout=3)
        if resp.ok:
            jobs = resp.json().get("jobs", [])
            if jobs:
                jobs_df = pd.DataFrame(jobs)
                st.dataframe(jobs_df, use_container_width=True, hide_index=True)
            else:
                st.warning("Flink is running but no jobs submitted yet. Run `make jobs`.")
        else:
            st.error(f"Flink API returned {resp.status_code}")
    except requests.ConnectionError:
        st.warning("Flink JobManager unreachable. Is it running? (`make up`)")

    # ── Kafka consumer lag ────────────────────────────────────────────────────
    st.subheader("Kafka Consumer Groups")
    try:
        from kafka import KafkaAdminClient
        from kafka.errors import KafkaError
        admin = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id="meridian-dashboard")
        groups = admin.list_consumer_groups()
        if groups:
            group_names = [g[0] for g in groups]
            st.write(f"Active consumer groups: {', '.join(group_names)}")

            lag_rows = []
            for group_id, _ in groups:
                try:
                    offsets = admin.list_consumer_group_offsets(group_id)
                    for tp, offset_info in offsets.items():
                        lag_rows.append({
                            "group_id": group_id,
                            "topic": tp.topic,
                            "partition": tp.partition,
                            "committed_offset": offset_info.offset,
                        })
                except Exception:
                    pass
            if lag_rows:
                st.dataframe(pd.DataFrame(lag_rows), use_container_width=True, hide_index=True)
        admin.close()
    except Exception as e:
        st.warning(f"Could not fetch Kafka consumer lag: {e}")

    # ── Late events ───────────────────────────────────────────────────────────
    st.subheader("Late Event Rate")
    late_df = ql.get_late_event_stats()
    if not late_df.empty:
        fig = px.line(
            late_df,
            x="created_date",
            y="late_event_count",
            title=None,
            labels={"created_date": "Date", "late_event_count": "Late Events"},
            color_discrete_sequence=["#f85149"],
        )
        fig.update_layout(
            plot_bgcolor="#0d1117",
            paper_bgcolor="#1a1d27",
            font_color="#e6edf3",
            height=250,
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No late events recorded yet.")

    # ── dbt last run ──────────────────────────────────────────────────────────
    st.subheader("dbt Status")
    dbt_target = Path("dbt_project/target")
    if dbt_target.exists():
        run_results = dbt_target / "run_results.json"
        if run_results.exists():
            import json
            with open(run_results) as f:
                rr = json.load(f)
            elapsed = rr.get("elapsed_time", 0)
            results = rr.get("results", [])
            passed = sum(1 for r in results if r.get("status") in ("success", "pass"))
            failed = sum(1 for r in results if r.get("status") in ("error", "fail"))
            warned = sum(1 for r in results if r.get("status") == "warn")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Last run", f"{elapsed:.1f}s")
            col2.metric("Passed", passed)
            col3.metric("Failed", failed)
            col4.metric("Warned", warned)
        else:
            st.info("No dbt run results found. Run `make dbt-run`.")
    else:
        st.info("dbt target directory not found. Run `make dbt-run`.")
