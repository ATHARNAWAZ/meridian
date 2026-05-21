"""
MeridianQueryLayer — unified query interface for the Streamlit dashboard.

All queries return pandas DataFrames. Connects to:
- Iceberg tables via DuckDB's iceberg extension (direct S3/MinIO reads)
- dbt mart tables (DuckDB file)

Caches results for 30 seconds to avoid hammering Iceberg on every refresh.
"""

import functools
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import duckdb
import pandas as pd

log = logging.getLogger(__name__)

# ─── Cache ────────────────────────────────────────────────────────────────────

_cache: dict[str, tuple[float, Any]] = {}
_CACHE_TTL = 30  # seconds


def _cached(ttl: int = _CACHE_TTL):
    """Simple time-based result cache for DuckDB queries."""
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(self, *args, **kwargs):
            key = f"{fn.__name__}:{hashlib.md5(json.dumps([args, kwargs], default=str).encode()).hexdigest()}"
            now = time.monotonic()
            if key in _cache:
                ts, result = _cache[key]
                if now - ts < ttl:
                    return result
            result = fn(self, *args, **kwargs)
            _cache[key] = (now, result)
            return result
        return wrapper
    return decorator


# ─── Query layer ──────────────────────────────────────────────────────────────

class MeridianQueryLayer:
    """
    Unified query interface for the Streamlit dashboard.
    All queries return pandas DataFrames.
    """

    def __init__(
        self,
        duckdb_path: Optional[str] = None,
        minio_endpoint: Optional[str] = None,
        minio_access_key: Optional[str] = None,
        minio_secret_key: Optional[str] = None,
    ):
        db_path = duckdb_path or os.environ.get(
            "MERIDIAN_DUCKDB_PATH",
            str(Path(__file__).parent / "meridian.duckdb"),  # duckdb_layer/meridian.duckdb
        )
        self._conn = duckdb.connect(db_path)
        self._setup_extensions(
            endpoint=minio_endpoint or os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
            access_key=minio_access_key or os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=minio_secret_key or os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        )

    def _setup_extensions(self, endpoint: str, access_key: str, secret_key: str) -> None:
        conn = self._conn
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        conn.execute("INSTALL iceberg; LOAD iceberg;")
        conn.execute(f"""
            SET s3_endpoint='{endpoint.replace("http://", "").replace("https://", "")}';
            SET s3_access_key_id='{access_key}';
            SET s3_secret_access_key='{secret_key}';
            SET s3_url_style='path';
            SET s3_use_ssl=false;
        """)

    def _q(self, sql: str, params: Optional[list] = None) -> pd.DataFrame:
        """Execute a query and return a DataFrame. Returns empty DF on error."""
        try:
            if params:
                return self._conn.execute(sql, params).df()
            return self._conn.execute(sql).df()
        except Exception as e:
            log.warning("Query failed: %s\n%s", e, sql[:200])
            return pd.DataFrame()

    # ── Live revenue (from Kafka-backed DuckDB table or Iceberg) ──────────────

    @_cached(ttl=5)
    def get_live_revenue(self, window_minutes: int = 5) -> pd.DataFrame:
        """Revenue in the last N minutes, directly from the orders Iceberg table."""
        cutoff_ms = int((datetime.now(timezone.utc).timestamp() - window_minutes * 60) * 1000)
        return self._q(f"""
            select
                to_timestamp(event_ts / 1000.0) as event_ts,
                amount,
                category,
                country,
                user_id,
                product_id
            from iceberg_scan('s3://iceberg-warehouse/meridian/orders',
                              allowed_extensions=['.parquet'])
            where event_ts >= {cutoff_ms}
              and amount > 0
            order by event_ts desc
            limit 500
        """)

    @_cached(ttl=30)
    def get_revenue_by_day(self, days_back: int = 30) -> pd.DataFrame:
        """Daily revenue from the daily_revenue dbt mart."""
        return self._q(f"""
            select *
            from read_parquet('dbt_project/target/compiled/meridian/models/marts/daily_revenue.parquet')
            order by order_date desc
            limit {days_back}
        """ if self._parquet_exists("daily_revenue") else """
            select
                cast(to_timestamp(event_ts / 1000.0) as date)  as order_date,
                category,
                country,
                count(*)                                         as order_count,
                sum(amount)                                      as gross_revenue,
                avg(amount)                                      as avg_order_value
            from iceberg_scan('s3://iceberg-warehouse/meridian/orders',
                              allowed_extensions=['.parquet'])
            where amount > 0
            group by 1, 2, 3
            order by order_date desc
            limit 300
        """)

    @_cached(ttl=10)
    def get_fraud_alerts(self, limit: int = 50) -> pd.DataFrame:
        """Recent fraud alerts — reads from orders table filtered by is_fraud."""
        return self._q(f"""
            select
                event_id        as order_id,
                user_id,
                product_id,
                category,
                amount,
                country,
                to_timestamp(event_ts / 1000.0) as detected_at
            from iceberg_scan('s3://iceberg-warehouse/meridian/orders',
                              allowed_extensions=['.parquet'])
            where is_fraud = true
            order by event_ts desc
            limit {limit}
        """)

    @_cached(ttl=15)
    def get_quarantine_stats(self) -> pd.DataFrame:
        """
        Quarantine counts by pipeline, failure_type, and date.
        Reads from the file-based quarantine store directly.
        """
        store_path = Path(
            os.environ.get("QUARANTINE_STORE_PATH", "./quarantine_store")
        )
        if not store_path.exists():
            return pd.DataFrame(columns=["pipeline", "failure_type", "created_date", "count"])

        rows = []
        for json_file in store_path.rglob("*.json"):
            try:
                import json as _json
                with open(json_file) as f:
                    rec = _json.load(f)
                rows.append({
                    "pipeline": rec.get("pipeline", "unknown"),
                    "stage": rec.get("stage", "unknown"),
                    "failure_type": rec.get("failure_type", "UNKNOWN"),
                    "created_date": rec.get("created_at", "")[:10],
                    "replayable": rec.get("replayable", False),
                    "replay_status": rec.get("replay_status"),
                    "record_id": rec.get("id", ""),
                })
            except Exception:
                pass

        if not rows:
            return pd.DataFrame(columns=["pipeline", "failure_type", "created_date", "count"])
        return pd.DataFrame(rows)

    @_cached(ttl=15)
    def get_late_event_stats(self) -> pd.DataFrame:
        """Late event counts from quarantine, filtered to LATE_ARRIVAL type."""
        df = self.get_quarantine_stats()
        if df.empty:
            return pd.DataFrame()
        late = df[df["failure_type"] == "LATE_ARRIVAL"].copy()
        if late.empty:
            return late
        return late.groupby("created_date").size().reset_index(name="late_event_count")

    @_cached(ttl=30)
    def get_product_performance(self, top_n: int = 10) -> pd.DataFrame:
        """Top N products by GMV."""
        return self._q(f"""
            select
                product_id,
                category,
                count(*)        as total_orders,
                sum(amount)     as gmv,
                avg(amount)     as avg_order_value,
                sum(quantity)   as total_units_sold
            from iceberg_scan('s3://iceberg-warehouse/meridian/orders',
                              allowed_extensions=['.parquet'])
            where amount > 0
            group by product_id, category
            order by gmv desc
            limit {top_n}
        """)

    @_cached(ttl=60)
    def get_iceberg_snapshots(self, table_name: str) -> pd.DataFrame:
        """Show Iceberg snapshot history for time travel demo."""
        try:
            from iceberg.time_travel import list_snapshots
            snaps = list_snapshots(table_name)
            if not snaps:
                return pd.DataFrame()
            return pd.DataFrame(snaps)
        except Exception as e:
            log.warning("get_iceberg_snapshots failed: %s", e)
            return pd.DataFrame()

    @_cached(ttl=10)
    def get_recent_events(self, limit: int = 20) -> pd.DataFrame:
        """Last N raw order events for the live events table."""
        return self._q(f"""
            select
                event_id,
                user_id,
                product_id,
                category,
                amount,
                quantity,
                country,
                to_timestamp(event_ts / 1000.0) as event_ts,
                is_fraud
            from iceberg_scan('s3://iceberg-warehouse/meridian/orders',
                              allowed_extensions=['.parquet'])
            order by event_ts desc
            limit {limit}
        """)

    @_cached(ttl=30)
    def get_revenue_per_minute(self, last_n_minutes: int = 30) -> pd.DataFrame:
        """Revenue aggregated per minute for the last N minutes."""
        cutoff_ms = int((datetime.now(timezone.utc).timestamp() - last_n_minutes * 60) * 1000)
        return self._q(f"""
            select
                date_trunc('minute', to_timestamp(event_ts / 1000.0)) as minute,
                sum(amount)         as revenue,
                count(*)            as order_count
            from iceberg_scan('s3://iceberg-warehouse/meridian/orders',
                              allowed_extensions=['.parquet'])
            where event_ts >= {cutoff_ms}
              and amount > 0
            group by 1
            order by 1
        """)

    @_cached(ttl=10)
    def get_hourly_metrics(self) -> dict:
        """Aggregated metrics for the last hour — used by the top metric cards."""
        df = self._q("""
            select
                sum(amount)             as revenue,
                count(*)                as order_count,
                sum(case when is_fraud then 1 else 0 end) as fraud_count
            from iceberg_scan('s3://iceberg-warehouse/meridian/orders',
                              allowed_extensions=['.parquet'])
            where event_ts >= (epoch_ms(now()) - 3600000)
              and amount > 0
        """)
        if df.empty:
            return {"revenue": 0.0, "order_count": 0, "fraud_count": 0}
        row = df.iloc[0]
        return {
            "revenue": float(row.get("revenue") or 0),
            "order_count": int(row.get("order_count") or 0),
            "fraud_count": int(row.get("fraud_count") or 0),
        }

    def _parquet_exists(self, model_name: str) -> bool:
        p = Path(f"dbt_project/target/compiled/meridian/models/marts/{model_name}.parquet")
        return p.exists()

    def close(self) -> None:
        self._conn.close()
