"""
Meridian metrics server.

Exposes custom Prometheus metrics on port 8888 for scraping.
Run alongside the producer:
    uv run python monitoring/metrics.py
"""

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from prometheus_client import Counter, Gauge, Histogram, start_http_server

# ─── Custom metrics ───────────────────────────────────────────────────────────

EVENTS_PRODUCED = Counter(
    "meridian_events_produced_total",
    "Total events produced to Kafka",
    labelnames=["topic"],
)
EVENTS_QUARANTINED = Counter(
    "meridian_events_quarantined_total",
    "Total events quarantined by quarantine-dq",
    labelnames=["failure_type"],
)
QUARANTINE_STORE_SIZE = Gauge(
    "meridian_quarantine_store_size",
    "Current number of records in the quarantine store",
    labelnames=["pipeline"],
)
ICEBERG_SNAPSHOTS = Gauge(
    "meridian_iceberg_snapshot_count",
    "Current snapshot count per Iceberg table",
    labelnames=["table_name"],
)
PIPELINE_LAG_SECONDS = Gauge(
    "meridian_pipeline_lag_seconds",
    "Estimated end-to-end lag from produce to Iceberg commit",
)


def update_quarantine_metrics() -> None:
    """Scan the quarantine store and update gauge."""
    store_path = Path(os.environ.get("QUARANTINE_STORE_PATH", "./quarantine_store"))
    if not store_path.exists():
        return
    count = sum(1 for _ in store_path.rglob("*.json"))
    QUARANTINE_STORE_SIZE.labels(pipeline="meridian").set(count)


def main() -> None:
    port = int(os.environ.get("PROMETHEUS_PORT", "8888"))
    start_http_server(port)
    print(f"Metrics server running on :{port}")
    print("Collecting from quarantine store every 15 seconds...")

    while True:
        update_quarantine_metrics()
        time.sleep(15)


if __name__ == "__main__":
    main()
