"""
Iceberg time travel utilities.

Demonstrates Iceberg's snapshot isolation and time travel capabilities.
Used by the Streamlit dashboard to show historical snapshots and diffs.

LEARNING NOTE — Iceberg snapshots:
  Every write to an Iceberg table creates a new snapshot. A snapshot is
  an immutable pointer to a set of data files. The metadata chain looks like:

    metadata v1 → snapshot 1 → manifest → data files (A, B)
    metadata v2 → snapshot 2 → manifest → data files (A, B, C)  ← new file C added
    metadata v3 → snapshot 3 → manifest → data files (A, B, D)  ← C deleted, D added

  Time travel = loading metadata v1 or v2 and querying those data files.
  No data is ever overwritten — only new files are added or old ones are
  marked as deleted in the manifest.

  This is why Iceberg is called an "open table format" — the data files
  are plain Parquet on S3, accessible by any engine. You're not locked in.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)


def list_snapshots(table_name: str) -> list[dict]:
    """Return all snapshots for a table, sorted by time ascending."""
    try:
        from iceberg.catalog import get_catalog
        catalog = get_catalog()
        table = catalog.load_table(table_name)
        snapshots = []

        for snap in table.history():
            snapshots.append({
                "snapshot_id": snap.snapshot_id,
                "timestamp_ms": snap.timestamp_ms,
                "timestamp_iso": datetime.fromtimestamp(
                    snap.timestamp_ms / 1000, tz=timezone.utc
                ).isoformat(),
                "is_current": snap.snapshot_id == (
                    table.current_snapshot().snapshot_id
                    if table.current_snapshot() else None
                ),
                "parent_id": snap.parent_snapshot_id,
            })

        return sorted(snapshots, key=lambda s: s["timestamp_ms"])

    except Exception as e:
        log.error("list_snapshots failed for %s: %s", table_name, e)
        return []


def query_at(table_name: str, snapshot_id: int) -> Optional[object]:
    """
    Return a PyArrow table of data as it was at a specific snapshot.

    This is the core time travel operation — it loads the table at an
    older snapshot ID and scans only those data files.
    """
    try:
        from iceberg.catalog import get_catalog
        catalog = get_catalog()
        table = catalog.load_table(table_name)
        # Use pyiceberg's scan with snapshot_id for time travel
        scan = table.scan(snapshot_id=snapshot_id)
        return scan.to_arrow()
    except Exception as e:
        log.error("query_at failed for %s@%s: %s", table_name, snapshot_id, e)
        return None


def diff_snapshots(
    table_name: str,
    snapshot_id_before: int,
    snapshot_id_after: int,
) -> dict:
    """
    Return a summary of what changed between two snapshots.

    Returns row counts for added and removed records.
    This demonstrates Iceberg's ability to answer "what changed?" without
    reading the full dataset.
    """
    try:
        from iceberg.catalog import get_catalog
        catalog = get_catalog()
        table = catalog.load_table(table_name)

        before_arrow = table.scan(snapshot_id=snapshot_id_before).to_arrow()
        after_arrow = table.scan(snapshot_id=snapshot_id_after).to_arrow()

        before_count = len(before_arrow)
        after_count = len(after_arrow)

        return {
            "table": table_name,
            "snapshot_before": snapshot_id_before,
            "snapshot_after": snapshot_id_after,
            "rows_before": before_count,
            "rows_after": after_count,
            "rows_added": max(0, after_count - before_count),
            "rows_removed": max(0, before_count - after_count),
            "net_change": after_count - before_count,
        }

    except Exception as e:
        log.error("diff_snapshots failed: %s", e)
        return {
            "error": str(e),
            "table": table_name,
            "snapshot_before": snapshot_id_before,
            "snapshot_after": snapshot_id_after,
        }


def get_table_stats(table_name: str) -> dict:
    """Return current stats for a table: row count, file count, snapshot info."""
    try:
        from iceberg.catalog import get_catalog
        catalog = get_catalog()
        table = catalog.load_table(table_name)
        snap = table.current_snapshot()

        stats = {
            "table": table_name,
            "current_snapshot_id": snap.snapshot_id if snap else None,
            "snapshot_count": len(list(table.history())),
            "schema_fields": len(table.schema().fields),
        }

        if snap and snap.summary:
            props = snap.summary.additional_properties
            stats["total_records"] = int(props.get("total-records", 0))
            stats["total_data_files"] = int(props.get("total-data-files", 0))
            stats["total_file_size_bytes"] = int(props.get("total-files-size", 0))

        return stats

    except Exception as e:
        log.error("get_table_stats failed for %s: %s", table_name, e)
        return {"table": table_name, "error": str(e)}
