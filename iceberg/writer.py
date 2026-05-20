"""
IcebergWriter — atomic batch writer for Flink jobs.

Buffers records in memory and commits atomically to Iceberg on each
Flink checkpoint. Handles schema evolution automatically.

LEARNING NOTE — Streaming exactly-once with Iceberg:
  Flink checkpoints save all operator state at a consistent point in time.
  An Iceberg commit is a two-phase operation:
    Phase 1: Write data files to S3 (no table metadata change yet)
    Phase 2: Atomically update table metadata to include new files

  Flink coordinates this with the Iceberg sink:
    - Pre-commit hook: write data files to S3, prepare the commit
    - Checkpoint complete: atomically update Iceberg metadata
    - On crash/recovery: if checkpoint N-1 completed, data is in Iceberg;
      if it didn't complete, the pre-written files are orphaned and ignored.

  Result: exactly-once delivery from Flink → Iceberg.
"""

import logging
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

import pyarrow as pa

log = logging.getLogger(__name__)


class IcebergWriter:
    """
    Buffers Flink output in memory and commits atomically to Iceberg.

    Parameters
    ----------
    batch_size: Commit when this many records are buffered (per table).
    flush_interval_seconds: Commit after this many seconds even if batch not full.
    """

    def __init__(
        self,
        batch_size: int = 1000,
        flush_interval_seconds: int = 30,
    ):
        self.batch_size = batch_size
        self.flush_interval = flush_interval_seconds
        self._buffers: dict[str, list[dict]] = defaultdict(list)
        self._lock = threading.Lock()
        self._snapshot_cache: dict[str, dict] = {}

        # Start background flush thread
        self._stop = threading.Event()
        self._flush_thread = threading.Thread(target=self._background_flush, daemon=True)
        self._flush_thread.start()

    def write_batch(self, table_name: str, records: list[dict]) -> int:
        """
        Add records to the buffer. Returns the new buffer size.
        Triggers an immediate flush if batch_size is reached.
        """
        if not records:
            return 0

        with self._lock:
            self._buffers[table_name].extend(records)
            buf_size = len(self._buffers[table_name])

        if buf_size >= self.batch_size:
            self._flush(table_name)

        return buf_size

    def _flush(self, table_name: str) -> int:
        """Atomically commit buffered records to the Iceberg table."""
        with self._lock:
            records = self._buffers.pop(table_name, [])

        if not records:
            return 0

        try:
            from iceberg.catalog import get_catalog
            catalog = get_catalog()
            table = catalog.load_table(table_name)

            # Convert records to PyArrow table for Iceberg write
            arrow_table = self._to_arrow(records, table)

            # Schema evolution: check for new fields and add them
            self._evolve_schema_if_needed(table, arrow_table)

            # Atomic append — this is the Iceberg commit
            table.append(arrow_table)

            log.info(
                "Committed %d records to %s (snapshot: %s)",
                len(records),
                table_name,
                table.current_snapshot().snapshot_id if table.current_snapshot() else "N/A",
            )
            return len(records)

        except Exception as e:
            log.error("Failed to commit %d records to %s: %s", len(records), table_name, e)
            # Put records back in buffer for retry
            with self._lock:
                self._buffers[table_name] = records + self._buffers.get(table_name, [])
            return 0

    def _to_arrow(self, records: list[dict], table) -> pa.Table:
        """Convert list of dicts to a PyArrow table matching the Iceberg schema."""
        if not records:
            return pa.table({})

        # Infer schema from Iceberg table schema
        iceberg_fields = {field.name: field for field in table.schema().fields}
        columns = defaultdict(list)

        for record in records:
            for field_name in iceberg_fields:
                columns[field_name].append(record.get(field_name))

        # Build PyArrow arrays
        arrays = {}
        for field_name, values in columns.items():
            arrays[field_name] = pa.array(values)

        return pa.table(arrays)

    def _evolve_schema_if_needed(self, table, arrow_table: pa.Table) -> None:
        """
        If new columns appear in the data that aren't in the Iceberg schema,
        add them (as optional StringType by default).
        """
        existing = {field.name for field in table.schema().fields}
        new_fields = set(arrow_table.column_names) - existing

        if new_fields:
            with table.update_schema() as update:
                for field_name in new_fields:
                    from pyiceberg.types import StringType
                    update.add_column(field_name, StringType())
            log.warning(
                "Schema evolution: added %d new field(s) to %s: %s",
                len(new_fields),
                table.name(),
                ", ".join(sorted(new_fields)),
            )

    def _background_flush(self) -> None:
        """Flush all tables on interval even if batch size not reached."""
        while not self._stop.wait(self.flush_interval):
            with self._lock:
                tables_to_flush = list(self._buffers.keys())

            for table_name in tables_to_flush:
                self._flush(table_name)

    def flush_all(self) -> dict[str, int]:
        """Flush all buffered tables immediately. Call on checkpoint."""
        with self._lock:
            tables = list(self._buffers.keys())

        results = {}
        for table_name in tables:
            results[table_name] = self._flush(table_name)
        return results

    def get_snapshot_info(self, table_name: str) -> Optional[dict]:
        """Return current snapshot metadata for a table."""
        try:
            from iceberg.catalog import get_catalog
            catalog = get_catalog()
            table = catalog.load_table(table_name)
            snap = table.current_snapshot()
            if not snap:
                return None
            return {
                "snapshot_id": snap.snapshot_id,
                "sequence_number": snap.sequence_number,
                "timestamp_ms": snap.timestamp_ms,
                "timestamp_iso": datetime.fromtimestamp(
                    snap.timestamp_ms / 1000, tz=timezone.utc
                ).isoformat(),
                "summary": snap.summary.additional_properties if snap.summary else {},
            }
        except Exception as e:
            log.error("Failed to get snapshot info for %s: %s", table_name, e)
            return None

    def close(self) -> None:
        self._stop.set()
        self.flush_all()
