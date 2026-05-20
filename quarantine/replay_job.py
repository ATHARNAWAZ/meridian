"""
quarantine-dq replay job for Meridian.

Reads quarantined records from the store and attempts to re-process them:
  - SCHEMA_VIOLATION: re-validates against current Pydantic models and
    re-produces to Kafka if now valid.
  - LATE_ARRIVAL: produces directly to the correct Iceberg table with
    a late_arrival=True flag.

Run:
    uv run python quarantine/replay_job.py
    uv run python quarantine/replay_job.py --pipeline meridian --type LATE_ARRIVAL
    uv run python quarantine/replay_job.py --dry-run
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from rich.console import Console
from rich.table import Table

from quarantine.context import FailureType
from quarantine.stores.factory import get_store

log = logging.getLogger(__name__)
console = Console()

# ─── Validators ───────────────────────────────────────────────────────────────

def _validate_order_event(record: dict) -> tuple[bool, Optional[str]]:
    """Re-validate a raw order event dict against current Pydantic schema."""
    from generator.models import OrderEvent
    try:
        OrderEvent(**record)
        return True, None
    except Exception as e:
        return False, str(e)


def _validate_record(record: dict, stage: str) -> tuple[bool, Optional[str]]:
    """Route to the correct validator based on the stage that produced the record."""
    if "orders" in stage or "order" in record:
        return _validate_order_event(record)
    return True, None  # unknown event types are replayed optimistically


# ─── Replay strategies ────────────────────────────────────────────────────────

def replay_schema_violation(
    qr,
    bootstrap_servers: str,
    schema_registry_url: str,
    dry_run: bool,
) -> str:
    """
    Re-validate the record. If valid now, produce to the original Kafka topic.
    Returns: SUCCESS | STILL_INVALID | SKIPPED
    """
    record = qr.original_record
    valid, err = _validate_record(record, qr.stage)

    if not valid:
        return f"STILL_INVALID: {err}"

    if dry_run:
        return "DRY_RUN_OK"

    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode(),
            key_serializer=lambda k: k.encode() if k else None,
        )
        topic = "orders"  # default topic for schema violations
        key = record.get("user_id", "replay")
        producer.send(topic, key=key, value=record)
        producer.flush(timeout=10)
        return "SUCCESS"
    except Exception as e:
        return f"KAFKA_ERROR: {e}"


def replay_late_arrival(
    qr,
    dry_run: bool,
) -> str:
    """
    Write the late event directly to Iceberg orders table with late_arrival=True.
    Returns: SUCCESS | ERROR
    """
    if dry_run:
        return "DRY_RUN_OK"

    try:
        record = dict(qr.original_record)
        record["late_arrival"] = True
        record["original_event_ts"] = record.get("event_ts")
        record["replay_ts"] = int(datetime.now(timezone.utc).timestamp() * 1000)

        # Write via Iceberg writer if available
        try:
            sys.path.insert(0, str(Path(__file__).parent.parent))
            from iceberg.writer import IcebergWriter
            writer = IcebergWriter()
            writer.write_batch("meridian.orders", [record])
            return "SUCCESS"
        except ImportError:
            # Iceberg writer not yet initialised — log and defer
            log.warning("IcebergWriter not available, deferred late replay for %s", qr.id)
            return "DEFERRED_NO_ICEBERG"
    except Exception as e:
        return f"ERROR: {e}"


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Meridian quarantine replay job")
    parser.add_argument("--pipeline", default="meridian", help="Pipeline filter (default: meridian)")
    parser.add_argument("--type", dest="failure_type", default=None,
                        help="Failure type filter: SCHEMA_VIOLATION, LATE_ARRIVAL, etc.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate but do not actually produce or write")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--schema-registry", default="http://localhost:8081")
    args = parser.parse_args()

    store = get_store()
    records = store.list_records(
        pipeline=args.pipeline,
        failure_type=args.failure_type,
        replayable_only=True,
    )

    if not records:
        console.print(f"[yellow]No replayable records found for pipeline={args.pipeline}, type={args.failure_type}[/yellow]")
        return

    console.print(f"[bold cyan]Found {len(records)} replayable records[/bold cyan]")

    table = Table(title="Replay Results", expand=True)
    table.add_column("ID", style="dim", max_width=12)
    table.add_column("Stage")
    table.add_column("Type")
    table.add_column("Created")
    table.add_column("Result")

    successes = 0
    failures = 0

    for qr in records:
        ft = qr.failure_type

        if ft == FailureType.SCHEMA_VIOLATION.value:
            result = replay_schema_violation(
                qr,
                bootstrap_servers=args.bootstrap_servers,
                schema_registry_url=args.schema_registry,
                dry_run=args.dry_run,
            )
        elif ft == FailureType.LATE_ARRIVAL.value:
            result = replay_late_arrival(qr, dry_run=args.dry_run)
        else:
            result = "SKIPPED (no strategy for this type)"

        style = "green" if result.startswith("SUCCESS") or result.startswith("DRY_RUN") else "red"
        if "SUCCESS" in result or "DRY_RUN" in result:
            successes += 1
            if not args.dry_run:
                store.mark_replayed(qr.id, "SUCCESS")
        else:
            failures += 1

        table.add_row(
            qr.id[:12],
            qr.stage,
            ft,
            qr.created_at.strftime("%Y-%m-%d %H:%M"),
            f"[{style}]{result}[/{style}]",
        )

    console.print(table)
    console.print(
        f"\n[bold]Summary:[/bold] {successes} succeeded, {failures} failed"
        + (" [dim](dry run)[/dim]" if args.dry_run else "")
    )


if __name__ == "__main__":
    main()
