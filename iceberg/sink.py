"""
Iceberg table definitions and schema setup.

Creates the 5 Meridian tables in the local MinIO catalog on first run.
Run this once before starting Flink jobs:
    uv run python iceberg/sink.py --init
"""

import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform

from iceberg.catalog import get_catalog

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ─── Table schemas ────────────────────────────────────────────────────────────

ORDERS_SCHEMA = Schema(
    NestedField(1, "event_id",   StringType(),    required=True),
    NestedField(2, "user_id",    StringType(),    required=True),
    NestedField(3, "product_id", StringType(),    required=True),
    NestedField(4, "category",   StringType(),    required=True),
    NestedField(5, "amount",     DoubleType(),    required=True),
    NestedField(6, "quantity",   IntegerType(),   required=True),
    NestedField(7, "currency",   StringType(),    required=False),
    NestedField(8, "country",    StringType(),    required=True),
    NestedField(9, "event_ts",   LongType(),      required=True),
    NestedField(10, "is_fraud",  BooleanType(),   required=False),
    NestedField(11, "late_arrival", BooleanType(), required=False),
    NestedField(12, "replay_ts", LongType(),      required=False),
)

CLICKS_SCHEMA = Schema(
    NestedField(1, "event_id",    StringType(),  required=True),
    NestedField(2, "user_id",     StringType(),  required=True),
    NestedField(3, "product_id",  StringType(),  required=True),
    NestedField(4, "page",        StringType(),  required=True),
    NestedField(5, "session_id",  StringType(),  required=True),
    NestedField(6, "referrer",    StringType(),  required=False),
    NestedField(7, "device_type", StringType(),  required=True),
    NestedField(8, "event_ts",    LongType(),    required=True),
)

PAYMENTS_SCHEMA = Schema(
    NestedField(1, "event_id",       StringType(),  required=True),
    NestedField(2, "order_id",       StringType(),  required=True),
    NestedField(3, "user_id",        StringType(),  required=True),
    NestedField(4, "amount",         DoubleType(),  required=True),
    NestedField(5, "currency",       StringType(),  required=False),
    NestedField(6, "payment_method", StringType(),  required=True),
    NestedField(7, "status",         StringType(),  required=True),
    NestedField(8, "gateway",        StringType(),  required=True),
    NestedField(9, "event_ts",       LongType(),    required=True),
)

INVENTORY_SCHEMA = Schema(
    NestedField(1, "event_id",          StringType(),  required=True),
    NestedField(2, "product_id",        StringType(),  required=True),
    NestedField(3, "warehouse_id",      StringType(),  required=True),
    NestedField(4, "stock_level",       IntegerType(), required=True),
    NestedField(5, "quantity_changed",  IntegerType(), required=True),
    NestedField(6, "reorder_threshold", IntegerType(), required=True),
    NestedField(7, "event_type",        StringType(),  required=True),
    NestedField(8, "event_ts",          LongType(),    required=True),
)

RETURNS_SCHEMA = Schema(
    NestedField(1, "event_id",      StringType(),  required=True),
    NestedField(2, "order_id",      StringType(),  required=True),
    NestedField(3, "user_id",       StringType(),  required=True),
    NestedField(4, "product_id",    StringType(),  required=True),
    NestedField(5, "reason",        StringType(),  required=True),
    NestedField(6, "refund_amount", DoubleType(),  required=True),
    NestedField(7, "event_ts",      LongType(),    required=True),
)

TABLE_CONFIGS = {
    "meridian.orders": {
        "schema": ORDERS_SCHEMA,
        "partition_spec": PartitionSpec(
            PartitionField(source_id=9, field_id=1000, transform=DayTransform(), name="event_day")
        ),
    },
    "meridian.clicks": {
        "schema": CLICKS_SCHEMA,
        "partition_spec": PartitionSpec(
            PartitionField(source_id=8, field_id=1000, transform=DayTransform(), name="event_day")
        ),
    },
    "meridian.payments": {
        "schema": PAYMENTS_SCHEMA,
        "partition_spec": PartitionSpec(
            PartitionField(source_id=9, field_id=1000, transform=DayTransform(), name="event_day")
        ),
    },
    "meridian.inventory": {
        "schema": INVENTORY_SCHEMA,
        "partition_spec": PartitionSpec(
            PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="product_id")
        ),
    },
    "meridian.returns": {
        "schema": RETURNS_SCHEMA,
        "partition_spec": PartitionSpec(
            PartitionField(source_id=7, field_id=1000, transform=DayTransform(), name="event_day")
        ),
    },
}


def init_tables(force_recreate: bool = False) -> None:
    """Create all 5 Iceberg tables if they don't exist."""
    catalog = get_catalog()

    # Ensure namespace exists
    try:
        catalog.create_namespace("meridian")
        log.info("Created namespace: meridian")
    except Exception:
        pass  # namespace already exists

    for table_name, config in TABLE_CONFIGS.items():
        try:
            if force_recreate:
                try:
                    catalog.drop_table(table_name)
                    log.info("Dropped table: %s", table_name)
                except Exception:
                    pass

            catalog.create_table(
                identifier=table_name,
                schema=config["schema"],
                partition_spec=config["partition_spec"],
                properties={
                    "write.format.default": "parquet",
                    "write.parquet.compression-codec": "snappy",
                    "write.target-file-size-bytes": "134217728",  # 128MB target
                },
            )
            log.info("Created table: %s", table_name)
        except Exception as e:
            if "already exists" in str(e).lower():
                log.info("Table already exists: %s", table_name)
            else:
                log.error("Failed to create table %s: %s", table_name, e)
                raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Iceberg table initialisation")
    parser.add_argument("--init", action="store_true", help="Create all tables")
    parser.add_argument("--force-recreate", action="store_true", help="Drop and recreate tables")
    args = parser.parse_args()

    if args.init or args.force_recreate:
        init_tables(force_recreate=args.force_recreate)
        print("Tables initialised.")
    else:
        parser.print_help()
