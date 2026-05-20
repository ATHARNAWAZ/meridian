"""
Flink Job 4 — Inventory Alerts using Keyed State.

LEARNING NOTE — Keyed state in Flink:
  Unlike batch processing, streaming jobs run continuously for hours or days.
  State = data that a job keeps between events.

  ValueState: one value per key (e.g. current stock level per product_id)
  ListState: a list per key
  MapState: a map per key

  Keyed state is automatically:
  - Partitioned by key (each TaskManager only holds state for its keys)
  - Checkpointed (saved to durable storage every N seconds)
  - Restored (if the job crashes, state is recovered from last checkpoint)

  This is why Flink can handle exactly-once even after a crash:
  the state is persisted atomically with the checkpoint, and the job
  resumes from that exact state after recovery.

  VALUSTATE LIFECYCLE:
    1. open() — called once per TaskManager slot at startup
    2. process_element() — called for every event
    3. State persists across events for the same key
    4. On crash: state is restored from last checkpoint
    5. on_timer() — allows scheduling future callbacks (e.g. TTL cleanup)

Run: uv run python flink/jobs/inventory_alerts.py
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from prometheus_client import Counter, Gauge, start_http_server
from pyflink.common import Duration, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor

# ─── Prometheus metrics ───────────────────────────────────────────────────────

INVENTORY_ALERTS_TOTAL = Counter(
    "meridian_inventory_alerts_total",
    "Total inventory alerts emitted",
    labelnames=["alert_type"],
)
PRODUCTS_BELOW_THRESHOLD = Gauge(
    "meridian_products_below_reorder_threshold",
    "Current count of products with stock at or below reorder threshold",
)

# ─── Keyed process function ───────────────────────────────────────────────────

class InventoryAlertFunction(KeyedProcessFunction):
    """
    Maintains current stock level per product_id in ValueState.

    Emits LOW_STOCK when stock falls to or below the reorder threshold.
    Emits CRITICAL_STOCK when stock drops below 10% of threshold.
    Emits STOCKOUT when stock reaches zero.

    The ValueState means each product_id gets its own independent counter,
    held in Flink's managed state and checkpointed with every checkpoint.
    """

    def open(self, runtime_context) -> None:
        # Declare state descriptors in open() — Flink registers them before
        # any events flow. Each descriptor creates a separate state cell per key.
        self.stock_state = runtime_context.get_state(
            ValueStateDescriptor("stock_level", "int")
        )
        self.threshold_state = runtime_context.get_state(
            ValueStateDescriptor("reorder_threshold", "int")
        )
        self.last_alert_state = runtime_context.get_state(
            ValueStateDescriptor("last_alert_type", "str")
        )

    def process_element(self, event: dict, ctx: "KeyedProcessFunction.Context") -> None:
        product_id = event.get("product_id", "unknown")

        # Read current state (returns None on first event for this key)
        current_stock = self.stock_state.value()
        threshold = self.threshold_state.value()

        if current_stock is None:
            current_stock = event.get("stock_level", 0)
        if threshold is None:
            threshold = event.get("reorder_threshold", 20)

        # Update stock based on event type
        qty = event.get("quantity_changed", 0)
        if event.get("event_type") == "DEPLETION":
            current_stock = max(0, current_stock - qty)
        elif event.get("event_type") == "RESTOCK":
            current_stock += qty

        # Persist updated state — this is checkpointed atomically
        self.stock_state.update(current_stock)
        self.threshold_state.update(threshold)

        # Determine alert type
        alert_type = None
        if current_stock == 0:
            alert_type = "STOCKOUT"
        elif current_stock <= threshold * 0.1:
            alert_type = "CRITICAL_STOCK"
        elif current_stock <= threshold:
            alert_type = "LOW_STOCK"

        if alert_type:
            last_alert = self.last_alert_state.value()
            # Avoid spamming — only emit if alert type changed or stock worsened
            if last_alert != alert_type:
                self.last_alert_state.update(alert_type)
                INVENTORY_ALERTS_TOTAL.labels(alert_type=alert_type).inc()

                alert = {
                    "alert_id": f"INV-{product_id}-{alert_type[:3]}",
                    "product_id": product_id,
                    "warehouse_id": event.get("warehouse_id", "unknown"),
                    "alert_type": alert_type,
                    "current_stock": current_stock,
                    "reorder_threshold": threshold,
                    "stock_pct_of_threshold": round(current_stock / threshold * 100, 1) if threshold > 0 else 0,
                    "alert_ts": int(datetime.now(timezone.utc).timestamp() * 1000),
                    "alert_ts_iso": datetime.now(timezone.utc).isoformat(),
                }
                yield json.dumps(alert)

        elif self.last_alert_state.value():
            # Stock recovered above threshold — clear the alert state
            self.last_alert_state.clear()


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    start_http_server(8891)

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.enable_checkpointing(60_000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap)
        .set_topics("inventory")
        .set_group_id("inventory-alerts")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10))
        .with_timestamp_assigner(
            lambda raw, _: json.loads(raw).get("event_ts", 0)
            if isinstance(raw, str) else 0
        )
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("inventory-alerts")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    raw_stream = env.from_source(source, watermark_strategy, "Kafka inventory")

    (
        raw_stream
        .map(lambda raw: json.loads(raw) if isinstance(raw, str) else raw)
        .filter(lambda e: e is not None and "product_id" in e)
        .key_by(lambda e: e["product_id"])  # ValueState is per product_id
        .process(InventoryAlertFunction())
        .sink_to(sink)
    )

    env.execute("Meridian Inventory Alerts")


if __name__ == "__main__":
    main()
