"""
Flink Job 1 — Revenue Aggregation with Event Time and Watermarks.

LEARNING NOTE — Event time vs processing time:
  Processing time = when Flink receives the event (now)
  Event time      = when the event actually happened (event_ts field)

  WHY THIS MATTERS:
    If network is slow, an order placed at 14:00:00 might arrive at Flink
    at 14:00:05. With processing time, it falls in the 14:00:05 window.
    With event time, it correctly falls in the 14:00:00 window.
    For revenue aggregation (business reporting), event time is mandatory.

WATERMARKS:
  Flink uses watermarks to know when a window is "done". A watermark at
  time T means "I believe all events with timestamp <= T have arrived".
  We set a 30-second watermark — meaning we wait 30 seconds past the
  window boundary before computing the result.

  WatermarkStrategy
    .forBoundedOutOfOrderness(Duration.ofSeconds(30))
    .withTimestampAssigner(lambda event, _: event["event_ts"])

TWO WINDOWS:
  - 1-minute tumbling window → topic: revenue-1m
  - 5-minute tumbling window → topic: revenue-5m

  A tumbling window never overlaps: each event belongs to exactly one window.

Run: uv run python flink/jobs/revenue_aggregator.py
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyflink.common import Duration, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows, Time


class RevenueAggregator(AggregateFunction):
    """
    Incrementally accumulates revenue within a window.

    AggregateFunction is more efficient than ReduceFunction for sums —
    it updates an accumulator per record rather than keeping all records.
    The accumulator is a small dict regardless of how many events arrive.
    """

    def create_accumulator(self) -> dict:
        return {"total": 0.0, "count": 0, "min": float("inf"), "max": 0.0}

    def add(self, value: dict, accumulator: dict) -> dict:
        amount = value.get("amount", 0.0)
        accumulator["total"] += amount
        accumulator["count"] += 1
        accumulator["min"] = min(accumulator["min"], amount)
        accumulator["max"] = max(accumulator["max"], amount)
        return accumulator

    def get_result(self, accumulator: dict) -> dict:
        return accumulator

    def merge(self, a: dict, b: dict) -> dict:
        # merge is called when combining partial aggregates (e.g. after restore)
        return {
            "total": a["total"] + b["total"],
            "count": a["count"] + b["count"],
            "min": min(a["min"], b["min"]),
            "max": max(a["max"], b["max"]),
        }


class RevenueWindowProcessor(ProcessWindowFunction):
    """
    Called once per window per key with the aggregated accumulator.
    Enriches the result with window metadata and produces the final record.

    ProcessWindowFunction receives the full window context (start, end time)
    which pure AggregateFunction cannot access.
    """

    def process(
        self,
        key: str,
        context: "ProcessWindowFunction.Context",
        elements: Iterable[dict],
    ) -> Iterable[str]:
        acc = next(iter(elements))
        window = context.window()
        avg = acc["total"] / acc["count"] if acc["count"] > 0 else 0.0

        result = {
            "window_start": window.start,
            "window_end": window.end,
            "window_start_iso": datetime.fromtimestamp(window.start / 1000, tz=timezone.utc).isoformat(),
            "window_end_iso": datetime.fromtimestamp(window.end / 1000, tz=timezone.utc).isoformat(),
            "total_revenue": round(acc["total"], 2),
            "order_count": acc["count"],
            "avg_order": round(avg, 2),
            "min_order": round(acc["min"] if acc["min"] != float("inf") else 0.0, 2),
            "max_order": round(acc["max"], 2),
            "currency": "EUR",
            "computed_at": int(datetime.now(timezone.utc).timestamp() * 1000),
        }
        yield json.dumps(result)


def build_pipeline(window_minutes: int, output_topic: str, env: StreamExecutionEnvironment):
    """Build and register one revenue aggregation pipeline for a given window size."""
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap)
        .set_topics("orders")
        .set_group_id(f"revenue-aggregator-{window_minutes}m")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Assign event-time watermarks with 30-second bounded out-of-orderness.
    # This tells Flink: "wait 30 seconds after the window end before closing it."
    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30))
        .with_timestamp_assigner(
            lambda event, _: json.loads(event).get("event_ts", 0)
            if isinstance(event, str)
            else 0
        )
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(output_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    stream = env.from_source(source, watermark_strategy, f"Kafka orders [{window_minutes}m]")

    (
        stream
        .map(lambda raw: json.loads(raw) if isinstance(raw, str) else raw)
        .filter(lambda e: e is not None and "amount" in e and e["amount"] > 0)
        .key_by(lambda e: "all")  # single global key for total revenue
        .window(TumblingEventTimeWindows.of(Time.minutes(window_minutes)))
        .aggregate(RevenueAggregator(), RevenueWindowProcessor())
        .sink_to(sink)
    )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    # Enable checkpointing for exactly-once semantics
    env.enable_checkpointing(60_000)  # checkpoint every 60 seconds

    # Build both window pipelines in the same job graph
    build_pipeline(window_minutes=1, output_topic="revenue-1m", env=env)
    build_pipeline(window_minutes=5, output_topic="revenue-5m", env=env)

    env.execute("Meridian Revenue Aggregator")


if __name__ == "__main__":
    main()
