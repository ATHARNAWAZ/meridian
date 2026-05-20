"""
Flink Job 2 — Fraud Detection using Complex Event Processing (CEP).

LEARNING NOTE — Complex Event Processing (CEP):
  CEP lets you define patterns across a sequence of events over time.
  Instead of looking at one event at a time, you look for patterns.

  Pattern: "user places 3 orders each > €500 within 10 minutes"
  → This cannot be done with simple windowing.
  → CEP maintains state per user_id and matches the pattern across events.
  → The key insight: CEP is pattern matching over a stream, not aggregation.

  Flink CEP concepts:
  - Pattern: the sequence definition (begin/next/followedBy)
  - PatternStream: a keyed stream with a pattern applied
  - PatternSelectFunction / PatternFlatSelectFunction: called on match

  WHY THIS IMPRESSES INTERVIEWERS:
    CEP is what separates engineers who understand stateful stream processing
    from those who only know batch. It is used in fraud detection, anomaly
    detection, clickstream analysis, and IoT alerting everywhere in Germany.

TWO FRAUD PATTERNS:
  1. HIGH_VALUE_VELOCITY: 3 consecutive orders each > €500 within 10 minutes
  2. BOT_DETECTION: same product_id ordered 5 times in 1 minute

Prometheus counter: meridian_fraud_alerts_total

Run: uv run python flink/jobs/fraud_detector.py
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from prometheus_client import Counter, start_http_server
from pyflink.cep import CEP, Pattern
from pyflink.cep.pattern_select_function import PatternFlatSelectFunction
from pyflink.common import Duration, Time, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

# ─── Prometheus metrics ───────────────────────────────────────────────────────

FRAUD_ALERTS_TOTAL = Counter(
    "meridian_fraud_alerts_total",
    "Total fraud alerts emitted by the fraud detector",
    labelnames=["pattern_type"],
)

# ─── Pattern select functions ─────────────────────────────────────────────────

class HighValueVelocityAlertFn(PatternFlatSelectFunction):
    """
    Emits a FraudAlert when 3 consecutive high-value orders match.

    PatternFlatSelectFunction is preferred over PatternSelectFunction when
    you may want to emit 0 or N results per match (flat = flatMap semantics).
    """

    def flat_select(self, pattern: Dict[str, List[dict]], out) -> None:
        events = (
            pattern.get("first_order", [])
            + pattern.get("second_order", [])
            + pattern.get("third_order", [])
        )
        if len(events) < 3:
            return

        user_id = events[0].get("user_id", "unknown")
        total = sum(e.get("amount", 0.0) for e in events)

        alert = {
            "alert_id": f"HVV-{events[0].get('event_id', 'x')[:8]}",
            "user_id": user_id,
            "pattern_type": "HIGH_VALUE_VELOCITY",
            "matched_event_ids": [e.get("event_id") for e in events],
            "total_amount": round(total, 2),
            "detected_at": int(datetime.now(timezone.utc).timestamp() * 1000),
            "detected_at_iso": datetime.now(timezone.utc).isoformat(),
        }
        FRAUD_ALERTS_TOTAL.labels(pattern_type="HIGH_VALUE_VELOCITY").inc()
        out.collect(json.dumps(alert))


class BotDetectionAlertFn(PatternFlatSelectFunction):
    """
    Emits a FraudAlert when the same product is ordered 5 times in 1 minute.
    This pattern catches automated bots buying limited-stock items.
    """

    def flat_select(self, pattern: Dict[str, List[dict]], out) -> None:
        events = []
        for i in range(1, 6):
            events.extend(pattern.get(f"order_{i}", []))

        if len(events) < 5:
            return

        user_id = events[0].get("user_id", "unknown")
        product_id = events[0].get("product_id", "unknown")
        total = sum(e.get("amount", 0.0) for e in events)

        alert = {
            "alert_id": f"BOT-{events[0].get('event_id', 'x')[:8]}",
            "user_id": user_id,
            "pattern_type": "BOT_DETECTION",
            "product_id": product_id,
            "matched_event_ids": [e.get("event_id") for e in events],
            "total_amount": round(total, 2),
            "detected_at": int(datetime.now(timezone.utc).timestamp() * 1000),
            "detected_at_iso": datetime.now(timezone.utc).isoformat(),
        }
        FRAUD_ALERTS_TOTAL.labels(pattern_type="BOT_DETECTION").inc()
        out.collect(json.dumps(alert))


# ─── Main ──────────────────────────────────────────────────────────────────────

def main():
    # Expose Prometheus metrics on port 8889
    start_http_server(8889)

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.enable_checkpointing(60_000)

    # ── Source ───────────────────────────────────────────────────────────────
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap)
        .set_topics("orders")
        .set_group_id("fraud-detector")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30))
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
            .set_topic("fraud-alerts")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    # ── Stream preparation ───────────────────────────────────────────────────
    raw_stream = env.from_source(source, watermark_strategy, "Kafka orders [fraud]")

    order_stream = (
        raw_stream
        .map(lambda raw: json.loads(raw) if isinstance(raw, str) else raw)
        .filter(lambda e: e is not None and "user_id" in e)
        .key_by(lambda e: e["user_id"])  # CEP state is partitioned per user
    )

    # ── Pattern 1: High-value velocity ───────────────────────────────────────
    # Three consecutive orders each over €500 within 10 minutes.
    # .next() means strict contiguity: no other events between matches.
    # .followedBy() would allow gaps — use that for looser matching.
    high_value_pattern = (
        Pattern.begin("first_order").where(lambda e, _: e.get("amount", 0) > 500)
        .next("second_order").where(lambda e, _: e.get("amount", 0) > 500)
        .next("third_order").where(lambda e, _: e.get("amount", 0) > 500)
        .within(Time.minutes(10))
    )

    hvv_alerts = (
        CEP.pattern(order_stream, high_value_pattern)
        .flat_select(HighValueVelocityAlertFn())
    )

    # ── Pattern 2: Bot detection ─────────────────────────────────────────────
    # Same product ordered 5 times within 1 minute.
    # Uses .followed_by_any() for non-contiguous matching across user events.
    # Each "order_N" event must be for the same product as the first.
    bot_pattern = (
        Pattern.begin("order_1").where(lambda e, _: True)
        .followed_by("order_2").where(lambda e, ctx: e.get("product_id") == ctx.get_events_for_pattern("order_1")[0].get("product_id"))
        .followed_by("order_3").where(lambda e, ctx: e.get("product_id") == ctx.get_events_for_pattern("order_1")[0].get("product_id"))
        .followed_by("order_4").where(lambda e, ctx: e.get("product_id") == ctx.get_events_for_pattern("order_1")[0].get("product_id"))
        .followed_by("order_5").where(lambda e, ctx: e.get("product_id") == ctx.get_events_for_pattern("order_1")[0].get("product_id"))
        .within(Time.minutes(1))
    )

    bot_alerts = (
        CEP.pattern(order_stream, bot_pattern)
        .flat_select(BotDetectionAlertFn())
    )

    # ── Merge both alert streams and sink to Kafka ────────────────────────────
    all_alerts = hvv_alerts.union(bot_alerts)
    all_alerts.sink_to(sink)

    env.execute("Meridian Fraud Detector")


if __name__ == "__main__":
    main()
