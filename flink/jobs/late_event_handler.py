"""
Flink Job 3 — Late Event Handler with Side Outputs and quarantine-dq.

LEARNING NOTE — Late events and side outputs:
  Even with watermarks, some events arrive VERY late (network partition,
  mobile app buffering, clock skew). After the watermark passes a window's
  end, the window closes and its state is discarded.

  If a late event arrives after the window closes, what happens?
  Without configuration: it is silently dropped. Data loss.
  With allowed lateness: Flink holds the window state open for an
  extra N minutes, re-fires the window with the updated result.
  After that: side output.

  SIDE OUTPUT:
    A side output is a secondary stream that collects events that didn't
    make it into the main output. It's Flink's native pattern for
    "handle the exception case without crashing the main pipeline".

    This is exactly what quarantine-dq does — but side outputs are
    Flink-native and quarantine-dq gives us persistence + replay.

  EXACTLY ONCE:
    Checkpointing + Iceberg atomic commits = exactly-once end-to-end.
    If Flink crashes mid-window, it resumes from the last checkpoint.
    The Iceberg write either committed fully or didn't commit at all.

Prometheus gauges:
  meridian_late_events_total
  meridian_late_events_by_bucket (histogram: <1m, 1-5m, >5m)

Run: uv run python flink/jobs/late_event_handler.py
"""

import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pyflink.common import Duration, OutputTag, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import ProcessWindowFunction, AggregateFunction
from pyflink.datastream.window import TumblingEventTimeWindows, Time

from quarantine.context import FailureType, QuarantineContext
from quarantine.stores.factory import get_store

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("late_event_handler")

# ─── Prometheus metrics ───────────────────────────────────────────────────────

LATE_EVENTS_TOTAL = Counter(
    "meridian_late_events_total",
    "Total late events captured via side output",
)
LATE_EVENTS_BY_BUCKET = Counter(
    "meridian_late_events_by_minutes_bucket",
    "Late events by lateness bucket",
    labelnames=["bucket"],  # <1m, 1-5m, >5m
)
WINDOWS_PROCESSED = Counter(
    "meridian_windows_processed_total",
    "Revenue windows processed",
)

# ─── Side output tag ──────────────────────────────────────────────────────────

# Events arriving more than 5 minutes after their window's end go here.
# The tag carries type information so Flink can route them correctly.
LATE_TAG = OutputTag("late-orders", Types.STRING())

# ─── Aggregate + Process functions ───────────────────────────────────────────

class LatencyAwareAggregator(AggregateFunction):
    """Identical to RevenueAggregator — reused here for the main output."""

    def create_accumulator(self):
        return {"total": 0.0, "count": 0}

    def add(self, value, acc):
        acc["total"] += value.get("amount", 0.0)
        acc["count"] += 1
        return acc

    def get_result(self, acc):
        return acc

    def merge(self, a, b):
        return {"total": a["total"] + b["total"], "count": a["count"] + b["count"]}


class WindowResultProcessor(ProcessWindowFunction):
    """Enriches the aggregated result with window metadata."""

    def process(self, key, context, elements):
        acc = next(iter(elements))
        window = context.window()
        WINDOWS_PROCESSED.inc()
        result = {
            "window_start": window.start,
            "window_end": window.end,
            "total_revenue": round(acc["total"], 2),
            "order_count": acc["count"],
            "avg_order": round(acc["total"] / acc["count"], 2) if acc["count"] > 0 else 0.0,
            "currency": "EUR",
            "source": "late_event_handler",
        }
        yield json.dumps(result)


# ─── Late event quarantine sink ───────────────────────────────────────────────

class QuarantineSinkFunction:
    """
    Consumes the side output stream and saves late events to quarantine-dq.
    Classifies lateness into three buckets for the Prometheus histogram.
    """

    def __init__(self):
        self._store = get_store()

    def process(self, raw: str) -> None:
        try:
            event = json.loads(raw) if isinstance(raw, str) else raw
            event_ts = event.get("event_ts", 0)
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            lateness_seconds = max(0, (now_ms - event_ts) / 1000)

            # Classify lateness bucket
            if lateness_seconds < 60:
                bucket = "<1m"
            elif lateness_seconds < 300:
                bucket = "1-5m"
            else:
                bucket = ">5m"

            LATE_EVENTS_TOTAL.inc()
            LATE_EVENTS_BY_BUCKET.labels(bucket=bucket).inc()

            ctx = QuarantineContext(
                pipeline="meridian",
                stage="late_event_handler",
                failure_type=FailureType.LATE_ARRIVAL,
                reason=f"Event arrived {lateness_seconds:.0f}s late, after allowed lateness window closed",
                detail={
                    "how_late_seconds": int(lateness_seconds),
                    "lateness_bucket": bucket,
                    "event_ts": event_ts,
                    "processing_ts": now_ms,
                },
                replayable=True,
            )
            record_id = self._store.save(event, ctx)
            log.debug("Quarantined late event %s (%.0fs late, bucket=%s)",
                      event.get("event_id"), lateness_seconds, bucket)
        except Exception as e:
            log.error("Failed to quarantine late event: %s", e)


# ─── Periodic stats logger ────────────────────────────────────────────────────

def _stats_logger(stop_event: threading.Event):
    """Logs a Rich summary every 60 seconds."""
    try:
        from rich.console import Console
        from rich.table import Table
        con = Console()
    except ImportError:
        return

    while not stop_event.wait(60):
        table = Table(title="[bold]Late Event Handler — 60s Summary[/bold]")
        table.add_column("Metric")
        table.add_column("Value", justify="right")
        table.add_row("Windows processed", str(int(WINDOWS_PROCESSED._value.get())))
        table.add_row("Late events total", str(int(LATE_EVENTS_TOTAL._value.get())))
        con.print(table)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    start_http_server(8890)

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.enable_checkpointing(60_000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap)
        .set_topics("orders")
        .set_group_id("late-event-handler")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # 30-second bounded out-of-orderness watermark
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
            .set_topic("revenue-late")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    raw_stream = env.from_source(source, watermark_strategy, "Kafka orders [late]")

    order_stream = (
        raw_stream
        .map(lambda raw: json.loads(raw) if isinstance(raw, str) else raw)
        .filter(lambda e: e is not None and "amount" in e)
        .key_by(lambda e: "all")
    )

    # 1-minute tumbling window with:
    # - 30-second watermark (already set above)
    # - 5-minute allowed lateness (Flink holds window state 5 extra minutes)
    # - Side output for events arriving after the 5-minute grace period
    windowed = (
        order_stream
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .allowed_lateness(Time.minutes(5))
        .side_output_late_data(LATE_TAG)
        .aggregate(LatencyAwareAggregator(), WindowResultProcessor())
    )

    # Main output → Kafka topic for dashboard
    windowed.sink_to(sink)

    # Late output → quarantine-dq
    late_stream = windowed.get_side_output(LATE_TAG)
    quarantine_sink_fn = QuarantineSinkFunction()
    late_stream.map(lambda raw: (quarantine_sink_fn.process(raw), raw)[1])

    # Start stats logger in background thread
    stop_event = threading.Event()
    stats_thread = threading.Thread(target=_stats_logger, args=(stop_event,), daemon=True)
    stats_thread.start()

    try:
        env.execute("Meridian Late Event Handler")
    finally:
        stop_event.set()


if __name__ == "__main__":
    main()
