# Meridian

> Real-time e-commerce analytics. Kafka → Flink → Iceberg → dbt → DuckDB → Streamlit.

![Python](https://img.shields.io/badge/Python-3.10-blue)
![Apache Flink](https://img.shields.io/badge/Apache%20Flink-1.18-orange)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5-black)
![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-0.6-teal)
![dbt](https://img.shields.io/badge/dbt--DuckDB-1.7-red)
![Tests](https://img.shields.io/badge/tests-28%20passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-green)

A production-grade streaming analytics platform demonstrating the German data engineering market stack end-to-end. Everything runs locally with a single `make demo`.

---

## The stack

| Layer | Technology | Purpose |
|---|---|---|
| Events | Apache Kafka + Confluent Schema Registry | 5 topics, Avro serialisation, partitioned by `user_id` |
| Processing | Apache Flink 1.18 (PyFlink) | Event-time windowing, CEP fraud detection, late events, keyed state |
| DLQ | quarantine-dq | Bad events caught, stored, replayed — never silently dropped |
| Storage | Apache Iceberg on MinIO | ACID lakehouse, time travel, schema evolution, exactly-once |
| Transform | dbt Core (DuckDB adapter) | Batch models, contracts, custom data tests |
| Query | DuckDB | Fast analytical queries directly over Iceberg via S3 |
| Dashboard | Streamlit | Live revenue, fraud alerts, quarantine console, time travel |
| Monitoring | Prometheus + Grafana | Kafka lag, Flink throughput, custom fraud/late-event metrics |

---

## One-command start

```bash
git clone https://github.com/ATHARNAWAZ/meridian
cd meridian
make demo
# Open http://localhost:8501
```

**Requirements:** Docker Desktop, `uv` (`pip install uv`), `make`.

---

## What this demonstrates

### 1. Event-time processing with watermarks

**The problem:** A mobile user places an order at 14:00:00 but the event arrives at Flink 35 seconds later due to a slow network. With *processing time*, the order falls in the wrong 1-minute window. Revenue for 14:00 is understated; 14:00:30 is overstated.

**The solution:** Event-time watermarks. Flink reads `event_ts` from the payload and uses it as the true event timestamp. A 30-second bounded-out-of-orderness watermark means Flink waits 30 seconds past a window boundary before closing it — late events within that window are included correctly.

```python
watermark_strategy = (
    WatermarkStrategy
    .for_bounded_out_of_orderness(Duration.of_seconds(30))
    .with_timestamp_assigner(lambda event, _: event["event_ts"])
)
```

See: [`flink/jobs/revenue_aggregator.py`](flink/jobs/revenue_aggregator.py)

---

### 2. Fraud detection using Flink CEP

**The problem:** You can't detect "3 high-value orders from the same user in 10 minutes" with simple windowing — you need to match a pattern *across* events.

**The solution:** Complex Event Processing. CEP maintains per-user state and matches patterns across a stream.

```python
fraud_pattern = (
    Pattern.begin("first_order").where(lambda e, _: e["amount"] > 500)
    .next("second_order").where(lambda e, _: e["amount"] > 500)
    .next("third_order").where(lambda e, _: e["amount"] > 500)
    .within(Time.minutes(10))
)
```

Two patterns: `HIGH_VALUE_VELOCITY` (3 orders > €500 in 10m) and `BOT_DETECTION` (same product 5 times in 1m). Alerts land in the `fraud-alerts` Kafka topic and the Streamlit dashboard.

See: [`flink/jobs/fraud_detector.py`](flink/jobs/fraud_detector.py)

---

### 3. Late event handling with side outputs

**The problem:** Even with 30-second watermarks, some events arrive minutes late (mobile buffering, clock skew). After the watermark closes a window, late events are silently dropped by default. That's data loss.

**The solution:**
1. `allowed_lateness(Time.minutes(5))` — Flink holds the window state open 5 extra minutes, re-firing with updated results for events that arrive late.
2. After 5 minutes, events go to a **side output** — a secondary stream for exception handling — and are saved to quarantine-dq for replay.

```python
windowed = (
    keyed_stream
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .allowed_lateness(Time.minutes(5))
    .side_output_late_data(late_tag)
    .aggregate(RevenueAggregator())
)
late_stream = windowed.get_side_output(late_tag)
# late_stream → quarantine-dq
```

See: [`flink/jobs/late_event_handler.py`](flink/jobs/late_event_handler.py)

---

### 4. Exactly-once semantics end-to-end

**The mechanism:**
1. Flink enables checkpointing every 60 seconds (`EXACTLY_ONCE` mode).
2. Each checkpoint saves all operator state to durable storage atomically.
3. The Iceberg sink uses Iceberg's two-phase commit: data files are written in phase 1, metadata is updated atomically in phase 2 — only on checkpoint completion.
4. If Flink crashes mid-checkpoint, it resumes from the last completed checkpoint. Pre-written Iceberg files from the failed checkpoint are orphaned (never referenced in metadata).

Result: every event is written to Iceberg **exactly once**, even under failures.

---

### 5. quarantine-dq integration

Bad events (null IDs, negative amounts, invalid currencies) are caught before they reach Iceberg. Instead of silently dropping them or crashing the pipeline, quarantine-dq:
1. Saves the bad event to a durable store with full context (failure type, reason, original payload)
2. Marks it as replayable
3. The Quarantine Console in the dashboard lets you inspect and replay them

```python
sq = StreamQuarantine(pipeline="meridian", stage="revenue_aggregator")
sq.quarantine_record(event, failure_type="NULL_KEY_FIELD", reason="event_id is null")

# Later: replay
uv run python quarantine_int/replay_job.py --pipeline meridian
```

See: [`quarantine_int/stream_quarantine.py`](quarantine_int/stream_quarantine.py)

---

### 6. Apache Iceberg time travel

Every Iceberg write creates an immutable snapshot. You can query any historical state:

```python
from iceberg.time_travel import query_at, list_snapshots

snapshots = list_snapshots("meridian.orders")
arrow_table = query_at("meridian.orders", snapshot_id=snapshots[-2]["snapshot_id"])
```

The Streamlit Time Travel page lets you browse snapshot history, query a historical snapshot, and diff two snapshots side-by-side.

See: [`iceberg/time_travel.py`](iceberg/time_travel.py)

---

## Architecture

```
                         ┌──────────────────────────────────────────┐
                         │              Docker Compose               │
                         │                                           │
  ┌──────────┐  Avro     │  ┌─────────┐    ┌──────────────────────┐ │
  │ Producer │──────────►│  │  Kafka  │───►│   Flink (4 jobs)     │ │
  │ (faker)  │  Schema   │  │ 5 topics│    │ ┌──────────────────┐ │ │
  └──────────┘  Registry │  └─────────┘    │ │Revenue Aggregator│ │ │
                         │                 │ │Fraud Detector CEP│ │ │
  ┌──────────────────┐   │                 │ │Late Event Handler│ │ │
  │ Bad events (2%)  │   │                 │ │Inventory Alerts  │ │ │
  │ null ID,         │   │                 │ └──────────────────┘ │ │
  │ negative amount  │   │                 └──────────┬───────────┘ │
  └─────────┬────────┘   │                            │             │
            │            │                            │ Atomic      │
            ▼            │                            ▼ Commit      │
  ┌─────────────────┐    │              ┌─────────────────────────┐ │
  │  quarantine-dq  │    │              │  Apache Iceberg on MinIO│ │
  │  (file store)   │    │              │  meridian.orders        │ │
  │  replay_job.py  │    │              │  meridian.clicks        │ │
  └─────────────────┘    │              │  meridian.payments      │ │
                         │              │  meridian.inventory     │ │
  ┌──────────────────┐   │              │  meridian.returns       │ │
  │  dbt Core        │   │              └───────────┬─────────────┘ │
  │  DuckDB adapter  │◄──┼──────────────────────────┘             │ │
  │  4 marts         │   │                                         │ │
  └─────────┬────────┘   │  ┌──────────────┐  ┌────────────────┐  │ │
            │            │  │  Streamlit   │  │  Prometheus    │  │ │
            └───────────►│  │  Dashboard   │  │  + Grafana     │  │ │
                         │  │  :8501       │  │  :9090/:3000   │  │ │
                         │  └──────────────┘  └────────────────┘  │ │
                         └──────────────────────────────────────────┘
```

---

## Kafka topics

| Topic | Partitions | Schema | Produced by |
|---|---|---|---|
| `orders` | 8 | `order_event.avsc` | generator |
| `clicks` | 8 | `click_event.avsc` | generator |
| `payments` | 4 | `payment_event.avsc` | generator |
| `inventory` | 4 | `inventory_event.avsc` | generator |
| `returns` | 4 | `return_event.avsc` | generator |
| `revenue-1m` | 1 | JSON | flink/revenue_aggregator |
| `revenue-5m` | 1 | JSON | flink/revenue_aggregator |
| `fraud-alerts` | 1 | JSON | flink/fraud_detector |
| `inventory-alerts` | 1 | JSON | flink/inventory_alerts |

---

## Flink jobs

| Job | Input | Output | Key concept demonstrated |
|---|---|---|---|
| `revenue_aggregator.py` | `orders` | `revenue-1m`, `revenue-5m` | Event time, tumbling windows, watermarks, AggregateFunction |
| `fraud_detector.py` | `orders` | `fraud-alerts` | CEP, pattern matching, stateful processing, side streams |
| `late_event_handler.py` | `orders` | `revenue-late`, quarantine-dq | Side outputs, allowed lateness, exactly-once |
| `inventory_alerts.py` | `inventory` | `inventory-alerts` | Keyed state, ValueState, checkpointing |

---

## Dashboard pages

| Page | What it shows |
|---|---|
| **Live Overview** | Revenue/min chart, orders by category, live event feed — auto-refreshes every 5s |
| **Fraud Alerts** | CEP-detected fraud events, scatter plot, per-event review workflow |
| **Quarantine Console** | quarantine-dq records by failure type, filter + replay button |
| **Iceberg Time Travel** | Snapshot browser, query-at-snapshot, snapshot diff viewer |
| **Pipeline Health** | Flink job status, Kafka consumer lag, dbt last run stats |

---

## Running tests

```bash
make test              # 28 unit tests (no Docker required)
make test-e2e          # end-to-end test (requires make up first)
make dbt-run           # dbt models + custom data tests
make benchmark         # latency benchmark: p50/p95 report
```

---

## Monitoring

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| Flink Web UI | http://localhost:8082 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| Streamlit Dashboard | http://localhost:8501 | — |

---

## Project structure

```
meridian/
├── generator/          # Synthetic event producer (Faker + Avro + Schema Registry)
├── kafka/              # Avro schemas + Schema Registry client
├── flink/jobs/         # 4 PyFlink jobs with learning notes
├── flink/lib/          # Shared Kafka utilities
├── quarantine_int/     # quarantine-dq integration layer + replay job
├── iceberg/            # Iceberg catalog, sink, time travel utilities
├── dbt_project/        # dbt Core: staging, intermediate, 4 marts, 3 custom tests
├── duckdb_layer/       # Unified query layer (Iceberg + dbt → pandas DataFrames)
├── dashboard/          # Streamlit app (5 pages)
├── monitoring/         # Prometheus config + pre-provisioned Grafana dashboard
├── libs/quarantine_dq/ # quarantine-dq library (local package)
├── infra/              # docker-compose.yml
├── tests/              # 28 unit tests + e2e + benchmark
└── Makefile            # make up, demo, producer, jobs, test, dbt-run
```

---

## License

MIT
