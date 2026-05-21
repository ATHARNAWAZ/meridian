# Meridian — Concepts Reference

> Personal learning reference for interviews and code review. Every concept used in this project explained from first principles with code examples.

---

## Table of Contents

1. [Event Time vs Processing Time](#1-event-time-vs-processing-time)
2. [Watermarks](#2-watermarks)
3. [Tumbling, Sliding, and Session Windows](#3-tumbling-sliding-and-session-windows)
4. [Flink CEP — Complex Event Processing](#4-flink-cep--complex-event-processing)
5. [Keyed State and Checkpointing](#5-keyed-state-and-checkpointing)
6. [Exactly-Once Semantics — Two-Phase Commit](#6-exactly-once-semantics--two-phase-commit)
7. [Apache Iceberg Table Format Internals](#7-apache-iceberg-table-format-internals)
8. [Why quarantine-dq Over Silent Drops](#8-why-quarantine-dq-over-silent-drops)

---

## 1. Event Time vs Processing Time

### The core distinction

Every event has two timestamps:

| Timestamp | Meaning | Controlled by |
|---|---|---|
| **Event time** | When the event actually *happened* in the real world | The device/service that produced it |
| **Processing time** | When the event *arrives* at Flink for processing | Network + system clock |

### Why it matters

```
Real world:
  14:00:00  User places order on mobile (event_ts = 14:00:00)
  14:00:35  Network delivers event to Flink (35s late)

Processing-time window [14:00:00–14:01:00]:
  → Closes at 14:01:00 wall-clock time
  → The 14:00:00 order arrives at 14:00:35 → ✓ captured
  → A 13:59:50 order that arrives at 14:01:05 → ✗ MISSED

Event-time window [14:00:00–14:01:00]:
  → Closes based on event_ts, not wall clock
  → The 13:59:50 order: event_ts says 13:59:50 → goes in the right window
  → Revenue for 14:00–14:01 is correct
```

### The diagram

```
Wall clock:  ──────────────────────────────────────────────►
             13:59    14:00    14:01    14:02    14:03

Events by
event_ts:       [E1]  [E2]  [E3]              [E4-late]
                                               ↑ arrives at 14:02:30
                                               but event_ts = 14:00:45

Processing      |_________P-window 1_________|_________P-window 2___|
time windows:   E4 falls into P-window 2 → WRONG window

Event-time      |_________E-window 1_________|_________E-window 2___|
windows:        E4 has event_ts 14:00:45 → correct window 1 → CORRECT
```

### In Meridian

```python
# flink/jobs/revenue_aggregator.py
watermark_strategy = (
    WatermarkStrategy
    .for_bounded_out_of_orderness(Duration.of_seconds(30))
    .with_timestamp_assigner(lambda event, _: event["event_ts"])
    #                                          ↑ tells Flink to use event_ts, not wall clock
)
```

### Interview answer

> "Processing time is simple and low-latency but gives wrong results when events arrive out-of-order or late. Event time is correct but requires watermarks to know when a window is 'done'. For revenue reporting, correctness matters — use event time."

---

## 2. Watermarks

### The problem watermarks solve

With event-time windows, Flink needs to know when to *close* a window. It can't wait forever for late events. Watermarks answer: **"I'm confident all events with timestamp < W have arrived."**

### How watermarks work

A watermark is a special record injected into the stream with a timestamp W. It means:
> "No event with event_ts < W will ever arrive in this partition again."

Flink uses the maximum event_ts seen so far, minus a lag allowance:

```
W = max(event_ts_seen) - allowed_out_of_orderness
```

### Bounded out-of-orderness watermark

```python
# You're saying: "Events arrive at most 30 seconds late"
WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30))
```

Timeline:
```
Event arrives with event_ts=14:00:45 → max_seen = 14:00:45
Watermark emitted = 14:00:45 - 30s = 14:00:15

When watermark passes 14:01:00 → window [14:00:00–14:01:00] closes
That happens when max_seen reaches 14:01:30 (wall-clock ~14:02:00 with normal delays)
```

### Watermark strategies in PyFlink

```python
# 1. Bounded out-of-orderness (most common for production)
WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30))

# 2. Monotonous timestamps (events always arrive in order — rare in practice)
WatermarkStrategy.for_monotonous_timestamps()

# 3. Custom (implement AssignerWithPeriodicWatermarks)
WatermarkStrategy.for_generator(MyCustomWatermarkGenerator())
```

### The tradeoff

| Allowed lag | Window closes | Latency | Completeness |
|---|---|---|---|
| 0s | Immediately | Lowest | Miss late events |
| 30s | 30s after window end | Medium | Miss events > 30s late |
| 5m | 5m after window end | High | Miss events > 5m late |
| ∞ | Never | N/A | Complete but infinite wait |

Meridian uses **30s** for the watermark + **5 min allowed lateness** + **side outputs** for anything later. This is a three-tier strategy — see section 3.

### Interview answer

> "A watermark is Flink's mechanism for tracking progress in event time. It's a timestamp W that means 'I've seen all events up to W'. Flink closes a window when the watermark passes the window's end time. The lag in `for_bounded_out_of_orderness` is your bet on how late events can arrive — pick it too small and you lose data, too large and you add latency."

---

## 3. Tumbling, Sliding, and Session Windows

### Tumbling windows

Fixed-size, non-overlapping. Each event belongs to exactly one window.

```
[──────1m──────][──────1m──────][──────1m──────]
  14:00–14:01     14:01–14:02     14:02–14:03
```

```python
# Meridian: revenue per minute
.window(TumblingEventTimeWindows.of(Time.minutes(1)))
```

**Use when**: You want non-overlapping aggregations (hourly revenue, daily counts). Each event counted once.

### Sliding windows

Fixed size, with overlap. An event can belong to multiple windows.

```
[────────5m────────]
      [────────5m────────]
            [────────5m────────]
   ↑ slide=1m
```

```python
# Revenue in last 5m, updated every 1m
.window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
```

**Use when**: You want a rolling aggregate ("revenue in the last 5 minutes"). One event contributes to multiple windows (5 here). More expensive — window count = size/slide.

### Session windows

Gap-based — a window closes when there's no activity for `gap` time.

```
[──events──][gap > 30m][──events──][gap > 30m][──event──]
  session1               session2               session3
```

```python
.window(EventTimeSessionWindows.with_gap(Time.minutes(30)))
```

**Use when**: You want to group user activity into natural sessions. Window size varies per user.

### The three-tier late event strategy in Meridian

```
Event arrives
│
├─► Within watermark lag (≤30s late)
│    → Included in the window normally
│
├─► After watermark but within allowed_lateness (30s–5m late)
│    → Window re-fires with updated result
│    .allowed_lateness(Time.minutes(5))
│
└─► After allowed_lateness (>5m late)
     → Goes to side output → quarantine-dq
     .side_output_late_data(LATE_TAG)
```

```python
windowed = (
    keyed_stream
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .allowed_lateness(Time.minutes(5))      # tier 2: re-fire
    .side_output_late_data(late_tag)        # tier 3: catch remainder
    .aggregate(RevenueAggregator())
)
late_stream = windowed.get_side_output(late_tag)
```

### Interview answer

> "Tumbling windows are non-overlapping and good for distinct time buckets. Sliding windows overlap and give a rolling view. Session windows are user-defined gaps — great for clickstream analysis. In practice the choice is: do you want each event counted once (tumbling), multiple times (sliding), or grouped by behaviour gap (session)?"

---

## 4. Flink CEP — Complex Event Processing

### What CEP solves

Simple windowed aggregation answers "how much?" or "how many?". CEP answers **"did this specific sequence of events happen?"**

Example: "Did this user place 3 orders > €500 within 10 minutes?"

You can't answer this with a GROUP BY + SUM. You need to match a *pattern across events*.

### Pattern anatomy

```python
from pyflink.cep import Pattern, CEP
from pyflink.cep.pattern import WithinType

fraud_pattern = (
    Pattern
    .begin("first_order")                           # name the first event in the pattern
    .where(lambda e, _: e["amount"] > 500)          # condition: amount > €500
    
    .next("second_order")                           # next: strict contiguity (no events between)
    .where(lambda e, _: e["amount"] > 500)
    
    .next("third_order")
    .where(lambda e, _: e["amount"] > 500)
    
    .within(Time.minutes(10))                       # all 3 must happen within 10 minutes
)
```

### Contiguity modes

| Operator | Meaning |
|---|---|
| `.next("B")` | B must immediately follow A (strict contiguity) |
| `.followed_by("B")` | B follows A with any events between (relaxed contiguity) |
| `.followed_by_any("B")` | All matching Bs after A (non-deterministic relaxed) |
| `.not_next("B")` | B must NOT immediately follow A |
| `.not_followed_by("B")` | B must NOT follow A at all |

### Pattern matching and emitting alerts

```python
# Apply the pattern to a keyed stream (keyed by user_id — patterns are per-user)
cep_stream = CEP.pattern(
    orders_stream.key_by(lambda e: e["user_id"]),
    fraud_pattern
)

# Extract matched events and emit an alert
alerts = cep_stream.flat_select(HighValueVelocityAlertFn())

class HighValueVelocityAlertFn(PatternFlatSelectFunction):
    def flat_select(self, pattern_match: dict, out: Collector):
        orders = [
            pattern_match["first_order"][0],
            pattern_match["second_order"][0],
            pattern_match["third_order"][0],
        ]
        total = sum(o["amount"] for o in orders)
        out.collect({
            "pattern_type": "HIGH_VALUE_VELOCITY",
            "user_id": orders[0]["user_id"],
            "total_amount": total,
        })
```

### How CEP state works internally

Flink CEP builds a Non-deterministic Finite Automaton (NFA) per key. For each user_id, Flink maintains a state machine tracking partial pattern matches. When a new event arrives:
1. It's offered to all active partial matches
2. Partial matches that accept it advance to the next state
3. Partial matches that time out are discarded
4. Complete matches trigger `flat_select`

This state is checkpointed like all other Flink state — CEP patterns survive job restarts.

### The two patterns in Meridian

```python
# Pattern 1: 3 high-value orders in 10 minutes
high_value_pattern = (
    Pattern.begin("first").where(lambda e, _: e["amount"] > 500)
    .next("second").where(lambda e, _: e["amount"] > 500)
    .next("third").where(lambda e, _: e["amount"] > 500)
    .within(Time.minutes(10))
)

# Pattern 2: Same product ordered 5 times in 1 minute (bot detection)
bot_pattern = (
    Pattern.begin("first").where(lambda e, _: True)
    .next("second").where(lambda e, ctx: e["product_id"] == ctx.get_events_for_pattern("first")[0]["product_id"])
    .next("third").where(...)
    .next("fourth").where(...)
    .next("fifth").where(...)
    .within(Time.minutes(1))
)
```

### Interview answer

> "CEP lets you detect patterns across multiple events in a stream, not just aggregate within a window. Flink maintains a per-key NFA state machine that tracks partial pattern matches. When events complete the pattern within the time constraint, an alert fires. It's the right tool when you need to detect sequences like 'login failed 3 times then succeeded' or 'rapid high-value orders from one user'."

---

## 5. Keyed State and Checkpointing

### What keyed state is

In Flink, you `key_by()` a stream on some field. After keying, each parallel operator instance owns a disjoint partition of keys. Keyed state is state that is **scoped to a key** — each user_id, product_id, etc. gets its own independent state.

```python
# This partitions the stream: all events for the same product_id go to the same subtask
stream.key_by(lambda e: e["product_id"])
```

### State primitives in PyFlink

```python
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor, MapStateDescriptor
from pyflink.common.typeinfo import Types

class InventoryAlertFunction(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        # ValueState: single value per key
        self.stock_state = runtime_context.get_state(
            ValueStateDescriptor("stock_level", Types.INT())
        )
        self.threshold_state = runtime_context.get_state(
            ValueStateDescriptor("alert_threshold", Types.INT())
        )
        self.last_alert_state = runtime_context.get_state(
            ValueStateDescriptor("last_alert_ts", Types.LONG())
        )

    def process_element(self, event, ctx):
        # Read
        current_stock = self.stock_state.value() or event["stock_quantity"]

        # Update
        self.stock_state.update(current_stock)

        # Emit alert
        if current_stock == 0:
            ctx.output(alert_tag, {"type": "STOCKOUT", "product_id": event["product_id"]})
```

### Other state types

| Type | API | When to use |
|---|---|---|
| `ValueState[T]` | `.value()`, `.update(v)` | Single value per key (counter, last seen) |
| `ListState[T]` | `.add(v)`, `.get()` | List of values per key (history, buffer) |
| `MapState[K,V]` | `.put(k,v)`, `.get(k)` | Map per key (per-key sub-index) |
| `ReducingState[T]` | `.add(v)` auto-reduces | Running aggregate per key |
| `AggregatingState[I,O]` | `.add(v)` | Custom aggregation per key |

### What checkpointing does

Flink periodically (every 60s in Meridian) takes a **consistent snapshot** of all operator state:

```
Timeline:
t=0s   Checkpoint 1 starts
       ├── Operator A state: {user1: 3 orders, user2: 1 order}
       ├── Operator B state: {window 14:00–14:01: [events...]}
       └── Kafka consumer offsets: {orders: partition0@offset1234}
t=5s   Checkpoint 1 complete → saved to S3/local FS

t=60s  Checkpoint 2 starts...

t=75s  JVM CRASH
       ↓
t=76s  Flink restores from Checkpoint 2
       ├── Operator state restored exactly as it was at t=60s
       └── Kafka consumption resumes from the committed offset
```

### Checkpoint configuration

```python
env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(60_000)  # every 60 seconds
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
env.get_checkpoint_config().set_min_pause_between_checkpoints(30_000)
env.get_checkpoint_config().set_checkpoint_timeout(120_000)
env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
```

### Interview answer

> "Keyed state in Flink is per-key operator state — each unique key (user_id, product_id) gets its own isolated state, automatically partitioned across Flink subtasks. Checkpointing saves a consistent snapshot of all state + Kafka offsets atomically. On recovery, Flink resumes from the last checkpoint — no data is lost or double-processed if you also use a transactional sink like Iceberg."

---

## 6. Exactly-Once Semantics — Two-Phase Commit

### The layers of delivery guarantees

| Guarantee | Meaning | How |
|---|---|---|
| **At-most-once** | Events may be lost, never duplicated | Fire and forget |
| **At-least-once** | Events may be duplicated, never lost | Retry on failure |
| **Exactly-once** | Events delivered precisely once | 2PC coordination |

### Why exactly-once is hard

The problem: Flink writes data to Iceberg, then crashes before it can record that the write happened. On recovery:
- If Flink re-writes → duplicates
- If Flink doesn't re-write → data loss

You need atomicity across: **Flink state commit** + **Iceberg write** + **Kafka offset commit**.

### Two-phase commit (2PC) protocol

```
Phase 1 — Pre-commit (during checkpoint):
  Flink triggers checkpoint
  ├── All operators flush state to durable storage
  ├── Iceberg sink: write Parquet data files (not yet visible to readers)
  │   Data files exist but NOT referenced in Iceberg metadata yet
  └── Checkpoint completes

Phase 2 — Commit (after successful checkpoint):
  Checkpoint acknowledged as complete
  ├── Iceberg sink: commit metadata → atomically adds data files to the table
  └── Kafka consumer: commit offsets

If crash happens in Phase 1:
  → Checkpoint never completes → Flink restores from previous checkpoint
  → Iceberg data files from failed run are orphaned (never committed)
  → Kafka offsets not advanced → events re-processed from last committed offset
  → Net result: exactly once ✓

If crash happens in Phase 2 (after data files written, before metadata commit):
  → Flink restores state from the completed checkpoint
  → Iceberg sink re-runs the metadata commit (idempotent — files already exist)
  → Net result: exactly once ✓
```

### Iceberg's role

Iceberg makes exactly-once possible because writes are **transactional**:
1. Write Parquet files to object storage (orphaned until committed)
2. Create a new snapshot pointing to those files
3. Atomically swap the metadata pointer (atomic rename / conditional write)

Step 3 is atomic — either the new snapshot is visible to all readers, or it isn't. There's no partial state.

### In code

```python
# flink/jobs/revenue_aggregator.py
env.enable_checkpointing(60_000)
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

# The Iceberg sink implements TwoPhaseCommitSinkFunction:
# - invoke() → buffer the row
# - pre_commit() → write Parquet files, return file handles
# - commit() → call table.new_append().append_file(f).commit()
# - abort() → delete the orphaned files
```

### Interview answer

> "Exactly-once in Flink works through two-phase commit coordinated with the checkpoint barrier. During checkpointing, Iceberg writes data files but doesn't commit metadata. Only after the checkpoint succeeds does Flink trigger the commit phase — atomically updating Iceberg's metadata to make those files visible. If Flink crashes mid-checkpoint, the orphaned data files are never referenced and the recovery re-processes from the last complete checkpoint."

---

## 7. Apache Iceberg Table Format Internals

### What Iceberg is

Iceberg is an **open table format** — a specification for how to store tabular data on object storage (S3, MinIO, GCS) with database-like guarantees.

It's not a query engine. It tells DuckDB, Spark, Flink *where* the data files are and what schema they have.

### The file hierarchy

```
iceberg-warehouse/
└── meridian/
    └── orders/
        ├── metadata/
        │   ├── v1.metadata.json          ← Table metadata v1 (initial schema)
        │   ├── v2.metadata.json          ← Table metadata v2 (after first write)
        │   ├── v3.metadata.json          ← Table metadata v3 (after second write)
        │   ├── snap-001.avro             ← Snapshot 1: list of manifest files
        │   ├── snap-002.avro             ← Snapshot 2
        │   └── manifest-xxx.avro         ← Manifest: list of data files + stats
        └── data/
            ├── event_date=2024-01-15/
            │   ├── 00000-0-abc.parquet
            │   └── 00000-1-def.parquet
            └── event_date=2024-01-16/
                └── 00000-0-ghi.parquet
```

### The metadata chain

```
current metadata pointer (version hint file)
        │
        ▼
v3.metadata.json
  ├── current-snapshot-id: snap-002
  ├── schemas: [{id:0, fields:[...]}, {id:1, fields:[..., new_col]}]
  ├── partition-specs: [{field: event_date, transform: day}]
  └── snapshots:
        ├── snap-001 → manifest-list-001.avro
        └── snap-002 → manifest-list-002.avro
                              │
                              ▼
                   manifest-list-002.avro
                     ├── manifest-001.avro (existing files from snap-001)
                     └── manifest-002.avro (new files added in snap-002)
                                  │
                                  ▼
                        manifest-002.avro
                          ├── data/event_date=2024-01-16/00000-0-ghi.parquet
                          │   ├── row_count: 10000
                          │   ├── null_count: {amount: 0}
                          │   └── lower_bound/upper_bound: {event_ts: ...}
                          └── ...
```

### Time travel

Because Iceberg never modifies old files — it only adds new ones — you can query any historical snapshot:

```python
# pyiceberg
from iceberg.time_travel import query_at, list_snapshots

snapshots = list_snapshots("meridian.orders")
# [{"snapshot_id": 1234, "timestamp_ms": 1705276800000, "operation": "append"}, ...]

df = query_at("meridian.orders", snapshot_id=snapshots[-2]["snapshot_id"])
# Returns data as it was after the second-to-last write

# DuckDB — via iceberg_scan with snapshot parameter
conn.execute("""
    SELECT * FROM iceberg_scan(
        's3://iceberg-warehouse/meridian/orders',
        snapshot_id=1234
    )
""")
```

### Schema evolution

Iceberg tracks schema versions by ID, not by name. This means:
- **Rename a column** → old Parquet files still read correctly (the ID maps to the new name)
- **Add a column** → old files return NULL for the new column
- **Drop a column** → old files silently ignore the dropped column

```python
# pyiceberg schema evolution
table = catalog.load_table("meridian.orders")
with table.update_schema() as update:
    update.add_column("discount_pct", DoubleType())
    # Old Parquet files: discount_pct reads as NULL
    # New Parquet files: contain the actual value
```

### ACID guarantees

| Property | How Iceberg achieves it |
|---|---|
| **Atomicity** | Metadata commit is a single atomic object write (conditional PUT / rename) |
| **Consistency** | Snapshot isolation — readers always see a consistent snapshot |
| **Isolation** | Optimistic concurrency — concurrent writers conflict on metadata commit, loser retries |
| **Durability** | Data files on S3/MinIO are durable; metadata chain is immutable |

### Interview answer

> "Iceberg is a table format that separates metadata (which files exist, what schema) from data files (Parquet on S3). Each write creates a new immutable snapshot pointing to manifest files that list the data files. This gives you time travel (query any snapshot), schema evolution (column IDs survive renames), and ACID commits (atomic metadata swap). The key insight: data files are never modified — only the metadata pointer changes."

---

## 8. Why quarantine-dq Over Silent Drops

### The problem with silent drops

```python
# What a naive pipeline does:
for event in stream:
    try:
        validate(event)
        write_to_iceberg(event)
    except Exception:
        pass  # ← silently dropped
```

This is production poison:
1. **You don't know how many events were lost** — no metric, no alert
2. **You can't investigate** — the event is gone
3. **You can't fix and replay** — even if you fix the validator, the original events are gone forever
4. **Revenue figures are silently wrong** — the business doesn't know

### What quarantine-dq does instead

```python
# meridian pattern:
sq = StreamQuarantine(pipeline="meridian", stage="revenue_aggregator")

for event in stream:
    try:
        validate(event)
        write_to_iceberg(event)
    except NullKeyField as e:
        sq.quarantine_record(
            record=event,
            failure_type=FailureType.NULL_KEY_FIELD,
            reason=str(e),
        )
        # Metrics bump: meridian_quarantine_total{failure_type="NULL_KEY_FIELD"} += 1
```

The quarantined record is saved to disk with full context:
```json
{
  "id": "uuid-...",
  "pipeline": "meridian",
  "stage": "revenue_aggregator",
  "failure_type": "NULL_KEY_FIELD",
  "reason": "event_id is null",
  "created_at": "2024-01-15T14:00:35Z",
  "replayable": true,
  "replay_status": null,
  "original_record": {"event_id": null, "user_id": "u-123", "amount": 42.50}
}
```

### The replay loop

```python
# quarantine_int/replay_job.py
# 1. Fetch all NULL_KEY_FIELD records for the meridian pipeline
records = store.list_records(pipeline="meridian", failure_type=FailureType.NULL_KEY_FIELD)

# 2. For each record, try to fix and re-process
for rec in records:
    fixed = attempt_fix(rec.original_record)  # e.g., generate a synthetic ID
    if fixed:
        produce_to_kafka("orders", fixed)     # re-enters the pipeline
        store.mark_replayed(rec.id, success=True)
```

### Failure types in Meridian

| FailureType | Cause | Replayable? |
|---|---|---|
| `NULL_KEY_FIELD` | `event_id` or `user_id` is null | Maybe (can generate synthetic ID) |
| `NEGATIVE_AMOUNT` | `amount < 0` (refund misrouted to orders) | Yes (route to returns topic) |
| `INVALID_CURRENCY` | Currency code not 3-letter ISO | Yes (default to EUR, flag for review) |
| `SCHEMA_VIOLATION` | Avro deserialization failed | Sometimes (if schema registry is behind) |
| `LATE_ARRIVAL` | `event_ts` > 5 minutes behind watermark | Yes (write to Iceberg with late flag) |
| `DESERIALIZATION_ERROR` | Corrupted bytes | No (original data is unrecoverable) |

### The observability benefit

With quarantine-dq, bad events become **observable**:
- Prometheus counter: `meridian_quarantine_total{failure_type="..."}` — alerts if spikes
- Dashboard: Quarantine Console shows counts, lets ops inspect individual records
- Replay: bad events aren't lost — they're fixed and re-processed when the root cause is fixed

### Alternative approaches and their tradeoffs

| Approach | Pros | Cons |
|---|---|---|
| **Silent drop** | Simple | Data loss, invisible, unrecoverable |
| **Dead letter queue (Kafka topic)** | Durable, replayable | Requires Kafka consumer to monitor DLQ; bytes only, no context |
| **Log to file** | Simple | Not structured, no replay mechanism |
| **quarantine-dq** | Structured context, metrics, replay, dashboard | Disk space, slightly more complex |
| **Fail the job** | Nothing lost | Pipeline outage for every bad event |

### Interview answer

> "Silent drops are silent failures. With quarantine-dq, every bad event is preserved with its failure context, increments a Prometheus counter, and is replayable once the root cause is fixed. The key insight is: a bad event is not just noise — it's a signal. You want to know *how many*, *what kind*, and *whether you can recover*. Quarantine gives you all three."

---

## Quick Reference Card

| Concept | One-liner | Where in Meridian |
|---|---|---|
| Event time | Use the timestamp from the payload, not the arrival clock | `revenue_aggregator.py` |
| Watermark | "I've seen all events up to time W" — Flink's clock for event-time windows | `WatermarkStrategy.for_bounded_out_of_orderness(30s)` |
| Tumbling window | Non-overlapping fixed buckets — each event in exactly one window | 1m and 5m revenue windows |
| Sliding window | Overlapping — events in multiple windows | Not used (use tumbling for revenue) |
| Session window | Gap-based — closes after inactivity | Not used (no clickstream sessions) |
| Allowed lateness | Hold window state open after watermark passes — re-fire on late events | `allowed_lateness(5m)` |
| Side output | Secondary stream for events that miss even allowed lateness | `get_side_output(late_tag)` |
| CEP | Pattern matching across multiple events in a stream | `fraud_detector.py` |
| Keyed state | Per-key operator state, checkpointed, survives failures | `inventory_alerts.py` ValueState |
| Checkpointing | Periodic consistent snapshot of all state + Kafka offsets | Every 60s, EXACTLY_ONCE mode |
| 2PC | Flink writes Iceberg files in pre-commit, commits metadata only on checkpoint success | Iceberg sink TwoPhaseCommitSinkFunction |
| Iceberg snapshot | Immutable, append-only metadata update — enables time travel | `iceberg/time_travel.py` |
| Schema evolution | Column IDs survive renames — old Parquet files read with new schema | `iceberg/writer.py _evolve_schema_if_needed` |
| quarantine-dq | Preserve bad events with context + metrics + replay | `quarantine_int/stream_quarantine.py` |
