"""
Meridian synthetic event producer.

Generates realistic e-commerce events at configurable throughput and
publishes them to Kafka topics using Confluent Avro serialisation via
Schema Registry.

Usage:
    uv run python generator/producer.py --rate 500 --error-rate 0.02
"""

import argparse
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Allow imports from repo root
sys.path.insert(0, str(Path(__file__).parent.parent))

from confluent_kafka import Producer
from dotenv import load_dotenv
from faker import Faker
from rich.console import Console
from rich.live import Live
from rich.table import Table

from generator.models import (
    ClickEvent,
    InventoryEvent,
    OrderEvent,
    PaymentEvent,
    ReturnEvent,
)
from kafka.schema_registry import SchemaRegistryClient

load_dotenv()

console = Console()
fake = Faker("de_DE")

# ─── Constants ────────────────────────────────────────────────────────────────

TOPICS = {
    "orders":    ("order_event",     0.20),
    "clicks":    ("click_event",     0.40),
    "payments":  ("payment_event",   0.15),
    "inventory": ("inventory_event", 0.10),
    "returns":   ("return_event",    0.15),
}

CATEGORIES = [
    "Electronics", "Fashion", "Home & Garden", "Sports", "Books",
    "Beauty", "Toys", "Automotive", "Food & Grocery", "Health",
]

PRODUCT_IDS = [f"PROD-{i:05d}" for i in range(1, 501)]
USER_IDS = [f"USER-{i:06d}" for i in range(1, 10001)]
WAREHOUSE_IDS = ["WH-BERLIN", "WH-HAMBURG", "WH-MUNICH", "WH-FRANKFURT", "WH-KOELN"]
PAYMENT_METHODS = ["card", "paypal", "sepa", "klarna", "apple_pay"]
GATEWAYS = ["Stripe", "Adyen", "PayPal", "Braintree"]
RETURN_REASONS = ["DEFECTIVE", "WRONG_ITEM", "NOT_AS_DESCRIBED", "CHANGED_MIND", "OTHER"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
GERMAN_CITIES = [
    "Berlin", "Hamburg", "Munich", "Frankfurt", "Cologne",
    "Stuttgart", "Düsseldorf", "Leipzig", "Dortmund", "Essen",
]

# ─── Event factories ──────────────────────────────────────────────────────────

def _ts_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def make_order(user_id: str | None = None, bad: bool = False) -> tuple[str, dict]:
    uid = user_id or random.choice(USER_IDS)
    pid = random.choice(PRODUCT_IDS)

    if bad:
        # Inject one of several known bad patterns
        fault = random.choice(["null_id", "negative_amount", "invalid_currency", "missing_field"])
        if fault == "null_id":
            return "orders", {
                "event_id": None,  # violates non-null
                "user_id": uid, "product_id": pid, "category": random.choice(CATEGORIES),
                "amount": round(random.uniform(5, 2500), 2), "quantity": 1,
                "currency": "EUR", "country": "DE", "event_ts": _ts_ms(), "is_fraud": False,
            }
        elif fault == "negative_amount":
            return "orders", {
                "event_id": str(uuid.uuid4()), "user_id": uid, "product_id": pid,
                "category": random.choice(CATEGORIES),
                "amount": -round(random.uniform(5, 100), 2),  # negative
                "quantity": 1, "currency": "EUR", "country": "DE",
                "event_ts": _ts_ms(), "is_fraud": False,
            }
        elif fault == "invalid_currency":
            return "orders", {
                "event_id": str(uuid.uuid4()), "user_id": uid, "product_id": pid,
                "category": random.choice(CATEGORIES),
                "amount": round(random.uniform(5, 2500), 2), "quantity": 1,
                "currency": "INVALID",  # not a valid ISO code
                "country": "DE", "event_ts": _ts_ms(), "is_fraud": False,
            }
        else:  # missing_field — omit required amount
            return "orders", {
                "event_id": str(uuid.uuid4()), "user_id": uid, "product_id": pid,
                "category": random.choice(CATEGORIES), "quantity": 1,
                "currency": "EUR", "country": "DE", "event_ts": _ts_ms(), "is_fraud": False,
                # amount intentionally missing
            }

    event = OrderEvent(
        event_id=str(uuid.uuid4()),
        user_id=uid,
        product_id=pid,
        category=random.choice(CATEGORIES),
        amount=round(random.uniform(5, 2500), 2),
        quantity=random.randint(1, 10),
        currency="EUR",
        country="DE",
        event_ts=_ts_ms(),
    )
    return "orders", event.model_dump()


def make_click(user_id: str | None = None) -> tuple[str, dict]:
    uid = user_id or random.choice(USER_IDS)
    pid = random.choice(PRODUCT_IDS)
    event = ClickEvent(
        event_id=str(uuid.uuid4()),
        user_id=uid,
        product_id=pid,
        page=f"/product/{pid.lower()}",
        session_id=str(uuid.uuid4()),
        referrer=random.choice([None, "https://google.de", "https://bing.com", None, None]),
        device_type=random.choice(DEVICE_TYPES),
        event_ts=_ts_ms(),
    )
    return "clicks", event.model_dump()


def make_payment(user_id: str | None = None) -> tuple[str, dict]:
    uid = user_id or random.choice(USER_IDS)
    event = PaymentEvent(
        event_id=str(uuid.uuid4()),
        order_id=str(uuid.uuid4()),
        user_id=uid,
        amount=round(random.uniform(5, 2500), 2),
        currency="EUR",
        payment_method=random.choice(PAYMENT_METHODS),
        status=random.choices(
            ["SUCCESS", "PENDING", "FAILED", "REFUNDED"],
            weights=[0.75, 0.10, 0.10, 0.05],
        )[0],
        gateway=random.choice(GATEWAYS),
        event_ts=_ts_ms(),
    )
    return "payments", event.model_dump()


def make_inventory() -> tuple[str, dict]:
    pid = random.choice(PRODUCT_IDS)
    et = random.choices(["RESTOCK", "DEPLETION"], weights=[0.3, 0.7])[0]
    delta = random.randint(1, 50) if et == "RESTOCK" else -random.randint(1, 20)
    base = random.randint(50, 500)
    event = InventoryEvent(
        event_id=str(uuid.uuid4()),
        product_id=pid,
        warehouse_id=random.choice(WAREHOUSE_IDS),
        stock_level=max(0, base + delta),
        quantity_changed=abs(delta),
        reorder_threshold=20,
        event_type=et,
        event_ts=_ts_ms(),
    )
    return "inventory", event.model_dump()


def make_return(user_id: str | None = None) -> tuple[str, dict]:
    uid = user_id or random.choice(USER_IDS)
    event = ReturnEvent(
        event_id=str(uuid.uuid4()),
        order_id=str(uuid.uuid4()),
        user_id=uid,
        product_id=random.choice(PRODUCT_IDS),
        reason=random.choice(RETURN_REASONS),
        refund_amount=round(random.uniform(5, 500), 2),
        event_ts=_ts_ms(),
    )
    return "returns", event.model_dump()


TOPIC_FACTORIES = {
    "orders":    make_order,
    "clicks":    make_click,
    "payments":  make_payment,
    "inventory": make_inventory,
    "returns":   make_return,
}

SCHEMA_FOR_TOPIC = {t: s for t, (s, _) in TOPICS.items()}
TOPIC_WEIGHTS = [w for _, (_, w) in TOPICS.items()]
TOPIC_NAMES = list(TOPICS.keys())

# ─── Producer ─────────────────────────────────────────────────────────────────

class MeridianProducer:
    """
    Produces synthetic e-commerce events to Kafka.
    Configurable rate, error injection rate, and topic weights.

    Usage:
        producer = MeridianProducer(bootstrap_servers="localhost:9092")
        producer.run(events_per_second=500, error_rate=0.02)
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        schema_registry_url: str = "http://localhost:8081",
    ):
        self.bootstrap_servers = bootstrap_servers
        self.sr = SchemaRegistryClient(url=schema_registry_url)
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "1",
            "linger.ms": 5,
            "batch.size": 16384,
        })
        self._counts: dict[str, int] = {t: 0 for t in TOPIC_NAMES}
        self._errors = 0
        self._total = 0

    def _setup_schemas(self):
        console.print("[bold cyan]Registering schemas with Schema Registry...[/bold cyan]")
        ids = self.sr.register_all()
        for name, sid in ids.items():
            console.print(f"  [green]✓[/green] {name} (id={sid})")

    def _produce(self, topic: str, schema_name: str, data: dict, user_id: str):
        try:
            payload = self.sr.serialize(schema_name, data)
            self._producer.produce(
                topic=topic,
                key=user_id.encode(),
                value=payload,
            )
            self._counts[topic] += 1
            self._total += 1
        except Exception as e:
            self._errors += 1

    def _delivery_report(self, err, msg):
        if err:
            self._errors += 1

    def _make_table(self) -> Table:
        table = Table(title="[bold]Meridian Producer[/bold]", expand=True)
        table.add_column("Topic", style="cyan")
        table.add_column("Events", justify="right", style="green")
        table.add_column("Rate/s", justify="right")

        for topic in TOPIC_NAMES:
            table.add_row(
                topic,
                str(self._counts[topic]),
                "—",
            )

        table.add_section()
        table.add_row("[bold]TOTAL[/bold]", f"[bold]{self._total}[/bold]", "—")
        table.add_row("[red]Errors[/red]", f"[red]{self._errors}[/red]", "—")
        return table

    def run(self, events_per_second: int = 500, error_rate: float = 0.02):
        self._setup_schemas()
        console.print(f"\n[bold green]Starting producer:[/bold green] {events_per_second} events/sec, "
                      f"error_rate={error_rate:.1%}")
        console.print("Press [bold]Ctrl+C[/bold] to stop.\n")

        interval = 1.0 / events_per_second
        prev_total = 0
        prev_time = time.monotonic()

        with Live(self._make_table(), refresh_per_second=4, console=console) as live:
            try:
                while True:
                    topic = random.choices(TOPIC_NAMES, weights=TOPIC_WEIGHTS)[0]
                    schema_name = SCHEMA_FOR_TOPIC[topic]
                    is_bad = random.random() < error_rate

                    if topic == "orders":
                        uid = random.choice(USER_IDS)
                        _, data = make_order(user_id=uid, bad=is_bad)
                    elif topic == "clicks":
                        uid = random.choice(USER_IDS)
                        _, data = make_click(user_id=uid)
                    elif topic == "payments":
                        uid = random.choice(USER_IDS)
                        _, data = make_payment(user_id=uid)
                    elif topic == "inventory":
                        uid = "system"
                        _, data = make_inventory()
                    else:
                        uid = random.choice(USER_IDS)
                        _, data = make_return(user_id=uid)

                    if is_bad and topic == "orders":
                        self._errors += 1
                        self._total += 1
                        self._counts[topic] += 1
                    else:
                        self._produce(topic, schema_name, data, uid)

                    now = time.monotonic()
                    if now - prev_time >= 0.5:
                        elapsed = now - prev_time
                        rate = (self._total - prev_total) / elapsed
                        prev_total = self._total
                        prev_time = now

                        table = self._make_table()
                        for i, t in enumerate(TOPIC_NAMES):
                            pass  # table is rebuilt fresh each time
                        live.update(self._make_table())
                        self._producer.poll(0)

                    time.sleep(interval)

            except KeyboardInterrupt:
                console.print("\n[yellow]Flushing remaining messages...[/yellow]")
                self._producer.flush(timeout=10)
                console.print(f"[bold green]Done.[/bold green] Total: {self._total}, Errors: {self._errors}")


# ─── CLI entry point ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Meridian synthetic event producer")
    parser.add_argument("--rate", type=int, default=500, help="Events per second (default: 500)")
    parser.add_argument("--error-rate", type=float, default=0.02, help="Fraction of bad events (default: 0.02)")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--schema-registry", default="http://localhost:8081", help="Schema Registry URL")
    args = parser.parse_args()

    producer = MeridianProducer(
        bootstrap_servers=args.bootstrap_servers,
        schema_registry_url=args.schema_registry,
    )
    producer.run(events_per_second=args.rate, error_rate=args.error_rate)


if __name__ == "__main__":
    main()
