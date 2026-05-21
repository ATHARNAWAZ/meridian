"""
Shared pytest fixtures for Meridian tests.

Tests that hit Kafka, Flink, or Iceberg are marked @pytest.mark.integration
and skipped by default. Run with: pytest -m integration
"""

import json
import os
import sys
import tempfile
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")


def is_kafka_available() -> bool:
    try:
        from kafka import KafkaAdminClient
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, client_id="test-probe")
        admin.close()
        return True
    except Exception:
        return False


def is_schema_registry_available() -> bool:
    import requests
    try:
        resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=3)
        return resp.ok
    except Exception:
        return False


# Marks
requires_kafka = pytest.mark.skipif(
    not is_kafka_available(),
    reason="Kafka not available (run `make up` first)",
)
requires_schema_registry = pytest.mark.skipif(
    not is_schema_registry_available(),
    reason="Schema Registry not available (run `make up` first)",
)


@pytest.fixture
def temp_quarantine_store(tmp_path):
    """A temporary quarantine store for unit tests."""
    os.environ["QUARANTINE_STORE_PATH"] = str(tmp_path / "quarantine_store")
    yield str(tmp_path / "quarantine_store")
    del os.environ["QUARANTINE_STORE_PATH"]


@pytest.fixture
def sample_order():
    return {
        "event_id": "test-order-001",
        "user_id": "USER-000001",
        "product_id": "PROD-00001",
        "category": "Electronics",
        "amount": 299.99,
        "quantity": 1,
        "currency": "EUR",
        "country": "DE",
        "event_ts": int(time.time() * 1000),
        "is_fraud": False,
    }


@pytest.fixture
def sample_bad_order():
    """An order with a null event_id — should be quarantined."""
    return {
        "event_id": None,
        "user_id": "USER-000001",
        "product_id": "PROD-00001",
        "category": "Electronics",
        "amount": 299.99,
        "quantity": 1,
        "currency": "EUR",
        "country": "DE",
        "event_ts": int(time.time() * 1000),
        "is_fraud": False,
    }
