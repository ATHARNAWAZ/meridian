"""Tests for Avro schema loading and serialisation/deserialisation."""

import json
import struct
import time
from pathlib import Path

import pytest


class TestSchemaLoading:
    def test_all_schema_files_exist(self):
        schema_dir = Path("kafka/schemas")
        for name in ["order_event", "click_event", "payment_event", "inventory_event", "return_event"]:
            assert (schema_dir / f"{name}.avsc").exists(), f"Missing schema: {name}.avsc"

    def test_schema_is_valid_json(self):
        schema_dir = Path("kafka/schemas")
        for avsc in schema_dir.glob("*.avsc"):
            with open(avsc) as f:
                data = json.load(f)
            assert data["type"] == "record"
            assert "name" in data
            assert "fields" in data

    def test_order_schema_required_fields(self):
        with open("kafka/schemas/order_event.avsc") as f:
            schema = json.load(f)
        field_names = [f["name"] for f in schema["fields"]]
        required = ["event_id", "user_id", "product_id", "amount", "event_ts"]
        for r in required:
            assert r in field_names, f"Missing required field: {r}"


class TestAvroParsing:
    def test_fastavro_can_parse_order_schema(self):
        import fastavro
        with open("kafka/schemas/order_event.avsc") as f:
            raw = json.load(f)
        parsed = fastavro.parse_schema(raw)
        assert parsed is not None

    def test_roundtrip_order_event(self):
        """Serialize then deserialize an order event and check value equality."""
        import fastavro
        import io
        with open("kafka/schemas/order_event.avsc") as f:
            raw = json.load(f)
        schema = fastavro.parse_schema(raw)

        record = {
            "event_id": "test-123",
            "user_id": "USER-001",
            "product_id": "PROD-001",
            "category": "Electronics",
            "amount": 199.99,
            "quantity": 2,
            "currency": "EUR",
            "country": "DE",
            "event_ts": int(time.time() * 1000),
            "is_fraud": False,
        }

        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, schema, record)
        buf.seek(0)
        result = fastavro.schemaless_reader(buf, schema)

        assert result["event_id"] == record["event_id"]
        assert result["amount"] == pytest.approx(record["amount"], rel=1e-5)
        assert result["is_fraud"] == False
