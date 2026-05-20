"""
Schema Registry client for Meridian.

Registers all 5 Avro schemas at startup and provides serialize/deserialize
helpers backed by fastavro. Raises a clear error on connection failure — no
silent drops.
"""

import io
import json
import os
import struct
from pathlib import Path
from typing import Any

import fastavro
import requests

SCHEMA_DIR = Path(__file__).parent / "schemas"

TOPIC_TO_SCHEMA = {
    "orders": "order_event",
    "clicks": "click_event",
    "payments": "payment_event",
    "inventory": "inventory_event",
    "returns": "return_event",
}

# Confluent wire format magic byte + 4-byte schema ID prefix
_MAGIC_BYTE = 0


class SchemaRegistryClient:
    def __init__(self, url: str | None = None):
        self.url = (url or os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")).rstrip("/")
        self._schema_cache: dict[str, Any] = {}   # schema_name -> parsed schema
        self._id_cache: dict[str, int] = {}         # schema_name -> schema ID

    def _load_schema(self, schema_name: str) -> dict:
        if schema_name not in self._schema_cache:
            path = SCHEMA_DIR / f"{schema_name}.avsc"
            if not path.exists():
                raise FileNotFoundError(f"Schema file not found: {path}")
            with open(path) as f:
                raw = json.load(f)
            self._schema_cache[schema_name] = fastavro.parse_schema(raw)
        return self._schema_cache[schema_name]

    def _check_reachable(self):
        try:
            resp = requests.get(f"{self.url}/subjects", timeout=5)
            resp.raise_for_status()
        except requests.ConnectionError as e:
            raise RuntimeError(
                f"Schema Registry unreachable at {self.url}. "
                "Ensure the schema-registry container is running. "
                f"Original error: {e}"
            ) from e

    def register_schema(self, schema_name: str) -> int:
        """Register an Avro schema and return its integer schema ID."""
        schema = self._load_schema(schema_name)
        subject = f"{schema_name}-value"
        payload = {"schema": json.dumps(schema)}
        resp = requests.post(
            f"{self.url}/subjects/{subject}/versions",
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            json=payload,
            timeout=10,
        )
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Failed to register schema '{schema_name}': "
                f"HTTP {resp.status_code} — {resp.text}"
            )
        schema_id = resp.json()["id"]
        self._id_cache[schema_name] = schema_id
        return schema_id

    def register_all(self) -> dict[str, int]:
        """Register all 5 event schemas. Returns {schema_name: schema_id}."""
        self._check_reachable()
        results = {}
        for schema_name in TOPIC_TO_SCHEMA.values():
            schema_id = self.register_schema(schema_name)
            results[schema_name] = schema_id
        return results

    def get_schema_id(self, schema_name: str) -> int:
        if schema_name not in self._id_cache:
            self.register_schema(schema_name)
        return self._id_cache[schema_name]

    def serialize(self, schema_name: str, data: dict) -> bytes:
        """Serialize data to Confluent Avro wire format (magic byte + schema ID + avro bytes)."""
        schema = self._load_schema(schema_name)
        schema_id = self.get_schema_id(schema_name)

        buf = io.BytesIO()
        buf.write(struct.pack(">bI", _MAGIC_BYTE, schema_id))
        fastavro.schemaless_writer(buf, schema, data)
        return buf.getvalue()

    def deserialize(self, schema_name: str, data: bytes) -> dict:
        """Deserialize Confluent Avro wire format back to a dict."""
        if len(data) < 5:
            raise ValueError(f"Message too short to be valid Avro wire format: {len(data)} bytes")

        magic, schema_id = struct.unpack(">bI", data[:5])
        if magic != _MAGIC_BYTE:
            raise ValueError(f"Invalid Confluent magic byte: {magic!r}, expected {_MAGIC_BYTE!r}")

        schema = self._load_schema(schema_name)
        buf = io.BytesIO(data[5:])
        return fastavro.schemaless_reader(buf, schema)


# Module-level singleton
_client: SchemaRegistryClient | None = None


def get_client(url: str | None = None) -> SchemaRegistryClient:
    global _client
    if _client is None:
        _client = SchemaRegistryClient(url)
    return _client


def serialize(schema_name: str, data: dict) -> bytes:
    return get_client().serialize(schema_name, data)


def deserialize(schema_name: str, data: bytes) -> dict:
    return get_client().deserialize(schema_name, data)
