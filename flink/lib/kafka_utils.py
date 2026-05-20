"""
Shared Kafka source/sink utilities for all Flink jobs.
Centralises bootstrap server, group ID, and deserialisation config.
"""

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)


def make_kafka_source(
    topic: str,
    group_id: str,
    bootstrap_servers: str | None = None,
    from_earliest: bool = True,
) -> KafkaSource:
    """Build a KafkaSource for a single topic with JSON deserialisation."""
    servers = bootstrap_servers or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    offset = KafkaOffsetsInitializer.earliest() if from_earliest else KafkaOffsetsInitializer.latest()

    return (
        KafkaSource.builder()
        .set_bootstrap_servers(servers)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(offset)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def parse_json_event(raw: str) -> dict | None:
    """Parse a JSON string, returning None on failure."""
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
