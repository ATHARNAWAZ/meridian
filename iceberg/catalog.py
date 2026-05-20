"""
Iceberg catalog configuration for Meridian.

Uses pyiceberg's REST catalog pointed at the local MinIO bucket.
All Flink jobs share this catalog configuration.

LEARNING NOTE — Why Apache Iceberg?
  Raw Parquet files have no ACID guarantees. Two writers = corruption.
  Iceberg adds:
  - ACID transactions (snapshot isolation): each write creates a new snapshot;
    readers always see a consistent view regardless of concurrent writers.
  - Time travel: SELECT ... AT SNAPSHOT <id> or AS OF TIMESTAMP <ts>
  - Schema evolution: add/rename/drop columns without rewriting data files
  - Hidden partitioning: partition by month(event_ts) without changing queries
  - Compaction: merge small files automatically (critical for streaming sinks
    which produce many small files per checkpoint)

  For streaming sinks, Iceberg's atomic commit is critical:
  either all events in a Flink checkpoint land in Iceberg, or none do.
  This gives exactly-once semantics end-to-end.

  THE ICEBERG FILE FORMAT:
    data/
      orders/
        data/            ← Parquet data files
        metadata/        ← JSON metadata files (one per snapshot)
          v1.metadata.json
          v2.metadata.json   ← each snapshot = one metadata file
          snap-123.avro       ← manifest list (points to manifests)
          manifest-abc.avro   ← manifest (points to data files)

  Each write appends to the metadata chain. Time travel = loading an
  older metadata file. Schema evolution = updating the metadata schema
  without rewriting data files.
"""

import os
from functools import lru_cache

from pyiceberg.catalog import Catalog, load_catalog


@lru_cache(maxsize=1)
def get_catalog() -> Catalog:
    """
    Returns a cached pyiceberg catalog pointing at the local MinIO/S3 warehouse.
    """
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    warehouse = os.environ.get("ICEBERG_WAREHOUSE", "s3://iceberg-warehouse")

    return load_catalog(
        "meridian",
        **{
            "type": "sql",
            "uri": "sqlite:///iceberg_catalog.db",
            "warehouse": warehouse,
            "s3.endpoint": endpoint,
            "s3.access-key-id": access_key,
            "s3.secret-access-key": secret_key,
            "s3.path-style-access": "true",
        },
    )
