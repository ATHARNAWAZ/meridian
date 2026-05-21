"""
Tests for the DuckDB query layer.
Uses in-memory DuckDB — does not require Iceberg or MinIO.
"""

import os
import time
from pathlib import Path

import pandas as pd
import pytest


class TestQueryLayerUnit:
    """Unit tests that run without Kafka/Iceberg (uses mock data via DuckDB views)."""

    @pytest.fixture
    def ql(self, tmp_path, monkeypatch):
        """Create a QueryLayer backed by an in-memory DuckDB."""
        monkeypatch.setenv("MINIO_ENDPOINT", "http://localhost:9000")
        monkeypatch.setenv("MINIO_ACCESS_KEY", "minioadmin")
        monkeypatch.setenv("MINIO_SECRET_KEY", "minioadmin")

        # Import here to avoid circular import at collection time
        import duckdb
        db_path = str(tmp_path / "test.duckdb")
        conn = duckdb.connect(db_path)

        # Pre-populate with test data so iceberg_scan calls fail gracefully
        conn.close()

        from duckdb_layer.query_layer import MeridianQueryLayer

        class MockQueryLayer(MeridianQueryLayer):
            def _setup_extensions(self, **kwargs):
                pass  # skip S3/iceberg setup in unit tests

        ql = MockQueryLayer(duckdb_path=db_path)
        return ql

    def test_get_quarantine_stats_empty(self, ql, tmp_path, monkeypatch):
        """Returns empty DataFrame when no quarantine store exists."""
        monkeypatch.setenv("QUARANTINE_STORE_PATH", str(tmp_path / "nonexistent"))
        df = ql.get_quarantine_stats()
        assert isinstance(df, pd.DataFrame)

    def test_get_quarantine_stats_with_data(self, ql, tmp_path, monkeypatch):
        """Returns DataFrame with correct columns when quarantine store has records."""
        store_path = tmp_path / "qstore"
        monkeypatch.setenv("QUARANTINE_STORE_PATH", str(store_path))

        # Write a fake quarantine record
        from quarantine.context import FailureType, QuarantineContext
        from quarantine.store import QuarantineStore
        store = QuarantineStore(str(store_path))
        ctx = QuarantineContext(
            pipeline="meridian", stage="test",
            failure_type=FailureType.SCHEMA_VIOLATION, reason="null id"
        )
        store.save({"event_id": None}, ctx)

        # Clear the module-level result cache so monkeypatched env var is picked up
        import duckdb_layer.query_layer as _ql_mod
        _ql_mod._cache.clear()

        df = ql.get_quarantine_stats()
        assert not df.empty
        assert "failure_type" in df.columns
        assert "pipeline" in df.columns

    def test_methods_return_dataframes(self, ql):
        """All public query methods return DataFrames (may be empty on no data)."""
        # These will fail gracefully when Iceberg is not available
        for method_name in [
            "get_fraud_alerts",
            "get_late_event_stats",
        ]:
            method = getattr(ql, method_name)
            result = method()
            assert isinstance(result, pd.DataFrame), f"{method_name} did not return DataFrame"
