"""Tests for quarantine-dq integration: bad events quarantined and replayable."""

import time

import pytest

from quarantine.context import FailureType, QuarantineContext
from quarantine.store import QuarantineStore
from quarantine.stores.factory import get_store
from quarantine_int.stream_quarantine import StreamQuarantine


class TestQuarantineStore:
    def test_save_and_retrieve(self, tmp_path):
        store = QuarantineStore(str(tmp_path / "qs"))
        ctx = QuarantineContext(
            pipeline="test",
            stage="unit_test",
            failure_type=FailureType.SCHEMA_VIOLATION,
            reason="event_id is null",
            detail={"field": "event_id"},
        )
        record = {"event_id": None, "amount": 100.0}
        record_id = store.save(record, ctx)
        assert record_id is not None

        retrieved = store.get(record_id)
        assert retrieved is not None
        assert retrieved.pipeline == "test"
        assert retrieved.failure_type == FailureType.SCHEMA_VIOLATION.value
        assert retrieved.original_record["amount"] == 100.0

    def test_list_records_by_pipeline(self, tmp_path):
        store = QuarantineStore(str(tmp_path / "qs"))
        ctx = QuarantineContext(
            pipeline="meridian", stage="s", failure_type=FailureType.NULL_KEY_FIELD,
            reason="null", replayable=True
        )
        store.save({"x": 1}, ctx)
        store.save({"x": 2}, ctx)

        records = store.list_records(pipeline="meridian")
        assert len(records) == 2

        records_other = store.list_records(pipeline="other")
        assert len(records_other) == 0

    def test_mark_replayed(self, tmp_path):
        store = QuarantineStore(str(tmp_path / "qs"))
        ctx = QuarantineContext(
            pipeline="test", stage="s",
            failure_type=FailureType.LATE_ARRIVAL, reason="late"
        )
        rid = store.save({"y": 1}, ctx)
        result = store.mark_replayed(rid, "SUCCESS")
        assert result is True

        rec = store.get(rid)
        assert rec.replay_status == "SUCCESS"

    def test_replayable_only_filter(self, tmp_path):
        store = QuarantineStore(str(tmp_path / "qs"))

        ctx_replayable = QuarantineContext(
            pipeline="test", stage="s",
            failure_type=FailureType.SCHEMA_VIOLATION, reason="r", replayable=True
        )
        ctx_not_replayable = QuarantineContext(
            pipeline="test", stage="s",
            failure_type=FailureType.UNKNOWN, reason="r", replayable=False
        )
        store.save({"a": 1}, ctx_replayable)
        store.save({"b": 2}, ctx_not_replayable)

        replayable = store.list_records(replayable_only=True)
        assert len(replayable) == 1
        assert replayable[0].replayable is True


class TestStreamQuarantine:
    def test_quarantine_record(self, tmp_path):
        sq = StreamQuarantine("meridian", "test_stage", store_path=str(tmp_path))
        record_id = sq.quarantine_record(
            {"event_id": None, "amount": 50.0},
            failure_type="SCHEMA_VIOLATION",
            reason="event_id is null",
        )
        assert record_id is not None

        stats = sq.get_quarantine_stats()
        assert stats.get("SCHEMA_VIOLATION", 0) == 1

    def test_quarantine_late_event(self, tmp_path):
        sq = StreamQuarantine("meridian", "late_handler", store_path=str(tmp_path))
        record_id = sq.quarantine_late_event(
            {"event_id": "x", "event_ts": int(time.time() * 1000) - 600_000},
            how_late_seconds=600,
            original_window="2024-01-01T00:00:00/2024-01-01T00:01:00",
        )
        assert record_id is not None

        all_stats = sq.get_all_stats()
        assert any("LATE_ARRIVAL" in k for k in all_stats)
