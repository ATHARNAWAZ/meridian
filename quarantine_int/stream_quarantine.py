"""
StreamQuarantine — bridges Flink side outputs and bad events to quarantine-dq.

Provides a unified interface for all Flink jobs to quarantine records.
Each Flink job instantiates StreamQuarantine with its pipeline+stage,
then calls quarantine_record() or quarantine_late_event() for bad events.

Usage in Flink job:
    sq = StreamQuarantine(pipeline="meridian", stage="fraud_detector")
    sq.quarantine_record(event, failure_type="SCHEMA_VIOLATION",
                         reason="order_id is null", detail={"field": "order_id"})
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from quarantine.context import FailureType, QuarantineContext
from quarantine.stores.factory import get_store

log = logging.getLogger(__name__)


class StreamQuarantine:
    """
    Bridges Flink side outputs and bad events to quarantine-dq.

    All quarantine calls are synchronous and write to the file store.
    For high-throughput scenarios, batch via a Flink sink instead.
    """

    def __init__(self, pipeline: str, stage: str, store_path: Optional[str] = None):
        self.pipeline = pipeline
        self.stage = stage
        self.store = get_store(store_path)
        self._counts: dict[str, int] = {}

    def quarantine_record(
        self,
        record: dict,
        failure_type: str | FailureType,
        reason: str,
        detail: Optional[dict] = None,
        replayable: bool = True,
    ) -> str:
        """Save a bad streaming event to the quarantine store. Returns the record ID."""
        if isinstance(failure_type, str):
            try:
                failure_type = FailureType(failure_type)
            except ValueError:
                failure_type = FailureType.UNKNOWN

        ctx = QuarantineContext(
            pipeline=self.pipeline,
            stage=self.stage,
            failure_type=failure_type,
            reason=reason,
            detail=detail or {},
            replayable=replayable,
        )

        record_id = self.store.save(record, ctx)
        ft_str = failure_type.value if hasattr(failure_type, "value") else str(failure_type)
        self._counts[ft_str] = self._counts.get(ft_str, 0) + 1
        log.debug("Quarantined record %s (type=%s)", record_id, ft_str)
        return record_id

    def quarantine_late_event(
        self,
        record: dict,
        how_late_seconds: int,
        original_window: str,
    ) -> str:
        """Special method for late events — sets failure_type=LATE_ARRIVAL."""
        return self.quarantine_record(
            record=record,
            failure_type=FailureType.LATE_ARRIVAL,
            reason=f"Event arrived {how_late_seconds}s late, past the allowed lateness boundary",
            detail={
                "how_late_seconds": how_late_seconds,
                "original_window": original_window,
                "event_ts": record.get("event_ts"),
                "processing_ts": int(datetime.now(timezone.utc).timestamp() * 1000),
            },
            replayable=True,
        )

    def quarantine_schema_violation(
        self,
        record: dict,
        field: str,
        violation: str,
    ) -> str:
        """Convenience method for schema violations."""
        return self.quarantine_record(
            record=record,
            failure_type=FailureType.SCHEMA_VIOLATION,
            reason=f"Schema violation on field '{field}': {violation}",
            detail={"field": field, "violation": violation},
            replayable=True,
        )

    def get_quarantine_stats(self) -> dict:
        """Returns in-memory counts by failure_type for this session."""
        return dict(self._counts)

    def get_all_stats(self) -> dict:
        """Returns counts from the persistent store (across all sessions)."""
        return self.store.get_stats()
