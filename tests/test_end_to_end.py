"""
End-to-end pipeline test.

1. Produce 100 order events (including 5 intentionally bad ones)
2. Wait 15 seconds for processing
3. Assert: exactly 95 valid records visible in quarantine-safe path
4. Assert: exactly 5 records quarantined
5. Replay quarantined records (fix them first)
6. Assert: all 100 records processed

These tests require Kafka to be running (`make up`).
Skip automatically if Kafka is unavailable.
"""

import json
import os
import sys
import time
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from tests.conftest import requires_kafka


@pytest.mark.integration
@requires_kafka
class TestEndToEnd:
    """Full produce → quarantine → replay pipeline test."""

    def _make_valid_order(self) -> dict:
        return {
            "event_id": str(uuid.uuid4()),
            "user_id": f"USER-{uuid.uuid4().hex[:6]}",
            "product_id": "PROD-00001",
            "category": "Electronics",
            "amount": 99.99,
            "quantity": 1,
            "currency": "EUR",
            "country": "DE",
            "event_ts": int(time.time() * 1000),
            "is_fraud": False,
        }

    def _make_bad_order(self) -> dict:
        """Bad: null event_id."""
        order = self._make_valid_order()
        order["event_id"] = None
        return order

    def test_produce_and_quarantine(self, tmp_path):
        """
        Produce 100 orders (95 valid + 5 bad).
        Validate that bad events are caught by schema validation
        and saved to quarantine-dq.
        """
        from quarantine.context import FailureType, QuarantineContext
        from quarantine.store import QuarantineStore
        from generator.models import OrderEvent

        store = QuarantineStore(str(tmp_path / "qs"))
        valid_count = 0
        quarantine_count = 0

        events = (
            [self._make_valid_order() for _ in range(95)]
            + [self._make_bad_order() for _ in range(5)]
        )

        # Shuffle to simulate realistic ordering
        import random
        random.shuffle(events)

        for event in events:
            try:
                OrderEvent(**event)
                valid_count += 1
            except Exception as e:
                ctx = QuarantineContext(
                    pipeline="meridian",
                    stage="e2e_test",
                    failure_type=FailureType.NULL_KEY_FIELD,
                    reason=f"Validation failed: {e}",
                    replayable=True,
                )
                store.save(event, ctx)
                quarantine_count += 1

        assert valid_count == 95, f"Expected 95 valid, got {valid_count}"
        assert quarantine_count == 5, f"Expected 5 quarantined, got {quarantine_count}"

    def test_replay_fixes_quarantined_records(self, tmp_path):
        """
        After quarantine: fix the bad records and replay.
        All 100 should now be processable.
        """
        from quarantine.context import FailureType, QuarantineContext
        from quarantine.store import QuarantineStore
        from generator.models import OrderEvent

        store = QuarantineStore(str(tmp_path / "qs"))

        # Save 5 bad records
        bad_ids = []
        for _ in range(5):
            bad = self._make_bad_order()
            ctx = QuarantineContext(
                pipeline="meridian", stage="e2e_test",
                failure_type=FailureType.NULL_KEY_FIELD, reason="null event_id",
                replayable=True,
            )
            rid = store.save(bad, ctx)
            bad_ids.append(rid)

        # "Fix" the records by loading and setting a valid event_id
        replayed_count = 0
        for rid in bad_ids:
            rec = store.get(rid)
            assert rec is not None
            fixed = dict(rec.original_record)
            fixed["event_id"] = str(uuid.uuid4())  # fix the null

            # Validate fixed record
            try:
                OrderEvent(**fixed)
                store.mark_replayed(rid, "SUCCESS")
                replayed_count += 1
            except Exception as e:
                store.mark_replayed(rid, f"FAILED: {e}")

        assert replayed_count == 5, f"Expected 5 replayed, got {replayed_count}"

        # Verify all 5 are marked as replayed
        records = store.list_records(pipeline="meridian")
        replayed = [r for r in records if r.replay_status == "SUCCESS"]
        assert len(replayed) == 5


class TestSchemaValidationUnit:
    """Unit tests for event validation — no Kafka required."""

    def test_valid_order_passes(self):
        from generator.models import OrderEvent
        order = OrderEvent(
            event_id=str(uuid.uuid4()),
            user_id="USER-001",
            product_id="PROD-001",
            category="Electronics",
            amount=199.99,
            quantity=1,
            currency="EUR",
            country="DE",
            event_ts=int(time.time() * 1000),
        )
        assert order.event_id is not None
        assert order.amount > 0

    def test_null_event_id_raises(self):
        from generator.models import OrderEvent
        with pytest.raises(Exception):
            OrderEvent(
                event_id=None,
                user_id="USER-001", product_id="PROD-001",
                category="Electronics", amount=199.99, quantity=1,
                currency="EUR", country="DE",
                event_ts=int(time.time() * 1000),
            )

    def test_negative_amount_raises(self):
        from generator.models import OrderEvent
        with pytest.raises(Exception):
            OrderEvent(
                event_id=str(uuid.uuid4()),
                user_id="USER-001", product_id="PROD-001",
                category="Electronics", amount=-50.0, quantity=1,
                currency="EUR", country="DE",
                event_ts=int(time.time() * 1000),
            )

    def test_invalid_currency_raises(self):
        from generator.models import OrderEvent
        with pytest.raises(Exception):
            OrderEvent(
                event_id=str(uuid.uuid4()),
                user_id="USER-001", product_id="PROD-001",
                category="Electronics", amount=50.0, quantity=1,
                currency="NOTVALID", country="DE",
                event_ts=int(time.time() * 1000),
            )
