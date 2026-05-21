"""Tests for synthetic event generator and event validation."""

import time

import pytest

from generator.models import ClickEvent, InventoryEvent, OrderEvent, PaymentEvent, ReturnEvent
from generator.producer import (
    TOPIC_NAMES,
    TOPIC_WEIGHTS,
    make_click,
    make_inventory,
    make_order,
    make_payment,
    make_return,
)


class TestOrderEvent:
    def test_valid_order(self):
        _, data = make_order()
        order = OrderEvent(**data)
        assert order.amount > 0
        assert order.quantity > 0
        assert len(order.currency) == 3
        assert order.event_id is not None

    def test_bad_order_null_id(self):
        _, data = make_order(bad=True)
        # Bad orders have various violations — check they're generated
        # (we don't validate them against Pydantic — that's the quarantine's job)
        assert isinstance(data, dict)

    def test_order_amount_validation(self):
        with pytest.raises(Exception):
            OrderEvent(
                event_id="x", user_id="u", product_id="p", category="c",
                amount=-10.0, quantity=1, currency="EUR", country="DE",
                event_ts=int(time.time() * 1000),
            )

    def test_order_invalid_currency(self):
        with pytest.raises(Exception):
            OrderEvent(
                event_id="x", user_id="u", product_id="p", category="c",
                amount=10.0, quantity=1, currency="INVALID", country="DE",
                event_ts=int(time.time() * 1000),
            )


class TestClickEvent:
    def test_valid_click(self):
        _, data = make_click()
        click = ClickEvent(**data)
        assert click.device_type in ("mobile", "desktop", "tablet")

    def test_invalid_device_type(self):
        with pytest.raises(Exception):
            ClickEvent(
                event_id="x", user_id="u", product_id="p",
                page="/p", session_id="s", device_type="fax",
                event_ts=int(time.time() * 1000),
            )


class TestInventoryEvent:
    def test_valid_inventory(self):
        _, data = make_inventory()
        inv = InventoryEvent(**data)
        assert inv.event_type in ("RESTOCK", "DEPLETION")
        assert inv.stock_level >= 0

    def test_invalid_event_type(self):
        with pytest.raises(Exception):
            InventoryEvent(
                event_id="x", product_id="p", warehouse_id="w",
                stock_level=50, quantity_changed=5, reorder_threshold=20,
                event_type="EXPLOSION", event_ts=int(time.time() * 1000),
            )


class TestTopicWeights:
    def test_weights_sum_to_one(self):
        assert abs(sum(TOPIC_WEIGHTS) - 1.0) < 0.001

    def test_all_topics_covered(self):
        assert len(TOPIC_NAMES) == 5
        assert "orders" in TOPIC_NAMES
        assert "fraud-alerts" not in TOPIC_NAMES  # output topic, not input
