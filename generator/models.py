"""
Pydantic models mirroring each Avro schema.
Used for Python-side validation before serialisation.
"""

from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, Field, field_validator


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


class OrderEvent(BaseModel):
    event_id: str
    user_id: str
    product_id: str
    category: str
    amount: float
    quantity: int
    currency: str = "EUR"
    country: str
    event_ts: int = Field(default_factory=_now_ms)
    is_fraud: bool = False

    @field_validator("amount")
    @classmethod
    def amount_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError(f"amount must be positive, got {v}")
        return v

    @field_validator("currency")
    @classmethod
    def currency_valid(cls, v: str) -> str:
        if len(v) != 3 or not v.isalpha():
            raise ValueError(f"currency must be a 3-letter ISO code, got '{v}'")
        return v.upper()

    @field_validator("quantity")
    @classmethod
    def quantity_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError(f"quantity must be positive, got {v}")
        return v


class ClickEvent(BaseModel):
    event_id: str
    user_id: str
    product_id: str
    page: str
    session_id: str
    referrer: Optional[str] = None
    device_type: str
    event_ts: int = Field(default_factory=_now_ms)

    @field_validator("device_type")
    @classmethod
    def device_valid(cls, v: str) -> str:
        allowed = {"mobile", "desktop", "tablet"}
        if v not in allowed:
            raise ValueError(f"device_type must be one of {allowed}")
        return v


class PaymentEvent(BaseModel):
    event_id: str
    order_id: str
    user_id: str
    amount: float
    currency: str = "EUR"
    payment_method: str
    status: str
    gateway: str
    event_ts: int = Field(default_factory=_now_ms)

    @field_validator("status")
    @classmethod
    def status_valid(cls, v: str) -> str:
        allowed = {"PENDING", "SUCCESS", "FAILED", "REFUNDED"}
        if v not in allowed:
            raise ValueError(f"status must be one of {allowed}")
        return v


class InventoryEvent(BaseModel):
    event_id: str
    product_id: str
    warehouse_id: str
    stock_level: int
    quantity_changed: int
    reorder_threshold: int
    event_type: str
    event_ts: int = Field(default_factory=_now_ms)

    @field_validator("event_type")
    @classmethod
    def event_type_valid(cls, v: str) -> str:
        allowed = {"RESTOCK", "DEPLETION"}
        if v not in allowed:
            raise ValueError(f"event_type must be one of {allowed}")
        return v


class ReturnEvent(BaseModel):
    event_id: str
    order_id: str
    user_id: str
    product_id: str
    reason: str
    refund_amount: float
    event_ts: int = Field(default_factory=_now_ms)

    @field_validator("reason")
    @classmethod
    def reason_valid(cls, v: str) -> str:
        allowed = {"DEFECTIVE", "WRONG_ITEM", "NOT_AS_DESCRIBED", "CHANGED_MIND", "OTHER"}
        if v not in allowed:
            raise ValueError(f"reason must be one of {allowed}")
        return v

    @field_validator("refund_amount")
    @classmethod
    def refund_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError(f"refund_amount cannot be negative, got {v}")
        return v
