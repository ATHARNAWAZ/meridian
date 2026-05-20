from enum import Enum
from dataclasses import dataclass, field
from typing import Optional


class FailureType(str, Enum):
    SCHEMA_VIOLATION = "SCHEMA_VIOLATION"
    LATE_ARRIVAL = "LATE_ARRIVAL"
    DESERIALIZATION_ERROR = "DESERIALIZATION_ERROR"
    NULL_KEY_FIELD = "NULL_KEY_FIELD"
    NEGATIVE_AMOUNT = "NEGATIVE_AMOUNT"
    INVALID_CURRENCY = "INVALID_CURRENCY"
    MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
    UNKNOWN = "UNKNOWN"


@dataclass
class QuarantineContext:
    pipeline: str
    stage: str
    failure_type: FailureType
    reason: str
    detail: dict = field(default_factory=dict)
    replayable: bool = True
