from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
import uuid


@dataclass
class QuarantineRecord:
    id: str
    pipeline: str
    stage: str
    failure_type: str
    reason: str
    detail: dict
    original_record: dict
    created_at: datetime
    replayable: bool
    replayed_at: Optional[datetime] = None
    replay_status: Optional[str] = None

    @classmethod
    def create(cls, record: dict, context) -> "QuarantineRecord":
        return cls(
            id=str(uuid.uuid4()),
            pipeline=context.pipeline,
            stage=context.stage,
            failure_type=context.failure_type.value if hasattr(context.failure_type, "value") else str(context.failure_type),
            reason=context.reason,
            detail=context.detail or {},
            original_record=record,
            created_at=datetime.now(timezone.utc),
            replayable=context.replayable,
        )
