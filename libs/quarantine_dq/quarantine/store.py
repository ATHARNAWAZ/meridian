import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from quarantine.record import QuarantineRecord
from quarantine.context import QuarantineContext


class QuarantineStore:
    """
    File-based quarantine store. Each record is saved as a JSON file.
    The store directory is partitioned by pipeline/date for easy browsing.
    """

    def __init__(self, store_path: str = "./quarantine_store"):
        self.store_path = Path(store_path)
        self.store_path.mkdir(parents=True, exist_ok=True)

    def save(self, record: dict, context: QuarantineContext) -> str:
        qr = QuarantineRecord.create(record, context)
        record_path = self._record_path(qr)
        record_path.parent.mkdir(parents=True, exist_ok=True)
        with open(record_path, "w") as f:
            json.dump(self._serialize(qr), f, default=str, indent=2)
        return qr.id

    def get(self, record_id: str) -> Optional[QuarantineRecord]:
        for path in self.store_path.rglob(f"{record_id}.json"):
            with open(path) as f:
                return self._deserialize(json.load(f))
        return None

    def list_records(
        self,
        pipeline: Optional[str] = None,
        failure_type: Optional[str] = None,
        replayable_only: bool = False,
    ) -> List[QuarantineRecord]:
        records = []
        for path in self.store_path.rglob("*.json"):
            try:
                with open(path) as f:
                    data = json.load(f)
                qr = self._deserialize(data)
                if pipeline and qr.pipeline != pipeline:
                    continue
                if failure_type and qr.failure_type != failure_type:
                    continue
                if replayable_only and not qr.replayable:
                    continue
                records.append(qr)
            except Exception:
                pass
        return records

    def mark_replayed(self, record_id: str, status: str = "SUCCESS") -> bool:
        for path in self.store_path.rglob(f"{record_id}.json"):
            with open(path) as f:
                data = json.load(f)
            data["replayed_at"] = datetime.now(timezone.utc).isoformat()
            data["replay_status"] = status
            with open(path, "w") as f:
                json.dump(data, f, indent=2)
            return True
        return False

    def get_stats(self) -> dict:
        counts: dict = {}
        for path in self.store_path.rglob("*.json"):
            try:
                with open(path) as f:
                    data = json.load(f)
                key = (data.get("pipeline", "unknown"), data.get("failure_type", "unknown"))
                counts[key] = counts.get(key, 0) + 1
            except Exception:
                pass
        return {f"{p}/{ft}": c for (p, ft), c in counts.items()}

    def _record_path(self, qr: QuarantineRecord) -> Path:
        date_str = qr.created_at.strftime("%Y-%m-%d")
        return self.store_path / qr.pipeline / date_str / f"{qr.id}.json"

    @staticmethod
    def _serialize(qr: QuarantineRecord) -> dict:
        return {
            "id": qr.id,
            "pipeline": qr.pipeline,
            "stage": qr.stage,
            "failure_type": qr.failure_type,
            "reason": qr.reason,
            "detail": qr.detail,
            "original_record": qr.original_record,
            "created_at": qr.created_at.isoformat(),
            "replayable": qr.replayable,
            "replayed_at": qr.replayed_at.isoformat() if qr.replayed_at else None,
            "replay_status": qr.replay_status,
        }

    @staticmethod
    def _deserialize(data: dict) -> QuarantineRecord:
        from datetime import datetime
        return QuarantineRecord(
            id=data["id"],
            pipeline=data["pipeline"],
            stage=data["stage"],
            failure_type=data["failure_type"],
            reason=data["reason"],
            detail=data.get("detail", {}),
            original_record=data.get("original_record", {}),
            created_at=datetime.fromisoformat(data["created_at"]),
            replayable=data.get("replayable", True),
            replayed_at=datetime.fromisoformat(data["replayed_at"]) if data.get("replayed_at") else None,
            replay_status=data.get("replay_status"),
        )
