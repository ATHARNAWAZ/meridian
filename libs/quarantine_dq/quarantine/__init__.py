"""
quarantine-dq: Dead-letter queue library for catching, storing, and replaying
bad streaming events before they corrupt downstream tables.

Core API:
    quarantine(record, context)  — save a bad record to the store
    QuarantineContext            — metadata about the failure
    FailureType                  — enum of known failure categories
"""

from quarantine.context import QuarantineContext, FailureType
from quarantine.store import QuarantineStore
from quarantine.record import QuarantineRecord

_default_store: QuarantineStore | None = None


def _get_default_store() -> QuarantineStore:
    global _default_store
    if _default_store is None:
        import os
        store_path = os.environ.get("QUARANTINE_STORE_PATH", "./quarantine_store")
        _default_store = QuarantineStore(store_path)
    return _default_store


def quarantine(record: dict, context: "QuarantineContext") -> str:
    """Save a bad record to the default quarantine store. Returns the record ID."""
    store = _get_default_store()
    return store.save(record, context)


__all__ = ["quarantine", "QuarantineContext", "FailureType", "QuarantineStore", "QuarantineRecord"]
