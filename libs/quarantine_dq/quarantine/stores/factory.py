import os
from quarantine.store import QuarantineStore


def get_store(store_path: str = None) -> QuarantineStore:
    """Returns the configured quarantine store (file-based by default)."""
    path = store_path or os.environ.get("QUARANTINE_STORE_PATH", "./quarantine_store")
    return QuarantineStore(path)
