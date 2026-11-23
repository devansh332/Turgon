import json
import sys
from datetime import datetime, timezone


def event(name: str, **fields) -> None:
    """One JSON object per line on stderr, so logs are greppable and machine-parseable."""
    record = {"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"), "event": name, **fields}
    print(json.dumps(record, default=str), file=sys.stderr, flush=True)
