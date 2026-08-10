"""Recording which shards a stage has finished, so an interrupted run resumes.

Reading a shard costs real bytes and rendering one costs real minutes, so a run that
dies part way through 14 shards must not start over. The record is append-only and
written *after* a shard's files are closed, so it can never claim work that is not on
disk; a shard present on disk but missing from the record is simply redone, which is
wasteful but never wrong.

Cheaper than it looks to get wrong the other way round: recording before the write would
skip a shard whose file is truncated, and the truncation would reach the published corpus.
"""

import json
import logging
import os
from collections.abc import Callable, Mapping
from pathlib import Path

LOGGER = logging.getLogger(__name__)


def read_completed(path: Path) -> dict[str, dict[str, int]]:
    """Shards already finished, mapped to the rows each split took from them."""
    if not path.exists():
        return {}
    completed: dict[str, dict[str, int]] = {}
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
            completed[str(record["shard"])] = {
                str(split): int(rows) for split, rows in record.get("rows", {}).items()
            }
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            # A crash mid-append can leave a partial final line. Everything before it
            # is still trustworthy, so keep it and redo the rest.
            LOGGER.warning(
                "%s line %d is unreadable; treating that shard as unfinished",
                path,
                line_number,
            )
    return completed


def record_completed(path: Path, shard: str, rows: Mapping[str, int]) -> None:
    """Append one finished shard, flushed to disk before returning."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps({"shard": shard, "rows": dict(rows)}) + "\n")
        f.flush()
        os.fsync(f.fileno())


def write_atomically(path: Path, write: Callable[[Path], None]) -> None:
    """Write via a temporary name and rename on success.

    Without this a failure part way leaves a truncated `.parquet` that a later run would
    treat as finished — or that publishing would upload.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_suffix(path.suffix + ".partial")
    try:
        write(partial)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise
    partial.replace(path)
