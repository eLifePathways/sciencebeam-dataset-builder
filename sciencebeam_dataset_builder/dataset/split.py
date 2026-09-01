"""Deterministic train/validation/test splitting that preserves the Parquet schema.

Two properties the previous pandas-based splitter lacked:

*Schema preservation.* Round-tripping a table through pandas silently rewrote the
schema - `string` became `large_string`, non-nullable columns became nullable, struct
child fields were reordered alphabetically, and an all-empty `list<string>` column
degraded to `list<null>`. Subsets split that way could no longer be concatenated with
subsets that were not. Splitting here slices the Arrow table directly, so every output
file carries exactly the input schema.

*Order independence.* Assignment is a hash bucket on `uid`, so it depends only on the
row's identity: it does not matter what order the input files were read in, and adding
rows later never reshuffles the rows that already exist.
"""

import hashlib
import logging
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

LOGGER = logging.getLogger(__name__)

SPLIT_NAMES = ("train", "validation", "test")

DEFAULT_FRACTIONS: dict[str, float] = {"train": 0.2, "validation": 0.3, "test": 0.5}

# Resolution of the hash bucketing. Fractions are honoured to within 1/_BUCKETS.
_BUCKETS = 10_000


class SplitError(Exception):
    """Raised when a split cannot be produced as requested."""


def validate_fractions(fractions: dict[str, float]) -> None:
    """Raise unless `fractions` covers exactly the known splits and sums to 1."""
    if set(fractions) != set(SPLIT_NAMES):
        raise SplitError(
            f"Fractions must cover exactly {SPLIT_NAMES}, got {sorted(fractions)}"
        )
    total = sum(fractions.values())
    if abs(total - 1.0) > 1e-9:
        raise SplitError(f"Fractions must sum to 1.0, got {total}")


def assign_split(uid: str, fractions: dict[str, float], salt: str = "") -> str:
    """Return the split `uid` belongs to.

    The bucket is derived from a SHA-256 digest of `uid`, so it is stable across
    machines, Python versions and runs. `salt` allows a deliberate reshuffle without
    changing any identifier.
    """
    digest = hashlib.sha256(f"{salt}{uid}".encode()).digest()
    bucket = int.from_bytes(digest[:8], "big") % _BUCKETS

    upper = 0.0
    for name in SPLIT_NAMES:
        upper += fractions[name]
        if bucket < upper * _BUCKETS:
            return name
    return SPLIT_NAMES[-1]


def split_table(
    table: pa.Table,
    fractions: dict[str, float] | None = None,
    split_map: dict[str, str] | None = None,
    salt: str = "",
    uid_column: str = "uid",
) -> dict[str, pa.Table]:
    """Partition `table` into train/validation/test, preserving its schema.

    Pass `split_map` (uid -> split) to reproduce an existing, frozen assignment; any
    uid missing from it falls back to the hash bucket. Row order within each split
    follows the input.
    """
    fractions = fractions or DEFAULT_FRACTIONS
    validate_fractions(fractions)

    if uid_column not in table.column_names:
        raise SplitError(
            f"Column {uid_column!r} not found; got {table.column_names}. "
            "Normalise the table before splitting."
        )

    uids = table.column(uid_column).to_pylist()
    if len(set(uids)) != len(uids):
        duplicates = [uid for uid, count in Counter(uids).items() if count > 1]
        raise SplitError(
            f"{len(duplicates)} duplicate {uid_column} value(s), e.g. {duplicates[:3]}"
        )

    indices: dict[str, list[int]] = {name: [] for name in SPLIT_NAMES}
    for position, uid in enumerate(uids):
        split = (split_map or {}).get(uid) or assign_split(uid, fractions, salt)
        if split not in indices:
            raise SplitError(f"Unknown split {split!r} for uid {uid!r}")
        indices[split].append(position)

    # Indices are typed explicitly: an empty Python list gives pyarrow a null-typed
    # array, and `take` has no kernel for it, so an empty split would fail.
    return {
        name: table.take(pa.array(positions, type=pa.int64()))
        for name, positions in indices.items()
    }


def write_splits(splits: dict[str, pa.Table], output_dir: Path) -> dict[str, Path]:
    """Write each split to `<output_dir>/<split>-00000-of-00001.parquet`."""
    output_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}
    for name in SPLIT_NAMES:
        table = splits[name]
        path = output_dir / f"{name}-00000-of-00001.parquet"
        pq.write_table(table, path, compression="snappy")
        written[name] = path
        LOGGER.info(
            "Wrote %s (%d rows, %.1f MB)",
            path.name,
            table.num_rows,
            path.stat().st_size / 1024 / 1024,
        )
    return written


def read_split_map(
    path: Path, uid_field: str = "uid", split_field: str = "split"
) -> dict[str, str]:
    """Read a frozen uid -> split assignment from a JSONL file."""
    import json

    split_map: dict[str, str] = {}
    with path.open(encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            record = json.loads(line)
            split_map[str(record[uid_field])] = str(record[split_field])
    return split_map
