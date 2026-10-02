"""What is in a release, and what has ever happened to a document.

`document-changes.csv` is cumulative and append only, one row per change.
`vN.M.P.csv` freezes membership per release. Both live under `releases/`, and a release
writes them together, so they share this module rather than drifting apart in two.

No `source` or `uid`: the repo already is one corpus, so `id` alone identifies a row
within it. `corpus` is the hyphenated, citable name, carried so a row stays self
describing if this file is ever read outside its own repo - a pooled audit, or a
manifest spanning corpora.
"""

import csv
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from enum import Enum
from pathlib import Path

from sciencebeam_dataset_builder.corpus.layout import version_key

LEDGER_FIELDS = (
    "change_id",
    "local_change_id",
    "corpus",
    "id",
    "split",
    "file_type_pair",
    "parquet_file_name",
    "base_release",
    "change_type",
    "change_reason",
    "change_timestamp",
    "changed_by",
)

MEMBERSHIP_FIELDS = ("corpus", "id", "split")


class LedgerError(ValueError):
    """A ledger or membership file that cannot be used as written."""


class ChangeType(Enum):
    """The closed set of things that can happen to a document.

    Closed on purpose: free text drifts within a few releases, and a derived-type check
    counts by this value. No `moved-repo`: a document moving between repos is `added` in
    the one that gains it and `removed` in the one that loses it. No `retired`: retiring
    a corpus is an act on the repo, not on its documents.
    """

    ADDED = "added"
    REMOVED = "removed"
    CONTENT_CORRECTED = "content-corrected"
    METADATA_CORRECTED = "metadata-corrected"


@dataclass(frozen=True)
class PendingChange:
    """A change that has been made but not yet published.

    Carries only what the operation knows. `change_id`, `base_release` and the timestamp
    are stamped by :func:`assign_change_ids` at publish time, so a change cannot be
    recorded against a release that did not happen. `local_change_id` is set only where a
    declarative file drives the change - `corrections/<id>/correction.yml`,
    `removals/removals.yml` - and is null for `added`, which arrives from a harvest with
    nothing to be idempotent against. `parquet_file_name` is likewise left blank for an
    `added` change where the shard it lands in is not yet known - `prepare_release` fills
    it in once sharding has actually happened; a `removed` or `content-corrected` change
    already names an existing shard, so its caller supplies it directly.
    """

    corpus: str
    id: str
    split: str
    file_type_pair: str
    change_type: ChangeType
    change_reason: str
    changed_by: str
    local_change_id: int | None = None
    parquet_file_name: str = ""


@dataclass(frozen=True)
class Change:
    """One published change, as a row of the ledger."""

    change_id: int
    local_change_id: int | None
    corpus: str
    id: str
    split: str
    file_type_pair: str
    parquet_file_name: str
    base_release: str
    change_type: ChangeType
    change_reason: str
    change_timestamp: str
    changed_by: str


def next_change_id(changes: Sequence[Change]) -> int:
    """One past the highest id in use.

    Never the lowest free one: an id is the stable handle for a change, so it is never
    reused even if a row is somehow absent.
    """
    return max((c.change_id for c in changes), default=0) + 1


def assign_change_ids(
    pending: Sequence[PendingChange],
    base_release: str,
    timestamp: datetime,
    existing: Sequence[Change] = (),
) -> list[Change]:
    """Stamp `pending` with the release they were made against, the timestamp and
    sequential change ids.

    `base_release` is the release this batch was made on top of, not the one it
    produces - empty for the very first release, which has nothing to be based on.
    """
    if base_release:
        version_key(base_release)
    stamped = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    start = next_change_id(existing)
    return [
        Change(
            change_id=start + offset,
            local_change_id=change.local_change_id,
            corpus=change.corpus,
            id=change.id,
            split=change.split,
            file_type_pair=change.file_type_pair,
            parquet_file_name=change.parquet_file_name,
            base_release=base_release,
            change_type=change.change_type,
            change_reason=change.change_reason,
            change_timestamp=stamped,
            changed_by=change.changed_by,
        )
        for offset, change in enumerate(pending)
    ]


def read_ledger(path: Path) -> list[Change]:
    """Read the cumulative ledger, oldest change first."""
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        _check_fields(reader.fieldnames, LEDGER_FIELDS, path)
        changes = [_change_from_row(row, path) for row in reader]
    _check_ids_unique(changes, path)
    _check_local_change_ids_unique(changes, path)
    return sorted(changes, key=lambda c: c.change_id)


def write_ledger(path: Path, changes: Iterable[Change]) -> None:
    """Write the whole ledger, ordered by id."""
    ordered = sorted(changes, key=lambda c: c.change_id)
    _check_ids_unique(ordered, path)
    _check_local_change_ids_unique(ordered, path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(LEDGER_FIELDS))
        writer.writeheader()
        for change in ordered:
            row = {field: getattr(change, field) for field in LEDGER_FIELDS}
            row["change_type"] = change.change_type.value
            row["local_change_id"] = (
                "" if change.local_change_id is None else change.local_change_id
            )
            writer.writerow(row)


def append_to_ledger(
    path: Path,
    pending: Sequence[PendingChange],
    base_release: str,
    timestamp: datetime,
    changed_by: str | None = None,
) -> list[Change]:
    """Stamp `pending` against the ledger at `path` and write it back.

    Returns the rows added. `changed_by` overrides each pending change's own value, for
    an operation run on one person's behalf.
    """
    existing = read_ledger(path) if path.exists() else []
    if changed_by is not None:
        pending = [replace(change, changed_by=changed_by) for change in pending]
    added = assign_change_ids(pending, base_release, timestamp, existing)
    write_ledger(path, [*existing, *added])
    return added


@dataclass(frozen=True)
class Membership:
    """One document's place in a release."""

    corpus: str
    id: str
    split: str


def read_membership(path: Path) -> list[Membership]:
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        _check_fields(reader.fieldnames, MEMBERSHIP_FIELDS, path)
        rows = [
            Membership(corpus=r["corpus"], id=r["id"], split=r["split"]) for r in reader
        ]
    ids = [row.id for row in rows]
    if len(set(ids)) != len(ids):
        raise LedgerError(f"{path}: an id appears more than once")
    return rows


def write_membership(path: Path, rows: Iterable[Membership]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(MEMBERSHIP_FIELDS))
        writer.writeheader()
        for row in rows:
            writer.writerow({field: getattr(row, field) for field in MEMBERSHIP_FIELDS})


def _change_from_row(row: dict[str, str | None], path: Path) -> Change:
    raw_id = (row.get("change_id") or "").strip()
    try:
        change_id = int(raw_id)
    except ValueError as exc:
        raise LedgerError(f"{path}: change_id {raw_id!r} is not an integer") from exc

    raw_local_id = (row.get("local_change_id") or "").strip()
    local_change_id = None
    if raw_local_id:
        try:
            local_change_id = int(raw_local_id)
        except ValueError as exc:
            raise LedgerError(
                f"{path}: local_change_id {raw_local_id!r} for change {change_id} is "
                f"not an integer"
            ) from exc

    raw_type = (row.get("change_type") or "").strip()
    try:
        change_type = ChangeType(raw_type)
    except ValueError as exc:
        known = ", ".join(t.value for t in ChangeType)
        raise LedgerError(
            f"{path}: change_type {raw_type!r} for change {change_id} is not one of {known}"
        ) from exc

    return Change(
        change_id=change_id,
        local_change_id=local_change_id,
        corpus=(row.get("corpus") or "").strip(),
        id=(row.get("id") or "").strip(),
        split=(row.get("split") or "").strip(),
        file_type_pair=(row.get("file_type_pair") or "").strip(),
        parquet_file_name=(row.get("parquet_file_name") or "").strip(),
        base_release=(row.get("base_release") or "").strip(),
        change_type=change_type,
        change_reason=(row.get("change_reason") or "").strip(),
        change_timestamp=(row.get("change_timestamp") or "").strip(),
        changed_by=(row.get("changed_by") or "").strip(),
    )


def _check_fields(
    present: Sequence[str] | None, required: Sequence[str], path: Path
) -> None:
    missing = [field for field in required if field not in (present or [])]
    if missing:
        raise LedgerError(
            f"{path} is missing column(s) {', '.join(missing)}; it has "
            f"{', '.join(present or [])}"
        )


def _check_ids_unique(changes: Sequence[Change], path: Path) -> None:
    """A document's `id` repeats across rows, so `change_id` is the only stable handle
    for one change. It must not repeat."""
    ids = [c.change_id for c in changes]
    duplicates = sorted({i for i in ids if ids.count(i) > 1})
    if duplicates:
        raise LedgerError(f"{path}: change_id(s) {duplicates} appear more than once")


def _check_local_change_ids_unique(changes: Sequence[Change], path: Path) -> None:
    """`(id, local_change_id)` is the pair a declarative file is checked against for
    idempotency, among the rows that carry one at all - `added` rows never do."""
    keys = [(c.id, c.local_change_id) for c in changes if c.local_change_id is not None]
    duplicates = sorted({k for k in keys if keys.count(k) > 1})
    if duplicates:
        raise LedgerError(
            f"{path}: (id, local_change_id) pair(s) {duplicates} appear more than once"
        )
