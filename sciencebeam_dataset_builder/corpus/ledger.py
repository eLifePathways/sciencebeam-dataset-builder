"""What is in a release, and what has ever happened to a document.

`document-changes.csv` is cumulative and append only, one row per change.
`vN.M.P.csv` freezes membership per release. Both live under `releases/`, and a release
writes them together, so they share this module rather than drifting apart in two.
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
    "uid",
    "source",
    "split",
    "release",
    "change_type",
    "reason",
    "change_timestamp",
    "changed_by",
)

MEMBERSHIP_FIELDS = ("uid", "source", "split")


class LedgerError(ValueError):
    """A ledger or membership file that cannot be used as written."""


class ChangeType(Enum):
    """The closed set of things that can happen to a document.

    Closed on purpose: free text drifts within a few releases, and the changelog counts
    by this value.
    """

    ADDED = "added"
    REMOVED = "removed"
    METADATA_CORRECTED = "metadata-corrected"
    PAYLOAD_CHANGED = "payload-changed"
    MOVED_REPO = "moved-repo"
    RETIRED = "retired"


@dataclass(frozen=True)
class PendingChange:
    """A change that has been made but not yet published.

    Carries only what the operation knows. The release, the timestamp and the id are
    stamped by :func:`assign_change_ids` at publish time, so a change cannot be recorded
    against a release that did not happen.
    """

    uid: str
    source: str
    split: str
    change_type: ChangeType
    reason: str
    changed_by: str


@dataclass(frozen=True)
class Change:
    """One published change, as a row of the ledger."""

    change_id: int
    uid: str
    source: str
    split: str
    release: str
    change_type: ChangeType
    reason: str
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
    release: str,
    timestamp: datetime,
    existing: Sequence[Change] = (),
) -> list[Change]:
    """Stamp `pending` with the release, the timestamp and sequential ids."""
    version_key(release)
    stamped = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    start = next_change_id(existing)
    return [
        Change(
            change_id=start + offset,
            uid=change.uid,
            source=change.source,
            split=change.split,
            release=release,
            change_type=change.change_type,
            reason=change.reason,
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
    return sorted(changes, key=lambda c: c.change_id)


def write_ledger(path: Path, changes: Iterable[Change]) -> None:
    """Write the whole ledger, ordered by id."""
    ordered = sorted(changes, key=lambda c: c.change_id)
    _check_ids_unique(ordered, path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(LEDGER_FIELDS))
        writer.writeheader()
        for change in ordered:
            row = {field: getattr(change, field) for field in LEDGER_FIELDS}
            row["change_type"] = change.change_type.value
            writer.writerow(row)


def append_to_ledger(
    path: Path,
    pending: Sequence[PendingChange],
    release: str,
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
    added = assign_change_ids(pending, release, timestamp, existing)
    write_ledger(path, [*existing, *added])
    return added


@dataclass(frozen=True)
class Membership:
    """One document's place in a release."""

    uid: str
    source: str
    split: str


def read_membership(path: Path) -> list[Membership]:
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        _check_fields(reader.fieldnames, MEMBERSHIP_FIELDS, path)
        rows = [
            Membership(uid=r["uid"], source=r["source"], split=r["split"])
            for r in reader
        ]
    uids = [row.uid for row in rows]
    if len(set(uids)) != len(uids):
        raise LedgerError(f"{path}: a uid appears more than once")
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
        uid=(row.get("uid") or "").strip(),
        source=(row.get("source") or "").strip(),
        split=(row.get("split") or "").strip(),
        release=(row.get("release") or "").strip(),
        change_type=change_type,
        reason=(row.get("reason") or "").strip(),
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
    """A uid repeats across rows, so the id is the only stable handle. It must not."""
    ids = [c.change_id for c in changes]
    duplicates = sorted({i for i in ids if ids.count(i) > 1})
    if duplicates:
        raise LedgerError(f"{path}: change_id(s) {duplicates} appear more than once")
