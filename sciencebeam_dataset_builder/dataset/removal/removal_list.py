"""Reading the manual-review verdicts under `data/removals/`.

A removal list is a `uid,reason` CSV: one row per document a reviewer judged unfit for
a conversion benchmark, with the reason they gave. Reviews arrive in parts, so several
lists are read together and are expected to cover disjoint sets of documents.

The reader is deliberately strict. A removal list drives deletion from a published
dataset, so a malformed row, a blank identifier or a repeated `uid` is an error to be
seen rather than something to be quietly worked around.
"""

import csv
from dataclasses import dataclass
from pathlib import Path

# The header every removal list must carry, so a list cannot be misread as headerless
# and lose its first document, and so the column order is never guessed.
REMOVAL_LIST_HEADER = ("uid", "reason")


class RemovalListError(Exception):
    """Raised when a removal list cannot be read as written."""


@dataclass(frozen=True)
class Removal:
    """One reviewer verdict: a document to delete, and why."""

    uid: str
    reason: str
    source_list: Path
    """The list the verdict was read from, so duplicates can name both files."""


def read_removal_list(path: Path) -> list[Removal]:
    """Return the verdicts in one `uid,reason` CSV.

    Raises :class:`RemovalListError` if the header is absent or wrong, a row is
    malformed, a `uid` is blank, or the same `uid` appears twice - a repeated `uid` is
    far more likely a copy-and-paste slip than a deliberate double verdict.
    """
    removals: list[Removal] = []
    seen: dict[str, int] = {}

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            raise RemovalListError(f"{path}: file is empty") from None

        if tuple(field.strip() for field in header) != REMOVAL_LIST_HEADER:
            raise RemovalListError(
                f"{path}: expected header {','.join(REMOVAL_LIST_HEADER)}, got "
                f"{','.join(header)!r}"
            )

        for line_number, row in enumerate(reader, start=2):
            if not row or not any(field.strip() for field in row):
                continue
            if len(row) != len(REMOVAL_LIST_HEADER):
                raise RemovalListError(
                    f"{path}:{line_number}: expected {len(REMOVAL_LIST_HEADER)} fields, "
                    f"got {len(row)}: {row!r}"
                )

            uid, reason = (field.strip() for field in row)
            if not uid:
                raise RemovalListError(f"{path}:{line_number}: blank uid")
            if uid in seen:
                raise RemovalListError(
                    f"{path}:{line_number}: duplicate uid {uid!r}, "
                    f"already listed on line {seen[uid]}"
                )

            seen[uid] = line_number
            removals.append(Removal(uid=uid, reason=reason, source_list=path))

    return removals


def read_removal_lists(paths: list[Path]) -> dict[str, Removal]:
    """Return the verdicts from several lists, keyed by `uid`.

    A `uid` listed in two different files is an error: parts of a review are meant to
    cover disjoint sets of documents, so an overlap means one of them is wrong.
    """
    merged: dict[str, Removal] = {}
    for path in paths:
        for removal in read_removal_list(path):
            existing = merged.get(removal.uid)
            if existing is not None:
                raise RemovalListError(
                    f"{path}: uid {removal.uid!r} is also listed in "
                    f"{existing.source_list}"
                )
            merged[removal.uid] = removal
    return merged
