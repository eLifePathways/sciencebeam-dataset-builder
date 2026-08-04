"""The split manifest: which document is in which split, published with the data.

It is provenance and re-derivation rather than the retrieval path — retrieval is by
split file. It is also what the next version's membership is checked against, since a
published benchmark is not re-derivable from its config alone.
"""

import csv
import dataclasses
from collections.abc import Iterable, Sequence
from pathlib import Path

MANIFEST_FIELDS = ["id", "stratum", "rank", "split"]


class ManifestError(ValueError):
    """A manifest that cannot be used as written."""


@dataclasses.dataclass(frozen=True)
class ManifestRow:
    """One document's membership: its stratum and rank in the archive, and its split."""

    id: str
    stratum: str
    rank: int
    split: str


def read_manifest(path: Path) -> list[ManifestRow]:
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        missing = sorted(set(MANIFEST_FIELDS) - set(reader.fieldnames or []))
        if missing:
            raise ManifestError(f"{path} is missing column(s): {', '.join(missing)}")
        rows = [_row_from_dict(row, path) for row in reader]
    _check_ids_unique(rows, path)
    return rows


def _row_from_dict(row: dict[str, str | None], path: Path) -> ManifestRow:
    rank = (row.get("rank") or "").strip()
    try:
        parsed_rank = int(rank)
    except ValueError as exc:
        raise ManifestError(
            f"{path}: rank {rank!r} for id {row.get('id')!r} is not an integer"
        ) from exc
    return ManifestRow(
        id=(row.get("id") or "").strip(),
        stratum=(row.get("stratum") or "").strip(),
        rank=parsed_rank,
        split=(row.get("split") or "").strip(),
    )


def _check_ids_unique(rows: Sequence[ManifestRow], path: Path) -> None:
    """Splits must be disjoint by id, so a repeated id is a corrupt manifest."""
    seen: dict[str, str] = {}
    for row in rows:
        if row.id in seen:
            raise ManifestError(
                f"{path}: id {row.id!r} appears in both {seen[row.id]!r} and "
                f"{row.split!r}"
            )
        seen[row.id] = row.split


def write_manifest(path: Path, rows: Iterable[ManifestRow]) -> None:
    """Write the manifest in (stratum, rank) order, so versions diff readably."""
    ordered = sorted(rows, key=lambda row: (row.stratum, row.rank))
    _check_ids_unique(ordered, path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_FIELDS)
        writer.writeheader()
        for row in ordered:
            writer.writerow(dataclasses.asdict(row))


def ids_by_stratum_split(
    rows: Iterable[ManifestRow],
) -> dict[tuple[str, str], set[str]]:
    """Group ids by (stratum, split) — the granularity nesting is checked at."""
    grouped: dict[tuple[str, str], set[str]] = {}
    for row in rows:
        grouped.setdefault((row.stratum, row.split), set()).add(row.id)
    return grouped
