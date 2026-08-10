"""The split manifest: which document is in which split, published with the data.

It is provenance and re-derivation rather than the retrieval path — retrieval is by
split file. It is also what the next version's membership is checked against, since a
published corpus is not re-derivable from its config alone.
"""

import csv
import dataclasses
from collections.abc import Iterable, Sequence
from pathlib import Path

# Only `split` is inherent to the manifest. The other three name the same things the
# data files do, so the manifest joins to the data without a reader having to know that
# `stratum` and `journal` are the same field.
SPLIT_FIELD = "split"


def manifest_fields(id_column: str, stratum_column: str, rank_column: str) -> list[str]:
    return [id_column, stratum_column, rank_column, SPLIT_FIELD]


class ManifestError(ValueError):
    """A manifest that cannot be used as written."""


@dataclasses.dataclass(frozen=True)
class ManifestRow:
    """One document's membership: its stratum and rank in the archive, and its split."""

    id: str
    stratum: str
    rank: int
    split: str


def read_manifest(
    path: Path,
    id_column: str = "id",
    stratum_column: str = "stratum",
    rank_column: str = "rank",
) -> list[ManifestRow]:
    """Read a manifest whose columns are named as the corpus names them."""
    fields = manifest_fields(id_column, stratum_column, rank_column)
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        present = list(reader.fieldnames or [])
        missing = [field for field in fields if field not in present]
        if missing:
            raise ManifestError(
                f"{path} is missing column(s) {', '.join(missing)}; it has "
                f"{', '.join(present)}. A manifest names its columns as the corpus "
                f"does, so a manifest written for a different configuration will not "
                f"read."
            )
        rows = [
            _row_from_dict(row, path, id_column, stratum_column, rank_column)
            for row in reader
        ]
    _check_ids_unique(rows, path)
    return rows


def _row_from_dict(
    row: dict[str, str | None],
    path: Path,
    id_column: str,
    stratum_column: str,
    rank_column: str,
) -> ManifestRow:
    rank = (row.get(rank_column) or "").strip()
    try:
        parsed_rank = int(rank)
    except ValueError as exc:
        raise ManifestError(
            f"{path}: {rank_column} {rank!r} for {id_column} "
            f"{row.get(id_column)!r} is not an integer"
        ) from exc
    return ManifestRow(
        id=(row.get(id_column) or "").strip(),
        stratum=(row.get(stratum_column) or "").strip(),
        rank=parsed_rank,
        split=(row.get(SPLIT_FIELD) or "").strip(),
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


def write_manifest(
    path: Path,
    rows: Iterable[ManifestRow],
    id_column: str = "id",
    stratum_column: str = "stratum",
    rank_column: str = "rank",
) -> None:
    """Write the manifest in (stratum, rank) order, so versions diff readably."""
    ordered = sorted(rows, key=lambda row: (row.stratum, row.rank))
    _check_ids_unique(ordered, path)
    fields = manifest_fields(id_column, stratum_column, rank_column)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in ordered:
            writer.writerow(
                {
                    id_column: row.id,
                    stratum_column: row.stratum,
                    rank_column: row.rank,
                    SPLIT_FIELD: row.split,
                }
            )


def ids_by_stratum_split(
    rows: Iterable[ManifestRow],
) -> dict[tuple[str, str], set[str]]:
    """Group ids by (stratum, split) — the granularity nesting is checked at."""
    grouped: dict[tuple[str, str], set[str]] = {}
    for row in rows:
        grouped.setdefault((row.stratum, row.split), set()).add(row.id)
    return grouped
