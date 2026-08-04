"""Allocating documents to splits, so that each split only ever grows.

The nesting guarantee lives here and nowhere else. Two rules produce it:

- **Published assignments are immutable.** A document already in a split stays in that
  split, whatever its rank, so no version moves a document between splits.
- **Remaining need is met from the lowest unclaimed ranks**, splits served in the order
  the config declares. Growing one split's count therefore appends to it without
  disturbing any other, which assigning contiguous rank blocks would not achieve.

Ranks come from the archive's metadata and are never recomputed: recomputing means
owning a seed, and a seed drift would silently break the guarantee.
"""

import dataclasses
import logging
from collections.abc import Iterable, Sequence

from sciencebeam_dataset_builder.benchmark.config import BenchmarkConfig
from sciencebeam_dataset_builder.benchmark.manifest import (
    ManifestRow,
    ids_by_stratum_split,
)

LOGGER = logging.getLogger(__name__)


class AllocationError(ValueError):
    """Allocation cannot proceed as configured."""


class ArchiveChangedError(AllocationError):
    """The archive is not the one the previous version was cut from."""


class MonotonicityError(AllocationError):
    """The requested allocation would shrink a split that has already been published."""


@dataclasses.dataclass(frozen=True)
class MetadataRow:
    """One archived document, as the archive's metadata describes it."""

    id: str
    stratum: str
    rank: int


@dataclasses.dataclass(frozen=True)
class Shortfall:
    """A stratum-split that could not be filled, because the stratum ran out."""

    stratum: str
    split: str
    requested: int
    allocated: int

    @property
    def missing(self) -> int:
        return self.requested - self.allocated

    def describe(self) -> str:
        return (
            f"stratum {self.stratum!r} split {self.split!r}: {self.allocated} of "
            f"{self.requested} requested ({self.missing} short)"
        )


@dataclasses.dataclass(frozen=True)
class Allocation:
    """The result: every row of the new version, and what could not be filled."""

    rows: tuple[ManifestRow, ...]
    shortfalls: tuple[Shortfall, ...]
    # Rows this version adds, which are the only ones needing fetching and rendering.
    added_ids: frozenset[str]

    def counts(self) -> dict[tuple[str, str], int]:
        return {key: len(ids) for key, ids in ids_by_stratum_split(self.rows).items()}


def allocate(
    metadata: Sequence[MetadataRow],
    config: BenchmarkConfig,
    previous: Sequence[ManifestRow] = (),
) -> Allocation:
    """Assign documents to splits for one benchmark version.

    `previous` is the published manifest of the version being extended, empty for a
    first cut. Raises rather than returning a result that would break nesting.
    """
    by_id = _index_metadata(metadata)
    excluded = set(config.exclude)
    _check_exclusions(excluded, by_id)
    _check_previous_against_archive(previous, by_id, excluded, config)
    _check_configured_strata_exist(config, metadata)

    # Keyed on every stratum the archive has, not only those with something eligible
    # left: a stratum excluded down to nothing must still report its shortfall rather
    # than disappearing from the report.
    eligible_by_stratum: dict[str, list[MetadataRow]] = {
        row.stratum: [] for row in metadata
    }
    for row in metadata:
        if row.id not in excluded:
            eligible_by_stratum[row.stratum].append(row)

    previous_by_stratum: dict[str, list[ManifestRow]] = {}
    for published in previous:
        previous_by_stratum.setdefault(published.stratum, []).append(published)

    rows: list[ManifestRow] = []
    shortfalls: list[Shortfall] = []
    added: set[str] = set()

    for stratum in sorted(eligible_by_stratum):
        stratum_rows, stratum_shortfalls, stratum_added = _allocate_stratum(
            stratum=stratum,
            eligible=eligible_by_stratum[stratum],
            published=previous_by_stratum.get(stratum, []),
            config=config,
        )
        rows.extend(stratum_rows)
        shortfalls.extend(stratum_shortfalls)
        added.update(stratum_added)

    result = Allocation(
        rows=tuple(sorted(rows, key=lambda row: (row.stratum, row.rank))),
        shortfalls=tuple(shortfalls),
        added_ids=frozenset(added),
    )
    # By construction this holds; checked anyway, because it is the requirement rather
    # than an implementation detail, and a future change here must not weaken it.
    _check_superset_of_previous(previous, result.rows)
    return result


def _index_metadata(metadata: Sequence[MetadataRow]) -> dict[str, MetadataRow]:
    by_id: dict[str, MetadataRow] = {}
    ranks_seen: set[tuple[str, int]] = set()
    for row in metadata:
        if row.id in by_id:
            raise AllocationError(f"archive metadata has duplicate id {row.id!r}")
        key = (row.stratum, row.rank)
        if key in ranks_seen:
            raise AllocationError(
                f"archive metadata has duplicate rank {row.rank} in stratum "
                f"{row.stratum!r}, so its ordering is ambiguous"
            )
        ranks_seen.add(key)
        by_id[row.id] = row
    return by_id


def _check_exclusions(excluded: set[str], by_id: dict[str, MetadataRow]) -> None:
    """An exclusion naming an id the archive does not have is a typo, not a no-op."""
    unknown = sorted(excluded - set(by_id))
    if unknown:
        raise AllocationError(
            f"exclude names id(s) absent from the archive metadata: "
            f"{', '.join(unknown)}"
        )


def _check_previous_against_archive(
    previous: Sequence[ManifestRow],
    by_id: dict[str, MetadataRow],
    excluded: set[str],
    config: BenchmarkConfig,
) -> None:
    """Confirm the archive is still the one the published version was cut from.

    The archive is write-once, so all of these should be impossible — which is why they
    are worth checking: cutting a new version from a changed corpus would produce a
    benchmark that claims to extend one it does not.
    """
    for published in previous:
        archived = by_id.get(published.id)
        if archived is None:
            raise ArchiveChangedError(
                f"published id {published.id!r} is absent from the archive metadata"
            )
        if archived.rank != published.rank:
            raise ArchiveChangedError(
                f"published id {published.id!r} has rank {archived.rank} in the "
                f"archive but {published.rank} in the manifest"
            )
        if archived.stratum != published.stratum:
            raise ArchiveChangedError(
                f"published id {published.id!r} is in stratum "
                f"{archived.stratum!r} in the archive but {published.stratum!r} in "
                f"the manifest"
            )
        if published.id in excluded:
            raise AllocationError(
                f"id {published.id!r} is already published in split "
                f"{published.split!r} and cannot now be excluded"
            )
        if published.split not in config.splits:
            raise MonotonicityError(
                f"split {published.split!r} is published but missing from the "
                f"config's splits, which would drop every document in it"
            )


def _check_configured_strata_exist(
    config: BenchmarkConfig, metadata: Sequence[MetadataRow]
) -> None:
    """A per-stratum override for a stratum the archive lacks is a typo."""
    strata = {row.stratum for row in metadata}
    unknown = sorted(set(config.allocation.overrides) - strata)
    if unknown:
        raise AllocationError(
            f"allocation.overrides names stratum/strata absent from the archive: "
            f"{', '.join(unknown)}"
        )


def _allocate_stratum(
    stratum: str,
    eligible: Sequence[MetadataRow],
    published: Sequence[ManifestRow],
    config: BenchmarkConfig,
) -> tuple[list[ManifestRow], list[Shortfall], set[str]]:
    by_rank = sorted(eligible, key=lambda row: row.rank)
    published_by_split: dict[str, list[ManifestRow]] = {}
    for row in published:
        published_by_split.setdefault(row.split, []).append(row)

    claimed = {row.id for row in published}
    counts = config.counts_for(stratum)

    rows: list[ManifestRow] = list(published)
    shortfalls: list[Shortfall] = []
    added: set[str] = set()

    for split in config.splits:
        requested = counts[split]
        kept = published_by_split.get(split, [])
        if len(kept) > requested:
            raise MonotonicityError(
                f"stratum {stratum!r} split {split!r} already has {len(kept)} "
                f"published document(s) but the config asks for {requested}"
            )
        need = requested - len(kept)
        taken = _take_lowest_unclaimed(by_rank, claimed, need)
        for selected in taken:
            claimed.add(selected.id)
            added.add(selected.id)
            rows.append(
                ManifestRow(
                    id=selected.id, stratum=stratum, rank=selected.rank, split=split
                )
            )
        allocated = len(kept) + len(taken)
        if allocated < requested:
            shortfalls.append(
                Shortfall(
                    stratum=stratum,
                    split=split,
                    requested=requested,
                    allocated=allocated,
                )
            )
    return rows, shortfalls, added


def _take_lowest_unclaimed(
    by_rank: Sequence[MetadataRow], claimed: set[str], need: int
) -> list[MetadataRow]:
    if need <= 0:
        return []
    taken: list[MetadataRow] = []
    for row in by_rank:
        if len(taken) == need:
            break
        if row.id not in claimed:
            taken.append(row)
    return taken


def _check_superset_of_previous(
    previous: Sequence[ManifestRow], rows: Sequence[ManifestRow]
) -> None:
    """Every published (stratum, split) must still contain everything it contained.

    This, not a comparison of configured counts, is the nesting guarantee: comparing
    counts misses any stratum-split that was previously capped, where the configured
    figure and the realised set differ.
    """
    current = ids_by_stratum_split(rows)
    for key, published_ids in ids_by_stratum_split(previous).items():
        stratum, split = key
        lost = sorted(published_ids - current.get(key, set()))
        if lost:
            raise MonotonicityError(
                f"stratum {stratum!r} split {split!r} would lose "
                f"{len(lost)} published document(s), first {lost[0]!r}"
            )


def log_allocation(allocation: Allocation, splits: Iterable[str]) -> None:
    """Report the plan: per stratum-split counts, then any shortfall."""
    counts = allocation.counts()
    split_list = list(splits)
    for stratum in sorted({stratum for stratum, _ in counts}):
        summary = " ".join(
            f"{split}={counts.get((stratum, split), 0)}" for split in split_list
        )
        LOGGER.info("Stratum %r: %s", stratum, summary)
    LOGGER.info(
        "Total %d document(s), %d new in this version",
        len(allocation.rows),
        len(allocation.added_ids),
    )
    for shortfall in allocation.shortfalls:
        LOGGER.warning("Shortfall — %s", shortfall.describe())
