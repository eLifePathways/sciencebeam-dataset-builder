"""Assembling a publishable corpus version from a previous one and a new cut.

A version adds files and never rewrites an earlier version's. Splits are Hive-partitioned
by stratum and chunked by payload bytes, so growing the corpus uploads only what it adds,
an interrupted upload costs one chunk, and rows published earlier keep the bytes -- and the
rendered PDFs -- they were published with. Nesting is unaffected by any of this: it is
about membership, which the manifest records.

Rows a version failed to render are dropped from the published manifest and added to the
config's exclusions, so the published data and its manifest agree, the same document is
not attempted again on every run, and a later version fills the gap.
"""

import dataclasses
import logging
from collections.abc import Mapping, Sequence, Set

import pyarrow as pa

from sciencebeam_dataset_builder.archive_cut.config import CorpusConfig
from sciencebeam_dataset_builder.archive_cut.manifest import ManifestRow
from sciencebeam_dataset_builder.archive_cut.render import RenderFailure

LOGGER = logging.getLogger(__name__)


class PublishError(ValueError):
    """The version cannot be published as it stands."""


@dataclasses.dataclass(frozen=True)
class PreparedSplit:
    """One split's publishable rows."""

    split: str
    table: pa.Table


def manifest_without_failures(
    rows: Sequence[ManifestRow], failures: Sequence[RenderFailure]
) -> tuple[list[ManifestRow], list[ManifestRow]]:
    """Split the manifest into what is publishable and what failed to render."""
    failed = {failure.id for failure in failures}
    kept = [row for row in rows if row.id not in failed]
    dropped = [row for row in rows if row.id in failed]
    return kept, dropped


def check_failures_resolved(
    failures: Sequence[RenderFailure], accept_unresolved: bool = False
) -> None:
    """Refuse to publish while a retryable failure is unresolved.

    Excluding a document that merely timed out would quietly shrink the corpus on the
    strength of an environmental hiccup. Rendering it again is nearly always the right
    answer, so this insists the choice is made rather than made silently.
    """
    unresolved = [failure for failure in failures if failure.retryable]
    if not unresolved or accept_unresolved:
        return
    listed = "\n".join(f"  {f.id}: {f.reason}" for f in unresolved)
    raise PublishError(
        f"{len(unresolved)} document(s) failed in a way that may succeed on another "
        f"attempt:\n{listed}\n"
        f"Render them again (`--retry-failed`, perhaps with a longer --timeout), or "
        f"pass --exclude-unresolved to publish without them."
    )


def config_with_exclusions(
    config: CorpusConfig, failures: Sequence[RenderFailure]
) -> CorpusConfig:
    """Add this version's render failures to the config's exclusions.

    Recorded by id in the config that travels with the data, so allocation does not
    depend on which run happened to hit the failure.
    """
    if not failures:
        return config
    excluded = list(config.exclude)
    for failure in failures:
        if failure.id not in excluded:
            excluded.append(failure.id)
    return dataclasses.replace(config, exclude=tuple(excluded))


def order_new_rows(
    split: str, table: pa.Table, order: Sequence[ManifestRow], id_column: str
) -> pa.Table:
    """Put this version's new rows into manifest order.

    Ordered by the manifest rather than by which shard they were read from, so
    republishing a version reproduces its bytes. Nothing is merged with the previously
    published rows: those live in their own files and are never rewritten.
    """
    position = {
        str(value): index
        for index, value in enumerate(table.column(id_column).to_pylist())
    }
    wanted = [row.id for row in order if row.split == split and row.id in position]
    unlisted = set(position) - {row.id for row in order if row.split == split}
    if unlisted:
        raise PublishError(
            f"split {split!r} holds {len(unlisted)} rendered document(s) the manifest "
            f"does not list for it, first {sorted(unlisted)[0]!r}"
        )
    return table.take(pa.array([position[i] for i in wanted], type=pa.int64()))


def check_manifest_is_covered(
    rows: Sequence[ManifestRow], published_ids: Set[str], new_ids: Set[str]
) -> None:
    """The manifest must be exactly what is published plus what is about to be.

    Append-only publishing makes a new failure possible that rewriting could not: a
    document already in an earlier version's file could be written again, leaving the split
    holding it twice. And a document the manifest lists could be in neither place, leaving
    a manifest that describes rows nobody can read.
    """
    duplicated = sorted(published_ids & new_ids)
    if duplicated:
        raise PublishError(
            f"{len(duplicated)} document(s) are already published and would be written "
            f"again, first {duplicated[0]!r}. A version must only add documents."
        )
    listed = {row.id for row in rows}
    missing = sorted(listed - published_ids - new_ids)
    if missing:
        raise PublishError(
            f"the manifest lists {len(missing)} document(s) that are neither published "
            f"nor being written, first {missing[0]!r}"
        )
    unlisted = sorted((published_ids | new_ids) - listed)
    if unlisted:
        raise PublishError(
            f"{len(unlisted)} document(s) would be published without appearing in the "
            f"manifest, first {unlisted[0]!r}"
        )


def check_output_schema(split: str, table: pa.Table, config: CorpusConfig) -> None:
    """Every configured column must be present; extra columns are fine.

    The configured list is where a corpus declares what its readers need, so checking
    against it keeps downstream column names out of this code.
    """
    missing = [column for column in config.columns if column not in table.schema.names]
    if missing:
        raise PublishError(
            f"split {split!r} is missing configured column(s): {', '.join(missing)}"
        )


def describe_publication(
    prepared: Sequence[PreparedSplit],
    dropped: Sequence[ManifestRow],
    failures: Sequence[RenderFailure],
) -> list[str]:
    lines = [f"{item.split}: {item.table.num_rows} document(s)" for item in prepared]
    if dropped:
        reasons: Mapping[str, str] = {f.id: f.reason for f in failures}
        lines.append(f"{len(dropped)} document(s) left out, having failed to render:")
        lines.extend(
            f"  {row.id} ({row.stratum}/{row.split}): {reasons.get(row.id, 'unknown')}"
            for row in dropped
        )
        lines.append(
            "Added to the config's exclusions; a later version can fill the gap"
        )
    return lines
