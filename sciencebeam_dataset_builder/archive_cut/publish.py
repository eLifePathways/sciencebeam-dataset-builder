"""Assembling a publishable corpus version from a previous one and a new cut.

One file per split, rewritten each version rather than appended to. That is not the
cheapest option — it re-uploads rows whose bytes have not changed — but downstream
readers name a single file per corpus, so a split spread over several files could not be
read without changing them. Nesting is unaffected either way: it is about membership, not
bytes.

Rows a version failed to render are dropped from the published manifest and added to the
config's exclusions, so the published data and its manifest agree, the same document is
not attempted again on every run, and a later version fills the gap.
"""

import dataclasses
import logging
from collections.abc import Mapping, Sequence

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


def merge_split(
    split: str,
    previous: pa.Table | None,
    added: pa.Table | None,
    order: Sequence[ManifestRow],
    id_column: str,
) -> pa.Table:
    """Combine a split's carried-over and new rows, in manifest order.

    Ordered by the manifest rather than by which table a row came from, so republishing
    the same version produces the same bytes.
    """
    tables = [table for table in (previous, added) if table is not None]
    if not tables:
        raise PublishError(f"split {split!r} has no rows to publish")
    if len(tables) == 2:
        _check_schemas_match(split, tables[0], tables[1])
    combined = pa.concat_tables(tables)

    wanted = [row.id for row in order if row.split == split]
    position = {
        str(value): index
        for index, value in enumerate(combined.column(id_column).to_pylist())
    }
    missing = [paper_id for paper_id in wanted if paper_id not in position]
    if missing:
        raise PublishError(
            f"split {split!r} is missing {len(missing)} document(s) the manifest lists, "
            f"first {missing[0]!r}. The previous version's data may not have been "
            f"provided."
        )
    extra = len(position) - len(wanted)
    if extra > 0:
        LOGGER.warning(
            "Split %r holds %d row(s) the manifest does not list; they are dropped",
            split,
            extra,
        )
    return combined.take(pa.array([position[i] for i in wanted], type=pa.int64()))


def _check_schemas_match(split: str, previous: pa.Table, added: pa.Table) -> None:
    previous_columns = set(previous.schema.names)
    added_columns = set(added.schema.names)
    if previous_columns != added_columns:
        only_previous = sorted(previous_columns - added_columns)
        only_added = sorted(added_columns - previous_columns)
        raise PublishError(
            f"split {split!r} cannot be merged: the published rows and the new rows "
            f"have different columns"
            + (f"; only published: {', '.join(only_previous)}" if only_previous else "")
            + (f"; only new: {', '.join(only_added)}" if only_added else "")
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
