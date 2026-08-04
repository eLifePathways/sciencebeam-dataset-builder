"""Cut a corpus version from an archive: the documents, the manifest and the config.

Writes only the documents this version *adds*, alongside the full manifest and the
config that produced it. Merging those with a previous version's published rows is the
publishing step's business, since that is where the previous rows — and any renderings
already made of them — are to hand.

A first version is this tool's first run rather than a separate path: there is exactly
one implementation of the nesting invariant, so no two code paths can disagree about it.
"""

import argparse
import dataclasses
import logging
import sys
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from sciencebeam_dataset_builder.archive_cut.allocate import (
    Allocation,
    AllocationError,
    MetadataRow,
    allocate,
    log_allocation,
)
from sciencebeam_dataset_builder.archive_cut.config import (
    CorpusConfig,
    ConfigError,
    dump_config,
    find_lowered_counts,
    load_config,
)
from sciencebeam_dataset_builder.archive_cut.layout import (
    ADDED_DIRECTORY,
    config_path_in_repo,
    latest_published_version,
    manifest_path_in_repo,
    version_name,
)
from sciencebeam_dataset_builder.archive_cut.manifest import (
    ManifestRow,
    read_manifest,
    write_manifest,
)
from sciencebeam_dataset_builder.archive_cut.upload import (
    HfPublishTarget,
    LocalPublishTarget,
    PublishTarget,
)
from sciencebeam_dataset_builder.archive_cut.source import (
    ArchiveSource,
    HfArchiveSource,
    LocalArchiveSource,
    ShardInfo,
    SourceError,
    describe_read_cost,
    iter_document_batches,
    parse_metadata,
    parse_shard_manifest,
    shards_for,
)

LOGGER = logging.getLogger(__name__)


def added_rows(allocation: Allocation) -> list[ManifestRow]:
    """The rows this version adds — the only ones needing reading or rendering."""
    return [row for row in allocation.rows if row.id in allocation.added_ids]


def as_metadata_rows(rows: Sequence[ManifestRow]) -> list[MetadataRow]:
    return [MetadataRow(id=row.id, stratum=row.stratum, rank=row.rank) for row in rows]


def write_added_documents(
    source: ArchiveSource,
    config: CorpusConfig,
    selected: Mapping[str, Sequence[MetadataRow]],
    split_of_id: Mapping[str, str],
    output_dir: Path,
) -> dict[str, int]:
    """Write this version's new documents, one file per split. Returns rows per split."""
    added_dir = output_dir / ADDED_DIRECTORY
    writers: dict[str, pq.ParquetWriter] = {}
    written: dict[str, int] = {}
    try:
        for table in iter_document_batches(source, selected, config):
            for split, subset in _by_split(table, config.id_column, split_of_id):
                if split not in writers:
                    # Created here rather than up front, so a version that adds nothing
                    # leaves no empty directory suggesting it did.
                    added_dir.mkdir(parents=True, exist_ok=True)
                    # zstd because the archive's own measurements settled on it; the
                    # published files' compression is the publishing step's choice.
                    writers[split] = pq.ParquetWriter(
                        added_dir / f"{split}.parquet",
                        subset.schema,
                        compression="zstd",
                    )
                writers[split].write_table(subset)
                written[split] = written.get(split, 0) + subset.num_rows
    finally:
        for writer in writers.values():
            writer.close()
    return written


def _by_split(
    table: pa.Table, id_column: str, split_of_id: Mapping[str, str]
) -> list[tuple[str, pa.Table]]:
    indices_by_split: dict[str, list[int]] = {}
    for index, value in enumerate(table.column(id_column).to_pylist()):
        indices_by_split.setdefault(split_of_id[str(value)], []).append(index)
    return [
        (split, table.take(indices))
        for split, indices in sorted(indices_by_split.items())
    ]


def build_source(args: argparse.Namespace, config: CorpusConfig) -> ArchiveSource:
    """Local directory or dataset repo, with identical allocation either way."""
    if args.source_dir:
        return LocalArchiveSource(args.source_dir)
    repo_id = args.source_repo or config.source.repo_id
    if not repo_id:
        raise ConfigError(
            "no archive to read: pass --source-dir or --source-repo, or set "
            "source.repo_id in the config"
        )
    return HfArchiveSource(repo_id, revision=args.source_revision)


def with_recorded_revision(config: CorpusConfig, source: ArchiveSource) -> CorpusConfig:
    """Record which archive revision was read, so a later version can check it."""
    revision = source.revision
    if revision is None:
        LOGGER.warning(
            "The archive has no revision to record (a local directory), so this "
            "version's config cannot say which archive it was cut from"
        )
        return config
    return dataclasses.replace(
        config, source=dataclasses.replace(config.source, revision=revision)
    )


def check_previous_config(config: CorpusConfig, previous_path: Path) -> None:
    """The cheap half of the monotonicity check, before anything is read."""
    regressions = find_lowered_counts(load_config(previous_path), config)
    if regressions:
        raise AllocationError(
            "the config asks for fewer documents than the previous version:\n"
            + "\n".join(f"  {regression.describe()}" for regression in regressions)
        )


@dataclasses.dataclass(frozen=True)
class PreviousVersion:
    """Where the version being extended was found."""

    version: int
    manifest_path: Path
    config_path: Path


def fetch_previous_version(
    target: PublishTarget,
    config: CorpusConfig,
    work_dir: Path,
    version: int | None = None,
) -> PreviousVersion | None:
    """Retrieve the published version this one extends, from the corpus's own repo.

    The published corpus is the record of how every version was made, so extending it
    should not require finding the right files by hand. Returns None only when nothing
    has been published for this corpus yet — a first cut.
    """
    published = target.list_files()
    wanted = (
        version
        if version is not None
        else latest_published_version(published, config.name)
    )
    if wanted is None:
        LOGGER.info(
            "Nothing published for corpus %r yet; treating this as a first cut",
            config.name,
        )
        return None
    if wanted >= config.version:
        raise AllocationError(
            f"version {config.version} cannot extend version {wanted}: the config's "
            f"version must be higher than the published one"
        )

    manifest_in_repo = manifest_path_in_repo(config.name, wanted)
    config_in_repo = config_path_in_repo(config.name, wanted)
    manifest_path = target.fetch(manifest_in_repo, work_dir)
    config_path = target.fetch(config_in_repo, work_dir)
    if manifest_path is None or config_path is None:
        missing = manifest_in_repo if manifest_path is None else config_in_repo
        raise AllocationError(
            f"version {wanted} of corpus {config.name!r} is published but {missing} "
            f"could not be retrieved, so this run cannot tell what it must contain"
        )
    LOGGER.info("Extending published version %d of corpus %r", wanted, config.name)
    return PreviousVersion(
        version=wanted, manifest_path=manifest_path, config_path=config_path
    )


def build_previous_target(
    args: argparse.Namespace, config: CorpusConfig
) -> PublishTarget | None:
    """Where published versions can be found, or None if nothing says."""
    if args.previous_dir:
        return LocalPublishTarget(args.previous_dir)
    repo_id = args.previous_repo or config.target_repo_id
    return HfPublishTarget(repo_id) if repo_id else None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cut a corpus version from an archive of ranked documents."
    )
    parser.add_argument("config", type=Path, help="Corpus config YAML to apply.")
    parser.add_argument(
        "output_dir", type=Path, help="Directory to write this version into."
    )
    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument(
        "--source-dir",
        type=Path,
        help="Read the archive from a local directory of shards.",
    )
    source_group.add_argument(
        "--source-repo",
        help="Read the archive from this dataset repo (default: source.repo_id).",
    )
    parser.add_argument(
        "--source-revision", help="Archive revision to read (default: the repo's head)."
    )
    previous_group = parser.add_argument_group(
        "the version being extended",
        "By default the corpus's own repo is consulted, since the published corpus is "
        "the record of how every version was made. Omit everything here for a first cut "
        "against a repo that has nothing published yet.",
    )
    previous_group.add_argument(
        "--previous-manifest",
        type=Path,
        help="Split manifest to extend, as a local file rather than from a repo.",
    )
    previous_group.add_argument(
        "--previous-config",
        type=Path,
        help="Config to check for lowered counts, as a local file rather than a repo.",
    )
    previous_group.add_argument(
        "--previous-repo",
        help="Repo to read the previous version from (default: target.repo_id).",
    )
    previous_group.add_argument(
        "--previous-dir",
        type=Path,
        help="Directory holding a published corpus, instead of a repo.",
    )
    previous_group.add_argument(
        "--previous-version",
        type=int,
        help="Extend this published version rather than the highest one.",
    )
    previous_group.add_argument(
        "--first-version",
        action="store_true",
        help="Assert there is nothing to extend; fail if the repo says otherwise.",
    )
    parser.add_argument(
        "--plan-only",
        action="store_true",
        help="Report the allocation and what would be read, then stop.",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        stream=sys.stderr,
    )
    try:
        _run(args)
    except (AllocationError, ConfigError, SourceError) as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)


def _resolve_previous(
    args: argparse.Namespace, config: CorpusConfig, work_dir: Path
) -> list[ManifestRow]:
    """Find the version being extended, and check the config against it.

    Local paths win when given, so a version can be extended without reaching a repo at
    all. Otherwise the corpus's own repo is consulted, since that is where every version
    is recorded.
    """
    manifest_path = args.previous_manifest
    config_path = args.previous_config
    target = build_previous_target(args, config)

    if args.first_version:
        # Asserted rather than assumed, where there is a repo to check against: a first
        # cut over a repo that already holds a version would quietly produce something
        # that does not contain it.
        if target is not None:
            published = latest_published_version(target.list_files(), config.name)
            if published is not None:
                raise AllocationError(
                    f"--first-version was given but version {published} of corpus "
                    f"{config.name!r} is already published"
                )
    elif manifest_path is None and config_path is None:
        if target is not None:
            found = fetch_previous_version(
                target, config, work_dir, version=args.previous_version
            )
            if found is not None:
                manifest_path, config_path = found.manifest_path, found.config_path
        elif config.version > 1:
            # Version 1 is unambiguously a first cut. A later version with nothing to
            # extend and nowhere to look for one is far more likely to be a mistake
            # than an intention, and guessing "first cut" would silently break nesting.
            raise ConfigError(
                f"version {config.version} has nothing to extend: pass "
                f"--previous-manifest, --previous-repo or --previous-dir, set "
                f"target.repo_id in the config, or state --first-version"
            )

    if config_path is not None:
        check_previous_config(config, config_path)
    return read_manifest(manifest_path) if manifest_path is not None else []


def _run(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    with tempfile.TemporaryDirectory(prefix="archive-cut-previous-") as work:
        previous = _resolve_previous(args, config, Path(work))
    source = build_source(args, config)
    metadata = parse_metadata(source.read_text(config.source.metadata_file), config)
    LOGGER.info("Archive metadata: %d document(s)", len(metadata))

    allocation = allocate(metadata, config, previous)
    log_allocation(allocation, config.splits)
    for shortfall in allocation.shortfalls:
        print(f"Shortfall — {shortfall.describe()}")

    added = added_rows(allocation)
    shard_manifest: list[ShardInfo] = parse_shard_manifest(
        source.read_text(config.source.shard_manifest_file), config
    )
    selected = shards_for(as_metadata_rows(added), shard_manifest)
    print(describe_read_cost(selected, shard_manifest))

    if args.plan_only:
        print(f"Would add {len(added)} document(s) — nothing written")
        return

    written = write_added_documents(
        source=source,
        config=config,
        selected=selected,
        split_of_id={row.id: row.split for row in added},
        output_dir=args.output_dir,
    )
    name = version_name(config)
    write_manifest(args.output_dir / f"{name}.csv", allocation.rows)
    dump_config(with_recorded_revision(config, source), args.output_dir / f"{name}.yml")

    for split in config.splits:
        print(f"{split}: {written.get(split, 0)} new document(s)")
    print(
        f"Wrote {name}.csv ({len(allocation.rows)} document(s)) and {name}.yml to "
        f"{args.output_dir}"
    )


if __name__ == "__main__":
    main()
