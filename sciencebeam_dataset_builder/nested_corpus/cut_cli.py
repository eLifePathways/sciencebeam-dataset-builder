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
from collections.abc import Mapping, Sequence
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from sciencebeam_dataset_builder.nested_corpus.allocate import (
    Allocation,
    AllocationError,
    MetadataRow,
    allocate,
    log_allocation,
)
from sciencebeam_dataset_builder.nested_corpus.config import (
    CorpusConfig,
    ConfigError,
    dump_config,
    find_lowered_counts,
    load_config,
)
from sciencebeam_dataset_builder.nested_corpus.manifest import (
    ManifestRow,
    read_manifest,
    write_manifest,
)
from sciencebeam_dataset_builder.nested_corpus.source import (
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

ADDED_DIRECTORY = "added"


def version_name(config: CorpusConfig) -> str:
    """The corpus's name and version, zero-padded so versions keep sorting past nine."""
    return f"{config.name}-v{config.version:03d}"


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
    parser.add_argument(
        "--previous-manifest",
        type=Path,
        help="Split manifest of the version being extended. Omit for a first cut.",
    )
    parser.add_argument(
        "--previous-config",
        type=Path,
        help="Config of the version being extended, checked for lowered counts.",
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


def _run(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    if args.previous_config:
        check_previous_config(config, args.previous_config)

    previous = read_manifest(args.previous_manifest) if args.previous_manifest else []
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
