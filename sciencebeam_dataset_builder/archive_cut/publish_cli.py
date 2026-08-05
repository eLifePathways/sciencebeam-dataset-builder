"""Publish a rendered corpus version: its new data files, the manifest and the config.

The last of three steps. The cut selects and reads, rendering adds the PDFs, and this
writes what they produced into the corpus repo — data first, then the manifest and config,
then a tag so a reader can pin the version immutably rather than trusting a branch.

A version only ever *adds* files. Nothing an earlier version published is rewritten, so
growing the corpus uploads only what it grew by, and rows keep the bytes — and the rendered
PDFs — they were published with. What constitutes a version is the manifest, not the layout.
"""

import argparse
import logging
import sys
from collections.abc import Sequence
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from sciencebeam_dataset_builder.archive_cut.config import (
    ConfigError,
    CorpusConfig,
    dump_config,
    load_config,
)
from sciencebeam_dataset_builder.archive_cut.layout import (
    FAILURES_FILENAME,
    PUBLISHED_DIRECTORY,
    RENDERED_DIRECTORY,
    SPLITS_DIRECTORY,
    config_path_in_repo,
    files_by_split,
    manifest_path_in_repo,
    published_split_paths,
    split_partition_path_in_repo,
    version_name,
)
from sciencebeam_dataset_builder.archive_cut.manifest import (
    ManifestRow,
    read_manifest,
    write_manifest,
)
from sciencebeam_dataset_builder.archive_cut.parquet_io import (
    DEFAULT_CHUNK_BYTES,
    chunk_table,
    write_table_with_byte_sized_row_groups,
)
from sciencebeam_dataset_builder.archive_cut.publish import (
    PreparedSplit,
    PublishError,
    check_failures_resolved,
    check_manifest_is_covered,
    check_output_schema,
    config_with_exclusions,
    describe_publication,
    manifest_without_failures,
    order_new_rows,
)
from sciencebeam_dataset_builder.archive_cut.render import RenderFailure
from sciencebeam_dataset_builder.archive_cut.render_cli import read_failures
from sciencebeam_dataset_builder.archive_cut.upload import (
    FileToPublish,
    HfPublishTarget,
    LocalPublishTarget,
    PublishTarget,
    UploadError,
    batched,
)

LOGGER = logging.getLogger(__name__)

# One commit per data file. Batching exists because one commit per file hit HTTP 429
# during the archive build — but that was 151 shards, whereas a chunk here is tens of MiB
# on a link where that is minutes. A failure should cost one chunk, not the run.
DEFAULT_DATA_FILES_PER_COMMIT = 1


def find_version_files(version_dir: Path) -> tuple[Path, Path]:
    """Locate the manifest and config a cut wrote, whatever the corpus is called."""
    configs = sorted(version_dir.glob("*-v[0-9][0-9][0-9].yml"))
    if len(configs) != 1:
        raise PublishError(
            f"expected exactly one <name>-vNNN.yml in {version_dir}, found "
            f"{len(configs)}"
        )
    manifest = configs[0].with_suffix(".csv")
    if not manifest.exists():
        raise PublishError(f"{manifest} not found beside {configs[0].name}")
    return configs[0], manifest


def prepare_new_rows(
    config: CorpusConfig, rows: list[ManifestRow], version_dir: Path
) -> list[PreparedSplit]:
    """This version's new rows per split, in manifest order.

    Only what rendering produced: everything else is already published, in files this run
    will not touch.
    """
    rendered = files_by_split(version_dir, RENDERED_DIRECTORY)
    prepared: list[PreparedSplit] = []
    for split in config.splits:
        table = _read_shard_files(rendered.get(split, []))
        if table is None:
            LOGGER.info("Split %r has no new documents", split)
            continue
        ordered = order_new_rows(split, table, rows, config.id_column)
        check_output_schema(split, ordered, config)
        prepared.append(PreparedSplit(split=split, table=ordered))
    return prepared


def _read_shard_files(paths: Sequence[Path]) -> pa.Table | None:
    """Concatenate the per-shard files a cut and render produced for one split."""
    tables = [pq.read_table(path) for path in paths]
    non_empty = [table for table in tables if table.num_rows]
    return pa.concat_tables(non_empty) if non_empty else None


def published_ids(target: PublishTarget, config: CorpusConfig) -> set[str]:
    """The ids already published, read a column at a time rather than downloaded.

    Only the id column of each published file is fetched — kilobytes — which is what makes
    checking the manifest against reality affordable on every publish.
    """
    files = target.list_files()
    found: set[str] = set()
    for split in config.splits:
        for path in published_split_paths(files, split):
            table = target.open_parquet(path).read(columns=[config.id_column])
            found.update(
                str(value) for value in table.column(config.id_column).to_pylist()
            )
    return found


def write_published(
    prepared: list[PreparedSplit],
    config: CorpusConfig,
    rows: list[ManifestRow],
    output_dir: Path,
    chunk_bytes: int = DEFAULT_CHUNK_BYTES,
) -> list[FileToPublish]:
    """Write this version's new files locally, so they can be inspected before upload."""
    output_dir.mkdir(parents=True, exist_ok=True)
    files: list[FileToPublish] = []
    for item in prepared:
        for stratum, subset in _by_stratum(item.table, config.stratum_column):
            # Hive convention: the stratum lives in the path, so it is not stored in the
            # file. Readers of the split reconstruct it — pyarrow and datasets both do.
            without_stratum = subset.drop_columns([config.stratum_column])
            for index, chunk in enumerate(chunk_table(without_stratum, chunk_bytes)):
                path_in_repo = split_partition_path_in_repo(
                    item.split, config.stratum_column, stratum, config.version, index
                )
                path = output_dir / path_in_repo
                groups = write_table_with_byte_sized_row_groups(chunk, path)
                LOGGER.info(
                    "%s: %d document(s), %d row group(s), %.1f MiB",
                    path_in_repo,
                    chunk.num_rows,
                    groups,
                    path.stat().st_size / 1024**2,
                )
                files.append(FileToPublish(local_path=path, path_in_repo=path_in_repo))

    splits_dir = output_dir / SPLITS_DIRECTORY
    write_manifest(splits_dir / f"{version_name(config)}.csv", rows)
    dump_config(config, splits_dir / f"{version_name(config)}.yml")
    return files


def _by_stratum(table: pa.Table, stratum_column: str) -> list[tuple[str, pa.Table]]:
    """Split a table by stratum, keeping each stratum's row order."""
    indices: dict[str, list[int]] = {}
    for index, value in enumerate(table.column(stratum_column).to_pylist()):
        indices.setdefault(str(value), []).append(index)
    return [
        (stratum, table.take(pa.array(rows, type=pa.int64())))
        for stratum, rows in sorted(indices.items())
    ]


def publish(
    target: PublishTarget,
    config: CorpusConfig,
    data_files: list[FileToPublish],
    output_dir: Path,
    batch_size: int = DEFAULT_DATA_FILES_PER_COMMIT,
    with_tag: bool = True,
) -> None:
    """Data first, then the manifest and config, then the tag."""
    name = version_name(config)
    for index, batch in enumerate(batched(data_files, batch_size), start=1):
        target.publish(batch, f"Add {name} data ({index}): {len(batch)} file(s)")

    # Only now: until every data file is present, a manifest would describe rows that
    # cannot be read.
    target.publish(
        [
            FileToPublish(
                local_path=output_dir / SPLITS_DIRECTORY / f"{name}.csv",
                path_in_repo=manifest_path_in_repo(config.name, config.version),
            ),
            FileToPublish(
                local_path=output_dir / SPLITS_DIRECTORY / f"{name}.yml",
                path_in_repo=config_path_in_repo(config.name, config.version),
            ),
        ],
        f"Add {name} manifest and config",
    )
    if with_tag:
        target.tag(name, f"Corpus {config.name} version {config.version}")


def build_target(args: argparse.Namespace, config: CorpusConfig) -> PublishTarget:
    if args.dry_run or args.target_dir:
        directory = args.target_dir or (args.version_dir / "dry-run")
        LOGGER.info("Publishing to %s rather than a repo", directory)
        return LocalPublishTarget(directory)
    repo_id = args.target_repo or config.target_repo_id
    if not repo_id:
        raise ConfigError(
            "nowhere to publish: pass --target-repo, --target-dir or --dry-run, or set "
            "target.repo_id in the config"
        )
    return HfPublishTarget(repo_id, create=args.create_repo)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish a rendered corpus version to a dataset repo."
    )
    parser.add_argument(
        "version_dir",
        type=Path,
        help="Directory holding the cut and its rendered documents.",
    )
    destination = parser.add_mutually_exclusive_group()
    destination.add_argument(
        "--target-repo",
        help="Dataset repo to publish to (default: target.repo_id from the config).",
    )
    destination.add_argument(
        "--target-dir",
        type=Path,
        help="Publish into a local directory instead of a repo.",
    )
    destination.add_argument(
        "--dry-run",
        action="store_true",
        help="Assemble everything into <version_dir>/dry-run without uploading.",
    )
    parser.add_argument(
        "--chunk-bytes",
        type=int,
        default=DEFAULT_CHUNK_BYTES,
        metavar="BYTES",
        help=f"Payload bytes per data file (default: {DEFAULT_CHUNK_BYTES}).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_DATA_FILES_PER_COMMIT,
        help=(
            f"Data files per commit (default: {DEFAULT_DATA_FILES_PER_COMMIT}). Raise it "
            f"only for a corpus with many small files."
        ),
    )
    parser.add_argument(
        "--create-repo",
        action="store_true",
        help="Create the target repo (private) if it does not exist yet.",
    )
    parser.add_argument(
        "--exclude-unresolved",
        action="store_true",
        help=(
            "Publish without documents whose failure might have succeeded on another "
            "attempt, excluding them like any other failure."
        ),
    )
    parser.add_argument(
        "--no-tag",
        action="store_true",
        help="Skip tagging the published version.",
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
    except (PublishError, ConfigError, UploadError) as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)


def _run(args: argparse.Namespace) -> None:
    config_path, manifest_path = find_version_files(args.version_dir)
    config = load_config(config_path)
    rows = read_manifest(manifest_path)
    failures: list[RenderFailure] = read_failures(args.version_dir / FAILURES_FILENAME)

    check_failures_resolved(failures, accept_unresolved=args.exclude_unresolved)
    kept, dropped = manifest_without_failures(rows, failures)
    published_config = config_with_exclusions(config, failures)
    target = build_target(args, config)
    output_dir = args.version_dir / PUBLISHED_DIRECTORY

    prepared = prepare_new_rows(config, kept, args.version_dir)
    already = published_ids(target, config)
    new = {
        str(value)
        for item in prepared
        for value in item.table.column(config.id_column).to_pylist()
    }
    check_manifest_is_covered(kept, already, new)
    if already:
        LOGGER.info(
            "%d document(s) already published by earlier versions; %d being added",
            len(already),
            len(new),
        )

    data_files = write_published(
        prepared, published_config, kept, output_dir, chunk_bytes=args.chunk_bytes
    )
    publish(
        target=target,
        config=published_config,
        data_files=data_files,
        output_dir=output_dir,
        batch_size=args.batch_size,
        with_tag=not args.no_tag,
    )

    for line in describe_publication(prepared, dropped, failures):
        print(line)
    print(
        f"Published {version_name(published_config)}: {len(data_files)} new data file(s), "
        f"{len(kept)} document(s) in the manifest"
    )


if __name__ == "__main__":
    main()
