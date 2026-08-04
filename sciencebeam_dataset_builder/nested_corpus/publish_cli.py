"""Publish a rendered corpus version: the split files, the manifest and the config.

The last of three steps. The cut selects and reads, rendering adds the PDFs, and this
assembles what those produced with the previously published rows and publishes the
result — data first, then the manifest and config, then a tag so a reader can pin the
version immutably rather than trusting a branch to stay put.
"""

import argparse
import logging
import sys
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from sciencebeam_dataset_builder.nested_corpus.config import (
    ConfigError,
    CorpusConfig,
    dump_config,
    load_config,
)
from sciencebeam_dataset_builder.nested_corpus.cut_cli import version_name
from sciencebeam_dataset_builder.nested_corpus.manifest import (
    ManifestRow,
    read_manifest,
    write_manifest,
)
from sciencebeam_dataset_builder.nested_corpus.publish import (
    PreparedSplit,
    PublishError,
    check_output_schema,
    config_with_exclusions,
    describe_publication,
    manifest_without_failures,
    merge_split,
)
from sciencebeam_dataset_builder.nested_corpus.render import RenderFailure
from sciencebeam_dataset_builder.nested_corpus.render_cli import (
    FAILURES_FILENAME,
    RENDERED_DIRECTORY,
    read_failures,
)
from sciencebeam_dataset_builder.nested_corpus.upload import (
    DEFAULT_BATCH_SIZE,
    FileToPublish,
    HfPublishTarget,
    LocalPublishTarget,
    PublishTarget,
    UploadError,
    batched,
)

LOGGER = logging.getLogger(__name__)

PUBLISHED_DIRECTORY = "published"
SPLITS_DIRECTORY = "splits"


def split_path_in_repo(split: str) -> str:
    """One file per split at the repo root, which is what readers expect to name."""
    return f"{split}.parquet"


def manifest_path_in_repo(config: CorpusConfig) -> str:
    return f"{SPLITS_DIRECTORY}/{version_name(config)}.csv"


def config_path_in_repo(config: CorpusConfig) -> str:
    return f"{SPLITS_DIRECTORY}/{version_name(config)}.yml"


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


def prepare_splits(
    config: CorpusConfig,
    rows: list[ManifestRow],
    rendered_dir: Path,
    target: PublishTarget,
    work_dir: Path,
) -> list[PreparedSplit]:
    """Merge each split's new rows with those already published."""
    prepared: list[PreparedSplit] = []
    for split in config.splits:
        wanted = [row for row in rows if row.split == split]
        if not wanted:
            LOGGER.info("Split %r has no documents; nothing to publish for it", split)
            continue
        added = _read_if_present(rendered_dir / f"{split}.parquet")
        previous = _fetch_previous(target, split, work_dir)
        if previous is None and added is None:
            raise PublishError(
                f"split {split!r} lists {len(wanted)} document(s) but neither new nor "
                f"published rows were found for it"
            )
        table = merge_split(split, previous, added, rows, config.id_column)
        check_output_schema(split, table, config)
        prepared.append(PreparedSplit(split=split, table=table))
    return prepared


def _read_if_present(path: Path) -> pa.Table | None:
    if not path.exists():
        return None
    table = pq.read_table(path)
    return table if table.num_rows else None


def _fetch_previous(
    target: PublishTarget, split: str, work_dir: Path
) -> pa.Table | None:
    fetched = target.fetch(split_path_in_repo(split), work_dir)
    if fetched is None:
        return None
    LOGGER.info("Carrying forward already-published rows for split %r", split)
    return _read_if_present(fetched)


def write_published(
    prepared: list[PreparedSplit],
    config: CorpusConfig,
    rows: list[ManifestRow],
    output_dir: Path,
) -> list[FileToPublish]:
    """Write what will be published locally, so it can be inspected before upload."""
    output_dir.mkdir(parents=True, exist_ok=True)
    files: list[FileToPublish] = []
    for item in prepared:
        path = output_dir / split_path_in_repo(item.split)
        pq.write_table(item.table, path, compression="zstd")
        files.append(
            FileToPublish(local_path=path, path_in_repo=split_path_in_repo(item.split))
        )

    splits_dir = output_dir / SPLITS_DIRECTORY
    manifest_path = splits_dir / f"{version_name(config)}.csv"
    config_path = splits_dir / f"{version_name(config)}.yml"
    write_manifest(manifest_path, rows)
    dump_config(config, config_path)
    return files


def publish(
    target: PublishTarget,
    config: CorpusConfig,
    data_files: list[FileToPublish],
    output_dir: Path,
    batch_size: int = DEFAULT_BATCH_SIZE,
    with_tag: bool = True,
) -> None:
    """Data first in batched commits, then the manifest and config, then the tag."""
    name = version_name(config)
    for index, batch in enumerate(batched(data_files, batch_size), start=1):
        target.publish(batch, f"Add {name} data ({index}): {len(batch)} file(s)")

    # Only now: until every data file is present, a manifest would describe rows that
    # cannot be read.
    target.publish(
        [
            FileToPublish(
                local_path=output_dir / SPLITS_DIRECTORY / f"{name}.csv",
                path_in_repo=manifest_path_in_repo(config),
            ),
            FileToPublish(
                local_path=output_dir / SPLITS_DIRECTORY / f"{name}.yml",
                path_in_repo=config_path_in_repo(config),
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
    return HfPublishTarget(repo_id)


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
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Data files per commit (default: {DEFAULT_BATCH_SIZE}).",
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

    kept, dropped = manifest_without_failures(rows, failures)
    published_config = config_with_exclusions(config, failures)
    target = build_target(args, config)
    output_dir = args.version_dir / PUBLISHED_DIRECTORY

    with tempfile.TemporaryDirectory(prefix="nested-corpus-publish-") as work:
        prepared = prepare_splits(
            config=config,
            rows=kept,
            rendered_dir=args.version_dir / RENDERED_DIRECTORY,
            target=target,
            work_dir=Path(work),
        )
        data_files = write_published(prepared, published_config, kept, output_dir)
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
    print(f"Published {version_name(published_config)}")


if __name__ == "__main__":
    main()
