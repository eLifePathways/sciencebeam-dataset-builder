"""Migrate the published Hub subsets onto the canonical schema.

Reads each config's Parquet files from the Hub, conforms them to
:data:`CANONICAL_SCHEMA` and writes them locally, mirroring the Hub layout so the
result can be uploaded config by config.

Downloads are kept as a pristine backup under `--input-dir`, untouched by
normalisation. That gives a local copy of exactly what the Hub held before migration,
and makes re-runs free: an input file that is already present is reused instead of
re-downloaded.

Split membership is preserved by construction: each input split file is normalised and
written back as the same split file, so no row changes split. The `xml` and `pdf`
payloads are compared before and after and the run fails if either differs.

    # what would change, reading only Parquet footers
    python -m sciencebeam_dataset_builder.dataset.migrate_cli data/output/migrated --dry-run

    # download to data/input (backup), normalise, write to data/output/migrated
    python -m sciencebeam_dataset_builder.dataset.migrate_cli data/output/migrated

    # then, once the local result has been checked
    python -m sciencebeam_dataset_builder.dataset.migrate_cli data/output/migrated --upload
"""

import argparse
import logging
import os
import shutil
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from sciencebeam_dataset_builder.dataset.normalise import (
    describe_changes,
    normalise_table,
)
from sciencebeam_dataset_builder.dataset.schema import SOURCES, Source
from sciencebeam_dataset_builder.dataset.split import SPLIT_NAMES

LOGGER = logging.getLogger(__name__)

DEFAULT_REPO_ID = "elifepathways/sciencebeam-v2-benchmarking"

DEFAULT_INPUT_DIR = Path("data/input")
DEFAULT_OUTPUT_DIR = Path("data/output/migrated")

SPLIT_FILENAME = "{split}-00000-of-00001.parquet"


def _token() -> str | None:
    return os.environ.get("HF_TOKEN")


def _hub_path(source: Source, split: str) -> str:
    return f"{source.config}/{SPLIT_FILENAME.format(split=split)}"


def read_hub_schema(repo_id: str, path: str) -> pa.Schema:
    """Return the Arrow schema of a Hub Parquet file, reading only its footer."""
    from huggingface_hub import HfFileSystem

    fs = HfFileSystem(token=_token())
    with fs.open(f"datasets/{repo_id}/{path}", "rb") as f:
        return pq.ParquetFile(f).schema_arrow


def fetch_to_input_dir(repo_id: str, path: str, input_dir: Path) -> Path:
    """Return a local, pristine copy of a Hub Parquet file, downloading it if needed.

    The copy under `input_dir` is the backup: it is never written to after this point,
    so it always reflects what the Hub held when the migration ran. An existing copy is
    reused, which makes repeat runs offline and free.
    """
    backup = input_dir / path
    if backup.exists():
        LOGGER.info("Reusing local input %s", backup)
        return backup

    from huggingface_hub import hf_hub_download

    downloaded = hf_hub_download(repo_id, path, repo_type="dataset", token=_token())
    backup.parent.mkdir(parents=True, exist_ok=True)
    # copy, not move: hf_hub_download hands back a path inside the shared HF cache.
    shutil.copyfile(downloaded, backup)
    return backup


def read_hub_table(repo_id: str, path: str, input_dir: Path) -> pa.Table:
    """Download (or reuse) a Hub Parquet file and return it as an Arrow table."""
    return pq.read_table(fetch_to_input_dir(repo_id, path, input_dir))


def _assert_payload_unchanged(before: pa.Table, after: pa.Table, label: str) -> None:
    """Fail unless the xml and pdf payloads survived normalisation untouched."""
    if before.num_rows != after.num_rows:
        raise SystemExit(
            f"{label}: row count changed {before.num_rows} -> {after.num_rows}"
        )
    for column in ("xml", "pdf"):
        if before.column(column).to_pylist() != after.column(column).to_pylist():
            raise SystemExit(f"{label}: {column} payload changed")


def migrate_source(
    source: Source,
    repo_id: str,
    input_dir: Path,
    output_dir: Path,
    nullify_empty: bool,
) -> None:
    """Normalise every split of `source` and write it under `output_dir`."""
    config_dir = output_dir / source.config
    config_dir.mkdir(parents=True, exist_ok=True)

    for split in SPLIT_NAMES:
        hub_path = _hub_path(source, split)
        label = f"{source.config}/{split}"
        print(f"  {label}: fetching...", flush=True)
        table = read_hub_table(repo_id, hub_path, input_dir)

        normalised = normalise_table(table, source, nullify_empty=nullify_empty)
        _assert_payload_unchanged(table, normalised, label)

        out_path = config_dir / SPLIT_FILENAME.format(split=split)
        pq.write_table(normalised, out_path, compression="snappy")
        size_mb = out_path.stat().st_size / 1024 / 1024
        print(f"  {label}: {normalised.num_rows} rows -> {out_path} ({size_mb:.1f} MB)")


def dry_run_source(source: Source, repo_id: str) -> None:
    """Print what migrating `source` would change, without downloading row data."""
    for split in SPLIT_NAMES:
        hub_path = _hub_path(source, split)
        schema = read_hub_schema(repo_id, hub_path)
        empty = pa.Table.from_pylist([], schema=schema)
        changes = describe_changes(empty, source)
        print(f"  {source.config}/{split}:")
        for change in changes:
            print(f"    - {change}")


def upload_source(source: Source, repo_id: str, output_dir: Path) -> None:
    """Upload a migrated config directory to the Hub."""
    from huggingface_hub import HfApi

    config_dir = output_dir / source.config
    missing = [
        split
        for split in SPLIT_NAMES
        if not (config_dir / SPLIT_FILENAME.format(split=split)).exists()
    ]
    if missing:
        raise SystemExit(
            f"{source.config}: cannot upload, missing local split(s): {missing}. "
            "Run the migration without --upload first."
        )

    api = HfApi(token=_token())
    print(f"  {source.config}: uploading {config_dir}...", flush=True)
    api.upload_folder(
        repo_id=repo_id,
        repo_type="dataset",
        folder_path=str(config_dir),
        path_in_repo=source.config,
        commit_message=f"Migrate {source.config} to the canonical schema",
    )
    print(f"  {source.config}: uploaded")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate published Hub subsets onto the canonical schema."
    )
    parser.add_argument(
        "output_dir",
        type=Path,
        nargs="?",
        default=DEFAULT_OUTPUT_DIR,
        help=(
            "Directory to write migrated Parquet files into, mirroring the Hub layout "
            f"(default: {DEFAULT_OUTPUT_DIR})."
        ),
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help=(
            "Directory holding pristine copies of the downloaded Parquet files, kept as "
            f"a backup and reused on re-runs (default: {DEFAULT_INPUT_DIR})."
        ),
    )
    parser.add_argument(
        "--repo-id",
        default=DEFAULT_REPO_ID,
        help=f"Hub dataset repo (default: {DEFAULT_REPO_ID}).",
    )
    parser.add_argument(
        "--source",
        action="append",
        choices=sorted(SOURCES),
        metavar="NAME",
        help="Migrate only this source; repeatable. Default: all.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the schema changes without downloading row data or writing files.",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload already-migrated local files to the Hub. Does not migrate.",
    )
    parser.add_argument(
        "--nullify-empty",
        action="store_true",
        help=(
            "Also convert empty strings to nulls. Off by default because it rewrites "
            "stored values: the historical builders wrote '' for absent metadata."
        ),
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

    if args.dry_run and args.upload:
        raise SystemExit("--dry-run and --upload are mutually exclusive")

    if not _token():
        print(
            "HF_TOKEN is not set; the dataset is private and cannot be read.",
            file=sys.stderr,
        )
        sys.exit(1)

    selected = [SOURCES[name] for name in (args.source or sorted(SOURCES))]

    for source in selected:
        print(f"{source.config} ({source.name}):")
        if args.dry_run:
            dry_run_source(source, args.repo_id)
        elif args.upload:
            upload_source(source, args.repo_id, args.output_dir)
        else:
            migrate_source(
                source,
                args.repo_id,
                args.input_dir,
                args.output_dir,
                args.nullify_empty,
            )

    if args.dry_run:
        print("\nDry run only; nothing downloaded or written.")


if __name__ == "__main__":
    main()
