"""Check migrated Parquet files against the pristine inputs they were derived from.

Run this between `migrate` and `migrate-upload`. It re-reads both sides from disk and
proves the migration preserved everything it was supposed to preserve:

* every split has the same number of rows, in the same order;
* the source-native identifiers are unchanged, value for value;
* the `xml` and `pdf` payloads are byte-identical;
* every other carried-over column still holds its original values;
* the output matches the canonical schema exactly;
* `source`, `uid` and `xml_format` are populated and internally consistent.

No network access: `data/input` is the copy taken at download time, so verification is
offline and can be repeated at no cost.
"""

import argparse
import logging
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from sciencebeam_dataset_builder.dataset.migrate_cli import (
    DEFAULT_INPUT_DIR,
    DEFAULT_OUTPUT_DIR,
    SPLIT_FILENAME,
)
from sciencebeam_dataset_builder.dataset.normalise import find_id_column
from sciencebeam_dataset_builder.dataset.schema import (
    CANONICAL_SCHEMA,
    CANONICAL_SOURCES,
    SOURCES,
    Source,
    make_uid,
)
from sciencebeam_dataset_builder.dataset.split import SPLIT_NAMES

LOGGER = logging.getLogger(__name__)

# Derived by the migration rather than carried over, so they have no counterpart input.
DERIVED_COLUMNS = frozenset({"source", "uid", "xml_format"})


def verify_split(
    original: pa.Table,
    migrated: pa.Table,
    source: Source,
) -> list[str]:
    """Return a list of failure messages; empty means the split verified."""
    failures: list[str] = []

    if original.num_rows != migrated.num_rows:
        return [f"row count {original.num_rows} -> {migrated.num_rows}"]

    if not migrated.schema.equals(CANONICAL_SCHEMA):
        failures.append("schema does not match the canonical schema")

    # A malformed table must produce findings, not a KeyError, so stop before the
    # value comparisons if the columns they read are not all present.
    absent = [
        name for name in CANONICAL_SCHEMA.names if name not in migrated.column_names
    ]
    if absent:
        failures.append(f"columns missing from the output: {absent}")
        return failures

    id_column = find_id_column(original)
    ids = migrated.column("id").to_pylist()
    if original.column(id_column).cast(pa.string()).to_pylist() != ids:
        failures.append(f"{id_column} values or their order changed")

    for column in ("xml", "pdf"):
        if original.column(column).to_pylist() != migrated.column(column).to_pylist():
            failures.append(f"{column} payload changed")

    # Everything the input already carried must survive untouched.
    carried = (
        (set(original.column_names) & set(CANONICAL_SCHEMA.names))
        - DERIVED_COLUMNS
        - {"id", "xml", "pdf"}
    )
    for column in sorted(carried):
        if original.column(column).to_pylist() != migrated.column(column).to_pylist():
            failures.append(f"{column} values changed")

    sources = set(migrated.column("source").to_pylist())
    if sources != {source.name}:
        failures.append(f"source is {sources}, expected {{{source.name!r}}}")

    formats = set(migrated.column("xml_format").to_pylist())
    if formats != {source.xml_format}:
        failures.append(f"xml_format is {formats}, expected {{{source.xml_format!r}}}")

    expected_uids = [make_uid(source.name, value) for value in ids]
    if migrated.column("uid").to_pylist() != expected_uids:
        failures.append("uid is not source__id for every row")

    return failures


def verify_source(source: Source, input_dir: Path, output_dir: Path) -> tuple[int, int]:
    """Verify every split of `source`. Returns (rows verified, failing splits)."""
    rows = 0
    failing = 0

    for split in SPLIT_NAMES:
        filename = SPLIT_FILENAME.format(split=split)
        original_path = input_dir / source.config / filename
        migrated_path = output_dir / source.config / filename

        if not migrated_path.exists():
            print(f"  {split:11s} SKIP  not migrated ({migrated_path} missing)")
            continue
        if not original_path.exists():
            print(f"  {split:11s} SKIP  no input backup ({original_path} missing)")
            continue

        original = pq.read_table(original_path)
        migrated = pq.read_table(migrated_path)
        failures = verify_split(original, migrated, source)
        rows += migrated.num_rows

        if failures:
            failing += 1
            print(f"  {split:11s} FAIL  {migrated.num_rows} rows")
            for failure in failures:
                print(f"              - {failure}")
        else:
            print(f"  {split:11s} ok    {migrated.num_rows} rows")

    return rows, failing


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify migrated Parquet files against their pristine inputs."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help=f"Pristine downloads (default: {DEFAULT_INPUT_DIR}).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Migrated output (default: {DEFAULT_OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--source",
        action="append",
        choices=sorted(SOURCES),
        metavar="NAME",
        help=(
            "Verify only this source; repeatable. Default: every source already on the "
            "canonical schema."
        ),
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.WARNING,
        format="%(asctime)s %(levelname)-8s %(message)s",
        stream=sys.stderr,
    )

    selected = [SOURCES[name] for name in (args.source or sorted(CANONICAL_SOURCES))]

    total_rows = 0
    total_failing = 0
    for source in selected:
        print(f"{source.config} ({source.name}):")
        rows, failing = verify_source(source, args.input_dir, args.output_dir)
        total_rows += rows
        total_failing += failing

    print()
    if total_failing:
        print(f"FAILED: {total_failing} split(s) did not verify. Do not upload.")
        sys.exit(1)
    print(f"All checks passed: {total_rows} rows verified against their inputs.")


if __name__ == "__main__":
    main()
