"""Check filtered Parquet files against the Hub snapshot they were derived from.

Run this between `remove` and `remove-upload`. It re-reads both sides from disk and
proves the removal deleted exactly what it was asked to delete:

* every row named by the removal lists is gone;
* no other row is gone - the surviving rows are the input's, in the input's order;
* every surviving value, `xml` and `pdf` payloads included, is unchanged;
* the schema is exactly the input's.

No network access: `--input-dir` is the snapshot taken when the removal ran, so
verification is offline and can be repeated at no cost.
"""

import argparse
import logging
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from sciencebeam_dataset_builder.dataset.migrate_cli import SPLIT_FILENAME
from sciencebeam_dataset_builder.dataset.removal.companions import METADATA_SOURCE
from sciencebeam_dataset_builder.dataset.removal.remove_cli import (
    DEFAULT_INPUT_DIR,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_REMOVALS_DIR,
    DEFAULT_SOURCE_NAMES,
    default_removal_lists,
    resolve_removal_set,
)
from sciencebeam_dataset_builder.dataset.removal.filter import verify_removal
from sciencebeam_dataset_builder.dataset.removal.removal_list import (
    RemovalListError,
    read_removal_lists,
)
from sciencebeam_dataset_builder.dataset.schema import SOURCES, Source
from sciencebeam_dataset_builder.dataset.split import SPLIT_NAMES

LOGGER = logging.getLogger(__name__)


def verify_source(
    source: Source,
    uids: set[str],
    input_dir: Path,
    output_dir: Path,
) -> tuple[int, int, int]:
    """Verify every split of `source`. Returns (rows kept, rows removed, failing)."""
    kept = 0
    removed = 0
    failing = 0

    for split in SPLIT_NAMES:
        filename = SPLIT_FILENAME.format(split=split)
        original_path = input_dir / source.config / filename
        filtered_path = output_dir / source.config / filename

        if not filtered_path.exists():
            print(f"  {split:11s} SKIP  not filtered ({filtered_path} missing)")
            continue
        if not original_path.exists():
            print(f"  {split:11s} SKIP  no input snapshot ({original_path} missing)")
            continue

        original = pq.read_table(original_path)
        filtered = pq.read_table(filtered_path)
        failures = verify_removal(original, filtered, uids, source)

        kept += filtered.num_rows
        removed += original.num_rows - filtered.num_rows

        if failures:
            failing += 1
            print(f"  {split:11s} FAIL  {filtered.num_rows} rows")
            for failure in failures:
                print(f"              - {failure}")
        else:
            print(
                f"  {split:11s} ok    {original.num_rows} -> {filtered.num_rows} rows "
                f"({original.num_rows - filtered.num_rows} removed)"
            )

    return kept, removed, failing


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify filtered Parquet files against their pre-removal snapshot."
    )
    parser.add_argument(
        "--removals-dir",
        type=Path,
        default=DEFAULT_REMOVALS_DIR,
        help=(
            "Directory holding the removal lists, every *.csv of which is read "
            f"(default: {DEFAULT_REMOVALS_DIR})."
        ),
    )
    parser.add_argument(
        "--removal-list",
        type=Path,
        action="append",
        metavar="CSV",
        help="A `uid,reason` CSV of documents that should be gone; repeatable. "
        "Overrides --removals-dir.",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help=f"Pre-removal Hub snapshot (default: {DEFAULT_INPUT_DIR}).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Filtered output (default: {DEFAULT_OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--source",
        action="append",
        choices=sorted(SOURCES),
        metavar="NAME",
        help="Verify only this source; repeatable. Default: every canonical source.",
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

    lists = args.removal_list or default_removal_lists(args.removals_dir)
    if not lists:
        raise SystemExit(
            f"No removal lists given and none found in {args.removals_dir}/."
        )
    try:
        removals = read_removal_lists(lists)
    except RemovalListError as error:
        raise SystemExit(str(error)) from error

    # The same resolution the removal made, so the Dublin Core rows deleted alongside
    # their JATS rendition verify as intended rather than as rows dropped unbidden. It
    # reads the snapshot under --input-dir, so this stays offline.
    def load_snapshot(source: Source, split: str) -> pa.Table:
        path = args.input_dir / source.config / SPLIT_FILENAME.format(split=split)
        if not path.exists():
            raise SystemExit(
                f"{path} is missing, so the companion rows cannot be resolved. "
                "Verification reads the snapshot the removal took; run the removal "
                "first, or point --input-dir at that snapshot."
            )
        return pq.read_table(path)

    reasons, _ = resolve_removal_set(removals, load_snapshot)
    uids = set(reasons)

    selected = [SOURCES[name] for name in (args.source or DEFAULT_SOURCE_NAMES)]
    # Nothing to check for a config that was never written.
    if not args.source and not (args.output_dir / METADATA_SOURCE.config).exists():
        selected = [s for s in selected if s is not METADATA_SOURCE]

    total_kept = 0
    total_removed = 0
    total_failing = 0
    for source in selected:
        print(f"{source.config} ({source.name}):")
        kept, removed, failing = verify_source(
            source, uids, args.input_dir, args.output_dir
        )
        total_kept += kept
        total_removed += removed
        total_failing += failing

    print()
    if total_failing:
        print(f"FAILED: {total_failing} split(s) did not verify. Do not upload.")
        sys.exit(1)
    print(
        f"All checks passed: {total_removed} rows removed, {total_kept} rows kept and "
        "unchanged."
    )


if __name__ == "__main__":
    main()
