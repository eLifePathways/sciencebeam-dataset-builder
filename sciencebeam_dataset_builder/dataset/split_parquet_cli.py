"""Split a source's Parquet files into train/validation/test.

Replaces the earlier pandas-based splitter, which rewrote the schema on the way through
and produced an assignment that depended on the order `os.listdir` happened to return.
See :mod:`sciencebeam_dataset_builder.dataset.split`.
"""

import argparse
import logging
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from sciencebeam_dataset_builder.dataset.normalise import normalise_table
from sciencebeam_dataset_builder.dataset.schema import SOURCES
from sciencebeam_dataset_builder.dataset.split import (
    DEFAULT_FRACTIONS,
    SPLIT_NAMES,
    read_split_map,
    split_table,
    write_splits,
)

LOGGER = logging.getLogger(__name__)


def read_input_tables(input_dir: Path) -> pa.Table:
    """Concatenate every Parquet file in `input_dir`, in sorted filename order."""
    paths = sorted(input_dir.glob("*.parquet"))
    if not paths:
        raise SystemExit(f"No Parquet files found in {input_dir}")
    LOGGER.info("Reading %d file(s): %s", len(paths), [p.name for p in paths])
    return pa.concat_tables([pq.read_table(p) for p in paths])


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Split Parquet files into train/validation/test sets."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Directory containing input Parquet files.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory to write split Parquet files into.",
    )
    parser.add_argument(
        "--source",
        choices=sorted(SOURCES),
        required=True,
        help="Which source these files belong to; sets the source and xml_format columns.",
    )
    parser.add_argument(
        "--split-map",
        type=Path,
        default=None,
        help=(
            "JSONL of frozen uid -> split assignments to reproduce exactly. "
            "Rows absent from it fall back to the hash bucket."
        ),
    )
    for name in SPLIT_NAMES:
        parser.add_argument(
            f"--{name}",
            type=float,
            default=DEFAULT_FRACTIONS[name],
            metavar="FRAC",
            help=f"Fraction for {name} (default: {DEFAULT_FRACTIONS[name]}).",
        )
    parser.add_argument(
        "--salt",
        default="",
        help="Salt for the hash bucket; changing it reshuffles every unlocked row.",
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

    source = SOURCES[args.source]
    fractions = {name: getattr(args, name) for name in SPLIT_NAMES}
    split_map = read_split_map(args.split_map) if args.split_map else None

    table = normalise_table(read_input_tables(args.input_dir), source)
    splits = split_table(table, fractions, split_map=split_map, salt=args.salt)

    total = sum(t.num_rows for t in splits.values())
    if total != table.num_rows:
        raise SystemExit(f"Split lost rows: {table.num_rows} -> {total}")

    write_splits(splits, args.output_dir)
    print(
        "Split "
        + " ".join(f"{name}={splits[name].num_rows}" for name in SPLIT_NAMES)
        + f" (total={total})"
    )


if __name__ == "__main__":
    main()
