"""Generate a stratified train/val/test split from a metadata CSV."""

import argparse
import csv
import logging
import random
import sys
from collections import defaultdict
from pathlib import Path

LOGGER = logging.getLogger(__name__)

SPLIT_FIELDS = ["ppr_id", "split"]

# Languages with enough documents to form their own stratum.
# Everything else is grouped as "other".
MAIN_LANGUAGES = {"pt", "es"}


def _stratum(language: str) -> str:
    return language if language in MAIN_LANGUAGES else "other"


def stratified_split(
    records: list[dict[str, str]],
    train_frac: float,
    val_frac: float,
    seed: int,
) -> list[dict[str, str]]:
    """Return records with a 'split' field added, using stratified sampling by language."""
    rng = random.Random(seed)

    by_stratum: dict[str, list[dict[str, str]]] = defaultdict(list)
    for record in records:
        by_stratum[_stratum(record["language"])].append(record)

    result: list[dict[str, str]] = []
    for stratum, group in sorted(by_stratum.items()):
        rng.shuffle(group)
        n = len(group)
        n_train = round(n * train_frac)
        n_val = round(n * val_frac)
        counts = {"train": n_train, "val": n_val, "test": n - n_train - n_val}
        LOGGER.info(
            "Stratum %r (%d docs): train=%d val=%d test=%d",
            stratum,
            n,
            counts["train"],
            counts["val"],
            counts["test"],
        )
        for i, record in enumerate(group):
            if i < n_train:
                split = "train"
            elif i < n_train + n_val:
                split = "val"
            else:
                split = "test"
            result.append({"ppr_id": record["ppr_id"], "split": split})

    rng.shuffle(result)
    return result


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a stratified train/val/test split from a metadata CSV."
    )
    parser.add_argument(
        "metadata_csv",
        type=Path,
        help="Metadata CSV produced by scielo_preprints_metadata_cli.",
    )
    parser.add_argument(
        "output_csv",
        type=Path,
        help="Path to write the split CSV (columns: ppr_id, split).",
    )
    parser.add_argument(
        "--train",
        type=float,
        default=0.2,
        metavar="FRAC",
        help="Fraction for training set (default: 0.2).",
    )
    parser.add_argument(
        "--val",
        type=float,
        default=0.3,
        metavar="FRAC",
        help="Fraction for validation set (default: 0.3).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        metavar="N",
        help="Random seed for reproducibility (default: 42).",
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

    if args.train + args.val >= 1.0:
        print("--train + --val must be less than 1.0", file=sys.stderr)
        sys.exit(1)

    with args.metadata_csv.open(newline="", encoding="utf-8") as f:
        records = list(csv.DictReader(f))

    if not records:
        print(f"No records found in {args.metadata_csv}", file=sys.stderr)
        sys.exit(1)

    result = stratified_split(records, args.train, args.val, args.seed)

    split_counts: dict[str, int] = defaultdict(int)
    for row in result:
        split_counts[row["split"]] += 1
    print(
        f"Split: train={split_counts['train']} "
        f"val={split_counts['val']} "
        f"test={split_counts['test']} "
        f"(total={len(result)})"
    )

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SPLIT_FIELDS)
        writer.writeheader()
        writer.writerows(result)

    print(f"Wrote split to {args.output_csv}")


if __name__ == "__main__":
    main()
