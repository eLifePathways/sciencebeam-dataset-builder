"""Derive every document's licence and report what it permits for training.

Reads a dated snapshot under `data/`, writes `reports/`. Reads only `uid`, `id`, `doi`
and `xml`, never `pdf`. Uploads and deletes nothing.
"""

import argparse
import csv
import logging
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow.parquet as pq

from sciencebeam_dataset_builder.dataset.license.classify_license import (
    TrainingUse,
    classify,
)
from sciencebeam_dataset_builder.dataset.license.extract_license import extract_licence
from sciencebeam_dataset_builder.dataset.migrate_cli import SPLIT_FILENAME
from sciencebeam_dataset_builder.dataset.schema import SOURCES_BY_CONFIG, make_uid
from sciencebeam_dataset_builder.dataset.split import SPLIT_NAMES

LOGGER = logging.getLogger(__name__)

DEFAULT_DATA_DIR = Path("data")
DEFAULT_OUTPUT_DIR = Path(__file__).parent / "reports"

# Snapshots are named for the day they were taken, so the newest sorts last.
SNAPSHOT_DIR_NAME = re.compile(r"^\d{4}-\d{2}-\d{2}$")

PER_DOCUMENT_FILENAME = "licence-per-document.csv"
EXCLUSIONS_FILENAME = "training-exclusions.csv"
NON_COMMERCIAL_FILENAME = "training-non-commercial.csv"

PER_DOCUMENT_HEADER = (
    "config",
    "split",
    "uid",
    "doi",
    "licence",
    "training_use",
    "excluded",
    "evidence",
)


class AuditError(Exception):
    """Raised when the audit cannot run over the directory it was given."""


def latest_snapshot(data_dir: Path) -> Path:
    """Return the most recent dated snapshot directory under `data_dir`."""
    if not data_dir.is_dir():
        raise AuditError(f"{data_dir} does not exist")
    snapshots = sorted(
        path
        for path in data_dir.iterdir()
        if path.is_dir() and SNAPSHOT_DIR_NAME.match(path.name)
    )
    if not snapshots:
        raise AuditError(
            f"No dated snapshot (YYYY-MM-DD) under {data_dir}; "
            f"pass --dataset-dir to name one explicitly"
        )
    return snapshots[-1]


def audit_config(config_dir: Path) -> list[dict[str, str]]:
    """Return one result row per document in one config's directory.

    `scielo-preprints-metadata` has no `uid` column, so its uid is derived the way
    removal derives it.
    """
    config = config_dir.name
    source = SOURCES_BY_CONFIG.get(config)
    if source is None:
        raise AuditError(f"{config_dir}: {config!r} is not a registered config")

    rows: list[dict[str, str]] = []
    for split in SPLIT_NAMES:
        path = config_dir / SPLIT_FILENAME.format(split=split)
        if not path.exists():
            LOGGER.warning("%s: no %s split, skipping", config, split)
            continue

        available = set(pq.ParquetFile(path).schema_arrow.names)
        columns = [name for name in ("uid", "id", "doi", "xml") if name in available]
        table = pq.read_table(path, columns=columns)
        LOGGER.info("%s/%s: %d rows", config, split, table.num_rows)

        for record in table.to_pylist():
            uid = record.get("uid") or make_uid(source.name, record["id"])
            licence = extract_licence(record["xml"])
            use = classify(licence)
            rows.append(
                {
                    "config": config,
                    "split": split,
                    "uid": uid,
                    "doi": record.get("doi") or "",
                    "licence": licence.label,
                    "training_use": use.value,
                    "excluded": "yes" if use.excluded else "no",
                    "evidence": licence.evidence[:200],
                }
            )
    return rows


def audit_snapshot(dataset_dir: Path) -> list[dict[str, str]]:
    """Return one result row per document across every config in `dataset_dir`."""
    if not dataset_dir.is_dir():
        raise AuditError(f"{dataset_dir} does not exist")
    config_dirs = sorted(
        path
        for path in dataset_dir.iterdir()
        if path.is_dir() and path.name in SOURCES_BY_CONFIG
    )
    if not config_dirs:
        raise AuditError(f"{dataset_dir} holds no registered config directories")

    rows: list[dict[str, str]] = []
    for config_dir in config_dirs:
        rows.extend(audit_config(config_dir))
    return rows


def _write_csv(path: Path, header: tuple[str, ...], rows: list[list[str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def write_reports(rows: list[dict[str, str]], output_dir: Path) -> None:
    """Write the per-document table and the two exclusion lists."""
    output_dir.mkdir(parents=True, exist_ok=True)

    _write_csv(
        output_dir / PER_DOCUMENT_FILENAME,
        PER_DOCUMENT_HEADER,
        [[row[name] for name in PER_DOCUMENT_HEADER] for row in rows],
    )

    ordered = sorted(rows, key=lambda row: (row["config"], row["uid"]))
    _write_csv(
        output_dir / EXCLUSIONS_FILENAME,
        ("uid", "reason"),
        [
            [row["uid"], f"{row['licence']} ({row['training_use']})"]
            for row in ordered
            if row["excluded"] == "yes"
        ],
    )
    _write_csv(
        output_dir / NON_COMMERCIAL_FILENAME,
        ("uid", "reason"),
        [
            [row["uid"], f"{row['licence']} - commercial use restricted"]
            for row in ordered
            if row["training_use"] == TrainingUse.NON_COMMERCIAL.value
        ],
    )


def print_summary(rows: list[dict[str, str]]) -> None:
    """Print the licence and verdict breakdown, per config and overall."""
    per_config: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        per_config[row["config"]][row["licence"]] += 1

    print(f"\nLicence by config ({len(rows)} documents)")
    for config in sorted(per_config):
        counts = per_config[config]
        total = sum(counts.values())
        print(f"\n  {config} ({total})")
        for licence, count in counts.most_common():
            print(f"    {100 * count / total:5.1f}%  {count:5d}  {licence}")

    verdicts = Counter(row["training_use"] for row in rows)
    print(f"\nTraining verdict ({len(rows)} documents)")
    for verdict, count in verdicts.most_common():
        print(f"    {100 * count / len(rows):5.1f}%  {count:5d}  {verdict}")

    excluded = sum(1 for row in rows if row["excluded"] == "yes")
    print(f"\n  excluded from training: {excluded} of {len(rows)}")


def main(argv: list[str] | None = None) -> int:
    """Run the audit and write its reports. Returns a process exit code."""
    parser = argparse.ArgumentParser(description="Audit dataset licences.")
    parser.add_argument(
        "--dataset-dir",
        type=Path,
        help="Snapshot to audit. Defaults to the newest dated directory under data/.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help=f"Where the dated snapshots live (default: {DEFAULT_DATA_DIR}).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Where to write the reports (default: the package's reports/).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    try:
        dataset_dir = args.dataset_dir or latest_snapshot(args.data_dir)
        print(f"Auditing {dataset_dir}")
        rows = audit_snapshot(dataset_dir)
    except AuditError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    write_reports(rows, args.output_dir)
    print_summary(rows)
    print(f"\nWrote {args.output_dir}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
