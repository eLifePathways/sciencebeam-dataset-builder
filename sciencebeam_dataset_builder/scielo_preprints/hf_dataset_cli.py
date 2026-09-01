"""Assemble HuggingFace-compatible Parquet dataset files from downloaded SciELO preprints.

Output layout (follows HF Parquet conventions):
    <output_dir>/
        train-00000-of-00001.parquet
        validation-00000-of-00001.parquet
        test-00000-of-00001.parquet

Rows conform to :data:`sciencebeam_dataset_builder.dataset.schema.CANONICAL_SCHEMA`,
which every source in the dataset shares. For this source `id` holds the EuropePMC
preprint accession (e.g. `PPR123456`), since the JATS comes from EuropePMC.
"""

import argparse
import csv
import json
import logging
import sys
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from sciencebeam_dataset_builder.dataset.schema import (
    CANONICAL_SCHEMA,
    SOURCES,
    make_uid,
)

LOGGER = logging.getLogger(__name__)

SOURCE = SOURCES["scielo_preprints"]

# HuggingFace uses "validation", the split CSV uses "val".
SPLIT_NAME_MAP = {"train": "train", "val": "validation", "test": "test"}

# Metadata fields copied straight across from the metadata JSONL.
_TEXT_FIELDS = (
    "doi",
    "version",
    "title",
    "pub_date",
    "license",
    "subject_heading",
    "subject_europepmc_category",
    "article_type",
    "language",
    "language_raw",
    "xml_source_url",
    "xml_downloaded_at",
    "pdf_source_url",
    "pdf_downloaded_at",
)


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    with path.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def _read_csv(path: Path) -> list[dict[str, object]]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _str(value: object) -> str:
    return str(value) if value is not None else ""


def _authors(meta: dict[str, object]) -> list[dict[str, object]]:
    raw_authors = cast(list[dict[str, object]], meta.get("authors") or [])
    return [
        {
            "name": _str(author.get("name")),
            "orcid": _str(author.get("orcid")),
            "affiliations": [
                _str(affiliation)
                for affiliation in cast(list[object], author.get("affiliations") or [])
            ],
        }
        for author in raw_authors
    ]


def build_row(
    document_id: str,
    meta: dict[str, object],
    xml: str,
    pdf: bytes,
) -> dict[str, Any]:
    """Return one canonical-schema row."""
    return {
        "source": SOURCE.name,
        "id": document_id,
        "uid": make_uid(SOURCE.name, document_id),
        "authors": _authors(meta),
        "keywords": [
            _str(keyword) for keyword in cast(list[object], meta.get("keywords") or [])
        ],
        "xml_format": SOURCE.xml_format,
        "xml_ftfy_applied": bool(meta.get("xml_ftfy_applied")),
        "xml": xml,
        "pdf": pdf,
        **{field: _str(meta.get(field)) for field in _TEXT_FIELDS},
    }


def _build_split_table(
    documents_dir: Path,
    split_rows: list[dict[str, object]],
    metadata_by_id: dict[str, dict[str, object]],
) -> pa.Table:
    rows: list[dict[str, Any]] = []

    for row in tqdm(split_rows, unit=" docs", leave=False):
        document_id = str(row["id"])
        xml_path = documents_dir / f"{document_id}.xml"
        pdf_path = documents_dir / f"{document_id}.pdf"

        if not xml_path.exists():
            raise FileNotFoundError(f"XML not found: {xml_path}")
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {pdf_path}")

        rows.append(
            build_row(
                document_id,
                metadata_by_id.get(document_id, {}),
                xml_path.read_text(encoding="utf-8"),
                pdf_path.read_bytes(),
            )
        )

    return pa.Table.from_pylist(rows, schema=CANONICAL_SCHEMA)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Assemble HuggingFace-compatible Parquet files from SciELO preprints."
    )
    parser.add_argument(
        "documents_dir",
        type=Path,
        help="Directory containing PPR*.xml and PPR*.pdf files.",
    )
    parser.add_argument(
        "split_csv",
        type=Path,
        help="Split CSV produced by split_cli (columns: id, split).",
    )
    parser.add_argument(
        "metadata_jsonl",
        type=Path,
        help="Metadata JSONL produced by metadata_cli.",
    )
    parser.add_argument(
        "output_dir",
        type=Path,
        help="Directory to write Parquet files into.",
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

    split_rows = _read_csv(args.split_csv)
    metadata_by_id = {
        str(record["id"]): record for record in _read_jsonl(args.metadata_jsonl)
    }

    by_split: dict[str, list[dict[str, object]]] = {}
    for row in split_rows:
        by_split.setdefault(str(row["split"]), []).append(row)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    for split, rows in sorted(by_split.items()):
        hf_split = SPLIT_NAME_MAP.get(split, split)
        out_path = args.output_dir / f"{hf_split}-00000-of-00001.parquet"

        print(f"Building {hf_split} ({len(rows)} docs)...")
        table = _build_split_table(args.documents_dir, rows, metadata_by_id)
        pq.write_table(table, out_path, compression="snappy")
        size_mb = out_path.stat().st_size / 1024 / 1024
        print(f"  wrote {out_path.name}  ({len(table)} rows, {size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
