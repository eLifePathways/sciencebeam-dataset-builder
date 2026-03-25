"""Assemble HuggingFace-compatible Parquet dataset files from downloaded SciELO preprints.

Output layout (follows HF Parquet conventions):
    <output_dir>/
        train-00000-of-00001.parquet
        validation-00000-of-00001.parquet
        test-00000-of-00001.parquet

Schema per row:
    ppr_id    : string   — document identifier (e.g. "PPR_123456")
    language  : string   — normalised ISO 639-1 language code
    xml       : string   — JATS XML content
    pdf       : binary   — PDF bytes
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

LOGGER = logging.getLogger(__name__)

# HuggingFace uses "validation", the split CSV uses "val".
SPLIT_NAME_MAP = {"train": "train", "val": "validation", "test": "test"}

SCHEMA = pa.schema(
    [
        pa.field("ppr_id", pa.string()),
        pa.field("language", pa.string()),
        pa.field("xml", pa.string()),
        pa.field("pdf", pa.binary()),
    ]
)


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    with path.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def _build_split_batches(
    documents_dir: Path,
    split_rows: list[dict[str, object]],
    metadata_by_id: dict[str, dict[str, object]],
) -> pa.Table:
    ppr_ids: list[str] = []
    languages: list[str] = []
    xmls: list[str] = []
    pdfs: list[bytes] = []

    for row in tqdm(split_rows, unit=" docs", leave=False):
        ppr_id = str(row["ppr_id"])
        xml_path = documents_dir / f"{ppr_id}.xml"
        pdf_path = documents_dir / f"{ppr_id}.pdf"

        if not xml_path.exists():
            raise FileNotFoundError(f"XML not found: {xml_path}")
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {pdf_path}")

        ppr_ids.append(ppr_id)
        languages.append(str(metadata_by_id.get(ppr_id, {}).get("language", "")))
        xmls.append(xml_path.read_text(encoding="utf-8"))
        pdfs.append(pdf_path.read_bytes())

    return pa.table(
        {"ppr_id": ppr_ids, "language": languages, "xml": xmls, "pdf": pdfs},
        schema=SCHEMA,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Assemble HuggingFace-compatible Parquet files from SciELO preprints."
    )
    parser.add_argument(
        "documents_dir",
        type=Path,
        help="Directory containing PPR_*.xml and PPR_*.pdf files.",
    )
    parser.add_argument(
        "split_csv",
        type=Path,
        help="Split CSV produced by dataset_split_cli (columns: ppr_id, split).",
    )
    parser.add_argument(
        "metadata_jsonl",
        type=Path,
        help="Metadata JSONL produced by scielo_preprints_metadata_cli.",
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

    split_rows = _read_jsonl(args.split_csv)
    metadata_by_id = {str(r["ppr_id"]): r for r in _read_jsonl(args.metadata_jsonl)}

    by_split: dict[str, list[dict[str, object]]] = {}
    for row in split_rows:
        by_split.setdefault(str(row["split"]), []).append(row)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    for split, rows in sorted(by_split.items()):
        hf_split = SPLIT_NAME_MAP.get(split, split)
        out_path = args.output_dir / f"{hf_split}-00000-of-00001.parquet"

        print(f"Building {hf_split} ({len(rows)} docs)...")
        table = _build_split_batches(args.documents_dir, rows, metadata_by_id)
        pq.write_table(table, out_path, compression="snappy")
        size_mb = out_path.stat().st_size / 1024 / 1024
        print(f"  wrote {out_path.name}  ({len(table)} rows, {size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
