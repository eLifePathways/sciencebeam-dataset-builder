"""Assemble HuggingFace-compatible Parquet dataset files from downloaded SciELO preprints.

Output layout (follows HF Parquet conventions):
    <output_dir>/
        train-00000-of-00001.parquet
        validation-00000-of-00001.parquet
        test-00000-of-00001.parquet

Schema per row:
    ppr_id                     : string        — document identifier (e.g. "PPR123456")
    doi                        : string
    version                    : string
    title                      : string
    authors                    : list<struct>  — name, orcid, affiliations
    pub_date                   : string        — ISO 8601 date
    license                    : string        — URL
    keywords                   : list<string>
    subject_heading            : string
    subject_europepmc_category : string
    article_type               : string
    language                   : string        — normalised ISO 639-1 language code
    language_raw               : string        — language as reported by source
    xml_source_url             : string
    xml_downloaded_at          : string        — ISO 8601 datetime
    xml_ftfy_applied           : bool
    pdf_source_url             : string
    pdf_downloaded_at          : string        — ISO 8601 datetime
    xml                        : string        — JATS XML content
    pdf                        : binary        — PDF bytes
"""

import argparse
import csv
import json
import logging
import sys
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

LOGGER = logging.getLogger(__name__)

# HuggingFace uses "validation", the split CSV uses "val".
SPLIT_NAME_MAP = {"train": "train", "val": "validation", "test": "test"}

AUTHOR_TYPE = pa.struct(
    [
        pa.field("name", pa.string()),
        pa.field("orcid", pa.string()),
        pa.field("affiliations", pa.list_(pa.string())),
    ]
)

SCHEMA = pa.schema(
    [
        pa.field("ppr_id", pa.string()),
        pa.field("doi", pa.string()),
        pa.field("version", pa.string()),
        pa.field("title", pa.string()),
        pa.field("authors", pa.list_(AUTHOR_TYPE)),
        pa.field("pub_date", pa.string()),
        pa.field("license", pa.string()),
        pa.field("keywords", pa.list_(pa.string())),
        pa.field("subject_heading", pa.string()),
        pa.field("subject_europepmc_category", pa.string()),
        pa.field("article_type", pa.string()),
        pa.field("language", pa.string()),
        pa.field("language_raw", pa.string()),
        pa.field("xml_source_url", pa.string()),
        pa.field("xml_downloaded_at", pa.string()),
        pa.field("xml_ftfy_applied", pa.bool_()),
        pa.field("pdf_source_url", pa.string()),
        pa.field("pdf_downloaded_at", pa.string()),
        pa.field("xml", pa.string(), nullable=False),
        pa.field("pdf", pa.binary(), nullable=False),
    ]
)


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    with path.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def _read_csv(path: Path) -> list[dict[str, object]]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _str(value: object) -> str:
    return str(value) if value is not None else ""


def _build_split_batches(
    documents_dir: Path,
    split_rows: list[dict[str, object]],
    metadata_by_id: dict[str, dict[str, object]],
) -> pa.Table:
    ppr_ids: list[str] = []
    dois: list[str] = []
    versions: list[str] = []
    titles: list[str] = []
    authors_list: list[list[dict[str, object]]] = []
    pub_dates: list[str] = []
    licenses: list[str] = []
    keywords_list: list[list[str]] = []
    subject_headings: list[str] = []
    subject_europepmc_categories: list[str] = []
    article_types: list[str] = []
    languages: list[str] = []
    languages_raw: list[str] = []
    xml_source_urls: list[str] = []
    xml_downloaded_ats: list[str] = []
    xml_ftfy_applied_list: list[bool] = []
    pdf_source_urls: list[str] = []
    pdf_downloaded_ats: list[str] = []
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

        meta = metadata_by_id.get(ppr_id, {})
        raw_authors = cast(list[dict[str, object]], meta.get("authors") or [])
        authors: list[dict[str, object]] = [
            {
                "name": _str(a.get("name")),
                "orcid": _str(a.get("orcid")),
                "affiliations": [
                    _str(af) for af in cast(list[object], a.get("affiliations") or [])
                ],
            }
            for a in raw_authors
        ]

        ppr_ids.append(ppr_id)
        dois.append(_str(meta.get("doi")))
        versions.append(_str(meta.get("version")))
        titles.append(_str(meta.get("title")))
        authors_list.append(authors)
        pub_dates.append(_str(meta.get("pub_date")))
        licenses.append(_str(meta.get("license")))
        keywords_list.append(
            [_str(k) for k in cast(list[object], meta.get("keywords") or [])]
        )
        subject_headings.append(_str(meta.get("subject_heading")))
        subject_europepmc_categories.append(
            _str(meta.get("subject_europepmc_category"))
        )
        article_types.append(_str(meta.get("article_type")))
        languages.append(_str(meta.get("language")))
        languages_raw.append(_str(meta.get("language_raw")))
        xml_source_urls.append(_str(meta.get("xml_source_url")))
        xml_downloaded_ats.append(_str(meta.get("xml_downloaded_at")))
        xml_ftfy_applied_list.append(bool(meta.get("xml_ftfy_applied")))
        pdf_source_urls.append(_str(meta.get("pdf_source_url")))
        pdf_downloaded_ats.append(_str(meta.get("pdf_downloaded_at")))
        xmls.append(xml_path.read_text(encoding="utf-8"))
        pdfs.append(pdf_path.read_bytes())

    return pa.table(
        {
            "ppr_id": ppr_ids,
            "doi": dois,
            "version": versions,
            "title": titles,
            "authors": pa.array(authors_list, type=pa.list_(AUTHOR_TYPE)),
            "pub_date": pub_dates,
            "license": licenses,
            "keywords": keywords_list,
            "subject_heading": subject_headings,
            "subject_europepmc_category": subject_europepmc_categories,
            "article_type": article_types,
            "language": languages,
            "language_raw": languages_raw,
            "xml_source_url": xml_source_urls,
            "xml_downloaded_at": xml_downloaded_ats,
            "xml_ftfy_applied": xml_ftfy_applied_list,
            "pdf_source_url": pdf_source_urls,
            "pdf_downloaded_at": pdf_downloaded_ats,
            "xml": xmls,
            "pdf": pdfs,
        },
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

    split_rows = _read_csv(args.split_csv)
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
