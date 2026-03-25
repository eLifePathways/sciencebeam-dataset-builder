"""Extract metadata from downloaded SciELO preprint XML files into a JSONL file."""

import argparse
import json
import logging
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import ftfy

LOGGER = logging.getLogger(__name__)

XML_NAMESPACE = "http://www.w3.org/XML/1998/namespace"

# Non-standard language codes observed in SciELO records, mapped to ISO 639-1.
LANGUAGE_NORMALISATION: dict[str, str] = {
    "pt": "pt",
    "PT": "pt",
    "po": "pt",  # non-standard variant used in some SciELO records
    "es": "es",
    "sp": "es",  # non-standard variant used in some SciELO records
    "en": "en",
    "fr": "fr",
    "de": "de",
}


def _extract_language(root: ET.Element) -> tuple[str, str]:
    """Return (normalised_language, raw_language) from the article root element."""
    raw = root.get(f"{{{XML_NAMESPACE}}}lang") or root.get("xml:lang") or ""
    normalised = LANGUAGE_NORMALISATION.get(raw, raw.lower())
    return normalised, raw


def _extract_article_meta(root: ET.Element) -> dict[str, Any]:
    _meta = root.find("front/article-meta")
    meta = _meta if _meta is not None else ET.Element("article-meta")

    doi = ""
    for id_elem in meta.findall("article-id"):
        if id_elem.get("pub-id-type") == "doi":
            doi = (id_elem.text or "").strip()

    version = ""
    for v in meta.findall("article-version-alternatives/article-version"):
        if v.get("article-version-type") == "number":
            version = (v.text or "").strip()

    title = ftfy.fix_text((meta.findtext("title-group/article-title") or "").strip())

    author_names: list[str] = []
    for contrib in meta.findall("contrib-group/contrib"):
        name = contrib.find("name")
        if name is not None:
            given = ftfy.fix_text((name.findtext("given-names") or "").strip())
            surname = ftfy.fix_text((name.findtext("surname") or "").strip())
            full = f"{given} {surname}".strip()
            if full:
                author_names.append(full)

    pub_date = ""
    for pd in meta.findall("pub-date"):
        if pd.get("pub-type") == "preprint":
            year = pd.findtext("year") or ""
            month = pd.findtext("month") or ""
            day = pd.findtext("day") or ""
            parts = [p for p in (year, month.zfill(2), day.zfill(2)) if p]
            pub_date = "-".join(parts)
            break

    return {
        "doi": doi,
        "version": version,
        "title": title,
        "author_names": author_names,
        "pub_date": pub_date,
    }


def extract_metadata(xml_path: Path) -> dict[str, Any]:
    ppr_id = xml_path.stem
    tree = ET.parse(xml_path)
    root = tree.getroot()
    language, language_raw = _extract_language(root)
    article_type = root.get("article-type", "")
    article_meta = _extract_article_meta(root)
    return {
        "ppr_id": ppr_id,
        **article_meta,
        "article_type": article_type,
        "language": language,
        "language_raw": language_raw,
        "has_pdf": xml_path.with_suffix(".pdf").exists(),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract metadata from SciELO preprint XML files into a JSONL file."
    )
    parser.add_argument(
        "input_dir",
        type=Path,
        help="Directory containing PPR_*.xml files.",
    )
    parser.add_argument(
        "output_jsonl",
        type=Path,
        help="Path to write the metadata JSONL file.",
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

    xml_paths = sorted(args.input_dir.glob("PPR_*.xml"))
    if not xml_paths:
        print(f"No PPR_*.xml files found in {args.input_dir}", file=sys.stderr)
        sys.exit(1)

    args.output_jsonl.parent.mkdir(parents=True, exist_ok=True)

    written = 0
    with args.output_jsonl.open("w", encoding="utf-8") as f:
        for xml_path in xml_paths:
            try:
                record = extract_metadata(xml_path)
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                written += 1
            except ET.ParseError as exc:
                LOGGER.warning("Failed to parse %s: %s", xml_path.name, exc)

    print(f"Wrote metadata for {written} documents to {args.output_jsonl}")


if __name__ == "__main__":
    main()
