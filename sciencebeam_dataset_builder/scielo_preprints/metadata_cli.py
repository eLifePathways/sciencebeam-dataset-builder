"""Extract metadata from downloaded SciELO preprint XML files into a JSONL file."""

import argparse
import json
import logging
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

PROVENANCE_FIELDS = [
    "xml_source_url",
    "xml_downloaded_at",
    "xml_ftfy_applied",
    "pdf_source_url",
    "pdf_downloaded_at",
]

LOGGER = logging.getLogger(__name__)

XML_NAMESPACE = "http://www.w3.org/XML/1998/namespace"
ALI_NAMESPACE = "http://www.niso.org/schemas/ali/1.0/"

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


def _aff_text(aff: ET.Element) -> str:
    """Return plain-text content of an <aff> element, excluding any <label> child."""
    parts = []
    if aff.text:
        parts.append(aff.text)
    for child in aff:
        # Exclude the label's own text but keep its tail and all other children.
        if child.tag != "label" and child.text:
            parts.append(child.text)
        if child.tail:
            parts.append(child.tail)
    return "".join(parts).strip()


def _extract_aff_map(meta: ET.Element) -> dict[str, str]:
    """Return a mapping of aff id → plain-text affiliation string for top-level affs."""
    return {
        aff_id: _aff_text(aff)
        for aff in meta.findall("aff")
        if (aff_id := aff.get("id"))
    }


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

    title = (meta.findtext("title-group/article-title") or "").strip()

    aff_map = _extract_aff_map(meta)

    authors: list[dict[str, Any]] = []
    for contrib in meta.findall("contrib-group/contrib"):
        name_elem = contrib.find("name")
        if name_elem is None:
            continue
        given = (name_elem.findtext("given-names") or "").strip()
        surname = (name_elem.findtext("surname") or "").strip()
        full = f"{given} {surname}".strip()
        if not full:
            continue

        orcid = ""
        for cid in contrib.findall("contrib-id"):
            if cid.get("contrib-id-type") == "orcid":
                orcid = (cid.text or "").strip()
                break

        affiliations: list[str] = []
        # Pattern 1: xref links to a top-level <aff id="..."> element.
        for xref in contrib.findall("xref"):
            if xref.get("ref-type") == "aff":
                rid = xref.get("rid", "")
                if rid in aff_map:
                    affiliations.append(aff_map[rid])
        # Pattern 2: <aff> is a direct child of <contrib> (no xref needed).
        if not affiliations:
            for aff in contrib.findall("aff"):
                text = _aff_text(aff)
                if text:
                    affiliations.append(text)

        authors.append({"name": full, "orcid": orcid, "affiliations": affiliations})

    pub_date = ""
    for pd in meta.findall("pub-date"):
        if pd.get("pub-type") == "preprint":
            year = pd.findtext("year") or ""
            month = pd.findtext("month") or ""
            day = pd.findtext("day") or ""
            parts = [p for p in (year, month.zfill(2), day.zfill(2)) if p]
            pub_date = "-".join(parts)
            break

    # License: prefer the ALI license_ref URL, fall back to empty string.
    license_url = ""
    for lr in meta.findall(f"permissions/license/{{{ALI_NAMESPACE}}}license_ref"):
        license_url = (lr.text or "").strip()
        break

    # Keywords: collect from all kwd-group elements.
    keywords: list[str] = [
        kw.text.strip()
        for kg in meta.findall("kwd-group")
        for kw in kg.findall("kwd")
        if kw.text and kw.text.strip()
    ]

    # Article subject categories: one field per subj-group-type (hyphens → underscores).
    subjects: dict[str, str] = {}
    for sg in meta.findall("article-categories/subj-group"):
        sg_type = sg.get("subj-group-type", "").replace("-", "_")
        subject_text = (sg.findtext("subject") or "").strip()
        if sg_type and subject_text:
            subjects[f"subject_{sg_type}"] = subject_text

    return {
        "doi": doi,
        "version": version,
        "title": title,
        "authors": authors,
        "pub_date": pub_date,
        "license": license_url,
        "keywords": keywords,
        **subjects,
    }


def _load_provenance(xml_path: Path) -> dict[str, str]:
    provenance_path = xml_path.with_suffix(".provenance.json")
    if not provenance_path.exists():
        return {}
    return json.loads(provenance_path.read_text(encoding="utf-8"))  # type: ignore[no-any-return]


def extract_metadata(xml_path: Path) -> dict[str, Any]:
    ppr_id = xml_path.stem.replace("PPR_", "PPR", 1)
    tree = ET.parse(xml_path)
    root = tree.getroot()
    language, language_raw = _extract_language(root)
    article_type = root.get("article-type", "")
    article_meta = _extract_article_meta(root)
    provenance = _load_provenance(xml_path)
    return {
        "ppr_id": ppr_id,
        **article_meta,
        "article_type": article_type,
        "language": language,
        "language_raw": language_raw,
        **{field: provenance.get(field, None) for field in PROVENANCE_FIELDS},
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
