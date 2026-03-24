"""Extract metadata from downloaded SciELO preprint XML files into a CSV."""

import argparse
import csv
import logging
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

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

METADATA_FIELDS = ["ppr_id", "language", "language_raw", "has_pdf"]


def _extract_language(root: ET.Element) -> tuple[str, str]:
    """Return (normalised_language, raw_language) from the article root element."""
    raw = root.get(f"{{{XML_NAMESPACE}}}lang") or root.get("xml:lang") or ""
    normalised = LANGUAGE_NORMALISATION.get(raw, raw.lower())
    return normalised, raw


def extract_metadata(xml_path: Path) -> dict[str, Any]:
    ppr_id = xml_path.stem
    tree = ET.parse(xml_path)
    root = tree.getroot()
    language, language_raw = _extract_language(root)
    return {
        "ppr_id": ppr_id,
        "language": language,
        "language_raw": language_raw,
        "has_pdf": xml_path.with_suffix(".pdf").exists(),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract metadata from SciELO preprint XML files into a CSV."
    )
    parser.add_argument(
        "input_dir",
        type=Path,
        help="Directory containing PPR_*.xml files.",
    )
    parser.add_argument(
        "output_csv",
        type=Path,
        help="Path to write the metadata CSV.",
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

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)

    with args.output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=METADATA_FIELDS)
        writer.writeheader()
        for xml_path in xml_paths:
            try:
                row = extract_metadata(xml_path)
                writer.writerow(row)
            except ET.ParseError as exc:
                LOGGER.warning("Failed to parse %s: %s", xml_path.name, exc)

    print(f"Wrote metadata for {len(xml_paths)} documents to {args.output_csv}")


if __name__ == "__main__":
    main()
