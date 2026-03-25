"""CLI for downloading SciELO preprints with JATS XML from EuropePMC."""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import ftfy
import requests
from tqdm import tqdm

from sciencebeam_dataset_builder.scielo_preprints.europepmc_api import (
    count_scielo_preprints,
    get_pdf_url,
    iter_scielo_preprints,
)
from sciencebeam_dataset_builder.scielo_preprints.europepmc_ftp import (
    get_batch_files,
    iter_articles_for_ids,
)

LOGGER = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download SciELO preprints with JATS XML (and PDF) from EuropePMC."
        )
    )
    parser.add_argument(
        "output_dir",
        type=Path,
        help="Directory to save downloaded files.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Stop after downloading N articles.",
    )
    parser.add_argument(
        "--query",
        default="",
        metavar="QUERY",
        help="Additional EuropePMC query terms (ANDed with the SciELO + full-text filter).",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        metavar="N",
        help="Results per API page when collecting IDs (default: 100).",
    )
    parser.add_argument(
        "--no-skip-existing",
        dest="skip_existing",
        action="store_false",
        default=True,
        help="Re-download files that already exist in output_dir.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser.parse_args(argv)


def _load_provenance(path: Path) -> dict[str, str]:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))  # type: ignore[no-any-return]
    return {}


def _download_pdf(url: str, dest: Path) -> None:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    dest.write_bytes(response.content)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.WARNING,
        format="%(asctime)s %(levelname)-8s %(message)s",
        stream=sys.stderr,
    )

    output_dir: Path = args.output_dir / "scielo-preprints"
    output_dir.mkdir(parents=True, exist_ok=True)

    extra_query = "IN_EPMC:Y"
    if args.query:
        extra_query = f"{extra_query} AND ({args.query})"

    # Phase 1: collect SciELO PPR IDs (and PDF URLs) that still need downloading.
    total = count_scielo_preprints(extra_query)

    target_ids: set[int] = set()
    articles_meta: dict[int, dict[str, Any]] = {}

    with tqdm(total=total, desc="Collecting IDs", unit=" preprints") as pbar:
        for article in iter_scielo_preprints(
            extra_query=extra_query, page_size=args.page_size
        ):
            pbar.update(1)

            ppr_id_str = article.get("id", "")
            if not ppr_id_str.startswith("PPR"):
                continue
            try:
                ppr_id = int(ppr_id_str[3:])
            except ValueError:
                continue

            xml_path = output_dir / f"PPR_{ppr_id}.xml"
            pdf_url = get_pdf_url(article)
            pdf_path = output_dir / f"PPR_{ppr_id}.pdf" if pdf_url else None

            xml_needed = not xml_path.exists()
            pdf_needed = pdf_path is not None and not pdf_path.exists()

            if args.skip_existing and not xml_needed and not pdf_needed:
                continue

            target_ids.add(ppr_id)
            articles_meta[ppr_id] = {
                **article,
                "_xml_needed": xml_needed,
                "_pdf_needed": pdf_needed,
                "_pdf_url": pdf_url,
            }

    if not target_ids:
        tqdm.write("Nothing to download.")
        return

    tqdm.write("Fetching FTP batch file index...")
    batch_files = get_batch_files()

    # Phase 2: stream batch files and save XML + PDF for each article.
    downloaded = 0
    with tqdm(total=len(target_ids), desc="Downloading", unit=" articles") as pbar:
        for result in iter_articles_for_ids(batch_files, target_ids):
            if args.limit is not None and downloaded >= args.limit:
                tqdm.write(f"Reached download limit of {args.limit}.")
                break

            meta = articles_meta[result.ppr_id]
            title = (meta.get("title") or "")[:70]
            provenance_path = output_dir / f"PPR_{result.ppr_id}.provenance.json"
            provenance = _load_provenance(provenance_path)

            if meta["_xml_needed"]:
                (output_dir / f"PPR_{result.ppr_id}.xml.original").write_text(
                    result.xml, encoding="utf-8"
                )
                (output_dir / f"PPR_{result.ppr_id}.xml").write_text(
                    ftfy.fix_text(result.xml), encoding="utf-8"
                )
                provenance["xml_source_url"] = result.batch_url
                provenance["xml_downloaded_at"] = result.downloaded_at.isoformat()

            if meta["_pdf_needed"] and meta["_pdf_url"]:
                try:
                    _download_pdf(
                        meta["_pdf_url"], output_dir / f"PPR_{result.ppr_id}.pdf"
                    )
                    provenance["pdf_source_url"] = meta["_pdf_url"]
                    provenance["pdf_downloaded_at"] = datetime.now(
                        timezone.utc
                    ).isoformat()
                except requests.RequestException as exc:
                    tqdm.write(f"  PDF download failed for PPR_{result.ppr_id}: {exc}")

            if provenance:
                provenance_path.write_text(
                    json.dumps(provenance, indent=2), encoding="utf-8"
                )

            downloaded += 1
            pbar.update(1)
            pbar.set_postfix_str(title)

    tqdm.write(f"Finished. Downloaded: {downloaded}")


if __name__ == "__main__":
    main()
