"""EuropePMC FTP batch file client for downloading preprint JATS XML."""

import gzip
import logging
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import IO, Iterator, cast

import requests

LOGGER = logging.getLogger(__name__)

PREPRINT_FULLTEXT_BASE = "https://europepmc.org/ftp/preprint_fulltext"

MAX_RETRIES = 3
_RETRY_DELAYS = (5, 30, 120)  # seconds between successive retries


@dataclass(frozen=True)
class BatchFile:
    url: str
    start_id: int
    end_id: int


@dataclass(frozen=True)
class ArticleResult:
    ppr_id: int
    xml: str
    batch_url: str
    downloaded_at: datetime


def get_batch_files() -> list[BatchFile]:
    """Fetch the FTP index and return metadata for all batch XML files."""
    response = requests.get(f"{PREPRINT_FULLTEXT_BASE}/", timeout=30)
    response.raise_for_status()

    batch_files = []
    for match in re.finditer(r"PPR(\d+)_PPR(\d+)\.xml\.gz", response.text):
        start_id = int(match.group(1))
        end_id = int(match.group(2))
        filename = match.group(0)
        batch_files.append(
            BatchFile(
                url=f"{PREPRINT_FULLTEXT_BASE}/{filename}",
                start_id=start_id,
                end_id=end_id,
            )
        )

    return sorted(batch_files, key=lambda b: b.start_id)


def _iter_articles_from_stream(
    f: IO[bytes], target_ids: set[int]
) -> Iterator[tuple[int, str]]:
    """Parse a gzip-decompressed XML stream, yielding (ppr_id, xml_str) for matching IDs.

    The stream format is:
        <articles><article ...>...</article><article ...>...</article>...</articles>
    """
    context = ET.iterparse(f, events=("start", "end"))

    # Grab the root <articles> element so we can remove children as we go.
    _, root = next(context)

    remaining = set(target_ids)

    for event, elem in context:
        if event != "end" or elem.tag != "article":
            continue

        ppr_id: int | None = None
        for id_elem in elem.findall(".//article-id"):
            if id_elem.get("pub-id-type") == "archive":
                text = id_elem.text or ""
                if text.startswith("PPR"):
                    try:
                        ppr_id = int(text[3:])
                    except ValueError:
                        pass
                break

        if ppr_id in remaining:
            xml_str = '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(
                elem, encoding="unicode"
            )
            remaining.discard(ppr_id)
            yield ppr_id, xml_str
            if not remaining:
                break  # found everything we need in this batch file

        root.remove(elem)  # free memory as we go


def iter_articles_for_ids(
    batch_files: list[BatchFile],
    target_ids: set[int],
) -> Iterator[ArticleResult]:
    """Stream relevant batch files and yield an ArticleResult for each target ID found.

    Retries each batch file up to MAX_RETRIES times on network errors, resuming
    from where it left off (already-yielded IDs are not re-fetched).
    """
    for batch_file in batch_files:
        ids_in_batch = {
            id_ for id_ in target_ids if batch_file.start_id <= id_ <= batch_file.end_id
        }
        if not ids_in_batch:
            continue

        filename = batch_file.url.rsplit("/", 1)[-1]
        LOGGER.info(
            "Streaming %s  (%d target IDs in range %d–%d)",
            filename,
            len(ids_in_batch),
            batch_file.start_id,
            batch_file.end_id,
        )

        remaining = set(ids_in_batch)
        for attempt in range(MAX_RETRIES + 1):
            try:
                response = requests.get(batch_file.url, stream=True, timeout=600)
                response.raise_for_status()
                response.raw.decode_content = True

                with gzip.open(response.raw, "rb") as gz_f:
                    for ppr_id, xml_str in _iter_articles_from_stream(
                        cast(IO[bytes], gz_f), remaining
                    ):
                        remaining.discard(ppr_id)
                        yield ArticleResult(
                            ppr_id=ppr_id,
                            xml=xml_str,
                            batch_url=batch_file.url,
                            downloaded_at=datetime.now(timezone.utc),
                        )
                break  # batch completed successfully
            except Exception as exc:
                if attempt == MAX_RETRIES:
                    LOGGER.error(
                        "Failed to stream %s after %d attempts: %s",
                        filename,
                        MAX_RETRIES + 1,
                        exc,
                    )
                    raise
                delay = _RETRY_DELAYS[min(attempt, len(_RETRY_DELAYS) - 1)]
                LOGGER.warning(
                    "Retrying %s in %ds (attempt %d/%d, %d IDs remaining)",
                    filename,
                    delay,
                    attempt + 1,
                    MAX_RETRIES,
                    len(remaining),
                )
                time.sleep(delay)
