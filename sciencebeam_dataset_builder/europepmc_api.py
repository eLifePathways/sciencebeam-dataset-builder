"""EuropePMC API client for fetching SciELO preprint metadata."""

import logging
from typing import Any, Iterator

import requests

LOGGER = logging.getLogger(__name__)

EUROPEPMC_API_BASE = "https://www.ebi.ac.uk/europepmc/webservices/rest"
SCIELO_PREPRINTS_QUERY = 'PUBLISHER:"SciELO Preprints"'


def count_scielo_preprints(extra_query: str = "") -> int:
    """Return the total number of SciELO preprints matching the query."""
    query = SCIELO_PREPRINTS_QUERY
    if extra_query:
        query = f"{query} AND ({extra_query})"
    params: dict[str, str | int] = {
        "query": query,
        "resultType": "idlist",
        "pageSize": 1,
        "format": "json",
    }
    response = requests.get(
        f"{EUROPEPMC_API_BASE}/search",
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    return int(response.json().get("hitCount", 0))


def get_pdf_url(article: dict[str, Any]) -> str | None:
    """Return the Europe PMC PDF download URL for an article, if available."""
    for entry in article.get("fullTextUrlList", {}).get("fullTextUrl", []):
        if entry.get("documentStyle") == "pdf" and entry.get("site") == "Europe_PMC":
            url = entry.get("url")
            return str(url) if url is not None else None
    return None


def iter_scielo_preprints(
    extra_query: str = "",
    page_size: int = 100,
) -> Iterator[dict[str, Any]]:
    """Iterate over SciELO preprints indexed in EuropePMC."""
    query = SCIELO_PREPRINTS_QUERY
    if extra_query:
        query = f"{query} AND ({extra_query})"

    cursor_mark = "*"

    while True:
        params: dict[str, str | int] = {
            "query": query,
            "resultType": "core",
            "pageSize": page_size,
            "cursorMark": cursor_mark,
            "format": "json",
        }

        LOGGER.debug("Fetching page (cursorMark=%r)", cursor_mark)
        response = requests.get(
            f"{EUROPEPMC_API_BASE}/search",
            params=params,
            timeout=30,
        )
        response.raise_for_status()

        data = response.json()
        results = data.get("resultList", {}).get("result", [])

        if not results:
            break

        yield from results

        next_cursor = data.get("nextCursorMark")
        if not next_cursor or next_cursor == cursor_mark:
            break
        cursor_mark = next_cursor
