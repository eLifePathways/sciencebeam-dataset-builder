"""A harvest repo on disk, laid out as `elifepathways/sciencebeam-v2-benchmarking` is."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from sciencebeam_dataset_builder.corpus.repo_reader import LocalRepo
from tests.corpus._repos import jats

BY = "https://creativecommons.org/licenses/by/4.0/"
BY_NC = "https://creativecommons.org/licenses/by-nc/4.0/"
COMMIT = "0b7d4a5c6e8f0a1b2c3d4e5f6a7b8c9d0e9f1c2e"


def harvest_file(ids: list[str], licence: str = BY) -> pa.Table:
    """The older schema's columns `build_table` reads, plus some it does not."""
    rows = len(ids)
    text: list[str | None] = [None] * rows
    return pa.table(
        {
            "source": ["biorxiv"] * rows,
            "id": ids,
            "doi": [f"10.1/{i}" for i in ids],
            "version": pa.array(text, pa.string()),
            "pub_date": ["2026-01-01"] * rows,
            "title": ["A title"] * rows,
            "xml_ftfy_applied": [False] * rows,
            "xml_source_url": [f"https://x/{i}.xml" for i in ids],
            "xml_downloaded_at": pa.array(text, pa.string()),
            "pdf_source_url": pa.array(text, pa.string()),
            "pdf_downloaded_at": pa.array(text, pa.string()),
            "xml": [jats(doi=f"10.1/{i}", licence=licence) for i in ids],
            "pdf": [f"pdf of {i}".encode() for i in ids],
        }
    )


def write_harvest(root: Path) -> LocalRepo:
    """biorxiv's n1 and n2 in train, n3 (non-commercial) in test; ore's o1 and its
    non-commercial o2."""
    for path, table in {
        "biorxiv-jats/train-00000-of-00001.parquet": harvest_file(["n1", "n2"]),
        "biorxiv-jats/test-00000-of-00001.parquet": harvest_file(["n3"], BY_NC),
        "ore-jats/train-00000-of-00001.parquet": harvest_file(["o1"]),
        "ore-jats/test-00000-of-00001.parquet": harvest_file(["o2"], BY_NC),
    }.items():
        (root / path).parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, root / path)
    return LocalRepo(root)
