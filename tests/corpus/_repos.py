"""Small published corpus repos on disk, for tests that read them."""

from collections.abc import Mapping
from pathlib import Path

import pyarrow as pa

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.ledger import Membership, write_membership
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.parquet_io import write_shard
from sciencebeam_dataset_builder.corpus.registry import Corpus
from sciencebeam_dataset_builder.corpus.repo_reader import LocalRepo
from sciencebeam_dataset_builder.corpus.schema import CORPUS_SCHEMA


def jats(
    doi: str = "10.1/a",
    title: str = "A title",
    licence: str = "https://creativecommons.org/licenses/by/4.0/",
) -> str:
    """Just enough JATS for the DOI, licence and root checks to read."""
    return (
        '<article xmlns:xlink="http://www.w3.org/1999/xlink"><front><article-meta>'
        f'<article-id pub-id-type="doi">{doi}</article-id>'
        f"<title-group><article-title>{title}</article-title></title-group>"
        f'<permissions><license xlink:href="{licence}"/></permissions>'
        "</article-meta></front></article>"
    )


def corpus_table(
    ids: list[str],
    xml: Mapping[str, str] | None = None,
    upstream: Mapping[str, str] | None = None,
) -> pa.Table:
    rows = len(ids)
    return pa.table(
        {
            "id": ids,
            "doi": [f"10.1/{i}" for i in ids],
            "version": [None] * rows,
            "pub_date": ["2026"] * rows,
            "licence": ["CC BY 4.0"] * rows,
            "xml_ftfy_applied": [False] * rows,
            "xml_upstream_sha": ["0" * 64] * rows,
            "xml_source_url": [None] * rows,
            "xml_downloaded_at": [None] * rows,
            "pdf_source_url": [None] * rows,
            "pdf_downloaded_at": [None] * rows,
            "row_updated_at": ["2026-10-01T00:00:00Z"] * rows,
            "pdf": [b"pdf"] * rows,
            "xml": [(xml or {}).get(i, "<article/>") for i in ids],
            "xml_upstream": [(upstream or {}).get(i) for i in ids],
        },
        schema=CORPUS_SCHEMA,
    )


def publish(
    tmp_path: Path,
    corpus: Corpus,
    tier: Tier,
    by_split: dict[str, list[str]],
    release: str = "v1.0.0",
    xml: Mapping[str, str] | None = None,
    upstream: Mapping[str, str] | None = None,
) -> Repo:
    """One shard per split and a release manifest, as `build_cli` writes them."""
    root = tmp_path / layout.repo_name(corpus.name, tier)
    for split, ids in by_split.items():
        path = root / layout.shard_path("pdf-jats", split, 0)
        path.parent.mkdir(parents=True, exist_ok=True)
        write_shard(corpus_table(ids, xml, upstream), str(path))
    write_membership(
        root / layout.release_manifest_path(release),
        [Membership(corpus.name, i, s) for s, ids in by_split.items() for i in ids],
    )
    return Repo(corpus, tier, LocalRepo(root))


def root_of(repo: Repo) -> Path:
    assert isinstance(repo.reader, LocalRepo)
    return repo.reader.root


def write_file(repo: Repo, path: str, text: str) -> None:
    local = root_of(repo) / path
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_text(text, encoding="utf-8")
