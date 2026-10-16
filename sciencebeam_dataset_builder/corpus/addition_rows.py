"""Reading the documents an addition's `from` names: the harvest, or another corpus repo.

The harvest repo keeps the older schema, one file per corpus and split; a corpus repo keeps
its shards. Either way a document's split is the one it has there, and its licence is
what its own XML states, extracted as the licence audit does.
"""

from collections.abc import Iterator, Sequence
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.build_cli import build_table
from sciencebeam_dataset_builder.corpus.registry import Corpus
from sciencebeam_dataset_builder.corpus.repo_reader import RepoReader
from sciencebeam_dataset_builder.corpus.schema import CORPUS_SCHEMA
from sciencebeam_dataset_builder.dataset.license.extract_license import extract_licence
from sciencebeam_dataset_builder.dataset.split import SPLIT_NAMES

# Where new documents are harvested to. A dated snapshot under `data/` is a copy of it.
HARVEST_REPO_ID = "elifepathways/sciencebeam-v2-benchmarking"

# The older columns `build_table` maps onto the corpus schema.
_HARVEST_COLUMNS = (
    "id",
    "doi",
    "version",
    "pub_date",
    "xml_ftfy_applied",
    "xml_source_url",
    "xml_downloaded_at",
    "pdf_source_url",
    "pdf_downloaded_at",
    "xml",
    "pdf",
)


class AdditionRowsError(ValueError):
    """What an addition's `from` holds cannot be read as asked."""


@dataclass(frozen=True)
class Available:
    """A document `from` holds: its split there, and the licence its XML states."""

    id: str
    split: str
    licence: str


def available(
    corpus: Corpus, from_repo: str, reader: RepoReader
) -> dict[str, Available]:
    """Every document of `corpus` in `from_repo`, reading only `id` and `xml`."""
    found: dict[str, Available] = {}
    for split, path in _files(corpus, from_repo, reader):
        table = reader.read_columns(path, ["id", "xml"])
        for id_value, xml in zip(
            table.column("id").to_pylist(), table.column("xml").to_pylist(), strict=True
        ):
            if id_value in found:
                raise AdditionRowsError(f"{id_value} is in {from_repo} more than once")
            found[id_value] = Available(id_value, split, extract_licence(xml).label)
    return found


def read_rows(
    corpus: Corpus,
    from_repo: str,
    reader: RepoReader,
    ids: Sequence[str],
    row_updated_at: str,
) -> tuple[pa.Table, dict[str, str]]:
    """The corpus-schema rows of `ids`, in that order, and each one's split.

    A file's PDFs are read only when it holds one of `ids`.
    """
    wanted = set(ids)
    if len(wanted) != len(ids):
        raise AdditionRowsError("an id is asked for more than once")
    if not ids:
        return CORPUS_SCHEMA.empty_table(), {}
    harvest = from_repo == HARVEST_REPO_ID
    tables: list[pa.Table] = []
    splits: dict[str, str] = {}
    for split, path in _files(corpus, from_repo, reader):
        here = wanted.intersection(
            reader.read_columns(path, ["id"]).column("id").to_pylist()
        )
        if not here:
            continue
        columns = _HARVEST_COLUMNS if harvest else CORPUS_SCHEMA.names
        table = reader.read_columns(path, columns)
        table = table.filter(pc.is_in(table.column("id"), pa.array(sorted(here))))
        tables.append(
            _from_harvest(table, row_updated_at)
            if harvest
            else _from_corpus_repo(table, row_updated_at)
        )
        for id_value in table.column("id").to_pylist():
            if id_value in splits:
                raise AdditionRowsError(f"{id_value} is in {from_repo} more than once")
            splits[id_value] = split
    if missing := [i for i in ids if i not in splits]:
        raise AdditionRowsError(f"not in {from_repo}: {missing[:5]}")
    rows = pa.concat_tables(tables)
    order = {id_value: n for n, id_value in enumerate(rows.column("id").to_pylist())}
    return rows.take(pa.array([order[i] for i in ids])), splits


def _files(
    corpus: Corpus, from_repo: str, reader: RepoReader
) -> Iterator[tuple[str, str]]:
    """(split, path) of each file holding `corpus`'s documents in `from_repo`."""
    files = reader.files()
    for split in SPLIT_NAMES:
        if from_repo == HARVEST_REPO_ID:
            prefix = f"{corpus.legacy_config}/{split}-"
            paths = [
                p for p in files if p.startswith(prefix) and p.endswith(".parquet")
            ]
        elif from_repo in (corpus.repo_id(tier) for tier in corpus.tiers):
            paths = layout.split_shards(files, corpus.pairings[0], split)
        else:
            raise AdditionRowsError(
                f"{from_repo} is neither {HARVEST_REPO_ID} nor a {corpus.name} repo"
            )
        for path in sorted(paths):
            yield split, path


def _from_harvest(table: pa.Table, row_updated_at: str) -> pa.Table:
    rows = [
        {**row, "_licence": extract_licence(row["xml"]).label}
        for row in table.to_pylist()
    ]
    return build_table(rows, row_updated_at)


def _from_corpus_repo(table: pa.Table, row_updated_at: str) -> pa.Table:
    """Copied as published, `xml_upstream_sha` included, with today's licence verdict."""
    licences = [extract_licence(xml).label for xml in table.column("xml").to_pylist()]
    for name, values in (
        ("licence", licences),
        ("row_updated_at", [row_updated_at] * table.num_rows),
    ):
        index = table.schema.get_field_index(name)
        field = table.schema.field(name)
        table = table.set_column(index, field, pa.array(values, type=field.type))
    return table.cast(CORPUS_SCHEMA)
