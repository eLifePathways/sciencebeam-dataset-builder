"""Tests for corpus.addition_rows — reading the documents an addition's `from` names."""

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pytest

from sciencebeam_dataset_builder.corpus.addition_rows import (
    HARVEST_REPO_ID,
    AdditionRowsError,
    Available,
    available,
    read_rows,
)
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from sciencebeam_dataset_builder.corpus.repo_reader import LocalRepo
from sciencebeam_dataset_builder.corpus.schema import CORPUS_SCHEMA
from tests.corpus._harvest import BY_NC, write_harvest
from tests.corpus._repos import jats, publish

BIORXIV = CORPORA["biorxiv"]
STAMP = "2026-10-16T09:00:00Z"


@pytest.fixture
def harvest(tmp_path: Path) -> LocalRepo:
    return write_harvest(tmp_path / "harvest")


@dataclass
class _Recording:
    """A reader that notes which files had their PDFs read."""

    inner: LocalRepo
    pdf_read: list[str] = field(default_factory=list)

    def files(self) -> list[str]:
        return self.inner.files()

    def fetch(self, path: str) -> Path:
        return self.inner.fetch(path)

    def read_columns(self, path: str, columns: Sequence[str]) -> pa.Table:
        if "pdf" in columns:
            self.pdf_read.append(path)
        return self.inner.read_columns(path, columns)

    def open_pull_requests(self) -> list:
        return []


class TestAvailable:
    def test_the_harvest_gives_each_documents_split_and_licence(self, harvest):
        assert available(BIORXIV, HARVEST_REPO_ID, harvest) == {
            "n1": Available("n1", "train", "CC BY 4.0"),
            "n2": Available("n2", "train", "CC BY 4.0"),
            "n3": Available("n3", "test", "CC BY-NC 4.0"),
        }

    def test_only_the_corpus_asked_for_is_listed(self, harvest):
        assert list(available(CORPORA["ore"], HARVEST_REPO_ID, harvest)) == ["o1", "o2"]

    def test_a_corpus_repo_gives_its_published_documents(self, tmp_path):
        repo = publish(
            tmp_path, BIORXIV, Tier.RESTRICTED, {"train": ["b"]}, xml={"b": jats()}
        )
        found = available(BIORXIV, repo.repo_id, repo.reader)
        assert found == {"b": Available("b", "train", "CC BY 4.0")}

    def test_another_repo_is_refused(self, harvest):
        with pytest.raises(AdditionRowsError, match="neither"):
            available(BIORXIV, "elifepathways/sciencebeam-dataset-ore", harvest)


class TestReadRows:
    def test_harvest_rows_are_mapped_onto_the_corpus_schema(self, harvest):
        table, splits = read_rows(
            BIORXIV, HARVEST_REPO_ID, harvest, ["n3", "n1"], STAMP
        )
        assert table.schema.equals(CORPUS_SCHEMA)
        assert table.column("id").to_pylist() == ["n3", "n1"]
        assert splits == {"n3": "test", "n1": "train"}
        row = table.to_pylist()[1]
        assert row["licence"] == "CC BY 4.0"
        assert row["pdf"] == b"pdf of n1"
        assert row["xml_upstream"] is None
        assert (
            row["xml_upstream_sha"] == hashlib.sha256(row["xml"].encode()).hexdigest()
        )
        assert row["row_updated_at"] == STAMP

    def test_a_row_from_a_corpus_repo_is_copied_with_a_fresh_licence(self, tmp_path):
        repo = publish(
            tmp_path,
            BIORXIV,
            Tier.RESTRICTED,
            {"train": ["b"]},
            xml={"b": jats(licence=BY_NC)},
        )
        table, splits = read_rows(BIORXIV, repo.repo_id, repo.reader, ["b"], STAMP)
        [row] = table.to_pylist()
        assert splits == {"b": "train"}
        assert row["xml_upstream_sha"] == "0" * 64
        assert row["licence"] == "CC BY-NC 4.0"
        assert row["row_updated_at"] == STAMP

    def test_only_files_holding_an_asked_id_have_their_pdfs_read(self, harvest):
        reader = _Recording(harvest)
        read_rows(BIORXIV, HARVEST_REPO_ID, reader, ["n3"], STAMP)
        assert reader.pdf_read == ["biorxiv-jats/test-00000-of-00001.parquet"]

    def test_an_id_not_there_is_refused(self, harvest):
        with pytest.raises(AdditionRowsError, match=r"not in .*\['n9'\]"):
            read_rows(BIORXIV, HARVEST_REPO_ID, harvest, ["n1", "n9"], STAMP)

    def test_an_id_asked_twice_is_refused(self, harvest):
        with pytest.raises(AdditionRowsError, match="more than once"):
            read_rows(BIORXIV, HARVEST_REPO_ID, harvest, ["n1", "n1"], STAMP)
