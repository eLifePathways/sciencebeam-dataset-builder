"""Tests for corpus.lookup — finding documents across the corpus repos."""

from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pytest

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.ledger import Membership, write_membership
from sciencebeam_dataset_builder.corpus.lookup import (
    METADATA_COLUMNS,
    QueryError,
    Repo,
    Status,
    local_repos,
    lookup,
    parse_query,
)
from sciencebeam_dataset_builder.corpus.parquet_io import write_shard
from sciencebeam_dataset_builder.corpus.registry import CORPORA, Corpus, repos
from sciencebeam_dataset_builder.corpus.repo_reader import LocalRepo
from sciencebeam_dataset_builder.corpus.schema import CORPUS_SCHEMA

BIORXIV = CORPORA["biorxiv"]
ORE = CORPORA["ore"]


def _table(ids: list[str]) -> pa.Table:
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
            "xml": ["<article/>"] * rows,
            "xml_upstream": [None] * rows,
        },
        schema=CORPUS_SCHEMA,
    )


def _write_shard(root: Path, split: str, index: int, ids: list[str]) -> None:
    path = root / layout.shard_path("pdf-jats", split, index)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_shard(_table(ids), str(path))


def _publish(
    tmp_path: Path,
    corpus: Corpus,
    tier: Tier,
    by_split: dict[str, list[str]],
    release: str = "v1.0.0",
) -> Repo:
    root = tmp_path / layout.repo_name(corpus.name, tier)
    for split, ids in by_split.items():
        _write_shard(root, split, 0, ids)
    write_membership(
        root / layout.release_manifest_path(release),
        [Membership(corpus.name, i, s) for s, ids in by_split.items() for i in ids],
    )
    return Repo(corpus, tier, LocalRepo(root))


def _root(repo: Repo) -> Path:
    assert isinstance(repo.reader, LocalRepo)
    return repo.reader.root


@dataclass
class _CountingReader:
    inner: LocalRepo
    reads: list[tuple[str, tuple[str, ...]]] = field(default_factory=list)

    def files(self):
        return self.inner.files()

    def fetch(self, path):
        return self.inner.fetch(path)

    def read_columns(self, path, columns):
        self.reads.append((path, tuple(columns)))
        return self.inner.read_columns(path, columns)


class TestParseQuery:
    def test_a_bare_id_names_no_corpus(self):
        assert parse_query(" 10.1101_000000 ") == (None, "10.1101_000000")

    def test_a_uid_names_its_corpus_by_source(self):
        """The source is the frozen `scielo_br`, not the hyphenated corpus name."""
        assert parse_query("scielo_br__S0000-000") == (
            CORPORA["scielo-br"],
            "S0000-000",
        )

    def test_an_unknown_source_lists_the_known_ones(self):
        with pytest.raises(QueryError, match="known are"):
            parse_query("arxiv__1234")

    @pytest.mark.parametrize("query", ["", "  ", "biorxiv__", "__x"])
    def test_a_query_with_nothing_to_find_is_refused(self, query):
        with pytest.raises(QueryError):
            parse_query(query)


class TestLookup:
    def test_a_found_document_says_where_it_is(self, tmp_path):
        repo = _publish(tmp_path, BIORXIV, Tier.OPEN, {"train": ["a"], "test": ["b"]})
        [result] = lookup(["b"], [repo])
        assert result.status is Status.FOUND
        [where] = result.locations
        assert where.repo.repo_id == "elifepathways/sciencebeam-dataset-biorxiv"
        assert (where.release, where.split, where.file_type_pair) == (
            "v1.0.0",
            "test",
            "pdf-jats",
        )
        assert where.shard == "pdf-jats/test/test-00000.parquet"
        assert where.parquet_file_name == "test-00000.parquet"
        assert where.uid == "biorxiv__b"
        assert where.metadata["licence"] == "CC BY 4.0"
        assert where.metadata["doi"] == "10.1/b"

    def test_payload_is_never_read(self, tmp_path):
        repo = _publish(tmp_path, BIORXIV, Tier.OPEN, {"train": ["a"]})
        [where] = lookup(["a"], [repo])[0].locations
        assert not {"pdf", "xml", "xml_upstream", "id"} & set(where.metadata)

    def test_a_document_in_no_repo_is_missing(self, tmp_path):
        repo = _publish(tmp_path, BIORXIV, Tier.OPEN, {"train": ["a"]})
        [result] = lookup(["z"], [repo])
        assert result.status is Status.MISSING
        assert result.locations == ()

    def test_an_id_in_two_corpora_is_ambiguous_until_its_source_is_given(
        self, tmp_path
    ):
        biorxiv = _publish(tmp_path, BIORXIV, Tier.OPEN, {"train": ["a"]})
        ore = _publish(tmp_path, ORE, Tier.OPEN, {"test": ["a"]})
        bare, uid = lookup(["a", "ore__a"], [biorxiv, ore])
        assert bare.status is Status.AMBIGUOUS
        assert "<source>__a" in bare.detail
        assert uid.status is Status.FOUND
        assert uid.locations[0].split == "test"

    def test_a_document_in_both_halves_of_a_corpus_is_an_error(self, tmp_path):
        """The halves are disjoint; finding both is broken data, not a choice."""
        open_repo = _publish(tmp_path, BIORXIV, Tier.OPEN, {"train": ["a"]})
        restricted = _publish(tmp_path, BIORXIV, Tier.RESTRICTED, {"train": ["a"]})
        [result] = lookup(["a"], [open_repo, restricted])
        assert result.status is Status.ERROR
        assert "cannot be in two repos" in result.detail

    def test_only_the_latest_release_counts(self, tmp_path):
        repo = _publish(tmp_path, BIORXIV, Tier.OPEN, {"train": ["a", "b"]})
        write_membership(
            _root(repo) / layout.release_manifest_path("v1.10.0"),
            [Membership("biorxiv", "a", "train")],
        )
        found, removed = lookup(["a", "b"], [repo])
        assert found.locations[0].release == "v1.10.0"
        assert removed.status is Status.MISSING

    def test_a_repo_with_no_release_yet_holds_nothing(self, tmp_path):
        (tmp_path / "empty").mkdir()
        repo = Repo(BIORXIV, Tier.OPEN, LocalRepo(tmp_path / "empty"))
        assert lookup(["a"], [repo])[0].status is Status.MISSING

    def test_an_unreadable_repo_is_an_error_not_a_miss(self, tmp_path):
        """The document might be in the repo that could not be read."""
        broken = Repo(BIORXIV, Tier.OPEN, LocalRepo(tmp_path / "missing"))
        ore = _publish(tmp_path, ORE, Tier.OPEN, {"train": ["a"]})
        bare, elsewhere = lookup(["z", "ore__a"], [broken, ore])
        assert bare.status is Status.ERROR
        assert "sciencebeam-dataset-biorxiv" in bare.detail
        assert elsewhere.status is Status.FOUND

    def test_a_document_in_the_manifest_but_in_no_shard_is_an_error(self, tmp_path):
        repo = _publish(tmp_path, BIORXIV, Tier.OPEN, {"train": ["a"]})
        write_membership(
            _root(repo) / layout.release_manifest_path("v1.0.0"),
            [Membership("biorxiv", "a", "train"), Membership("biorxiv", "b", "test")],
        )
        [result] = lookup(["b"], [repo])
        assert result.status is Status.ERROR
        assert "in 0 test shards" in result.detail

    def test_a_document_in_the_wrong_split_directory_is_an_error(self, tmp_path):
        repo = _publish(tmp_path, BIORXIV, Tier.OPEN, {"train": ["a"]})
        _write_shard(_root(repo), "test", 0, ["b"])
        write_membership(
            _root(repo) / layout.release_manifest_path("v1.0.0"),
            [Membership("biorxiv", "a", "train"), Membership("biorxiv", "b", "train")],
        )
        assert lookup(["b"], [repo])[0].status is Status.ERROR

    def test_a_later_shard_of_the_split_is_searched_too(self, tmp_path):
        repo = _publish(tmp_path, BIORXIV, Tier.OPEN, {"train": ["a"]})
        _write_shard(_root(repo), "train", 1, ["b"])
        write_membership(
            _root(repo) / layout.release_manifest_path("v1.0.0"),
            [Membership("biorxiv", "a", "train"), Membership("biorxiv", "b", "train")],
        )
        [where] = lookup(["b"], [repo])[0].locations
        assert where.parquet_file_name == "train-00001.parquet"

    def test_each_shard_is_read_once_and_without_payload(self, tmp_path):
        published = _publish(tmp_path, BIORXIV, Tier.OPEN, {"train": ["a", "b"]})
        reader = _CountingReader(LocalRepo(_root(published)))
        repo = Repo(BIORXIV, Tier.OPEN, reader)
        lookup(["a", "b", "a"], [repo])
        assert reader.reads == [
            ("pdf-jats/train/train-00000.parquet", METADATA_COLUMNS)
        ]

    def test_results_keep_the_order_and_text_of_the_queries(self, tmp_path):
        repo = _publish(tmp_path, BIORXIV, Tier.OPEN, {"train": ["a"]})
        results = lookup([" a ", "", "z", "a"], [repo])
        assert [r.query for r in results] == [" a ", "", "z", "a"]
        assert [r.status for r in results] == [
            Status.FOUND,
            Status.ERROR,
            Status.MISSING,
            Status.FOUND,
        ]


class TestLocalRepos:
    def test_every_registry_repo_is_read_from_its_own_directory(self, tmp_path):
        found = local_repos(tmp_path)
        assert [(r.corpus, r.tier) for r in found] == repos()
        assert _root(found[0]) == tmp_path / layout.repo_name(
            found[0].corpus.name, found[0].tier
        )
