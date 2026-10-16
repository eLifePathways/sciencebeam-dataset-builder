"""Tests for corpus.apply_changes — applying merged additions."""

from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.addition_rows import HARVEST_REPO_ID
from sciencebeam_dataset_builder.corpus.apply_changes import (
    MINOR,
    ApplyError,
    OpenRepo,
    apply,
    read_pending,
)
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.ledger import (
    ChangeType,
    read_ledger,
    read_membership,
)
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from sciencebeam_dataset_builder.corpus.repo_reader import LocalRepo, RepoReader
from tests.corpus._harvest import BY, COMMIT, write_harvest
from tests.corpus._repos import jats, publish, write_file

BIORXIV = CORPORA["biorxiv"]
WHEN = datetime(2026, 11, 5, 10, 22, 41, tzinfo=UTC)
STAMP = "2026-11-05T10:22:41Z"
RESTRICTED = BIORXIV.repo_id(Tier.RESTRICTED)


def _additions(*entries: tuple[str, int], from_repo: str = HARVEST_REPO_ID) -> str:
    return "additions:\n" + "".join(
        f"  - id: {i}\n    local_change_id: {n}\n    change_reason: added {i}\n"
        f"    changed_by: hazal\n    from: {from_repo}@{COMMIT}\n"
        for i, n in entries
    )


@pytest.fixture
def repo(tmp_path: Path) -> Repo:
    return publish(tmp_path / "hub", BIORXIV, Tier.OPEN, {"train": ["a", "b"]})


@pytest.fixture
def harvest(tmp_path: Path) -> LocalRepo:
    return write_harvest(tmp_path / "harvest")


@pytest.fixture
def losing(tmp_path: Path) -> dict[str, Repo]:
    """The restricted repo at the commit an addition names, and on `main` now."""
    xml = {"m": jats(licence=BY)}
    return {
        COMMIT: publish(
            tmp_path / "then", BIORXIV, Tier.RESTRICTED, {"train": ["m"]}, xml=xml
        ),
        "main": publish(tmp_path / "now", BIORXIV, Tier.RESTRICTED, {"train": ["x"]}),
    }


@pytest.fixture
def open_repo(harvest: LocalRepo, losing: dict[str, Repo]) -> OpenRepo:
    def opened(repo_id: str, revision: str) -> RepoReader:
        if repo_id == HARVEST_REPO_ID:
            return harvest
        assert repo_id == RESTRICTED
        return losing[revision].reader

    return opened


def _apply(repo, open_repo, tmp_path, **kwargs):
    return apply(
        repo, read_pending(repo), "v1.1.0", tmp_path / "out", WHEN, open_repo, **kwargs
    )


class TestPending:
    def test_an_addition_needs_a_minor_version(self, repo):
        write_file(repo, layout.ADDITIONS_PATH, _additions(("n1", 1)))
        pending = read_pending(repo)
        assert pending.change_types == [ChangeType.ADDED]
        assert pending.bump == MINOR

    def test_a_document_already_in_the_release_is_refused(self, repo):
        write_file(repo, layout.ADDITIONS_PATH, _additions(("a", 1)))
        with pytest.raises(ApplyError, match="already in the release"):
            read_pending(repo)


class TestHarvest:
    @pytest.fixture
    def release(self, repo, open_repo, tmp_path):
        write_file(repo, layout.ADDITIONS_PATH, _additions(("n2", 1), ("n1", 1)))
        return _apply(repo, open_repo, tmp_path)

    def test_they_go_in_a_new_shard_in_the_order_declared(self, release):
        path = layout.shard_path("pdf-jats", "train", 1)
        assert path in release.paths
        assert layout.shard_path("pdf-jats", "train", 0) not in release.paths
        rows = pq.read_table(release.root / path).to_pylist()
        assert [r["id"] for r in rows] == ["n2", "n1"]
        assert {(r["licence"], r["row_updated_at"]) for r in rows} == {
            ("CC BY 4.0", STAMP)
        }

    def test_the_manifest_appends_them_after_everything_published(self, release):
        manifest = release.root / layout.release_manifest_path("v1.1.0")
        assert [m.id for m in read_membership(manifest)] == ["a", "b", "n2", "n1"]

    def test_the_ledger_records_each_with_its_shard(self, release):
        changes = read_ledger(release.root / layout.LEDGER_PATH)
        assert [
            (c.id, c.change_type, c.local_change_id, c.parquet_file_name, c.split)
            for c in changes
        ] == [
            ("n2", ChangeType.ADDED, 1, "train-00001.parquet", "train"),
            ("n1", ChangeType.ADDED, 1, "train-00001.parquet", "train"),
        ]
        assert changes[0].change_reason == "added n2"

    def test_a_shard_size_limit_cuts_more_new_shards(self, repo, open_repo, tmp_path):
        write_file(repo, layout.ADDITIONS_PATH, _additions(("n1", 1), ("n2", 1)))
        release = _apply(repo, open_repo, tmp_path, max_shard_bytes=1)
        assert {layout.shard_path("pdf-jats", "train", i) for i in (1, 2)} <= set(
            release.paths
        )

    def test_a_licence_that_now_routes_elsewhere_is_refused(
        self, repo, open_repo, tmp_path
    ):
        """n3 is non-commercial, so it can never be in the open repo."""
        write_file(repo, layout.ADDITIONS_PATH, _additions(("n3", 1)))
        with pytest.raises(ApplyError, match="now puts it in the restricted repo"):
            _apply(repo, open_repo, tmp_path)


class TestMove:
    def test_it_waits_for_the_losing_repos_removal(
        self, repo, open_repo, losing, tmp_path
    ):
        losing["main"] = losing[COMMIT]
        write_file(
            repo, layout.ADDITIONS_PATH, _additions(("m", 1), from_repo=RESTRICTED)
        )
        with pytest.raises(ApplyError, match="release their removal there first"):
            _apply(repo, open_repo, tmp_path)

    def test_once_removed_there_the_row_is_copied_as_published(
        self, repo, open_repo, tmp_path
    ):
        write_file(
            repo, layout.ADDITIONS_PATH, _additions(("m", 1), from_repo=RESTRICTED)
        )
        release = _apply(repo, open_repo, tmp_path)
        [row] = pq.read_table(
            release.root / layout.shard_path("pdf-jats", "train", 1)
        ).to_pylist()
        assert (row["id"], row["xml_upstream_sha"]) == ("m", "0" * 64)
        assert row["licence"] == "CC BY 4.0"
