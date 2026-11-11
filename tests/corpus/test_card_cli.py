"""Tests for corpus.card_cli — rewriting a repo's card without a release."""

from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from sciencebeam_dataset_builder.corpus.card_cli import (
    CardError,
    published_card,
    release_card,
    upload_card,
)
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from tests.corpus._repos import publish, write_file

ORE = CORPORA["ore"]
REPO_ID = ORE.repo_id(Tier.OPEN)


@dataclass
class FakeApi:
    root: Path
    tamper: bool = False
    sent: dict[str, Any] = field(default_factory=dict)

    def create_commit(
        self, repo_id, operations, commit_message, repo_type, parent_commit
    ):
        [op] = operations
        self.sent = {
            "path": op.path_in_repo,
            "data": op.path_or_fileobj,
            "parent": parent_commit,
        }
        return SimpleNamespace(oid="c2")

    def hf_hub_download(self, repo_id, filename, repo_type, revision, force_download):
        path = self.root / "README.md"
        path.write_bytes(self.sent["data"] + (b"!" if self.tamper else b""))
        return str(path)


@pytest.fixture
def repo(tmp_path):
    return publish(tmp_path, ORE, Tier.OPEN, {"train": ["a", "b"], "test": ["c"]})


class TestReleaseCard:
    def test_it_counts_the_latest_release_by_split(self, repo):
        latest, card = release_card(repo)
        assert latest == "v1.0.0"
        assert "| `pdf-jats` | 2 | 0 | 1 | 3 |" in card

    def test_a_missing_card_reads_as_empty(self, repo):
        assert published_card(repo) == ""

    def test_the_published_card_is_read_as_it_is(self, repo):
        write_file(repo, "README.md", "old card")
        assert published_card(repo) == "old card"


class TestUpload:
    def test_it_commits_onto_the_commit_it_read_and_reads_it_back(self, tmp_path):
        api = FakeApi(tmp_path)
        assert upload_card(api, REPO_ID, "new card", "c1") == "c2"
        assert api.sent == {"path": "README.md", "data": b"new card", "parent": "c1"}

    def test_a_card_that_reads_back_different_is_refused(self, tmp_path):
        with pytest.raises(CardError, match="not the one sent"):
            upload_card(FakeApi(tmp_path, tamper=True), REPO_ID, "new card", "c1")
