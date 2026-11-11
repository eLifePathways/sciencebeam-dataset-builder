"""Tests for corpus.card_cli — rewriting a repo's generated files without a release."""

import csv
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace

import pytest

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.card_cli import (
    CardError,
    attributions_summary,
    published,
    release_files,
    upload_files,
)
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from tests.corpus._repos import jats, publish, write_file

ORE = CORPORA["ore"]
BIORXIV = CORPORA["biorxiv"]
REPO_ID = ORE.repo_id(Tier.OPEN)


@dataclass
class FakeApi:
    root: Path
    tamper: str = ""
    sent: dict[str, bytes] = field(default_factory=dict)
    parent: str = ""

    def create_commit(
        self, repo_id, operations, commit_message, repo_type, parent_commit
    ):
        self.sent = {op.path_in_repo: op.path_or_fileobj for op in operations}
        self.parent = parent_commit
        return SimpleNamespace(oid="c2")

    def hf_hub_download(self, repo_id, filename, repo_type, revision, force_download):
        path = self.root / filename
        extra = b"!" if filename == self.tamper else b""
        path.write_bytes(self.sent[filename] + extra)
        return str(path)


def _csv(*rows: tuple[str, str]) -> str:
    lines = ["id,doi,title,authors,licence"]
    lines += [f"{i},10.1/{i},{title},,CC BY 4.0" for i, title in rows]
    return "\n".join(lines) + "\n"


@pytest.fixture
def repo(tmp_path):
    return publish(
        tmp_path,
        ORE,
        Tier.OPEN,
        {"train": ["a", "b"], "test": ["c"]},
        xml={"a": jats(title="A title")},
    )


class TestReleaseFiles:
    def test_the_card_counts_the_latest_release_by_split(self, repo):
        latest, files = release_files(repo)
        assert latest == "v1.0.0"
        assert "| `pdf-jats` | 2 | 0 | 1 | 3 |" in files[layout.CARD_PATH]

    def test_an_open_repo_also_gets_its_attributions_in_release_order(self, repo):
        _, files = release_files(repo)
        rows = list(csv.DictReader(files[layout.ATTRIBUTIONS_PATH].splitlines()))
        assert [(r["id"], r["title"]) for r in rows] == [
            ("a", "A title"),
            ("b", ""),
            ("c", ""),
        ]

    def test_a_restricted_repo_gets_only_its_card(self, tmp_path):
        restricted = publish(tmp_path, BIORXIV, Tier.RESTRICTED, {"train": ["a"]})
        _, files = release_files(restricted)
        assert list(files) == [layout.CARD_PATH]

    def test_a_missing_file_reads_as_empty(self, repo):
        assert published(repo, layout.ATTRIBUTIONS_PATH) == ""

    def test_a_published_file_is_read_as_it_is(self, repo):
        write_file(repo, layout.CARD_PATH, "old card")
        assert published(repo, layout.CARD_PATH) == "old card"


class TestSummary:
    def test_a_new_file_counts_its_documents(self):
        assert attributions_summary("", _csv(("a", "T"), ("b", "T"))) == (
            "new, 2 documents"
        )

    def test_rows_added_removed_and_changed_are_counted(self):
        old = _csv(("a", "T"), ("b", "T"), ("c", "T"))
        new = _csv(("a", "T"), ("b", "Fixed"), ("d", "T"))
        assert attributions_summary(old, new) == "1 added, 1 removed, 1 changed"


class TestUpload:
    def test_it_commits_onto_the_commit_it_read_and_reads_each_back(self, tmp_path):
        api = FakeApi(tmp_path)
        files = {layout.CARD_PATH: "new card", layout.ATTRIBUTIONS_PATH: "id\n"}
        assert upload_files(api, REPO_ID, files, "c1") == "c2"
        assert api.sent == {"README.md": b"new card", "attributions.csv": b"id\n"}
        assert api.parent == "c1"

    def test_a_file_that_reads_back_different_is_refused(self, tmp_path):
        api = FakeApi(tmp_path, tamper=layout.ATTRIBUTIONS_PATH)
        files = {layout.CARD_PATH: "new card", layout.ATTRIBUTIONS_PATH: "id\n"}
        with pytest.raises(CardError, match="attributions.csv read back"):
            upload_files(api, REPO_ID, files, "c1")
