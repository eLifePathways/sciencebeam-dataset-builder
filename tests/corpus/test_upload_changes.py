"""Tests for corpus.upload_changes — uploading a checked release, or nothing."""

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.apply_changes import (
    AppliedRelease,
    apply,
    read_pending,
    record_path,
)
from sciencebeam_dataset_builder.corpus.change_cli import main
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from sciencebeam_dataset_builder.corpus.repo_reader import HubRepo
from sciencebeam_dataset_builder.corpus.upload_changes import (
    UploadError,
    upload_release,
)
from tests.corpus._fake_hub import FakeHub
from tests.corpus._repos import publish, root_of, write_file

BIORXIV = CORPORA["biorxiv"]
WHEN = datetime(2026, 11, 5, 10, 22, 41, tzinfo=UTC)
REMOVAL = (
    "removals:\n  - id: b\n    local_change_id: 1\n    change_reason: editorial\n"
    "    changed_by: hazal\n"
)


@pytest.fixture
def local(tmp_path):
    repo = publish(tmp_path / "hub", BIORXIV, Tier.OPEN, {"train": ["a", "b"]})
    write_file(repo, layout.REMOVALS_PATH, REMOVAL)
    return repo


@pytest.fixture
def hub(local, monkeypatch):
    hub = FakeHub({local.repo_id: root_of(local)})
    hub.install(monkeypatch)
    return hub


@pytest.fixture
def release(local: Repo, hub: FakeHub, tmp_path: Path) -> AppliedRelease:
    repo = Repo(BIORXIV, Tier.OPEN, HubRepo(local.repo_id))
    return apply(repo, read_pending(repo), "v2.0.0", tmp_path / "out", WHEN)


class TestTheRecord:
    def test_it_names_the_commit_and_every_file_by_sha256(self, release):
        record = json.loads(record_path(release.root).read_text(encoding="utf-8"))
        assert record["commit"] == "c1"
        assert record["base_release"] == "v1.0.0"
        assert record["changes"] == {"removed": 1}
        assert set(record["files"]) == set(release.paths)
        assert all(len(sha) == 64 for sha in record["files"].values())


class TestUpload:
    def test_one_commit_onto_the_checked_commit_then_a_tag(self, hub, release):
        uploaded = upload_release(release.root)
        [commit] = hub.landed
        assert commit["parent"] == "c1"
        assert set(commit["files"]) == set(release.paths)
        for path, content in commit["files"].items():
            assert content == (release.root / path).read_bytes()
        assert "1 removed" in commit["description"]
        assert hub.tags == {"v2.0.0": commit["oid"]}
        assert (uploaded.commit, uploaded.version) == (commit["oid"], "v2.0.0")

    def test_nothing_is_uploaded_if_main_moved(self, hub, release):
        hub.commits = ["c2"]
        with pytest.raises(UploadError, match="main has moved"):
            upload_release(release.root)
        assert hub.landed == [] and hub.tags == {}

    def test_nothing_is_uploaded_if_a_file_changed_after_it_was_written(
        self, hub, release
    ):
        (release.root / layout.CARD_PATH).write_text("edited", encoding="utf-8")
        with pytest.raises(UploadError, match="README.md changed"):
            upload_release(release.root)
        assert hub.landed == []

    def test_a_version_already_tagged_is_refused(self, hub, release):
        hub.tags["v2.0.0"] = "old"
        with pytest.raises(UploadError, match="already a tag"):
            upload_release(release.root)
        assert hub.landed == []

    def test_a_transfer_that_fails_is_tried_again(self, hub, release):
        hub.flaky = 2
        upload_release(release.root)
        assert len(hub.landed) == 1
        assert "v2.0.0" in hub.tags

    def test_a_commit_that_landed_unseen_is_not_made_twice(self, hub, release):
        """The reply was lost, but main now holds exactly the written files."""
        hub.lost_reply = True
        uploaded = upload_release(release.root)
        assert len(hub.landed) == 1
        assert uploaded.commit == hub.landed[0]["oid"]
        assert hub.tags == {"v2.0.0": uploaded.commit}

    def test_files_that_do_not_read_back_are_not_tagged(self, hub, release):
        hub.tamper = True
        with pytest.raises(UploadError, match="not tagged"):
            upload_release(release.root)
        assert hub.tags == {}

    def test_a_release_made_from_a_local_directory_cannot_be_uploaded(
        self, local, tmp_path
    ):
        written = apply(local, read_pending(local), "v2.0.0", tmp_path / "out", WHEN)
        with pytest.raises(UploadError, match="local directory"):
            upload_release(written.root)

    def test_nothing_written_is_nothing_to_upload(self, tmp_path):
        with pytest.raises(UploadError, match="write the release first"):
            upload_release(tmp_path / "out" / "sciencebeam-dataset-biorxiv" / "v2.0.0")


class TestCli:
    def _cli(self, tmp_path: Path, *extra: str) -> None:
        main(
            [
                "--corpus",
                "biorxiv",
                "--tier",
                "open",
                "--output-dir",
                str(tmp_path / "out"),
                *extra,
            ]
        )

    def test_write_then_upload(self, hub, tmp_path, capsys):
        self._cli(tmp_path, "--version", "v2.0.0")
        assert hub.landed == []
        self._cli(tmp_path, "--version", "v2.0.0", "--upload")
        assert "verified, and tagged v2.0.0" in capsys.readouterr().out
        assert "v2.0.0" in hub.tags

    def test_upload_needs_a_version(self, tmp_path):
        with pytest.raises(SystemExit):
            self._cli(tmp_path, "--upload")

    def test_a_refused_upload_says_why(self, hub, tmp_path):
        self._cli(tmp_path, "--version", "v2.0.0")
        hub.commits = ["c2"]
        with pytest.raises(SystemExit, match="Not uploaded: main has moved"):
            self._cli(tmp_path, "--version", "v2.0.0", "--upload")
