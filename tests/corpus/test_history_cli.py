"""Tests for corpus.history_cli — removing an open repo's history before it goes public."""

from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace

import pytest

from sciencebeam_dataset_builder.corpus.history_cli import (
    HistoryError,
    parse_args,
    read_history,
    report,
    squash,
)
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from tests.corpus._fake_hub import removals_change
from tests.corpus._repos import publish, write_file

ORE = CORPORA["ore"]
BIORXIV = CORPORA["biorxiv"]
REPO_ID = ORE.repo_id(Tier.OPEN)
SHARD = "pdf-jats/train/train-00000.parquet"


@dataclass
class FakeApi:
    """`main` newest first; each commit's tree maps a path to its blob, which doubles as
    a shard's sha256. `stored` is every large file the Hub keeps."""

    main: list[str]
    trees: dict[str, dict[str, str]]
    tags: dict[str, str]
    stored: list[str]
    pull_requests: list[int] = field(default_factory=list)
    calls: list[str] = field(default_factory=list)

    def list_repo_refs(self, repo_id, repo_type, include_pull_requests=False):
        return SimpleNamespace(
            tags=[
                SimpleNamespace(name=t, target_commit="tag-object") for t in self.tags
            ],
            pull_requests=[
                SimpleNamespace(ref=f"refs/pr/{n}") for n in self.pull_requests
            ],
        )

    def list_repo_commits(self, repo_id, repo_type, revision="main"):
        start = self.main[0] if revision == "main" else self.tags[revision]
        commits = self.main[self.main.index(start) :] if start in self.main else [start]
        return [SimpleNamespace(commit_id=c) for c in commits]

    def list_repo_tree(self, repo_id, repo_type, revision, recursive):
        folders = [SimpleNamespace(path="pdf-jats", tree_id="t")]
        return folders + [
            SimpleNamespace(
                path=p,
                blob_id=b,
                lfs=SimpleNamespace(sha256=b) if p.endswith(".parquet") else None,
            )
            for p, b in self.trees[revision].items()
        ]

    def list_lfs_files(self, repo_id, repo_type):
        return [SimpleNamespace(file_oid=oid) for oid in self.stored]

    def super_squash_history(self, repo_id, repo_type, commit_message):
        self.calls.append("squash")
        self.trees["squashed"] = dict(self.trees[self.main[0]])
        self.main = ["squashed"]

    def delete_tag(self, repo_id, tag, repo_type):
        self.calls.append(f"delete {tag}")
        del self.tags[tag]

    def create_tag(self, repo_id, tag, revision, repo_type, tag_message):
        self.calls.append(f"tag {tag}")
        self.tags[tag] = revision


def _api(pull_requests: list[int] | None = None) -> FakeApi:
    """Release v1.0.0 and its README fix, after a v0 whose shard held a removed document."""
    return FakeApi(
        main=["c3", "c2", "c1"],
        trees={
            "c1": {SHARD: "old", "README.md": "r1"},
            "c2": {SHARD: "new", "README.md": "r1"},
            "c3": {SHARD: "new", "README.md": "r2"},
        },
        tags={"v0.1.0": "c1", "v1.0.0": "c2"},
        stored=["old", "new"],
        pull_requests=pull_requests or [],
    )


def _report(api: FakeApi) -> str:
    return "\n".join(report(REPO_ID, read_history(api, REPO_ID), "v1.0.0"))


@pytest.fixture
def repo(tmp_path: Path) -> Repo:
    return publish(tmp_path, ORE, Tier.OPEN, {"train": ["a", "b"]})


class TestHistory:
    def test_it_counts_the_stored_files_main_does_not_use(self):
        assert read_history(_api(), REPO_ID).unused_files == 1

    def test_tags_resolve_to_their_commits(self):
        assert read_history(_api(), REPO_ID).tags == {"v0.1.0": "c1", "v1.0.0": "c2"}

    def test_each_pull_request_ref_links_to_its_page(self):
        [url] = read_history(_api([2]), REPO_ID).pull_requests
        assert url == f"https://huggingface.co/datasets/{REPO_ID}/discussions/2"


class TestReport:
    def test_pull_request_refs_come_before_the_squash(self):
        text = _report(_api([2]))
        assert f"datasets/{REPO_ID}/discussions/2" in text
        assert 'Then pass RUN_ARGS="--version v1.0.0 --squash"' in text

    def test_older_files_in_the_history_ask_for_the_squash(self):
        assert 'Pass RUN_ARGS="--version v1.0.0 --squash"' in _report(_api())

    def test_after_the_squash_it_waits_for_the_hub_to_delete_them(self, repo):
        api = _api()
        squash(api, repo, "v1.0.0")
        text = _report(api)
        assert "up to 36 hours" in text
        assert "--squash" not in text

    def test_a_repo_with_nothing_older_says_so(self):
        api = _api()
        api.stored = ["new"]
        assert "nothing older to remove" in _report(api)


class TestSquash:
    def test_it_leaves_one_commit_with_the_release_tag_and_the_same_files(self, repo):
        api = _api()
        after = squash(api, repo, "v1.0.0")
        assert api.calls == ["squash", "delete v0.1.0", "delete v1.0.0", "tag v1.0.0"]
        assert (after.commits, after.tags) == (1, {"v1.0.0": "squashed"})
        assert api.trees["squashed"] == {SHARD: "new", "README.md": "r2"}

    def test_it_refuses_while_a_pull_request_ref_remains(self, repo):
        api = _api([2])
        with pytest.raises(HistoryError, match="discussions/2"):
            squash(api, repo, "v1.0.0")
        assert api.calls == []

    def test_it_refuses_a_version_that_is_not_the_latest_release(self, repo):
        api = _api()
        with pytest.raises(HistoryError, match="latest release is v1.0.0, not v2.0.0"):
            squash(api, repo, "v2.0.0")
        assert api.calls == []

    def test_it_refuses_while_changes_wait_to_be_released(self, repo):
        [(path, text)] = removals_change("a").items()
        write_file(repo, path, text.decode())
        api = _api()
        with pytest.raises(HistoryError, match="not yet released"):
            squash(api, repo, "v1.0.0")
        assert api.calls == []

    def test_it_refuses_a_restricted_repo(self, tmp_path):
        restricted = publish(tmp_path, BIORXIV, Tier.RESTRICTED, {"train": ["a"]})
        with pytest.raises(HistoryError, match="not an open repo"):
            squash(_api(), restricted, "v1.0.0")

    def test_files_that_changed_in_the_squash_are_reported(self, repo):
        api = _api()
        squash_history = api.super_squash_history

        def squash_and_lose_a_file(*args, **kwargs):
            squash_history(*args, **kwargs)
            del api.trees["squashed"]["README.md"]

        api.super_squash_history = squash_and_lose_a_file  # type: ignore[method-assign]
        with pytest.raises(HistoryError, match="the files at main changed"):
            squash(api, repo, "v1.0.0")

    def test_a_run_cut_short_can_be_run_again(self, repo):
        api = _api()
        api.super_squash_history(REPO_ID, repo_type="dataset", commit_message="")
        api.delete_tag(REPO_ID, tag="v0.1.0", repo_type="dataset")
        api.delete_tag(REPO_ID, tag="v1.0.0", repo_type="dataset")
        assert squash(api, repo, "v1.0.0").tags == {"v1.0.0": "squashed"}


class TestArgs:
    def test_squash_needs_the_version(self):
        with pytest.raises(SystemExit):
            parse_args(["--corpus", "ore", "--tier", "open", "--squash"])

    def test_a_version_alone_is_refused(self):
        with pytest.raises(SystemExit):
            parse_args(["--corpus", "ore", "--tier", "open", "--version", "v1.0.0"])
