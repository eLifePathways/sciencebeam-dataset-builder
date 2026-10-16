"""Tests for corpus.repo_reader — reading a repo's files."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from huggingface_hub.errors import EntryNotFoundError

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.repo_reader import (
    HubRepo,
    LocalRepo,
    RepoReadError,
)
from tests.corpus._fake_hub import (
    FakeHub,
    FakePullRequest,
    additions_change,
    removals_change,
)

REPO_ID = "elifepathways/sciencebeam-dataset-biorxiv"


class TestLocalRepo:
    def test_files_are_repo_relative_and_sorted(self, tmp_path):
        (tmp_path / "releases").mkdir()
        (tmp_path / "releases" / "v1.0.0.csv").write_text("x", encoding="utf-8")
        (tmp_path / "README.md").write_text("x", encoding="utf-8")
        assert LocalRepo(tmp_path).files() == ["README.md", "releases/v1.0.0.csv"]

    def test_a_repo_that_is_not_there_is_an_error_not_an_empty_repo(self, tmp_path):
        with pytest.raises(RepoReadError, match="does not exist"):
            LocalRepo(tmp_path / "missing").files()

    def test_fetching_a_missing_file_is_an_error(self, tmp_path):
        with pytest.raises(RepoReadError, match="does not exist"):
            LocalRepo(tmp_path).fetch("README.md")

    def test_only_the_asked_columns_are_read(self, tmp_path):
        pq.write_table(pa.table({"id": ["a"], "pdf": [b"x"]}), tmp_path / "s.parquet")
        table = LocalRepo(tmp_path).read_columns("s.parquet", ["id"])
        assert table.column_names == ["id"]


class TestHubRepo:
    @pytest.fixture
    def hub(self, tmp_path, monkeypatch):
        (tmp_path / "README.md").write_text("card", encoding="utf-8")
        pq.write_table(pa.table({"id": ["a"], "pdf": [b"x"]}), tmp_path / "s.parquet")
        hub = FakeHub({REPO_ID: tmp_path})
        hub.install(monkeypatch)
        return hub

    def test_nothing_is_fetched_until_a_read(self, hub):
        HubRepo(REPO_ID)
        assert hub.calls == []

    def test_every_read_is_pinned_to_the_commit_main_named_first(self, hub):
        """A later commit to `main` must not change what one lookup sees."""
        hub.commits = ["c1", "c2"]
        repo = HubRepo(REPO_ID)
        repo.files()
        repo.fetch("README.md")
        repo.read_columns("s.parquet", ["id"])
        assert repo.commit == "c1"
        assert [call for call, _, _ in hub.calls].count("dataset_info") == 1
        assert {rev for call, _, rev in hub.calls if call != "dataset_info"} == {"c1"}

    def test_files_are_sorted(self, hub):
        assert HubRepo(REPO_ID).files() == ["README.md", "s.parquet"]

    def test_fetch_returns_a_local_copy(self, hub):
        local = HubRepo(REPO_ID).fetch("README.md")
        assert local.read_text(encoding="utf-8") == "card"

    def test_only_the_asked_columns_are_read(self, hub):
        table = HubRepo(REPO_ID).read_columns("s.parquet", ["id"])
        assert table.column_names == ["id"]
        assert ("open", REPO_ID, "c1") in hub.calls

    def test_a_hub_failure_is_a_repo_read_error(self, hub):
        hub.fail = EntryNotFoundError("no such file")
        with pytest.raises(RepoReadError, match="EntryNotFoundError: no such file"):
            HubRepo(REPO_ID).files()

    def test_a_revision_that_names_no_commit_is_an_error(self, hub):
        hub.commits = [None]
        with pytest.raises(RepoReadError, match="names no commit"):
            HubRepo(REPO_ID).files()

    def test_an_open_pull_request_says_what_it_changes_in_changes(self, hub):
        """Read from the PR's files against the commit it branched from, not a diff:
        the Hub does not always send one."""
        hub.pull_requests = [FakePullRequest(7, "Remove b", removals_change("b"))]
        [pr] = HubRepo(REPO_ID).open_pull_requests()
        assert (pr.num, pr.title) == (7, "Remove b")
        assert pr.url.endswith("/discussions/7")
        assert pr.files == (layout.REMOVALS_PATH,)
        assert pr.removal_ids == frozenset({"b"})

    def test_an_open_pull_request_says_which_documents_it_adds(self, hub):
        hub.pull_requests = [FakePullRequest(7, "Add n1", additions_change("n1"))]
        [pr] = HubRepo(REPO_ID).open_pull_requests()
        assert pr.files == (layout.ADDITIONS_PATH,)
        assert (pr.addition_ids, pr.removal_ids) == (frozenset({"n1"}), frozenset())

    def test_only_entries_the_pull_request_adds_count(self, hub, tmp_path):
        """Entries already on main are not the PR's own."""
        main_text = removals_change("a")[layout.REMOVALS_PATH]
        (tmp_path / layout.REMOVALS_PATH).parent.mkdir(parents=True)
        (tmp_path / layout.REMOVALS_PATH).write_bytes(main_text)
        both = (
            main_text
            + b"  - id: b\n    local_change_id: 1\n"
            + (b"    change_reason: proposed\n    changed_by: someone\n")
        )
        hub.pull_requests = [
            FakePullRequest(8, "Remove b", {layout.REMOVALS_PATH: both})
        ]
        [pr] = HubRepo(REPO_ID).open_pull_requests()
        assert pr.removal_ids == frozenset({"b"})

    def test_files_outside_changes_are_not_counted(self, hub):
        hub.pull_requests = [FakePullRequest(9, "Card", {"README.md": b"new card"})]
        [pr] = HubRepo(REPO_ID).open_pull_requests()
        assert pr.files == ()

    def test_an_unreadable_removals_file_is_marked_unknown(self, hub):
        """So a check that needs it fails closed."""
        hub.pull_requests = [
            FakePullRequest(9, "?", {layout.REMOVALS_PATH: b"removals: [\n"})
        ]
        [pr] = HubRepo(REPO_ID).open_pull_requests()
        assert pr.files == (layout.REMOVALS_PATH,)
        assert pr.removal_ids is None

    def test_a_hub_failure_reading_pull_requests_is_a_repo_read_error(self, hub):
        hub.pull_requests = [FakePullRequest(7, "Remove b", removals_change("b"))]
        hub.fail, hub.fail_on = RuntimeError("502"), "list_repo_tree"
        with pytest.raises(RepoReadError, match="502"):
            HubRepo(REPO_ID).open_pull_requests()
