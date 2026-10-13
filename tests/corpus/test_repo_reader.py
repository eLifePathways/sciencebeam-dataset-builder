"""Tests for corpus.repo_reader — reading a repo's files."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from huggingface_hub.errors import EntryNotFoundError

from sciencebeam_dataset_builder.corpus.repo_reader import (
    HubRepo,
    LocalRepo,
    PullRequest,
    RepoReadError,
    parse_diff,
)
from tests.corpus._fake_hub import FakeHub, FakePullRequest, removals_diff

REPO_ID = "elifepathways/sciencebeam-dataset-biorxiv"

DIFF = """\
diff --git a/changes/removals/removals.yml b/changes/removals/removals.yml
index 1111111..2222222 100644
--- a/changes/removals/removals.yml
+++ b/changes/removals/removals.yml
@@ -1,2 +1,4 @@
 removals:
   - id: a
+  - id: 'b'
+    local_change_id: 1
diff --git a/README.md b/README.md
new file mode 100644
--- /dev/null
+++ b/README.md
@@ -0,0 +1 @@
+card
diff --git a/old.txt b/old.txt
deleted file mode 100644
--- a/old.txt
+++ /dev/null
@@ -1 +0,0 @@
-gone
"""


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

    def test_open_pull_requests_say_what_they_add(self, hub):
        hub.pull_requests = [FakePullRequest(7, "Remove b", removals_diff("b"))]
        [pr] = HubRepo(REPO_ID).open_pull_requests()
        assert (pr.num, pr.title) == (7, "Remove b")
        assert pr.url.endswith("/discussions/7")
        assert pr.files == ("changes/removals/removals.yml",)
        assert "  - id: 'b'" in pr.added["changes/removals/removals.yml"]

    def test_a_pull_request_without_a_diff_cannot_be_checked(self, hub):
        hub.pull_requests = [FakePullRequest(7, "?", None)]
        with pytest.raises(RepoReadError, match="no diff"):
            HubRepo(REPO_ID).open_pull_requests()


class TestParseDiff:
    def test_each_file_gets_the_lines_it_gains(self):
        assert parse_diff(DIFF) == {
            "changes/removals/removals.yml": ("  - id: 'b'", "    local_change_id: 1"),
            "README.md": ("card",),
            "old.txt": (),
        }

    def test_file_headers_are_not_mistaken_for_added_lines(self):
        added = parse_diff(DIFF)["README.md"]
        assert not any(line.startswith("++") for line in added)

    def test_an_empty_diff_touches_nothing(self):
        assert parse_diff("") == {}

    def test_files_lists_every_path_touched(self):
        pr = PullRequest(1, "t", "u", parse_diff(DIFF))
        assert pr.files == ("changes/removals/removals.yml", "README.md", "old.txt")
