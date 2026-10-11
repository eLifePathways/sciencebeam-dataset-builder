"""Tests for corpus.repo_reader — reading a repo's files."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sciencebeam_dataset_builder.corpus.repo_reader import LocalRepo, RepoReadError


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
