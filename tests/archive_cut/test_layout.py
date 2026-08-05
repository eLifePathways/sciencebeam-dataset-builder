"""Tests for archive_cut.layout — how versions are named and found."""

from pathlib import Path

import pytest

from sciencebeam_dataset_builder.archive_cut.layout import (
    config_path_in_repo,
    latest_published_version,
    manifest_path_in_repo,
    published_split_paths,
    published_versions,
    split_partition_path_in_repo,
    version_name,
    version_stem,
)

from tests.archive_cut._helpers import archive_config

SPLITS = ["test", "validation"]


class TestVersionNaming:
    def test_the_version_is_zero_padded_to_three_digits(self):
        assert version_stem("sample", 1) == "sample-v001"
        assert version_stem("sample", 12) == "sample-v012"
        assert version_stem("sample", 120) == "sample-v120"

    def test_past_999_the_number_is_still_read_correctly(self):
        """Padding is three digits, so filenames past 999 stop sorting lexically.

        Discovery parses the number rather than sorting strings, so it is unaffected;
        only a directory listing would look out of order.
        """
        assert version_stem("s", 1000) == "s-v1000"
        files = ["splits/s-v999.yml", "splits/s-v1000.yml"]
        assert latest_published_version(files, "s") == 1000

    def test_the_name_comes_from_the_config(self):
        config = archive_config(
            splits=SPLITS, default={"test": 1, "validation": 0}, name="other-corpus"
        )
        assert version_name(config) == "other-corpus-v001"


class TestRepoPaths:
    def test_a_split_is_hive_partitioned_and_version_stamped(self):
        assert (
            split_partition_path_in_repo("test", "journal", "alpha", 1, 0)
            == "test/journal=alpha/v001-00000.parquet"
        )

    def test_later_versions_add_files_beside_the_earlier_ones(self):
        """Append-only: a version's files are named for it, so none are overwritten."""
        first = split_partition_path_in_repo("test", "journal", "alpha", 1, 0)
        later = split_partition_path_in_repo("test", "journal", "alpha", 2, 0)
        assert first != later
        assert Path(first).parent == Path(later).parent

    def test_chunks_within_a_version_are_numbered(self):
        assert split_partition_path_in_repo("test", "journal", "beta", 1, 3).endswith(
            "v001-00003.parquet"
        )

    def test_the_partition_column_name_comes_from_the_config(self):
        assert (
            split_partition_path_in_repo("test", "language", "pt", 1, 0)
            == "test/language=pt/v001-00000.parquet"
        )

    def test_a_stratum_that_cannot_name_a_path_is_rejected(self):
        with pytest.raises(ValueError):
            split_partition_path_in_repo("test", "journal", "a/b", 1, 0)

    def test_a_splits_published_files_are_found_across_partitions(self):
        files = [
            "test/alpha.parquet",
            "test/beta.parquet",
            "validation/alpha.parquet",
            "splits/sample-v001.csv",
            "README.md",
        ]
        assert published_split_paths(files, "test") == [
            "test/alpha.parquet",
            "test/beta.parquet",
        ]
        assert published_split_paths(files, "validation") == [
            "validation/alpha.parquet"
        ]

    def test_a_split_with_nothing_published_finds_nothing(self):
        assert published_split_paths(["splits/sample-v001.csv"], "test") == []

    def test_the_manifest_and_config_sit_under_splits(self):
        assert manifest_path_in_repo("sample", 2) == "splits/sample-v002.csv"
        assert config_path_in_repo("sample", 2) == "splits/sample-v002.yml"


class TestPublishedVersions:
    def test_versions_are_found_from_the_config_files(self):
        files = [
            "test.parquet",
            "splits/sample-v001.yml",
            "splits/sample-v001.csv",
            "splits/sample-v003.yml",
            "splits/sample-v003.csv",
        ]
        assert published_versions(files, "sample") == [1, 3]
        assert latest_published_version(files, "sample") == 3

    def test_another_corpus_in_the_same_repo_is_not_counted(self):
        """A repo may hold several corpora; each has its own version sequence."""
        files = ["splits/sample-v001.yml", "splits/other-v009.yml"]
        assert published_versions(files, "sample") == [1]
        assert latest_published_version(files, "sample") == 1
        assert latest_published_version(files, "other") == 9

    def test_a_corpus_whose_name_contains_a_hyphen_is_matched_exactly(self):
        files = ["splits/alpha-beta-gamma-v002.yml"]
        assert latest_published_version(files, "alpha-beta-gamma") == 2
        # A prefix of the name is a different corpus, not this one.
        assert latest_published_version(files, "alpha") is None

    def test_nothing_published_reports_none(self):
        assert latest_published_version(["test.parquet"], "sample") is None
        assert published_versions([], "sample") == []

    def test_files_outside_splits_are_ignored(self):
        assert published_versions(["sample-v001.yml"], "sample") == []

    def test_the_manifest_alone_does_not_count_as_a_version(self):
        """The config is what a later run must read, so it is what marks a version."""
        assert published_versions(["splits/sample-v001.csv"], "sample") == []
