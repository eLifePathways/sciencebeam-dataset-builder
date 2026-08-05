"""Tests for archive_cut.layout — how versions are named and found."""

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
    def test_a_split_is_partitioned_by_stratum(self):
        assert split_partition_path_in_repo("test", "pbio") == "test/pbio.parquet"

    def test_a_stratum_that_cannot_name_a_file_is_rejected(self):
        import pytest

        with pytest.raises(ValueError):
            split_partition_path_in_repo("test", "a/b")

    def test_a_splits_published_files_are_found_across_partitions(self):
        files = [
            "test/pbio.parquet",
            "test/pcbi.parquet",
            "validation/pbio.parquet",
            "splits/sample-v001.csv",
            "README.md",
        ]
        assert published_split_paths(files, "test") == [
            "test/pbio.parquet",
            "test/pcbi.parquet",
        ]
        assert published_split_paths(files, "validation") == ["validation/pbio.parquet"]

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
        files = ["splits/plos-rsrch-2064-v002.yml"]
        assert latest_published_version(files, "plos-rsrch-2064") == 2
        assert latest_published_version(files, "plos") is None

    def test_nothing_published_reports_none(self):
        assert latest_published_version(["test.parquet"], "sample") is None
        assert published_versions([], "sample") == []

    def test_files_outside_splits_are_ignored(self):
        assert published_versions(["sample-v001.yml"], "sample") == []

    def test_the_manifest_alone_does_not_count_as_a_version(self):
        """The config is what a later run must read, so it is what marks a version."""
        assert published_versions(["splits/sample-v001.csv"], "sample") == []
