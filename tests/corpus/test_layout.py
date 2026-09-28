"""Tests for corpus.layout — what things are called and where they live."""

from fnmatch import fnmatch

import pytest

from sciencebeam_dataset_builder.corpus.layout import (
    LayoutError,
    Tier,
    aggregate_repo_id,
    config_name,
    correction_paths,
    corrected_ids,
    data_files,
    latest_release,
    next_shard_index,
    published_releases,
    release_manifest_path,
    repo_id,
    repo_name,
    shard_index,
    shard_path,
    split_glob,
    split_shards,
)
from sciencebeam_dataset_builder.dataset.split import SPLIT_NAMES


class TestRepoNaming:
    def test_a_repo_is_named_for_its_corpus_and_what_may_be_done_with_it(self):
        assert repo_name("biorxiv", Tier.OPEN) == "sciencebeam-corpus-biorxiv-open"
        assert (
            repo_name("scielo-br", Tier.RESTRICTED)
            == "sciencebeam-corpus-scielo-br-restricted"
        )

    def test_the_hub_id_carries_the_org(self):
        assert repo_id("ore", Tier.OPEN) == "elifepathways/sciencebeam-corpus-ore-open"
        assert aggregate_repo_id() == "elifepathways/sciencebeam-corpora-open"

    def test_only_the_open_tier_is_public(self):
        assert Tier.OPEN.public
        assert not Tier.RESTRICTED.public

    def test_a_corpus_name_that_cannot_be_a_path_component_is_refused(self):
        for bad in ["scielo/br", ".hidden", "", " biorxiv"]:
            with pytest.raises(LayoutError):
                repo_name(bad, Tier.OPEN)


class TestShardPaths:
    def test_the_split_names_both_the_directory_and_the_file(self):
        assert (
            shard_path("pdf-jats", "train", 0) == "pdf-jats/train/train-00000.parquet"
        )
        assert shard_path("pdf-jats", "test", 12) == "pdf-jats/test/test-00012.parquet"

    def test_every_shard_path_matches_its_own_config_glob(self):
        """The two are generated separately and a drift between them publishes unloadable
        data, so they are checked against each other rather than eyeballed."""
        for split in SPLIT_NAMES:
            for index in (0, 7, 99999):
                assert fnmatch(
                    shard_path("pdf-jats", split, index), split_glob("pdf-jats", split)
                )

    def test_a_shard_in_the_wrong_split_directory_does_not_read_as_a_shard(self):
        """Which is also why its config glob will not load it."""
        assert shard_index("pdf-jats/test/train-00003.parquet") is None
        assert shard_index("pdf-jats/test/test-00003.parquet") == 3

    def test_files_that_are_not_shards_are_ignored(self):
        for path in [
            "README.md",
            "releases/v1.0.0.csv",
            "pdf-jats/train/train-0.parquet",
            "pdf-jats/train/notes.txt",
            "train-00000.parquet",
        ]:
            assert shard_index(path) is None

    def test_an_unknown_split_or_a_negative_index_is_refused(self):
        with pytest.raises(LayoutError):
            shard_path("pdf-jats", "holdout", 0)
        with pytest.raises(LayoutError):
            shard_path("pdf-jats", "train", -1)


class TestShardNumbering:
    FILES = [
        "pdf-jats/train/train-00000.parquet",
        "pdf-jats/train/train-00002.parquet",
        "pdf-jats/test/test-00000.parquet",
        "README.md",
    ]

    def test_shards_are_listed_per_split_in_order(self):
        assert split_shards(self.FILES, "pdf-jats", "train") == [
            "pdf-jats/train/train-00000.parquet",
            "pdf-jats/train/train-00002.parquet",
        ]
        assert split_shards(self.FILES, "pdf-jats", "validation") == []

    def test_a_gap_left_by_a_removal_is_never_filled(self):
        """A number is never reused, so the identity of a published shard cannot be taken
        over by a later file."""
        assert next_shard_index(self.FILES, "pdf-jats", "train") == 3

    def test_the_first_shard_of_an_empty_split_is_zero(self):
        assert next_shard_index([], "pdf-jats", "train") == 0


class TestConfigs:
    def test_the_config_name_carries_the_corpus_and_the_pairing(self):
        assert config_name("biorxiv", "pdf-jats") == "biorxiv-pdf-jats"

    def test_data_files_covers_every_split_once(self):
        entries = data_files("pdf-jats")
        assert [e["split"] for e in entries] == list(SPLIT_NAMES)
        assert entries[0]["path"] == "pdf-jats/train/train-*.parquet"


class TestReleases:
    def test_a_release_manifest_is_named_for_its_version(self):
        assert release_manifest_path("v1.0.0") == "releases/v1.0.0.csv"

    def test_something_that_is_not_a_semver_name_is_refused(self):
        for bad in ["1.0.0", "v1.0", "v1.0.0-rc1", "latest"]:
            with pytest.raises(LayoutError):
                release_manifest_path(bad)

    def test_releases_are_ordered_numerically_rather_than_lexically(self):
        """v1.10.0 follows v1.9.0. Sorting the strings would put it before."""
        files = [
            "releases/v1.9.0.csv",
            "releases/v1.10.0.csv",
            "releases/v1.0.0.csv",
            "releases/document-changes.csv",
        ]
        assert published_releases(files) == ["v1.0.0", "v1.9.0", "v1.10.0"]
        assert latest_release(files) == "v1.10.0"

    def test_a_repo_with_no_releases_has_no_latest(self):
        assert latest_release(["README.md"]) is None


class TestCorrections:
    def test_a_correction_is_flat_by_id(self):
        paths = correction_paths("10.1101_2021.04.23.440814")
        assert paths.directory == "corrections/10.1101_2021.04.23.440814"
        assert paths.upstream == "corrections/10.1101_2021.04.23.440814/upstream.xml"
        assert paths.corrected == "corrections/10.1101_2021.04.23.440814/corrected.xml"
        assert paths.metadata == "corrections/10.1101_2021.04.23.440814/correction.yml"

    def test_corrected_documents_are_found_by_their_corrected_file(self):
        files = [
            "corrections/4-121_v2/upstream.xml",
            "corrections/4-121_v2/corrected.xml",
            "corrections/PPR459180/corrected.xml",
            "corrections/PPR459180/correction.yml",
            "pdf-jats/train/train-00000.parquet",
        ]
        assert corrected_ids(files) == ["4-121_v2", "PPR459180"]

    def test_an_id_that_cannot_be_a_path_component_is_refused(self):
        with pytest.raises(LayoutError):
            correction_paths("10.1101/2021.04.23.440814")
