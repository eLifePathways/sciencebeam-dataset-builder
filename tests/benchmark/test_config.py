"""Tests for benchmark.config — validation, round-tripping and the cheap count check."""

import pytest

from sciencebeam_dataset_builder.benchmark.config import (
    ConfigError,
    config_from_dict,
    config_to_dict,
    dump_config,
    find_lowered_counts,
    load_config,
)

from tests.benchmark._helpers import config

SPLITS = ["test", "validation"]


class TestValidation:
    def test_unknown_top_level_key_is_rejected(self):
        with pytest.raises(ConfigError) as exc_info:
            config_from_dict(
                {
                    "splits": SPLITS,
                    "columns": ["id"],
                    "source": {
                        "metadata_file": "m.jsonl",
                        "shard_manifest_file": "s.jsonl",
                    },
                    "allocation": {"default": {"test": 1, "validation": 0}},
                    "startum_column": "journal",
                }
            )
        assert "startum_column" in str(exc_info.value)

    def test_default_must_cover_every_split(self):
        with pytest.raises(ConfigError) as exc_info:
            config(splits=SPLITS, default={"test": 1})
        assert "validation" in str(exc_info.value)

    def test_a_count_for_an_unknown_split_is_rejected(self):
        with pytest.raises(ConfigError) as exc_info:
            config(splits=SPLITS, default={"test": 1, "validation": 0, "train": 2})
        assert "train" in str(exc_info.value)

    def test_an_override_for_an_unknown_split_is_rejected(self):
        with pytest.raises(ConfigError):
            config(
                splits=SPLITS,
                default={"test": 1, "validation": 0},
                overrides={"alpha": {"train": 1}},
            )

    def test_a_negative_count_is_rejected(self):
        with pytest.raises(ConfigError):
            config(splits=SPLITS, default={"test": -1, "validation": 0})

    def test_a_non_integer_count_is_rejected(self):
        with pytest.raises(ConfigError):
            config_from_dict(
                {
                    "splits": SPLITS,
                    "columns": ["id"],
                    "source": {
                        "metadata_file": "m.jsonl",
                        "shard_manifest_file": "s.jsonl",
                    },
                    "allocation": {"default": {"test": 1.5, "validation": 0}},
                }
            )

    def test_duplicate_splits_are_rejected(self):
        with pytest.raises(ConfigError):
            config(splits=["test", "test"], default={"test": 1})

    def test_empty_splits_are_rejected(self):
        with pytest.raises(ConfigError):
            config(splits=[], default={})

    def test_the_id_column_must_be_among_the_output_columns(self):
        with pytest.raises(ConfigError) as exc_info:
            config_from_dict(
                {
                    "splits": SPLITS,
                    "columns": ["stratum", "xml"],
                    "source": {
                        "metadata_file": "m.jsonl",
                        "shard_manifest_file": "s.jsonl",
                    },
                    "allocation": {"default": {"test": 1, "validation": 0}},
                }
            )
        assert "id" in str(exc_info.value)

    def test_the_source_files_are_required(self):
        with pytest.raises(ConfigError):
            config_from_dict(
                {
                    "splits": SPLITS,
                    "columns": ["id"],
                    "source": {"metadata_file": "m.jsonl"},
                    "allocation": {"default": {"test": 1, "validation": 0}},
                }
            )

    def test_duplicate_exclusions_are_rejected(self):
        with pytest.raises(ConfigError):
            config(
                splits=SPLITS,
                default={"test": 1, "validation": 0},
                exclude=["a", "a"],
            )


class TestOverrideMerging:
    def test_an_override_merges_per_split_rather_than_replacing(self):
        cfg = config(
            splits=SPLITS,
            default={"test": 25, "validation": 10},
            overrides={"gamma": {"test": 11}},
        )
        assert cfg.counts_for("gamma") == {"test": 11, "validation": 10}

    def test_a_stratum_without_an_override_uses_the_default(self):
        cfg = config(splits=SPLITS, default={"test": 25, "validation": 10})
        assert cfg.counts_for("alpha") == {"test": 25, "validation": 10}


class TestRoundTrip:
    def test_dump_then_load_preserves_the_config(self, tmp_path):
        cfg = config(
            splits=SPLITS,
            default={"test": 25, "validation": 10},
            overrides={"gamma": {"test": 11, "validation": 0}},
            exclude=["alpha-004"],
            version=3,
        )
        path = tmp_path / "benchmark-v003.yml"
        dump_config(cfg, path)
        assert load_config(path) == cfg

    def test_the_recorded_archive_revision_survives_a_round_trip(self, tmp_path):
        data = config_to_dict(
            config(splits=SPLITS, default={"test": 1, "validation": 0})
        )
        data["source"]["revision"] = "0123456789abcdef"
        path = tmp_path / "config.yml"
        dump_config(config_from_dict(data), path)
        assert load_config(path).source.revision == "0123456789abcdef"

    def test_an_empty_file_is_rejected(self, tmp_path):
        path = tmp_path / "config.yml"
        path.write_text("", encoding="utf-8")
        with pytest.raises(ConfigError):
            load_config(path)


class TestFindLoweredCounts:
    def test_an_unchanged_config_has_no_regression(self):
        cfg = config(splits=SPLITS, default={"test": 25, "validation": 10})
        assert find_lowered_counts(cfg, cfg) == []

    def test_a_raised_count_has_no_regression(self):
        previous = config(splits=SPLITS, default={"test": 25, "validation": 10})
        current = config(
            splits=SPLITS, default={"test": 50, "validation": 10}, version=2
        )
        assert find_lowered_counts(previous, current) == []

    def test_a_lowered_default_is_reported(self):
        previous = config(splits=SPLITS, default={"test": 25, "validation": 10})
        current = config(
            splits=SPLITS, default={"test": 20, "validation": 10}, version=2
        )
        regressions = find_lowered_counts(previous, current)
        assert [(r.stratum, r.split, r.previous, r.current) for r in regressions] == [
            (None, "test", 25, 20)
        ]

    def test_a_lowered_override_is_reported(self):
        previous = config(
            splits=SPLITS,
            default={"test": 25, "validation": 10},
            overrides={"gamma": {"test": 11}},
        )
        current = config(
            splits=SPLITS,
            default={"test": 25, "validation": 10},
            overrides={"gamma": {"test": 5}},
            version=2,
        )
        regressions = find_lowered_counts(previous, current)
        assert [(r.stratum, r.split, r.previous, r.current) for r in regressions] == [
            ("gamma", "test", 11, 5)
        ]

    def test_removing_an_override_that_raised_a_count_is_reported(self):
        previous = config(
            splits=SPLITS,
            default={"test": 25, "validation": 10},
            overrides={"alpha": {"test": 50}},
        )
        current = config(
            splits=SPLITS, default={"test": 25, "validation": 10}, version=2
        )
        regressions = find_lowered_counts(previous, current)
        assert [(r.stratum, r.split) for r in regressions] == [("alpha", "test")]

    def test_dropping_a_split_is_reported(self):
        previous = config(splits=SPLITS, default={"test": 25, "validation": 10})
        current = config(splits=["test"], default={"test": 25}, version=2)
        regressions = find_lowered_counts(previous, current)
        assert [(r.split, r.current) for r in regressions] == [("validation", 0)]

    def test_a_lowered_count_is_reported_even_where_the_previous_version_was_capped(
        self,
    ):
        """Conservative on purpose: the configured figure is the declared intent."""
        previous = config(splits=SPLITS, default={"test": 25, "validation": 10})
        current = config(
            splits=SPLITS, default={"test": 24, "validation": 10}, version=2
        )
        assert find_lowered_counts(previous, current) != []

    def test_the_message_names_the_stratum_and_split(self):
        previous = config(
            splits=SPLITS,
            default={"test": 25, "validation": 10},
            overrides={"gamma": {"test": 11}},
        )
        current = config(
            splits=SPLITS,
            default={"test": 25, "validation": 10},
            overrides={"gamma": {"test": 5}},
            version=2,
        )
        described = find_lowered_counts(previous, current)[0].describe()
        assert "gamma" in described
        assert "test" in described
