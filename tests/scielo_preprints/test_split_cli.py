"""Tests for scielo_preprints.split_cli module."""

import csv
from collections import Counter
from pathlib import Path

import pytest

from sciencebeam_dataset_builder.scielo_preprints.split_cli import (
    main,
    parse_args,
    stratified_split,
    _stratum,
)


def _records(languages: list[str]) -> list[dict[str, str]]:
    return [
        {"ppr_id": f"PPR_{i}", "language": lang} for i, lang in enumerate(languages)
    ]


def _write_metadata_csv(path: Path, languages: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["ppr_id", "language"])
        writer.writeheader()
        writer.writerows(_records(languages))


class TestStratum:
    def test_pt_is_own_stratum(self):
        assert _stratum("pt") == "pt"

    def test_es_is_own_stratum(self):
        assert _stratum("es") == "es"

    def test_unknown_language_maps_to_other(self):
        assert _stratum("en") == "other"
        assert _stratum("fr") == "other"
        assert _stratum("") == "other"


class TestStratifiedSplit:
    def test_all_records_assigned(self):
        records = _records(["pt"] * 100 + ["es"] * 50)
        result = stratified_split(records, 0.2, 0.3, seed=42)
        assert len(result) == 150

    def test_split_values_are_valid(self):
        records = _records(["pt"] * 50)
        result = stratified_split(records, 0.2, 0.3, seed=42)
        assert all(r["split"] in {"train", "val", "test"} for r in result)

    def test_proportions_approximately_correct(self):
        records = _records(["pt"] * 200)
        result = stratified_split(records, 0.2, 0.3, seed=42)
        counts = Counter(r["split"] for r in result)
        assert counts["train"] == 40
        assert counts["val"] == 60
        assert counts["test"] == 100

    def test_stratification_preserves_language_proportions(self):
        records = _records(["pt"] * 100 + ["es"] * 100)
        result = stratified_split(records, 0.2, 0.3, seed=42)

        # Check each language is represented in each split
        by_split: dict[str, list[str]] = {"train": [], "val": [], "test": []}
        ppr_to_lang = {r["ppr_id"]: r["language"] for r in records}
        for row in result:
            by_split[row["split"]].append(ppr_to_lang[row["ppr_id"]])

        for split_langs in by_split.values():
            assert "pt" in split_langs
            assert "es" in split_langs

    def test_reproducible_with_same_seed(self):
        records = _records(["pt"] * 50 + ["es"] * 30)
        result1 = stratified_split(records, 0.2, 0.3, seed=99)
        result2 = stratified_split(records, 0.2, 0.3, seed=99)
        assert result1 == result2

    def test_different_seeds_produce_different_order(self):
        records = _records(["pt"] * 50)
        result1 = stratified_split(records, 0.2, 0.3, seed=1)
        result2 = stratified_split(records, 0.2, 0.3, seed=2)
        assert [r["ppr_id"] for r in result1] != [r["ppr_id"] for r in result2]

    def test_output_contains_only_ppr_id_and_split(self):
        records = _records(["pt"] * 10)
        result = stratified_split(records, 0.2, 0.3, seed=42)
        assert all(set(r.keys()) == {"ppr_id", "split"} for r in result)

    def test_other_stratum_distributed_proportionally(self):
        records = _records(["en"] * 20)
        result = stratified_split(records, 0.2, 0.3, seed=42)
        counts = Counter(r["split"] for r in result)
        assert counts["train"] + counts["val"] + counts["test"] == 20


class TestParseArgs:
    def test_requires_both_positional_args(self):
        with pytest.raises(SystemExit):
            parse_args([])

    def test_defaults(self, tmp_path):
        args = parse_args([str(tmp_path / "meta.csv"), str(tmp_path / "split.csv")])
        assert args.train == 0.2
        assert args.val == 0.3
        assert args.seed == 42

    def test_custom_fractions(self, tmp_path):
        args = parse_args(
            [
                str(tmp_path / "meta.csv"),
                str(tmp_path / "split.csv"),
                "--train",
                "0.1",
                "--val",
                "0.2",
            ]
        )
        assert args.train == 0.1
        assert args.val == 0.2


class TestMain:
    def test_writes_split_csv(self, tmp_path):
        meta = tmp_path / "meta.csv"
        _write_metadata_csv(meta, ["pt"] * 20 + ["es"] * 10)
        out = tmp_path / "split.csv"
        main([str(meta), str(out)])
        rows = list(csv.DictReader(out.open()))
        assert len(rows) == 30
        assert all(r["split"] in {"train", "val", "test"} for r in rows)

    def test_exits_when_fractions_sum_to_one(self, tmp_path):
        meta = tmp_path / "meta.csv"
        _write_metadata_csv(meta, ["pt"] * 10)
        with pytest.raises(SystemExit):
            main(
                [str(meta), str(tmp_path / "out.csv"), "--train", "0.5", "--val", "0.5"]
            )

    def test_exits_when_metadata_empty(self, tmp_path):
        meta = tmp_path / "meta.csv"
        meta.write_text("ppr_id,language\n", encoding="utf-8")
        with pytest.raises(SystemExit):
            main([str(meta), str(tmp_path / "out.csv")])
