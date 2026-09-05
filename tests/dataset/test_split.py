import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sciencebeam_dataset_builder.dataset.schema import CANONICAL_SCHEMA, SOURCES
from sciencebeam_dataset_builder.dataset.split import (
    DEFAULT_FRACTIONS,
    SPLIT_NAMES,
    SplitError,
    assign_split,
    read_split_map,
    split_table,
    validate_fractions,
    write_splits,
)
from sciencebeam_dataset_builder.dataset.normalise import normalise_table

BIORXIV = SOURCES["biorxiv"]


def _table(rows: int) -> pa.Table:
    return normalise_table(
        pa.table(
            {
                "ppr_id": [f"doc{i}" for i in range(rows)],
                "xml": [f"<article>{i}</article>" for i in range(rows)],
                "pdf": [b"%PDF-" + str(i).encode() for i in range(rows)],
            }
        ),
        BIORXIV,
    )


class TestValidateFractions:
    def test_accepts_the_defaults(self):
        validate_fractions(DEFAULT_FRACTIONS)

    def test_rejects_fractions_that_do_not_sum_to_one(self):
        with pytest.raises(SplitError, match="sum to 1.0"):
            validate_fractions({"train": 0.2, "validation": 0.3, "test": 0.4})

    def test_rejects_a_missing_split(self):
        with pytest.raises(SplitError, match="cover exactly"):
            validate_fractions({"train": 0.5, "test": 0.5})


class TestAssignSplit:
    def test_is_deterministic(self):
        assert assign_split("a__b", DEFAULT_FRACTIONS) == assign_split(
            "a__b", DEFAULT_FRACTIONS
        )

    def test_returns_a_known_split(self):
        assert assign_split("a__b", DEFAULT_FRACTIONS) in SPLIT_NAMES

    def test_salt_changes_the_assignment_for_some_uid(self):
        uids = [f"src__doc{i}" for i in range(50)]
        unsalted = [assign_split(u, DEFAULT_FRACTIONS) for u in uids]
        salted = [assign_split(u, DEFAULT_FRACTIONS, salt="v2") for u in uids]
        assert unsalted != salted

    def test_approximates_the_requested_fractions(self):
        uids = [f"src__doc{i}" for i in range(20_000)]
        counts = {name: 0 for name in SPLIT_NAMES}
        for uid in uids:
            counts[assign_split(uid, DEFAULT_FRACTIONS)] += 1
        for name, expected in DEFAULT_FRACTIONS.items():
            assert abs(counts[name] / len(uids) - expected) < 0.02


class TestSplitTable:
    def test_partitions_every_row_exactly_once(self):
        table = _table(200)
        splits = split_table(table)
        assert sum(t.num_rows for t in splits.values()) == 200
        uids = sorted(u for t in splits.values() for u in t.column("uid").to_pylist())
        assert uids == sorted(table.column("uid").to_pylist())

    def test_preserves_the_schema(self):
        for split in split_table(_table(100)).values():
            assert split.schema.equals(CANONICAL_SCHEMA)

    def test_is_independent_of_input_row_order(self):
        table = _table(100)
        reversed_table = table.take(list(reversed(range(table.num_rows))))
        forward = split_table(table)
        backward = split_table(reversed_table)
        for name in SPLIT_NAMES:
            assert sorted(forward[name].column("uid").to_pylist()) == sorted(
                backward[name].column("uid").to_pylist()
            )

    def test_adding_rows_does_not_reshuffle_existing_ones(self):
        before = split_table(_table(100))
        after = split_table(_table(150))
        for name in SPLIT_NAMES:
            kept = set(before[name].column("uid").to_pylist())
            assert kept <= set(after[name].column("uid").to_pylist())

    def test_split_map_overrides_the_hash_bucket(self):
        table = _table(10)
        forced = {uid: "test" for uid in table.column("uid").to_pylist()}
        splits = split_table(table, split_map=forced)
        assert splits["test"].num_rows == 10
        assert splits["train"].num_rows == 0

    def test_split_map_falls_back_for_unlisted_rows(self):
        table = _table(10)
        splits = split_table(table, split_map={"biorxiv__doc0": "train"})
        assert "biorxiv__doc0" in splits["train"].column("uid").to_pylist()
        assert sum(t.num_rows for t in splits.values()) == 10

    def test_rejects_duplicate_uids(self):
        table = _table(2)
        with pytest.raises(SplitError, match="duplicate"):
            split_table(pa.concat_tables([table, table]))

    def test_rejects_a_table_without_a_uid_column(self):
        with pytest.raises(SplitError, match="uid"):
            split_table(pa.table({"id": ["a"]}))

    def test_rejects_an_unknown_split_in_the_map(self):
        table = _table(1)
        with pytest.raises(SplitError, match="Unknown split"):
            split_table(table, split_map={"biorxiv__doc0": "holdout"})


class TestWriteSplits:
    def test_writes_one_file_per_split(self, tmp_path):
        written = write_splits(split_table(_table(50)), tmp_path)
        assert set(written) == set(SPLIT_NAMES)
        for name, path in written.items():
            assert path.name == f"{name}-00000-of-00001.parquet"
            assert path.exists()

    def test_written_files_keep_the_canonical_schema(self, tmp_path):
        written = write_splits(split_table(_table(50)), tmp_path)
        for path in written.values():
            assert pq.ParquetFile(path).schema_arrow.equals(CANONICAL_SCHEMA)


class TestReadSplitMap:
    def test_reads_uid_to_split(self, tmp_path):
        path = tmp_path / "splits.jsonl"
        path.write_text(
            "\n".join(
                json.dumps({"uid": f"src__{i}", "split": "test"}) for i in range(3)
            ),
            encoding="utf-8",
        )
        assert read_split_map(path) == {f"src__{i}": "test" for i in range(3)}

    def test_ignores_blank_lines(self, tmp_path):
        path = tmp_path / "splits.jsonl"
        path.write_text('\n{"uid": "a", "split": "train"}\n\n', encoding="utf-8")
        assert read_split_map(path) == {"a": "train"}
