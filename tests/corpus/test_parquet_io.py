"""Tests for corpus.parquet_io — writing shards the Hub can deduplicate."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sciencebeam_dataset_builder.corpus.parquet_io import (
    ParquetIOError,
    check_writer_version,
    read_shard,
    row_payload_bytes,
    shard_tables,
    write_shard,
)
from sciencebeam_dataset_builder.corpus.schema import CORPUS_SCHEMA


def _table(
    sizes: list[int], upstream_sizes: list[int | None] | None = None
) -> pa.Table:
    rows = len(sizes)
    upstream_sizes = upstream_sizes or [None] * rows
    columns = {
        "id": [f"d{i}" for i in range(rows)],
        "doi": [None] * rows,
        "version": [None] * rows,
        "pub_date": [None] * rows,
        "licence": ["CC BY 4.0"] * rows,
        "xml_ftfy_applied": [None] * rows,
        "xml_upstream_sha": ["0" * 64] * rows,
        "xml_source_url": [None] * rows,
        "xml_downloaded_at": [None] * rows,
        "pdf_source_url": [None] * rows,
        "pdf_downloaded_at": [None] * rows,
        "row_updated_at": ["2026-10-01T00:00:00Z"] * rows,
        "pdf": [b"p" * size for size in sizes],
        "xml": ["x" * size for size in sizes],
        "xml_upstream": [("u" * s) if s is not None else None for s in upstream_sizes],
    }
    return pa.table(columns, schema=CORPUS_SCHEMA)


class TestWriting:
    def test_a_shard_round_trips_and_keeps_the_schema(self, tmp_path):
        table = _table([10, 20])
        path = str(tmp_path / "train-00000.parquet")
        write_shard(table, path)
        assert read_shard(path).schema.equals(CORPUS_SCHEMA)
        assert read_shard(path).num_rows == 2

    def test_content_defined_chunking_is_on(self, tmp_path):
        """Load-bearing: without it, correcting one row re-encodes the whole shard and
        costs its full size against quota. It cannot be turned on retroactively."""
        table = _table([1000])
        path = str(tmp_path / "train-00000.parquet")
        write_shard(table, path)
        with pytest.raises(ValueError, match="Missing options"):
            pq.write_table(
                table, path, use_content_defined_chunking={"min_chunk_size": 1}
            )

    def test_an_unpinned_pyarrow_refuses_to_write(self):
        with pytest.raises(ParquetIOError, match="pinned at"):
            check_writer_version("0.0.0")


class TestSharding:
    def test_payload_size_is_measured_per_row(self):
        assert row_payload_bytes(_table([10, 20])) == [20, 40]

    def test_xml_upstream_counts_toward_payload_size_where_corrected(self):
        """A corrected row carries a full second copy of the text, which has to be
        weighed like any other payload column or shards silently grow past target."""
        assert row_payload_bytes(_table([10, 20], upstream_sizes=[15, None])) == [
            35,
            40,
        ]

    def test_a_table_under_the_limit_is_one_shard(self):
        assert len(shard_tables(_table([10, 20]), max_bytes=1000)) == 1

    def test_shards_are_cut_at_the_limit_and_keep_row_order(self):
        shards = shard_tables(_table([100, 100, 100]), max_bytes=400)
        assert [s.num_rows for s in shards] == [2, 1]
        assert shards[0].column("id").to_pylist() == ["d0", "d1"]
        assert shards[1].column("id").to_pylist() == ["d2"]

    def test_a_row_larger_than_the_limit_gets_its_own_shard(self):
        """Rather than being split, which would make it unreadable."""
        shards = shard_tables(_table([10, 5000, 10]), max_bytes=100)
        assert [s.num_rows for s in shards] == [1, 1, 1]

    def test_an_empty_table_produces_no_shards(self):
        assert shard_tables(_table([])) == []

    def test_a_nonsense_limit_is_refused(self):
        with pytest.raises(ParquetIOError, match="must be positive"):
            shard_tables(_table([10]), max_bytes=0)
