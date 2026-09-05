"""Row groups sized by bytes, because a row here is a whole document.

pyarrow's default is about a million rows per group, so 331 documents landed in one group
of 729 MiB — which defeated both things row groups are for: a reader had to materialise the
lot to read one document, and no filter could skip anything because the group's statistics
spanned every value in the file.
"""

import pyarrow as pa
import pyarrow.parquet as pq

from sciencebeam_dataset_builder.archive_cut.parquet_io import (
    row_group_boundaries,
    row_payload_sizes,
    write_table_with_byte_sized_row_groups,
)

MIB = 1024**2


def _table(sizes: list[int], strata: list[str] | None = None) -> pa.Table:
    return pa.table(
        {
            "id": [f"doc-{i:03d}" for i in range(len(sizes))],
            "stratum": strata or ["alpha"] * len(sizes),
            "doc": [b"x" * size for size in sizes],
        }
    )


class TestRowPayloadSizes:
    def test_variable_width_columns_are_counted(self):
        table = _table([10, 20, 30])
        sizes = row_payload_sizes(table)
        # 'doc' plus the id and stratum strings, so at least the doc bytes.
        assert sizes[0] >= 10 and sizes[1] >= 20 and sizes[2] >= 30
        assert sizes[1] - sizes[0] == 10

    def test_an_empty_table_has_no_sizes(self):
        assert row_payload_sizes(_table([])) == []


class TestRowGroupBoundaries:
    def test_rows_are_grouped_until_the_byte_target(self):
        assert row_group_boundaries([4, 4, 4, 4], target_bytes=8, max_rows=100) == [
            (0, 2),
            (2, 2),
        ]

    def test_a_trailing_partial_group_is_kept(self):
        assert row_group_boundaries([4, 4, 4], target_bytes=8, max_rows=100) == [
            (0, 2),
            (2, 1),
        ]

    def test_a_row_larger_than_the_target_becomes_its_own_group(self):
        """Parquet rows are atomic and this archive holds documents over 100 MiB."""
        assert row_group_boundaries([100, 1, 1], target_bytes=8, max_rows=100) == [
            (0, 1),
            (1, 2),
        ]

    def test_the_row_cap_applies_to_small_rows(self):
        assert row_group_boundaries([1] * 5, target_bytes=1000, max_rows=2) == [
            (0, 2),
            (2, 2),
            (4, 1),
        ]

    def test_no_rows_makes_no_groups(self):
        assert row_group_boundaries([]) == []


class TestWriting:
    def test_many_row_groups_rather_than_one(self, tmp_path):
        path = tmp_path / "out.parquet"
        groups = write_table_with_byte_sized_row_groups(
            _table([MIB] * 8), path, target_bytes=2 * MIB
        )
        assert groups == 4
        assert pq.read_metadata(path).num_row_groups == 4

    def test_row_order_is_preserved(self, tmp_path):
        path = tmp_path / "out.parquet"
        write_table_with_byte_sized_row_groups(
            _table([MIB] * 6), path, target_bytes=2 * MIB
        )
        assert pq.read_table(path).column("id").to_pylist() == [
            f"doc-{i:03d}" for i in range(6)
        ]

    def test_statistics_let_a_reader_skip_groups(self, tmp_path):
        """The point of small groups: a filter can rule most of the file out."""
        path = tmp_path / "out.parquet"
        strata = ["alpha"] * 4 + ["beta"] * 4
        write_table_with_byte_sized_row_groups(
            _table([MIB] * 8, strata=strata), path, target_bytes=2 * MIB
        )
        meta = pq.read_metadata(path)
        column = meta.schema.names.index("stratum")
        ranges = [
            (
                meta.row_group(g).column(column).statistics.min,
                meta.row_group(g).column(column).statistics.max,
            )
            for g in range(meta.num_row_groups)
        ]
        # Some group covers only 'beta', so a filter for it can skip the others.
        assert ("beta", "beta") in ranges
        assert any(low == "alpha" and high == "alpha" for low, high in ranges)

    def test_a_filter_returns_only_the_matching_rows(self, tmp_path):
        path = tmp_path / "out.parquet"
        strata = ["alpha"] * 4 + ["beta"] * 4
        write_table_with_byte_sized_row_groups(
            _table([MIB] * 8, strata=strata), path, target_bytes=2 * MIB
        )
        filtered = pq.read_table(path, filters=[("stratum", "==", "beta")])
        assert filtered.num_rows == 4
        assert set(filtered.column("stratum").to_pylist()) == {"beta"}

    def test_an_empty_table_still_writes_a_readable_file(self, tmp_path):
        path = tmp_path / "out.parquet"
        assert write_table_with_byte_sized_row_groups(_table([]), path) == 0
        assert pq.read_table(path).num_rows == 0
