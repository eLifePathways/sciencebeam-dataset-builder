"""Tests for archive_cut.manifest — the published record of which document is where."""

import pytest

from sciencebeam_dataset_builder.archive_cut.manifest import (
    MANIFEST_FIELDS,
    ManifestError,
    ManifestRow,
    ids_by_stratum_split,
    read_manifest,
    write_manifest,
)

ROWS = [
    ManifestRow(id="beta-000", stratum="beta", rank=0, split="test"),
    ManifestRow(id="alpha-002", stratum="alpha", rank=2, split="validation"),
    ManifestRow(id="alpha-000", stratum="alpha", rank=0, split="test"),
]


class TestRoundTrip:
    def test_write_then_read_preserves_the_rows(self, tmp_path):
        path = tmp_path / "sample-v001.csv"
        write_manifest(path, ROWS)
        assert set(read_manifest(path)) == set(ROWS)

    def test_rows_are_written_in_stratum_then_rank_order(self, tmp_path):
        path = tmp_path / "sample-v001.csv"
        write_manifest(path, ROWS)
        assert [row.id for row in read_manifest(path)] == [
            "alpha-000",
            "alpha-002",
            "beta-000",
        ]

    def test_the_header_is_the_documented_field_list(self, tmp_path):
        path = tmp_path / "sample-v001.csv"
        write_manifest(path, ROWS)
        header = path.read_text(encoding="utf-8").splitlines()[0]
        assert header == ",".join(MANIFEST_FIELDS)

    def test_the_parent_directory_is_created(self, tmp_path):
        path = tmp_path / "splits" / "sample-v001.csv"
        write_manifest(path, ROWS)
        assert path.exists()


class TestValidation:
    def test_an_id_in_two_splits_is_rejected_on_write(self, tmp_path):
        rows = [
            ManifestRow(id="alpha-000", stratum="alpha", rank=0, split="test"),
            ManifestRow(id="alpha-000", stratum="alpha", rank=0, split="validation"),
        ]
        with pytest.raises(ManifestError) as exc_info:
            write_manifest(tmp_path / "m.csv", rows)
        assert "alpha-000" in str(exc_info.value)

    def test_an_id_in_two_splits_is_rejected_on_read(self, tmp_path):
        path = tmp_path / "m.csv"
        path.write_text(
            "id,stratum,rank,split\n"
            "alpha-000,alpha,0,test\n"
            "alpha-000,alpha,0,validation\n",
            encoding="utf-8",
        )
        with pytest.raises(ManifestError):
            read_manifest(path)

    def test_a_missing_column_is_rejected(self, tmp_path):
        path = tmp_path / "m.csv"
        path.write_text("id,split\nalpha-000,test\n", encoding="utf-8")
        with pytest.raises(ManifestError) as exc_info:
            read_manifest(path)
        assert "rank" in str(exc_info.value)

    def test_a_non_integer_rank_is_rejected(self, tmp_path):
        path = tmp_path / "m.csv"
        path.write_text(
            "id,stratum,rank,split\nalpha-000,alpha,first,test\n", encoding="utf-8"
        )
        with pytest.raises(ManifestError) as exc_info:
            read_manifest(path)
        assert "first" in str(exc_info.value)

    def test_an_empty_manifest_reads_as_no_rows(self, tmp_path):
        path = tmp_path / "m.csv"
        path.write_text("id,stratum,rank,split\n", encoding="utf-8")
        assert read_manifest(path) == []


class TestGrouping:
    def test_ids_are_grouped_by_stratum_and_split(self):
        assert ids_by_stratum_split(ROWS) == {
            ("beta", "test"): {"beta-000"},
            ("alpha", "validation"): {"alpha-002"},
            ("alpha", "test"): {"alpha-000"},
        }
