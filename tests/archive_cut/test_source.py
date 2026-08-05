"""Tests for archive_cut.source — selective reading, and detecting an archive that moved."""

import json

import pyarrow.parquet as pq
import pytest

from sciencebeam_dataset_builder.archive_cut.allocate import MetadataRow
from sciencebeam_dataset_builder.archive_cut.source import (
    LocalArchiveSource,
    ShardInfo,
    SourceError,
    _row_groups_for_rows,
    describe_read_cost,
    iter_document_batches,
    parse_metadata,
    parse_shard_manifest,
    resolve_columns,
    shards_for,
)

from tests.archive_cut._helpers import (
    archive_config,
    document_bytes,
    document_xml,
    paper_id,
    write_archive,
)

SPLITS = ["test", "validation"]


def _config(**kwargs):
    return archive_config(splits=SPLITS, default={"test": 2, "validation": 1}, **kwargs)


class CountingSource(LocalArchiveSource):
    """Records which shards were opened, so selectivity can be asserted."""

    def __init__(self, directory):
        super().__init__(directory)
        self.opened: list[str] = []

    def open_parquet(self, name):
        self.opened.append(name)
        return super().open_parquet(name)


class TestParseMetadata:
    def test_reads_id_stratum_and_rank(self, tmp_path):
        write_archive(tmp_path, {"alpha": 3})
        source = LocalArchiveSource(tmp_path)
        rows = parse_metadata(source.read_text("metadata.jsonl"), _config())
        assert rows == [
            MetadataRow(id=paper_id("alpha", rank), stratum="alpha", rank=rank)
            for rank in range(3)
        ]

    def test_a_missing_field_is_reported_with_the_line_number(self, tmp_path):
        path = tmp_path / "metadata.jsonl"
        path.write_text(
            '{"id": "alpha-000", "stratum": "alpha", "rank": 0}\n'
            '{"id": "alpha-001", "stratum": "alpha"}\n',
            encoding="utf-8",
        )
        with pytest.raises(SourceError) as exc_info:
            parse_metadata(path.read_text(encoding="utf-8"), _config())
        assert "line 2" in str(exc_info.value)
        assert "rank" in str(exc_info.value)

    def test_a_non_integer_rank_is_rejected(self, tmp_path):
        text = '{"id": "alpha-000", "stratum": "alpha", "rank": "first"}\n'
        with pytest.raises(SourceError):
            parse_metadata(text, _config())

    def test_invalid_json_is_rejected(self):
        with pytest.raises(SourceError) as exc_info:
            parse_metadata("{not json}\n", _config())
        assert "valid JSON" in str(exc_info.value)

    def test_an_empty_metadata_file_is_rejected(self):
        with pytest.raises(SourceError):
            parse_metadata("\n", _config())


class TestParseShardManifest:
    def test_reads_the_shard_ranges(self, tmp_path):
        write_archive(tmp_path, {"alpha": 6}, rows_per_shard=4)
        source = LocalArchiveSource(tmp_path)
        shards = parse_shard_manifest(source.read_text("shards.jsonl"), _config())
        assert [(s.rank_from, s.rank_to, s.rows) for s in shards] == [
            (0, 3, 4),
            (4, 5, 2),
        ]

    def test_a_missing_field_is_rejected(self):
        with pytest.raises(SourceError):
            parse_shard_manifest('{"filename": "a.parquet"}\n', _config())


class TestShardsFor:
    def test_only_shards_holding_wanted_documents_are_selected(self, tmp_path):
        write_archive(tmp_path, {"alpha": 12}, rows_per_shard=4)
        source = LocalArchiveSource(tmp_path)
        shards = parse_shard_manifest(source.read_text("shards.jsonl"), _config())
        wanted = [
            MetadataRow(id=paper_id("alpha", rank), stratum="alpha", rank=rank)
            for rank in (0, 1)
        ]
        selected = shards_for(wanted, shards)
        assert list(selected) == ["alpha-00000-of-00003.parquet"]

    def test_documents_are_grouped_by_shard_in_rank_order(self, tmp_path):
        write_archive(tmp_path, {"alpha": 8}, rows_per_shard=4)
        source = LocalArchiveSource(tmp_path)
        shards = parse_shard_manifest(source.read_text("shards.jsonl"), _config())
        wanted = [
            MetadataRow(id=paper_id("alpha", rank), stratum="alpha", rank=rank)
            for rank in (5, 0, 4)
        ]
        selected = shards_for(wanted, shards)
        assert {
            name: [row.rank for row in item.rows] for name, item in selected.items()
        } == {
            "alpha-00000-of-00002.parquet": [0],
            "alpha-00001-of-00002.parquet": [4, 5],
        }

    def test_strata_are_kept_apart(self, tmp_path):
        write_archive(tmp_path, {"alpha": 4, "beta": 4}, rows_per_shard=4)
        source = LocalArchiveSource(tmp_path)
        shards = parse_shard_manifest(source.read_text("shards.jsonl"), _config())
        wanted = [MetadataRow(id=paper_id("beta", 0), stratum="beta", rank=0)]
        assert list(shards_for(wanted, shards)) == ["beta-00000-of-00001.parquet"]

    def test_a_rank_no_shard_holds_is_rejected(self):
        shards = [
            ShardInfo(
                filename="alpha-00000.parquet",
                stratum="alpha",
                rank_from=0,
                rank_to=3,
                rows=4,
            )
        ]
        wanted = [MetadataRow(id="alpha-009", stratum="alpha", rank=9)]
        with pytest.raises(SourceError) as exc_info:
            shards_for(wanted, shards)
        assert "does not describe this archive" in str(exc_info.value)


class TestRowGroupSelection:
    def test_only_the_row_groups_holding_wanted_rows_are_named(self, tmp_path):
        write_archive(tmp_path, {"alpha": 8}, rows_per_shard=8, rows_per_row_group=2)
        parquet_file = pq.ParquetFile(tmp_path / "alpha-00000-of-00001.parquet")
        assert parquet_file.num_row_groups == 4
        assert _row_groups_for_rows(parquet_file, [0, 1]) == [0]
        assert _row_groups_for_rows(parquet_file, [0, 5]) == [0, 2]
        assert _row_groups_for_rows(parquet_file, [7]) == [3]


class TestReadingDocuments:
    def test_the_wanted_rows_are_returned_with_their_content(self, tmp_path):
        write_archive(tmp_path, {"alpha": 8}, rows_per_shard=8, rows_per_row_group=2)
        source = LocalArchiveSource(tmp_path)
        config = _config()
        wanted = [
            MetadataRow(id=paper_id("alpha", rank), stratum="alpha", rank=rank)
            for rank in (0, 3)
        ]
        selected = shards_for(
            wanted, parse_shard_manifest(source.read_text("shards.jsonl"), config)
        )
        tables = [
            table for _name, table in iter_document_batches(source, selected, config)
        ]
        assert len(tables) == 1
        table = tables[0]
        assert table.column("id").to_pylist() == [
            paper_id("alpha", 0),
            paper_id("alpha", 3),
        ]
        assert table.column("doc").to_pylist() == [
            document_bytes("alpha", 0),
            document_bytes("alpha", 3),
        ]
        assert table.column("xml").to_pylist() == [
            document_xml("alpha", 0),
            document_xml("alpha", 3),
        ]

    def test_only_the_needed_shards_are_opened(self, tmp_path):
        write_archive(tmp_path, {"alpha": 12, "beta": 12}, rows_per_shard=4)
        source = CountingSource(tmp_path)
        config = _config()
        wanted = [
            MetadataRow(id=paper_id(stratum, 0), stratum=stratum, rank=0)
            for stratum in ("alpha", "beta")
        ]
        selected = shards_for(
            wanted, parse_shard_manifest(source.read_text("shards.jsonl"), config)
        )
        list(iter_document_batches(source, selected, config))
        assert sorted(set(source.opened)) == [
            "alpha-00000-of-00003.parquet",
            "beta-00000-of-00003.parquet",
        ]

    def test_columns_absent_from_the_archive_are_skipped(self, tmp_path):
        write_archive(tmp_path, {"alpha": 4})
        source = LocalArchiveSource(tmp_path)
        config = _config()
        wanted = [MetadataRow(id=paper_id("alpha", 0), stratum="alpha", rank=0)]
        selected = shards_for(
            wanted, parse_shard_manifest(source.read_text("shards.jsonl"), config)
        )
        _name, table = next(iter(iter_document_batches(source, selected, config)))
        # 'pdf' is configured for the output but the archive has no such column: a
        # later step adds it.
        assert "pdf" in config.columns
        assert "pdf" not in table.schema.names


class TestResolveColumns:
    def test_configured_columns_the_archive_lacks_are_dropped(self):
        config = _config()
        assert resolve_columns(config, {"id", "stratum", "xml"}) == [
            "id",
            "stratum",
            "xml",
        ]

    def test_a_missing_id_column_is_rejected(self):
        with pytest.raises(SourceError) as exc_info:
            resolve_columns(_config(), {"stratum", "xml"})
        assert "id" in str(exc_info.value)


class TestArchiveMovedDetection:
    def test_a_document_absent_from_the_shard_it_should_be_in_is_reported(
        self, tmp_path
    ):
        """The check that turns "never re-shard the archive" into a caught error."""
        write_archive(tmp_path, {"alpha": 8}, rows_per_shard=4)
        # Claim the second shard holds the first shard's ranks, as a re-sharded
        # archive with a stale manifest would.
        (tmp_path / "shards.jsonl").write_text(
            json.dumps(
                {
                    "filename": "alpha-00001-of-00002.parquet",
                    "stratum": "alpha",
                    "rank_from": 0,
                    "rank_to": 7,
                    "rows": 4,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        source = LocalArchiveSource(tmp_path)
        config = _config()
        wanted = [MetadataRow(id=paper_id("alpha", 0), stratum="alpha", rank=0)]
        selected = shards_for(
            wanted, parse_shard_manifest(source.read_text("shards.jsonl"), config)
        )
        with pytest.raises(SourceError) as exc_info:
            list(iter_document_batches(source, selected, config))
        assert "re-sorted or re-sharded" in str(exc_info.value)

    def test_a_missing_shard_file_is_reported(self, tmp_path):
        write_archive(tmp_path, {"alpha": 4})
        (tmp_path / "alpha-00000-of-00001.parquet").unlink()
        source = LocalArchiveSource(tmp_path)
        with pytest.raises(SourceError):
            source.open_parquet("alpha-00000-of-00001.parquet")

    def test_a_missing_sidecar_is_reported(self, tmp_path):
        source = LocalArchiveSource(tmp_path)
        with pytest.raises(SourceError):
            source.read_text("metadata.jsonl")


class TestDescribeReadCost:
    def test_reports_the_share_of_the_archive_that_holds_the_documents(self, tmp_path):
        write_archive(tmp_path, {"alpha": 16}, rows_per_shard=4)
        source = LocalArchiveSource(tmp_path)
        config = _config()
        shards = parse_shard_manifest(source.read_text("shards.jsonl"), config)
        wanted = [MetadataRow(id=paper_id("alpha", 0), stratum="alpha", rank=0)]
        described = describe_read_cost(shards_for(wanted, shards), shards)
        assert "1 of 4 shard(s)" in described
        # The figure must not read as bytes to be downloaded: whole shards are bigger
        # than the row groups a read actually fetches.
        assert "Only the row groups holding them are fetched" in described

    def test_survives_a_manifest_without_byte_counts(self):
        shards = [
            ShardInfo(
                filename="alpha-00000.parquet",
                stratum="alpha",
                rank_from=0,
                rank_to=3,
                rows=4,
            )
        ]
        wanted = [MetadataRow(id="alpha-000", stratum="alpha", rank=0)]
        assert "unknown" in describe_read_cost(shards_for(wanted, shards), shards)


class TestLocalSourceRevision:
    def test_a_directory_has_no_revision_to_record(self, tmp_path):
        assert LocalArchiveSource(tmp_path).revision is None
