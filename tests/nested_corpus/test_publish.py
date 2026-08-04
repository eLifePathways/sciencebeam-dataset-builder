"""Tests for nested_corpus.publish and upload — merging, exclusions, batching, retries."""

from pathlib import Path

import pyarrow as pa
import pytest

from sciencebeam_dataset_builder.nested_corpus.manifest import ManifestRow
from sciencebeam_dataset_builder.nested_corpus.publish import (
    PreparedSplit,
    PublishError,
    check_output_schema,
    config_with_exclusions,
    describe_publication,
    manifest_without_failures,
    merge_split,
)
from sciencebeam_dataset_builder.nested_corpus.render import RenderFailure
from sciencebeam_dataset_builder.nested_corpus.upload import (
    FileToPublish,
    LocalPublishTarget,
    UploadError,
    _is_retryable,
    _retry_after,
    batched,
)

from tests.nested_corpus._helpers import archive_config

SPLITS = ["test", "validation"]


def _rows(*specs):
    return [ManifestRow(id=i, stratum=s, rank=r, split=sp) for i, s, r, sp in specs]


def _table(ids, extra_column=None):
    data = {
        "id": ids,
        "stratum": ["alpha"] * len(ids),
        "doc": [f"doc:{i}".encode() for i in ids],
        "doc_ext": ["docx"] * len(ids),
        "xml": [f"<a>{i}</a>" for i in ids],
        "pdf": [b"%PDF-1.4 " + i.encode() for i in ids],
        "pdf_converter_version": ["FakeConverter 1.2.3"] * len(ids),
    }
    if extra_column:
        data[extra_column] = ["x"] * len(ids)
    return pa.table(data)


class TestManifestWithoutFailures:
    def test_failed_documents_are_separated_out(self):
        rows = _rows(
            ("a", "alpha", 0, "test"),
            ("b", "alpha", 1, "test"),
            ("c", "alpha", 2, "test"),
        )
        kept, dropped = manifest_without_failures(
            rows, [RenderFailure(id="b", reason="converter exited 3")]
        )
        assert [row.id for row in kept] == ["a", "c"]
        assert [row.id for row in dropped] == ["b"]

    def test_no_failures_keeps_everything(self):
        rows = _rows(("a", "alpha", 0, "test"))
        kept, dropped = manifest_without_failures(rows, [])
        assert kept == rows
        assert dropped == []


class TestConfigWithExclusions:
    def test_failures_are_added_to_the_exclusions(self):
        config = archive_config(splits=SPLITS, default={"test": 2, "validation": 0})
        updated = config_with_exclusions(
            config, [RenderFailure(id="alpha-001", reason="broken")]
        )
        assert updated.exclude == ("alpha-001",)

    def test_existing_exclusions_are_kept(self):
        config = archive_config(
            splits=SPLITS, default={"test": 2, "validation": 0}, exclude=["alpha-000"]
        )
        updated = config_with_exclusions(
            config, [RenderFailure(id="alpha-001", reason="broken")]
        )
        assert updated.exclude == ("alpha-000", "alpha-001")

    def test_an_already_excluded_id_is_not_repeated(self):
        config = archive_config(
            splits=SPLITS, default={"test": 2, "validation": 0}, exclude=["alpha-000"]
        )
        updated = config_with_exclusions(
            config, [RenderFailure(id="alpha-000", reason="broken again")]
        )
        assert updated.exclude == ("alpha-000",)

    def test_no_failures_leaves_the_config_alone(self):
        config = archive_config(splits=SPLITS, default={"test": 2, "validation": 0})
        assert config_with_exclusions(config, []) is config


class TestMergeSplit:
    def test_new_rows_alone_are_published_in_manifest_order(self):
        rows = _rows(("b", "alpha", 1, "test"), ("a", "alpha", 0, "test"))
        merged = merge_split("test", None, _table(["a", "b"]), rows, "id")
        # Manifest order, not table order.
        assert merged.column("id").to_pylist() == ["b", "a"]

    def test_published_and_new_rows_are_combined(self):
        rows = _rows(
            ("a", "alpha", 0, "test"),
            ("b", "alpha", 1, "test"),
            ("c", "alpha", 2, "test"),
        )
        merged = merge_split("test", _table(["a", "b"]), _table(["c"]), rows, "id")
        assert merged.column("id").to_pylist() == ["a", "b", "c"]

    def test_the_result_is_ordered_the_same_however_the_inputs_arrive(self):
        rows = _rows(
            ("a", "alpha", 0, "test"),
            ("b", "alpha", 1, "test"),
            ("c", "alpha", 2, "test"),
        )
        one = merge_split("test", _table(["a", "b"]), _table(["c"]), rows, "id")
        other = merge_split("test", _table(["b", "a"]), _table(["c"]), rows, "id")
        assert one.column("id").to_pylist() == other.column("id").to_pylist()

    def test_only_the_named_split_is_taken(self):
        rows = _rows(("a", "alpha", 0, "test"), ("b", "alpha", 1, "validation"))
        merged = merge_split("test", None, _table(["a", "b"]), rows, "id")
        assert merged.column("id").to_pylist() == ["a"]

    def test_a_document_the_manifest_lists_but_no_table_holds_is_reported(self):
        rows = _rows(("a", "alpha", 0, "test"), ("missing", "alpha", 1, "test"))
        with pytest.raises(PublishError) as exc_info:
            merge_split("test", None, _table(["a"]), rows, "id")
        assert "missing" in str(exc_info.value)

    def test_mismatched_columns_between_versions_are_reported(self):
        rows = _rows(("a", "alpha", 0, "test"), ("b", "alpha", 1, "test"))
        with pytest.raises(PublishError) as exc_info:
            merge_split(
                "test",
                _table(["a"]),
                _table(["b"], extra_column="surprise"),
                rows,
                "id",
            )
        assert "surprise" in str(exc_info.value)

    def test_no_rows_at_all_is_reported(self):
        with pytest.raises(PublishError):
            merge_split("test", None, None, [], "id")


class TestCheckOutputSchema:
    def test_the_configured_columns_must_be_present(self):
        config = archive_config(splits=SPLITS, default={"test": 1, "validation": 0})
        check_output_schema("test", _table(["a"]), config)

    def test_a_missing_configured_column_is_reported(self):
        config = archive_config(splits=SPLITS, default={"test": 1, "validation": 0})
        without_pdf = _table(["a"]).drop_columns(["pdf"])
        with pytest.raises(PublishError) as exc_info:
            check_output_schema("test", without_pdf, config)
        assert "pdf" in str(exc_info.value)

    def test_extra_columns_are_allowed(self):
        config = archive_config(splits=SPLITS, default={"test": 1, "validation": 0})
        check_output_schema("test", _table(["a"], extra_column="extra"), config)


class TestDescribePublication:
    def test_counts_and_dropped_documents_are_reported(self):
        prepared = [PreparedSplit(split="test", table=_table(["a"]))]
        dropped = _rows(("b", "alpha", 1, "test"))
        lines = describe_publication(
            prepared, dropped, [RenderFailure(id="b", reason="converter exited 3")]
        )
        joined = "\n".join(lines)
        assert "test: 1 document(s)" in joined
        assert "converter exited 3" in joined
        assert "later version" in joined

    def test_a_clean_publication_reports_only_counts(self):
        prepared = [PreparedSplit(split="test", table=_table(["a"]))]
        assert describe_publication(prepared, [], []) == ["test: 1 document(s)"]


class TestBatching:
    def test_files_are_grouped_into_batches(self):
        files = [
            FileToPublish(local_path=Path(f"{i}"), path_in_repo=f"{i}")
            for i in range(5)
        ]
        assert [len(batch) for batch in batched(files, 2)] == [2, 2, 1]

    def test_a_single_batch_when_they_fit(self):
        files = [FileToPublish(local_path=Path("a"), path_in_repo="a")]
        assert len(batched(files, 16)) == 1

    def test_no_files_makes_no_batches(self):
        assert batched([], 16) == []

    def test_a_zero_batch_size_is_rejected(self):
        with pytest.raises(ValueError):
            batched([], 0)


class TestRetryPolicy:
    class _Response:
        def __init__(self, status_code, headers=None):
            self.status_code = status_code
            self.headers = headers or {}

    def _error(self, status_code, headers=None):
        error = RuntimeError("boom")
        error.response = self._Response(status_code, headers)  # type: ignore[attr-defined]
        return error

    def test_rate_limiting_is_retried(self):
        assert _is_retryable(self._error(429))

    def test_server_errors_are_retried(self):
        assert _is_retryable(self._error(500))
        assert _is_retryable(self._error(503))

    def test_client_errors_are_not_retried(self):
        assert not _is_retryable(self._error(403))
        assert not _is_retryable(self._error(404))

    def test_an_error_without_a_response_is_not_retried(self):
        assert not _is_retryable(RuntimeError("no response attribute"))

    def test_retry_after_is_honoured(self):
        assert _retry_after(self._error(429, {"Retry-After": "12"})) == 12.0

    def test_a_missing_retry_after_falls_back(self):
        assert _retry_after(self._error(429)) is None

    def test_an_unparsable_retry_after_falls_back(self):
        assert _retry_after(self._error(429, {"Retry-After": "soon"})) is None


class TestLocalPublishTarget:
    def test_files_are_written_at_their_repo_paths(self, tmp_path):
        source = tmp_path / "source.parquet"
        source.write_bytes(b"data")
        target = LocalPublishTarget(tmp_path / "repo")
        target.publish(
            [FileToPublish(local_path=source, path_in_repo="splits/a-v001.csv")], "msg"
        )
        assert (tmp_path / "repo" / "splits" / "a-v001.csv").read_bytes() == b"data"

    def test_commits_are_recorded(self, tmp_path):
        source = tmp_path / "s"
        source.write_bytes(b"x")
        target = LocalPublishTarget(tmp_path / "repo")
        target.publish([FileToPublish(local_path=source, path_in_repo="a")], "first")
        assert target.commits == [(["a"], "first")]

    def test_fetch_returns_none_for_an_absent_file(self, tmp_path):
        target = LocalPublishTarget(tmp_path / "repo")
        assert target.fetch("test.parquet", tmp_path / "work") is None


class TestUploadErrorType:
    def test_it_is_a_runtime_error(self):
        assert issubclass(UploadError, RuntimeError)
