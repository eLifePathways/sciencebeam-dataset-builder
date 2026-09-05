"""Tests for archive_cut.publish and upload — merging, exclusions, batching, retries."""

from pathlib import Path

import pyarrow as pa
import pytest

from sciencebeam_dataset_builder.archive_cut.manifest import ManifestRow
from sciencebeam_dataset_builder.archive_cut.publish import (
    PreparedSplit,
    PublishError,
    check_manifest_is_covered,
    check_output_schema,
    config_with_exclusions,
    describe_publication,
    manifest_without_failures,
    order_new_rows,
)
from sciencebeam_dataset_builder.archive_cut.render import RenderFailure
from sciencebeam_dataset_builder.archive_cut.upload import (
    FileToPublish,
    LocalPublishTarget,
    UploadError,
    _is_retryable,
    _retry_after,
    batched,
)

from sciencebeam_dataset_builder.archive_cut.config import (
    config_from_dict,
    config_to_dict,
)

from tests.archive_cut._helpers import archive_config

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


class TestOrderNewRows:
    def test_rows_come_out_in_manifest_order(self):
        rows = _rows(("b", "alpha", 1, "test"), ("a", "alpha", 0, "test"))
        ordered = order_new_rows("test", _table(["a", "b"]), rows, "id")
        # Manifest order, not table order.
        assert ordered.column("id").to_pylist() == ["b", "a"]

    def test_a_document_belonging_to_another_split_is_refused(self):
        """Rendering writes one directory per split, so a mixed table is a real fault."""
        rows = _rows(("a", "alpha", 0, "test"), ("b", "alpha", 1, "validation"))
        with pytest.raises(PublishError) as exc_info:
            order_new_rows("test", _table(["a", "b"]), rows, "id")
        assert "b" in str(exc_info.value)

    def test_rows_already_published_are_simply_absent(self):
        """Nothing is merged: a version writes only what rendering produced."""
        rows = _rows(
            ("a", "alpha", 0, "test"),
            ("b", "alpha", 1, "test"),
            ("c", "alpha", 2, "test"),
        )
        ordered = order_new_rows("test", _table(["c"]), rows, "id")
        assert ordered.column("id").to_pylist() == ["c"]

    def test_a_rendered_document_the_manifest_does_not_list_is_reported(self):
        rows = _rows(("a", "alpha", 0, "test"))
        with pytest.raises(PublishError) as exc_info:
            order_new_rows("test", _table(["a", "surprise"]), rows, "id")
        assert "surprise" in str(exc_info.value)


class TestCheckManifestIsCovered:
    def test_published_plus_new_equal_to_the_manifest_is_fine(self):
        rows = _rows(("a", "alpha", 0, "test"), ("b", "alpha", 1, "test"))
        check_manifest_is_covered(rows, {"a"}, {"b"})

    def test_a_first_version_has_nothing_published(self):
        rows = _rows(("a", "alpha", 0, "test"))
        check_manifest_is_covered(rows, set(), {"a"})

    def test_writing_an_already_published_document_is_refused(self):
        """Append-only makes this possible in a way rewriting did not."""
        rows = _rows(("a", "alpha", 0, "test"))
        with pytest.raises(PublishError) as exc_info:
            check_manifest_is_covered(rows, {"a"}, {"a"})
        assert "already published" in str(exc_info.value)

    def test_a_manifest_row_in_neither_place_is_refused(self):
        rows = _rows(("a", "alpha", 0, "test"), ("b", "alpha", 1, "test"))
        with pytest.raises(PublishError) as exc_info:
            check_manifest_is_covered(rows, {"a"}, set())
        assert "b" in str(exc_info.value)

    def test_publishing_something_unlisted_is_refused(self):
        rows = _rows(("a", "alpha", 0, "test"))
        with pytest.raises(PublishError) as exc_info:
            check_manifest_is_covered(rows, set(), {"a", "extra"})
        assert "extra" in str(exc_info.value)


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


class TestWhereAlreadyPublishedIdsAreRead:
    """A dry run writes locally but must check against the repo, or a version after the
    first reports every previously published document as missing."""

    def _args(self, **kwargs):
        import argparse

        return argparse.Namespace(
            dry_run=False, target_dir=None, target_repo=None, **kwargs
        )

    def test_a_normal_publish_checks_where_it_writes(self, tmp_path):
        from sciencebeam_dataset_builder.archive_cut.publish_cli import (
            build_verify_target,
        )

        writing_to = LocalPublishTarget(tmp_path / "repo")
        config = archive_config(splits=SPLITS, default={"test": 1, "validation": 0})
        assert build_verify_target(self._args(), config, writing_to) is writing_to

    def test_target_dir_stands_in_for_the_repo_entirely(self, tmp_path):
        from sciencebeam_dataset_builder.archive_cut.publish_cli import (
            build_verify_target,
        )

        writing_to = LocalPublishTarget(tmp_path / "repo")
        config = archive_config(splits=SPLITS, default={"test": 1, "validation": 0})
        args = self._args()
        args.target_dir = tmp_path / "repo"
        assert build_verify_target(args, config, writing_to) is writing_to

    def test_a_dry_run_without_a_configured_repo_falls_back(self, tmp_path):
        from sciencebeam_dataset_builder.archive_cut.publish_cli import (
            build_verify_target,
        )

        writing_to = LocalPublishTarget(tmp_path / "dry-run")
        config = archive_config(splits=SPLITS, default={"test": 1, "validation": 0})
        args = self._args()
        args.dry_run = True
        assert build_verify_target(args, config, writing_to) is writing_to

    def test_a_dry_run_with_a_configured_repo_checks_the_repo(
        self, tmp_path, monkeypatch
    ):
        from sciencebeam_dataset_builder.archive_cut import publish_cli

        built = []

        class FakeHfTarget:
            def __init__(self, repo_id, **kwargs):
                built.append(repo_id)

        monkeypatch.setattr(publish_cli, "HfPublishTarget", FakeHfTarget)
        writing_to = LocalPublishTarget(tmp_path / "dry-run")
        data = config_to_dict(
            archive_config(splits=SPLITS, default={"test": 1, "validation": 0})
        )
        data["target"] = {"repo_id": "owner/corpus"}
        config = config_from_dict(data)
        args = self._args()
        args.dry_run = True
        verify = publish_cli.build_verify_target(args, config, writing_to)
        assert verify is not writing_to
        assert built == ["owner/corpus"]
