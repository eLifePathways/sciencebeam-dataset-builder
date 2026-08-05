"""A timeout says nothing about the document, so it must not quietly exclude it.

A 200 KiB docx exceeding a five-minute limit is evidence that the converter hung, not
that the document is unconvertible. These cover the three places that has to be true:
the render retries it, the record distinguishes it, and publishing refuses to treat it
as an ordinary exclusion.
"""

import stat
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sciencebeam_dataset_builder.archive_cut.layout import (
    ADDED_DIRECTORY,
    FAILURES_FILENAME,
    RENDERED_DIRECTORY,
    completed_path,
)
from sciencebeam_dataset_builder.archive_cut.progress import read_completed
from sciencebeam_dataset_builder.archive_cut.publish import (
    PublishError,
    check_failures_resolved,
)
from sciencebeam_dataset_builder.archive_cut.render import RenderFailure
from sciencebeam_dataset_builder.archive_cut.render_cli import (
    main as render_main,
)
from sciencebeam_dataset_builder.archive_cut.render_cli import (
    read_failures,
    write_failures,
)

from tests.archive_cut._helpers import rendered_table, stage_files

FAKE_CONVERTER = Path(__file__).parent / "_fake_converter.py"


@pytest.fixture
def converter(tmp_path):
    path = tmp_path / "fake-lowriter"
    path.write_text(
        f'#!/bin/sh\nexec "{sys.executable}" "{FAKE_CONVERTER}" "$@"\n',
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return str(path)


def _added(
    version_dir: Path, split: str, ids: list[str], shard: str = "alpha-00000"
) -> None:
    directory = version_dir / ADDED_DIRECTORY / split
    directory.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "id": ids,
                "stratum": ["alpha"] * len(ids),
                "doc": [f"source:{i}".encode() for i in ids],
                "doc_ext": ["docx"] * len(ids),
                "xml": [f"<a>{i}</a>" for i in ids],
            }
        ),
        directory / f"{shard}.parquet",
    )


class TestTimeoutIsRetriedWithinARun:
    def test_a_document_that_times_out_once_still_renders(self, tmp_path, converter):
        version_dir = tmp_path / "v1"
        _added(version_dir, "test", ["alpha-000-flaky"])
        # Generous, because the second attempt has to actually render: a budget close to
        # the cost of starting the converter races it and fails under load.
        render_main([str(version_dir), "--converter", converter, "--timeout", "5"])
        assert rendered_table(version_dir, "test").num_rows == 1
        assert read_failures(version_dir / FAILURES_FILENAME) == []

    def test_a_document_that_always_times_out_is_recorded_as_retryable(
        self, tmp_path, converter
    ):
        version_dir = tmp_path / "v1"
        _added(version_dir, "test", ["alpha-000-hangs"])
        render_main([str(version_dir), "--converter", converter, "--timeout", "1"])
        failures = read_failures(version_dir / FAILURES_FILENAME)
        assert [(f.id, f.retryable) for f in failures] == [("alpha-000-hangs", True)]

    def test_a_refused_document_is_not_retryable(self, tmp_path, converter):
        version_dir = tmp_path / "v1"
        _added(version_dir, "test", ["alpha-000-fails"])
        render_main([str(version_dir), "--converter", converter])
        failures = read_failures(version_dir / FAILURES_FILENAME)
        assert [(f.id, f.retryable) for f in failures] == [("alpha-000-fails", False)]


class TestFailureFileFormat:
    def test_retryability_round_trips(self, tmp_path):
        path = tmp_path / FAILURES_FILENAME
        write_failures(
            path,
            [
                RenderFailure(id="a", reason="converter timed out", retryable=True),
                RenderFailure(id="b", reason="converter exited 3"),
            ],
        )
        assert [(f.id, f.retryable) for f in read_failures(path)] == [
            ("a", True),
            ("b", False),
        ]

    def test_a_file_without_the_column_infers_from_the_reason(self, tmp_path):
        """Files written before the column existed must still be read correctly."""
        path = tmp_path / FAILURES_FILENAME
        path.write_text(
            "id,reason\n"
            "alpha-000-hangs,converter timed out after 300s\n"
            "other,converter exited 3\n",
            encoding="utf-8",
        )
        assert [(f.id, f.retryable) for f in read_failures(path)] == [
            ("alpha-000-hangs", True),
            ("other", False),
        ]


class TestRetryFailed:
    def test_the_shard_is_rendered_again_and_can_succeed(self, tmp_path, converter):
        version_dir = tmp_path / "v1"
        _added(version_dir, "test", ["alpha-000-flaky", "alpha-001"])
        render_main([str(version_dir), "--converter", converter, "--timeout", "5"])

        # Simulate the operator's situation: the flaky document was recorded as failed.
        write_failures(
            version_dir / FAILURES_FILENAME,
            [
                RenderFailure(
                    id="alpha-000-flaky",
                    reason="converter timed out after 1s",
                    retryable=True,
                )
            ],
        )
        # The stand-in times out on its first attempt in a fresh working directory and
        # succeeds on the second, as a hanging converter does.
        render_main(
            [
                str(version_dir),
                "--converter",
                converter,
                "--retry-failed",
                "--timeout",
                "5",
            ]
        )
        ids = rendered_table(version_dir, "test").column("id").to_pylist()
        assert "alpha-000-flaky" in ids
        assert read_failures(version_dir / FAILURES_FILENAME) == []

    def test_the_shards_completion_record_is_dropped_and_rebuilt(
        self, tmp_path, converter
    ):
        version_dir = tmp_path / "v1"
        _added(version_dir, "test", ["alpha-000-hangs"], shard="alpha-00000")
        _added(version_dir, "test", ["alpha-004"], shard="alpha-00001")
        render_main([str(version_dir), "--converter", converter, "--timeout", "1"])
        assert len(read_completed(completed_path(version_dir, RENDERED_DIRECTORY))) == 2

        render_main(
            [
                str(version_dir),
                "--converter",
                converter,
                "--retry-failed",
                "--timeout",
                "1",
            ]
        )
        # The unaffected shard keeps its record; the affected one was redone.
        completed = read_completed(completed_path(version_dir, RENDERED_DIRECTORY))
        assert sorted(completed) == [
            "test/alpha-00000.parquet",
            "test/alpha-00001.parquet",
        ]

    def test_nothing_to_retry_says_so(self, tmp_path, converter, capsys):
        version_dir = tmp_path / "v1"
        _added(version_dir, "test", ["alpha-000"])
        render_main([str(version_dir), "--converter", converter])
        render_main([str(version_dir), "--converter", converter, "--retry-failed"])
        assert "No retryable failures" in capsys.readouterr().out

    def test_an_unaffected_shard_is_not_rendered_again(self, tmp_path, converter):
        version_dir = tmp_path / "v1"
        _added(version_dir, "test", ["alpha-000"], shard="alpha-00000")
        _added(version_dir, "test", ["alpha-004-hangs"], shard="alpha-00001")
        render_main([str(version_dir), "--converter", converter, "--timeout", "1"])
        untouched = stage_files(version_dir, RENDERED_DIRECTORY, "test")[0]
        before = untouched.stat().st_mtime_ns

        render_main(
            [
                str(version_dir),
                "--converter",
                converter,
                "--retry-failed",
                "--timeout",
                "1",
            ]
        )
        assert untouched.stat().st_mtime_ns == before


class TestPublishRefusesUnresolvedFailures:
    def test_a_retryable_failure_stops_publication(self):
        with pytest.raises(PublishError) as exc_info:
            check_failures_resolved(
                [
                    RenderFailure(
                        id="alpha-000-hangs",
                        reason="converter timed out after 300s",
                        retryable=True,
                    )
                ]
            )
        message = str(exc_info.value)
        assert "alpha-000-hangs" in message
        assert "--retry-failed" in message
        assert "--exclude-unresolved" in message

    def test_a_terminal_failure_does_not(self):
        check_failures_resolved(
            [RenderFailure(id="a", reason="converter exited 3", retryable=False)]
        )

    def test_no_failures_does_not(self):
        check_failures_resolved([])

    def test_it_can_be_accepted_deliberately(self):
        check_failures_resolved(
            [RenderFailure(id="a", reason="timed out", retryable=True)],
            accept_unresolved=True,
        )
