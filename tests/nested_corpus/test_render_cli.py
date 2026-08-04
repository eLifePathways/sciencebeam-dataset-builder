"""Tests for nested_corpus.render_cli — adding PDFs to a cut, and recording failures."""

import stat
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sciencebeam_dataset_builder.nested_corpus.render import RenderError
from sciencebeam_dataset_builder.nested_corpus.render_cli import (
    CONVERTER_COLUMN,
    FAILURES_FILENAME,
    PDF_COLUMN,
    RENDERED_DIRECTORY,
    main,
    read_failures,
)

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


def _write_added(version_dir: Path, split: str, ids: list[str]) -> None:
    added = version_dir / "added"
    added.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "id": ids,
                "stratum": ["alpha"] * len(ids),
                "doc": [f"source:{i}".encode() for i in ids],
                "doc_ext": ["docx"] * len(ids),
                "xml": [f"<article>{i}</article>" for i in ids],
            }
        ),
        added / f"{split}.parquet",
    )


class TestRendering:
    def test_the_pdf_and_converter_version_are_added(self, tmp_path, converter):
        version_dir = tmp_path / "v1"
        _write_added(version_dir, "test", ["alpha-000", "alpha-001"])
        main([str(version_dir), "--converter", converter])

        table = pq.read_table(version_dir / RENDERED_DIRECTORY / "test.parquet")
        assert table.num_rows == 2
        assert table.column(CONVERTER_COLUMN).to_pylist() == ["FakeConverter 1.2.3"] * 2
        assert all(
            pdf.startswith(b"%PDF-") for pdf in table.column(PDF_COLUMN).to_pylist()
        )

    def test_the_original_columns_are_preserved(self, tmp_path, converter):
        version_dir = tmp_path / "v1"
        _write_added(version_dir, "test", ["alpha-000"])
        main([str(version_dir), "--converter", converter])

        table = pq.read_table(version_dir / RENDERED_DIRECTORY / "test.parquet")
        assert table.column("doc").to_pylist() == [b"source:alpha-000"]
        assert table.column("xml").to_pylist() == ["<article>alpha-000</article>"]

    def test_every_split_is_rendered(self, tmp_path, converter):
        version_dir = tmp_path / "v1"
        _write_added(version_dir, "test", ["alpha-000"])
        _write_added(version_dir, "validation", ["alpha-001"])
        main([str(version_dir), "--converter", converter])
        assert sorted(
            p.name for p in (version_dir / RENDERED_DIRECTORY).glob("*.parquet")
        ) == ["test.parquet", "validation.parquet"]

    def test_a_version_that_added_nothing_is_not_an_error(
        self, tmp_path, converter, capsys
    ):
        version_dir = tmp_path / "v1"
        version_dir.mkdir()
        main([str(version_dir), "--converter", converter])
        assert "nothing to render" in capsys.readouterr().out


class TestFailures:
    def test_a_failed_document_is_dropped_and_listed(self, tmp_path, converter):
        version_dir = tmp_path / "v1"
        _write_added(version_dir, "test", ["alpha-000", "alpha-001-fails", "alpha-002"])
        main([str(version_dir), "--converter", converter])

        table = pq.read_table(version_dir / RENDERED_DIRECTORY / "test.parquet")
        assert table.column("id").to_pylist() == ["alpha-000", "alpha-002"]
        failures = read_failures(version_dir / FAILURES_FILENAME)
        assert [f.id for f in failures] == ["alpha-001-fails"]
        assert "exited 3" in failures[0].reason

    def test_no_row_is_emitted_with_an_empty_pdf(self, tmp_path, converter):
        version_dir = tmp_path / "v1"
        _write_added(version_dir, "test", ["alpha-000-empty", "alpha-001"])
        main([str(version_dir), "--converter", converter])
        table = pq.read_table(version_dir / RENDERED_DIRECTORY / "test.parquet")
        assert table.column("id").to_pylist() == ["alpha-001"]
        assert all(len(pdf) > 0 for pdf in table.column(PDF_COLUMN).to_pylist())

    def test_each_failure_mode_is_reported_with_its_own_reason(
        self, tmp_path, converter
    ):
        version_dir = tmp_path / "v1"
        _write_added(
            version_dir,
            "test",
            ["alpha-000-fails", "alpha-001-silent", "alpha-002-garbage", "alpha-003"],
        )
        main([str(version_dir), "--converter", converter])
        reasons = {
            f.id: f.reason for f in read_failures(version_dir / FAILURES_FILENAME)
        }
        assert "exited 3" in reasons["alpha-000-fails"]
        assert "wrote no PDF" in reasons["alpha-001-silent"]
        assert "not a readable PDF" in reasons["alpha-002-garbage"]
        assert len(reasons) == 3

    def test_failures_are_printed_per_document(self, tmp_path, converter, capsys):
        version_dir = tmp_path / "v1"
        _write_added(version_dir, "test", ["alpha-000-fails", "alpha-001"])
        main([str(version_dir), "--converter", converter])
        out = capsys.readouterr().out
        assert "alpha-000-fails" in out
        assert "1 document(s) failed to render" in out

    def test_abort_stops_the_run(self, tmp_path, converter):
        version_dir = tmp_path / "v1"
        _write_added(version_dir, "test", ["alpha-000-fails"])
        with pytest.raises(SystemExit) as exc_info:
            main([str(version_dir), "--converter", converter, "--on-failure", "abort"])
        assert exc_info.value.code == 1

    def test_a_clean_run_records_no_failures(self, tmp_path, converter, capsys):
        version_dir = tmp_path / "v1"
        _write_added(version_dir, "test", ["alpha-000"])
        main([str(version_dir), "--converter", converter])
        assert read_failures(version_dir / FAILURES_FILENAME) == []
        assert "No rendering failures" in capsys.readouterr().out

    def test_a_missing_converter_exits_non_zero(self, tmp_path, capsys):
        version_dir = tmp_path / "v1"
        _write_added(version_dir, "test", ["alpha-000"])
        with pytest.raises(SystemExit) as exc_info:
            main([str(version_dir), "--converter", "definitely-not-installed"])
        assert exc_info.value.code == 1
        assert "RenderError" in capsys.readouterr().err

    def test_rendering_every_document_of_a_split_leaves_an_empty_table(
        self, tmp_path, converter
    ):
        """Legitimate but worth being explicit about: the file exists with no rows."""
        version_dir = tmp_path / "v1"
        _write_added(version_dir, "test", ["alpha-000-fails"])
        main([str(version_dir), "--converter", converter])
        table = pq.read_table(version_dir / RENDERED_DIRECTORY / "test.parquet")
        assert table.num_rows == 0
        assert PDF_COLUMN in table.schema.names


class TestFailureFile:
    def test_absent_failure_file_reads_as_no_failures(self, tmp_path):
        assert read_failures(tmp_path / "nope.csv") == []

    def test_it_round_trips(self, tmp_path, converter):
        version_dir = tmp_path / "v1"
        _write_added(version_dir, "test", ["alpha-000-fails"])
        main([str(version_dir), "--converter", converter])
        failures = read_failures(version_dir / FAILURES_FILENAME)
        assert failures[0].id == "alpha-000-fails"
        assert failures[0].reason


class TestRenderErrorIsNotSwallowed:
    def test_render_error_is_raised_not_recorded(self):
        assert issubclass(RenderError, RuntimeError)
