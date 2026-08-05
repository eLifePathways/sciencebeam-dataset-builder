"""Tests for archive_cut.render — every failure mode surfaces per document."""

import os
import stat
import sys
from pathlib import Path

import pytest

from sciencebeam_dataset_builder.archive_cut.render import (
    RenderError,
    converter_version,
    pdf_page_count,
    render_document,
)

FAKE_CONVERTER = Path(__file__).parent / "_fake_converter.py"


@pytest.fixture
def converter(tmp_path):
    """A shell wrapper around the fake converter, invoked exactly as lowriter would be."""
    path = tmp_path / "fake-lowriter"
    path.write_text(
        f'#!/bin/sh\nexec "{sys.executable}" "{FAKE_CONVERTER}" "$@"\n',
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return str(path)


def _render(converter, tmp_path, document_id, extension="docx", **kwargs):
    return render_document(
        document=b"source bytes",
        document_id=document_id,
        extension=extension,
        work_dir=tmp_path / "work",
        command=converter,
        **kwargs,
    )


class TestConverterVersion:
    def test_reports_the_version_string(self, converter):
        assert converter_version(converter) == "FakeConverter 1.2.3"

    def test_a_missing_converter_names_what_is_needed(self):
        with pytest.raises(RenderError) as exc_info:
            converter_version("definitely-not-installed")
        assert "LibreOffice" in str(exc_info.value)


class TestPdfPageCount:
    def test_counts_the_pages_of_a_real_pdf(self, converter, tmp_path):
        rendered = _render(converter, tmp_path, "alpha-000-twopage")
        assert pdf_page_count(rendered.pdf) == 2

    def test_bytes_that_are_not_a_pdf_are_rejected(self):
        with pytest.raises(ValueError):
            pdf_page_count(b"this is not a pdf")

    def test_empty_bytes_are_rejected(self):
        with pytest.raises(ValueError):
            pdf_page_count(b"")


class TestRenderDocument:
    def test_a_rendered_document_reports_its_pages(self, converter, tmp_path):
        rendered = _render(converter, tmp_path, "alpha-000")
        assert rendered.pages == 1
        assert rendered.pdf.startswith(b"%PDF-")

    def test_an_id_with_awkward_characters_still_renders(self, converter, tmp_path):
        rendered = _render(converter, tmp_path, "journal.alpha.0012345")
        assert rendered.pages == 1

    def test_a_legacy_extension_is_passed_through(self, converter, tmp_path):
        assert _render(converter, tmp_path, "alpha-000", extension="doc").pages == 1

    def test_a_non_zero_exit_is_reported(self, converter, tmp_path):
        with pytest.raises(ValueError) as exc_info:
            _render(converter, tmp_path, "alpha-000-fails")
        assert "exited 3" in str(exc_info.value)
        assert "conversion refused" in str(exc_info.value)

    def test_exiting_zero_without_writing_a_pdf_is_reported(self, converter, tmp_path):
        with pytest.raises(ValueError) as exc_info:
            _render(converter, tmp_path, "alpha-000-silent")
        assert "wrote no PDF" in str(exc_info.value)

    def test_an_empty_pdf_is_reported(self, converter, tmp_path):
        with pytest.raises(ValueError) as exc_info:
            _render(converter, tmp_path, "alpha-000-empty")
        assert "empty PDF" in str(exc_info.value)

    def test_output_that_is_not_a_pdf_is_reported(self, converter, tmp_path):
        with pytest.raises(ValueError) as exc_info:
            _render(converter, tmp_path, "alpha-000-garbage")
        assert "not a readable PDF" in str(exc_info.value)

    def test_a_timeout_is_reported_per_document(self, converter, tmp_path):
        with pytest.raises(ValueError) as exc_info:
            _render(converter, tmp_path, "alpha-000-hangs", timeout=1)
        assert "timed out" in str(exc_info.value)

    def test_a_timeout_kills_the_grandchild_too(self, tmp_path):
        """`lowriter` starts soffice.bin as a grandchild.

        Killing only the direct child leaves the real converter running, and those
        survivors interfere with every later conversion — which is how one slow document
        turns into one that times out however often it is retried.
        """
        marker = tmp_path / "grandchild-alive"
        # A wrapper that backgrounds a long-lived grandchild, as lowriter does, then
        # waits. The grandchild keeps the marker present while it lives.
        wrapper = tmp_path / "fake-wrapper"
        wrapper.write_text(
            "#!/bin/sh\n"
            f"( touch {marker}; sleep 60; rm -f {marker} ) &\n"
            "echo $! > " + str(tmp_path / "grandchild.pid") + "\n"
            "sleep 60\n",
            encoding="utf-8",
        )
        wrapper.chmod(wrapper.stat().st_mode | stat.S_IXUSR)

        with pytest.raises(ValueError):
            _render(str(wrapper), tmp_path, "alpha-000", timeout=2)

        pid = int((tmp_path / "grandchild.pid").read_text().strip())
        # Signal 0 only checks for existence.
        with pytest.raises(OSError):
            os.kill(pid, 0)

    def test_a_missing_converter_raises_rather_than_failing_the_document(
        self, tmp_path
    ):
        """Not a per-document failure: nothing would render, so it must stop the run."""
        with pytest.raises(RenderError):
            _render("definitely-not-installed", tmp_path, "alpha-000")
