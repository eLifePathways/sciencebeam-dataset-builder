"""Rendering source documents to PDF, one document at a time.

The source document is the more authoritative artifact and is kept; the PDF exists
because downstream tooling requires one. Two consequences shape this module:

- **the converter version is recorded per row.** PDF fidelity depends on the converter,
  and a corpus grown over time holds PDFs from more than one version of it, so a row
  that cannot say which one made it is not comparable with the others;
- **a failure is reported per document and never papered over.** Legacy binary formats
  are the likely failures, and a row carrying an empty or placeholder PDF would be worse
  than a missing row: it would be silently unparseable rather than visibly absent.

Rendering needs a converter binary on the machine, which is why it is a step of its own
rather than part of the cut: the cut stays pure pyarrow and its tests need no such thing.
"""

import dataclasses
import io
import logging
import re
import subprocess
from pathlib import Path

from pypdf import PdfReader
from pypdf.errors import PdfReadError

LOGGER = logging.getLogger(__name__)

DEFAULT_CONVERTER = "lowriter"
DEFAULT_TIMEOUT_SECONDS = 300


class RenderError(RuntimeError):
    """Rendering cannot proceed at all — a missing converter, say."""


class RenderTimeout(ValueError):
    """The converter did not finish in time.

    Kept apart from every other per-document failure because it says nothing about the
    document: LibreOffice hangs on occasion, and a 200 KiB file exceeding a five-minute
    limit is evidence of that rather than of an unconvertible document. Treating it like
    a corrupt file would drop a perfectly good document from the corpus.
    """


@dataclasses.dataclass(frozen=True)
class RenderFailure:
    """One document that could not be rendered, and why."""

    id: str
    reason: str
    # Whether trying again might succeed. A timeout might; a file the converter refuses
    # will not. Publishing treats the two differently, since excluding a document that
    # merely timed out would quietly shrink the corpus.
    retryable: bool = False


@dataclasses.dataclass(frozen=True)
class RenderedDocument:
    """A rendered PDF and the number of pages it reports."""

    pdf: bytes
    pages: int


def converter_version(command: str = DEFAULT_CONVERTER) -> str:
    """Return the converter's version string, recorded against every row it renders."""
    try:
        completed = subprocess.run(
            [command, "--version"],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    except FileNotFoundError as exc:
        raise RenderError(
            f"converter {command!r} not found. Rendering needs LibreOffice; install it "
            f"or pass --converter."
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise RenderError(f"converter {command!r} did not report a version") from exc

    version = (completed.stdout or completed.stderr or "").strip().splitlines()
    if not version:
        raise RenderError(f"converter {command!r} reported no version")
    # First line only: LibreOffice adds a build hash and locale notes after it.
    return version[0].strip()


def pdf_page_count(pdf: bytes) -> int:
    """Pages the PDF reports, or raise if it cannot be opened.

    Parsed rather than pattern-matched: the point is to establish that the bytes are a
    PDF a reader can open, which counting `/Type /Page` occurrences would not.
    """
    try:
        reader = PdfReader(io.BytesIO(pdf))
        return len(reader.pages)
    except (PdfReadError, ValueError, OSError) as exc:
        raise ValueError(f"not a readable PDF: {exc}") from exc


def render_document(
    document: bytes,
    document_id: str,
    extension: str,
    work_dir: Path,
    command: str = DEFAULT_CONVERTER,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
) -> RenderedDocument:
    """Render one document, raising ValueError with a per-document reason on failure."""
    work_dir.mkdir(parents=True, exist_ok=True)
    # The converter derives the output name from the input's, so a filesystem-safe stem
    # is needed; ids in this archive contain dots, which is fine, but slashes are not.
    stem = re.sub(r"[^A-Za-z0-9._-]", "_", document_id)
    source_path = work_dir / f"{stem}.{extension.lstrip('.')}"
    source_path.write_bytes(document)
    output_dir = work_dir / "out"
    output_dir.mkdir(exist_ok=True)

    # A dedicated profile directory: LibreOffice refuses to start a second instance
    # against a profile already in use, which makes a shared one a source of
    # intermittent failures that look like document problems.
    profile_dir = work_dir / "profile"
    try:
        completed = subprocess.run(
            [
                command,
                "--headless",
                f"-env:UserInstallation=file://{profile_dir}",
                "--convert-to",
                "pdf",
                "--outdir",
                str(output_dir),
                str(source_path),
            ],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError as exc:
        raise RenderError(f"converter {command!r} not found") from exc
    except subprocess.TimeoutExpired as exc:
        raise RenderTimeout(f"converter timed out after {timeout}s") from exc

    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip().splitlines()
        raise ValueError(
            f"converter exited {completed.returncode}"
            + (f": {detail[-1]}" if detail else "")
        )

    rendered_path = output_dir / f"{stem}.pdf"
    if not rendered_path.exists():
        # LibreOffice exits 0 having written nothing for some inputs, so a zero exit
        # status is not by itself evidence of a rendering.
        raise ValueError("converter exited 0 but wrote no PDF")
    pdf = rendered_path.read_bytes()
    if not pdf:
        raise ValueError("converter wrote an empty PDF")

    pages = pdf_page_count(pdf)
    if pages < 1:
        raise ValueError("rendered PDF reports no pages")
    return RenderedDocument(pdf=pdf, pages=pages)
