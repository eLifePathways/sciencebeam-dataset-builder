"""Add a rendered PDF to a cut corpus version, reporting every document that failed.

Reads the documents a cut added and writes them back with `pdf` and
`pdf_converter_version` columns. Failures are listed by id in `render-failures.csv`,
which is what lets the publishing step leave them out of the published manifest and add
them to the config's exclusions, so the same document is not attempted again on every
run and a later version backfills the gap.
"""

import argparse
import csv
import logging
import shutil
import sys
import tempfile
from collections.abc import Iterator, Sequence
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from sciencebeam_dataset_builder.archive_cut.layout import (
    ADDED_DIRECTORY,
    FAILURES_FILENAME,
    RENDERED_DIRECTORY,
)
from sciencebeam_dataset_builder.archive_cut.render import (
    DEFAULT_CONVERTER,
    DEFAULT_TIMEOUT_SECONDS,
    RenderError,
    RenderFailure,
    converter_version,
    render_document,
)

LOGGER = logging.getLogger(__name__)

FAILURE_FIELDS = ["id", "reason"]

PDF_COLUMN = "pdf"
CONVERTER_COLUMN = "pdf_converter_version"


def read_failures(path: Path) -> list[RenderFailure]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as f:
        return [
            RenderFailure(id=row["id"], reason=row.get("reason", ""))
            for row in csv.DictReader(f)
        ]


def write_failures(path: Path, failures: Sequence[RenderFailure]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FAILURE_FIELDS)
        writer.writeheader()
        for failure in failures:
            writer.writerow({"id": failure.id, "reason": failure.reason})


def render_table(
    table: pa.Table,
    version: str,
    *,
    id_column: str = "id",
    document_column: str = "doc",
    extension_column: str = "doc_ext",
    command: str = DEFAULT_CONVERTER,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    work_dir: Path | None = None,
) -> tuple[pa.Table, list[RenderFailure]]:
    """Return the table with a PDF per row, plus the rows that could not be rendered.

    A failed row is dropped rather than carried with an empty PDF: a row that cannot be
    parsed is worse than a row that is visibly absent.
    """
    ids = [str(value) for value in table.column(id_column).to_pylist()]
    documents = table.column(document_column).to_pylist()
    extensions = [str(value) for value in table.column(extension_column).to_pylist()]

    keep: list[int] = []
    pdfs: list[bytes] = []
    failures: list[RenderFailure] = []

    with _temporary_directory(work_dir) as root:
        for index, (document_id, document, extension) in enumerate(
            zip(ids, documents, extensions, strict=True)
        ):
            try:
                rendered = render_document(
                    document=bytes(document),
                    document_id=document_id,
                    extension=extension,
                    work_dir=root / str(index),
                    command=command,
                    timeout=timeout,
                )
            except ValueError as exc:
                LOGGER.warning("Failed to render %s: %s", document_id, exc)
                failures.append(RenderFailure(id=document_id, reason=str(exc)))
                continue
            LOGGER.debug("Rendered %s (%d page(s))", document_id, rendered.pages)
            keep.append(index)
            pdfs.append(rendered.pdf)

    # Typed explicitly: an empty Python list infers as null, which take() cannot use,
    # and every document of a split failing to render is a case that must still work.
    rendered_table = table.take(pa.array(keep, type=pa.int64()))
    rendered_table = rendered_table.append_column(
        PDF_COLUMN, pa.array(pdfs, type=pa.binary())
    )
    return (
        rendered_table.append_column(
            CONVERTER_COLUMN, pa.array([version] * len(keep), type=pa.string())
        ),
        failures,
    )


def _temporary_directory(work_dir: Path | None) -> "_WorkDir":
    return _WorkDir(work_dir)


class _WorkDir:
    """A working directory, kept if one was named so a failure can be inspected."""

    def __init__(self, path: Path | None) -> None:
        self._path = path
        self._temporary: str | None = None

    def __enter__(self) -> Path:
        if self._path is not None:
            self._path.mkdir(parents=True, exist_ok=True)
            return self._path
        self._temporary = tempfile.mkdtemp(prefix="archive-cut-render-")
        return Path(self._temporary)

    def __exit__(self, *exc_info: object) -> None:
        if self._temporary is not None:
            shutil.rmtree(self._temporary, ignore_errors=True)


def split_files(directory: Path) -> Iterator[Path]:
    yield from sorted(directory.glob("*.parquet"))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Add a rendered PDF to the documents a corpus cut added."
    )
    parser.add_argument(
        "version_dir", type=Path, help="Directory a cut wrote, holding added/."
    )
    parser.add_argument(
        "--converter",
        default=DEFAULT_CONVERTER,
        help=f"Converter command (default: {DEFAULT_CONVERTER}).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        metavar="SECONDS",
        help=f"Per-document timeout (default: {DEFAULT_TIMEOUT_SECONDS}).",
    )
    parser.add_argument(
        "--on-failure",
        choices=["exclude", "abort"],
        default="exclude",
        help=(
            "exclude: drop failed documents and list them (default). "
            "abort: stop on the first failure."
        ),
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        help="Keep intermediate files here instead of a temporary directory.",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        stream=sys.stderr,
    )
    try:
        _run(args)
    except RenderError as exc:
        print(f"RenderError: {exc}", file=sys.stderr)
        sys.exit(1)


def _run(args: argparse.Namespace) -> None:
    added_dir = args.version_dir / ADDED_DIRECTORY
    if not added_dir.is_dir():
        # A version that added nothing is legitimate — a rerun of an unchanged config.
        print(f"No {ADDED_DIRECTORY}/ in {args.version_dir}: nothing to render")
        return

    version = converter_version(args.converter)
    LOGGER.info("Converter: %s", version)

    rendered_dir = args.version_dir / RENDERED_DIRECTORY
    all_failures: list[RenderFailure] = []
    rendered_counts: dict[str, int] = {}

    for source_path in split_files(added_dir):
        table = pq.read_table(source_path)
        LOGGER.info(
            "Rendering %d document(s) from %s", table.num_rows, source_path.name
        )
        rendered, failures = render_table(
            table,
            version,
            command=args.converter,
            timeout=args.timeout,
            work_dir=args.work_dir,
        )
        if failures and args.on_failure == "abort":
            raise RenderError(
                f"{len(failures)} document(s) failed to render, first "
                f"{failures[0].id!r}: {failures[0].reason}"
            )
        all_failures.extend(failures)
        rendered_dir.mkdir(parents=True, exist_ok=True)
        pq.write_table(rendered, rendered_dir / source_path.name, compression="zstd")
        rendered_counts[source_path.stem] = rendered.num_rows

    write_failures(args.version_dir / FAILURES_FILENAME, all_failures)

    for split, count in sorted(rendered_counts.items()):
        print(f"{split}: {count} document(s) rendered")
    if all_failures:
        print(f"{len(all_failures)} document(s) failed to render:")
        for failure in all_failures:
            print(f"  {failure.id}: {failure.reason}")
        print(f"Listed in {FAILURES_FILENAME}, to be excluded when publishing")
    else:
        print("No rendering failures")


if __name__ == "__main__":
    main()
