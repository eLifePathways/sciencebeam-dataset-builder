"""Add a rendered PDF to a cut corpus version, reporting every document that failed.

Reads the documents a cut added and writes them back with `pdf` and
`pdf_converter_version` columns. Failures are listed by id in `render-failures.csv`,
which is what lets the publishing step leave them out of the published manifest and add
them to the config's exclusions, so the same document is not attempted again on every
run and a later version backfills the gap.
"""

import argparse
import csv
import json
import logging
import shutil
import sys
import tempfile
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from sciencebeam_dataset_builder.archive_cut.layout import (
    ADDED_DIRECTORY,
    FAILURES_FILENAME,
    RENDERED_DIRECTORY,
    completed_path,
    files_by_split,
    shard_output_path,
)
from sciencebeam_dataset_builder.archive_cut.progress import (
    read_completed,
    record_completed,
    write_atomically,
)
from sciencebeam_dataset_builder.archive_cut.render import (
    DEFAULT_CONVERTER,
    DEFAULT_TIMEOUT_SECONDS,
    RenderError,
    RenderFailure,
    RenderTimeout,
    RenderedDocument,
    converter_version,
    render_document,
)

LOGGER = logging.getLogger(__name__)

FAILURE_FIELDS = ["id", "reason", "retryable"]
TIMEOUT_ATTEMPTS = 2

PDF_COLUMN = "pdf"
CONVERTER_COLUMN = "pdf_converter_version"


def read_failures(path: Path) -> list[RenderFailure]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as f:
        return [_failure_from_row(row) for row in csv.DictReader(f)]


def _failure_from_row(row: dict[str, str | None]) -> RenderFailure:
    reason = row.get("reason") or ""
    recorded = row.get("retryable")
    # Files written before the column existed still have to be read correctly, so fall
    # back to what the reason says.
    retryable = (
        recorded.strip().lower() in {"true", "1", "yes"}
        if recorded is not None
        else "timed out" in reason
    )
    return RenderFailure(id=row.get("id") or "", reason=reason, retryable=retryable)


def merge_failures(
    existing: Sequence[RenderFailure], found: Sequence[RenderFailure]
) -> list[RenderFailure]:
    """Keep every failure ever recorded for this version, newest reason winning.

    A resumed run only renders the shards it has left, so rewriting the file from what
    this run saw would forget the failures of the shards it skipped.
    """
    merged = {failure.id: failure for failure in existing}
    for failure in found:
        merged[failure.id] = failure
    return [merged[key] for key in sorted(merged)]


def write_failures(path: Path, failures: Sequence[RenderFailure]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FAILURE_FIELDS)
        writer.writeheader()
        for failure in failures:
            writer.writerow(
                {
                    "id": failure.id,
                    "reason": failure.reason,
                    "retryable": str(failure.retryable).lower(),
                }
            )


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
    on_progress: Callable[[int], object] | None = None,
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
                rendered = _render_with_timeout_retries(
                    document=bytes(document),
                    document_id=document_id,
                    extension=extension,
                    root=root,
                    index=index,
                    command=command,
                    timeout=timeout,
                )
            except RenderTimeout as exc:
                LOGGER.warning("Gave up rendering %s: %s", document_id, exc)
                failures.append(
                    RenderFailure(id=document_id, reason=str(exc), retryable=True)
                )
                if on_progress is not None:
                    on_progress(1)
                continue
            except ValueError as exc:
                LOGGER.warning("Failed to render %s: %s", document_id, exc)
                failures.append(RenderFailure(id=document_id, reason=str(exc)))
                if on_progress is not None:
                    on_progress(1)
                continue
            LOGGER.debug("Rendered %s (%d page(s))", document_id, rendered.pages)
            keep.append(index)
            pdfs.append(rendered.pdf)
            if on_progress is not None:
                on_progress(1)

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


def _render_with_timeout_retries(
    *,
    document: bytes,
    document_id: str,
    extension: str,
    root: Path,
    index: int,
    command: str,
    timeout: int,
    attempts: int = TIMEOUT_ATTEMPTS,
) -> RenderedDocument:
    """Render one document, giving a timeout another go in a fresh directory.

    LibreOffice hangs occasionally rather than predictably, so one timeout is not
    evidence about the document. Each attempt gets its own working directory, so a stale
    profile from the hung attempt cannot cause the next one.
    """
    for attempt in range(1, attempts + 1):
        try:
            return render_document(
                document=document,
                document_id=document_id,
                extension=extension,
                work_dir=root / f"{index}-{attempt}",
                command=command,
                timeout=timeout,
            )
        except RenderTimeout:
            if attempt == attempts:
                raise
            LOGGER.warning(
                "Rendering %s timed out (attempt %d/%d); trying again",
                document_id,
                attempt,
                attempts,
            )
    raise RenderTimeout(f"converter timed out after {timeout}s")


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
        "--retry-failed",
        action="store_true",
        help=(
            "Render again the shards holding documents that failed retryably, such as "
            "a timeout. Useful with a longer --timeout."
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
    added = files_by_split(args.version_dir, ADDED_DIRECTORY)
    if not added:
        # A version that added nothing is legitimate — a rerun of an unchanged config.
        print(
            f"Nothing in {ADDED_DIRECTORY}/ under {args.version_dir}: nothing to render"
        )
        return

    version = converter_version(args.converter)
    LOGGER.info("Converter: %s", version)

    completed_file = completed_path(args.version_dir, RENDERED_DIRECTORY)
    failures_path = args.version_dir / FAILURES_FILENAME
    if args.retry_failed:
        reopen_retryable(args.version_dir, added, completed_file, failures_path)
    completed = read_completed(completed_file)
    all_failures: list[RenderFailure] = []
    rendered_counts: dict[str, int] = {}
    for rows in completed.values():
        for split, count in rows.items():
            rendered_counts[split] = rendered_counts.get(split, 0) + count

    todo = [
        (split, path)
        for split, paths in sorted(added.items())
        for path in paths
        if f"{split}/{path.name}" not in completed
    ]
    if completed:
        LOGGER.info("Resuming: %d shard file(s) already rendered", len(completed))

    total = sum(pq.read_metadata(path).num_rows for _, path in todo)
    progress = tqdm(total=total, unit="doc", desc="Rendering")
    try:
        for split, source_path in todo:
            table = pq.read_table(source_path)
            rendered, failures = render_table(
                table,
                version,
                command=args.converter,
                timeout=args.timeout,
                work_dir=args.work_dir,
                on_progress=progress.update,
            )
            if failures and args.on_failure == "abort":
                raise RenderError(
                    f"{len(failures)} document(s) failed to render, first "
                    f"{failures[0].id!r}: {failures[0].reason}"
                )
            target = shard_output_path(
                args.version_dir, RENDERED_DIRECTORY, split, source_path.name
            )
            write_atomically(
                target,
                _parquet_writer(rendered),
            )
            # Recorded only once the file is closed and renamed, and the failures are
            # persisted first so a crash cannot lose them while claiming the shard done.
            all_failures.extend(failures)
            write_failures(
                failures_path,
                merge_failures(read_failures(failures_path), failures),
            )
            record_completed(
                completed_file,
                f"{split}/{source_path.name}",
                {split: rendered.num_rows},
            )
            rendered_counts[split] = rendered_counts.get(split, 0) + rendered.num_rows
    finally:
        progress.close()

    recorded = read_failures(failures_path)
    for split, count in sorted(rendered_counts.items()):
        print(f"{split}: {count} document(s) rendered")
    if recorded:
        print(f"{len(recorded)} document(s) failed to render:")
        for failure in recorded:
            print(f"  {failure.id}: {failure.reason}")
        print(f"Listed in {FAILURES_FILENAME}, to be excluded when publishing")
    else:
        print("No rendering failures")


def reopen_retryable(
    version_dir: Path,
    added: Mapping[str, list[Path]],
    completed_file: Path,
    failures_path: Path,
) -> None:
    """Forget the shards holding retryably-failed documents, so they render again.

    Their completion records and failure rows are dropped together: a shard is either
    finished with its failures recorded, or not finished at all.
    """
    failures = read_failures(failures_path)
    retryable = {failure.id for failure in failures if failure.retryable}
    if not retryable:
        print("No retryable failures to render again")
        return

    completed = read_completed(completed_file)
    reopened: set[str] = set()
    for split, paths in added.items():
        for path in paths:
            key = f"{split}/{path.name}"
            if key not in completed:
                continue
            ids = {
                str(value)
                for value in pq.read_table(path, columns=["id"])
                .column("id")
                .to_pylist()
            }
            if ids & retryable:
                reopened.add(key)
                target = shard_output_path(
                    version_dir, RENDERED_DIRECTORY, split, path.name
                )
                target.unlink(missing_ok=True)

    remaining = {key: rows for key, rows in completed.items() if key not in reopened}
    _rewrite_completed(completed_file, remaining)
    write_failures(failures_path, [f for f in failures if f.id not in retryable])
    print(
        f"Rendering {len(reopened)} shard file(s) again for "
        f"{len(retryable)} retryable failure(s)"
    )


def _rewrite_completed(path: Path, records: Mapping[str, Mapping[str, int]]) -> None:
    lines = [
        json.dumps({"shard": shard, "rows": dict(rows)})
        for shard, rows in records.items()
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f"{line}\n" for line in lines), encoding="utf-8")


def _parquet_writer(table: pa.Table) -> "Callable[[Path], None]":
    def write(target: Path) -> None:
        pq.write_table(table, target, compression="zstd")

    return write


if __name__ == "__main__":
    main()
