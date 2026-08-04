"""Reading an archive selectively, from a local directory or a dataset repo.

Producing a corpus must not download shards it does not need. Two properties of the
archive make that possible, and neither is this module's to create:

- the shard manifest maps each shard to its stratum and the range of ranks it holds, so
  the shards worth opening are known without probing files;
- rows within a shard are in rank order, so a shard's lowest ranks are at its front and
  only the row groups holding wanted rows need reading.

Both are assumptions about an archive this module does not write, so they are checked
rather than trusted: a wanted document absent from the shard the manifest points at
means the archive has been re-sorted or re-sharded, and that is reported rather than
silently worked around.
"""

import dataclasses
import json
import logging
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol

import pyarrow as pa
import pyarrow.parquet as pq

from sciencebeam_dataset_builder.nested_corpus.allocate import MetadataRow
from sciencebeam_dataset_builder.nested_corpus.config import CorpusConfig

LOGGER = logging.getLogger(__name__)

SHARD_FILENAME_FIELD = "filename"
SHARD_RANK_FROM_FIELD = "rank_from"
SHARD_RANK_TO_FIELD = "rank_to"
SHARD_ROWS_FIELD = "rows"
SHARD_FILE_BYTES_FIELD = "file_bytes"


class SourceError(ValueError):
    """The archive does not match what its own sidecar files describe."""


@dataclasses.dataclass(frozen=True)
class ShardInfo:
    """One shard, as the archive's shard manifest describes it."""

    filename: str
    stratum: str
    rank_from: int
    rank_to: int
    rows: int
    file_bytes: int | None = None

    def covers(self, rank: int) -> bool:
        return self.rank_from <= rank <= self.rank_to


class ArchiveSource(Protocol):
    """Byte access to an archive, wherever it lives.

    Deliberately narrow: parsing and row selection are the same code for a local
    directory and a dataset repo, so identical allocation either way is structural
    rather than something to keep in step by hand.
    """

    @property
    def revision(self) -> str | None:
        """Which revision of the archive this is, where that is knowable."""

    def read_text(self, name: str) -> str:
        """Return a whole sidecar file — the metadata and shard manifests."""

    def open_parquet(self, name: str) -> pq.ParquetFile:
        """Open one shard for reading, ideally without fetching all of it."""


class LocalArchiveSource:
    """An archive as a directory of shards, as it exists just after being built.

    Cutting a first corpus immediately after a local build must not require
    uploading and re-downloading the shards.
    """

    def __init__(self, directory: Path) -> None:
        self.directory = directory

    @property
    def revision(self) -> str | None:
        # A directory has no revision to record. The resulting config says so with a
        # null rather than implying provenance it does not have.
        return None

    def read_text(self, name: str) -> str:
        path = self.directory / name
        if not path.exists():
            raise SourceError(f"{path} not found in the archive directory")
        return path.read_text(encoding="utf-8")

    def open_parquet(self, name: str) -> pq.ParquetFile:
        path = self.directory / name
        if not path.exists():
            raise SourceError(f"{path} not found in the archive directory")
        return pq.ParquetFile(path)


class HfArchiveSource:
    """An archive as a dataset repo, read over ranged requests.

    Sidecars are downloaded whole, being small and cached. Shards are opened through
    the Hub filesystem so that reading a few row groups fetches a few ranges rather
    than the whole file.
    """

    def __init__(self, repo_id: str, revision: str | None = None) -> None:
        from huggingface_hub import HfApi, HfFileSystem

        self.repo_id = repo_id
        self._api = HfApi()
        self._fs = HfFileSystem()
        info = self._api.repo_info(repo_id, repo_type="dataset", revision=revision)
        # Resolve to a commit so the config records which archive was read, not a
        # branch name that could later point elsewhere.
        self._revision = str(info.sha) if info.sha else revision

    @property
    def revision(self) -> str | None:
        return self._revision

    def read_text(self, name: str) -> str:
        from huggingface_hub import hf_hub_download

        path = hf_hub_download(
            repo_id=self.repo_id,
            filename=name,
            repo_type="dataset",
            revision=self._revision,
        )
        return Path(path).read_text(encoding="utf-8")

    def open_parquet(self, name: str) -> pq.ParquetFile:
        revision = self._revision or "main"
        return pq.ParquetFile(
            self._fs.open(f"datasets/{self.repo_id}@{revision}/{name}", "rb")
        )


def parse_metadata(text: str, config: CorpusConfig) -> list[MetadataRow]:
    """Parse the archive's metadata, keeping only id, stratum and rank.

    Every other field the archive records (sizes, source format) belongs to whoever
    wrote it; allocation needs these three.
    """
    rows: list[MetadataRow] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        record = _parse_json_line(line, line_number, config.source.metadata_file)
        rows.append(
            MetadataRow(
                id=str(_require_field(record, config.id_column, line_number)),
                stratum=str(_require_field(record, config.stratum_column, line_number)),
                rank=_require_int(record, config.rank_column, line_number),
            )
        )
    if not rows:
        raise SourceError(f"{config.source.metadata_file} has no records")
    return rows


def parse_shard_manifest(text: str, config: CorpusConfig) -> list[ShardInfo]:
    shards: list[ShardInfo] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        record = _parse_json_line(line, line_number, config.source.shard_manifest_file)
        file_bytes = record.get(SHARD_FILE_BYTES_FIELD)
        shards.append(
            ShardInfo(
                filename=str(_require_field(record, SHARD_FILENAME_FIELD, line_number)),
                stratum=str(_require_field(record, config.stratum_column, line_number)),
                rank_from=_require_int(record, SHARD_RANK_FROM_FIELD, line_number),
                rank_to=_require_int(record, SHARD_RANK_TO_FIELD, line_number),
                rows=_require_int(record, SHARD_ROWS_FIELD, line_number),
                file_bytes=None if file_bytes is None else int(file_bytes),
            )
        )
    if not shards:
        raise SourceError(f"{config.source.shard_manifest_file} has no records")
    return shards


def _parse_json_line(line: str, line_number: int, name: str) -> dict[str, Any]:
    try:
        record = json.loads(line)
    except json.JSONDecodeError as exc:
        raise SourceError(
            f"{name} line {line_number} is not valid JSON: {exc}"
        ) from exc
    if not isinstance(record, dict):
        raise SourceError(f"{name} line {line_number} is not a JSON object")
    return record


def _require_field(record: Mapping[str, Any], field: str, line_number: int) -> Any:
    if field not in record:
        raise SourceError(
            f"line {line_number} has no {field!r} field; found "
            f"{', '.join(sorted(record))}"
        )
    return record[field]


def _require_int(record: Mapping[str, Any], field: str, line_number: int) -> int:
    value = _require_field(record, field, line_number)
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise SourceError(
            f"line {line_number} field {field!r} is not an integer: {value!r}"
        ) from exc


def shards_for(
    wanted: Sequence[MetadataRow], shards: Sequence[ShardInfo]
) -> dict[str, list[MetadataRow]]:
    """Map each shard that must be read to the documents wanted from it.

    Shards holding nothing wanted are simply absent, which is the whole point: a cut
    reads the front of each stratum rather than the archive.
    """
    by_stratum: dict[str, list[ShardInfo]] = {}
    for shard in shards:
        by_stratum.setdefault(shard.stratum, []).append(shard)

    selected: dict[str, list[MetadataRow]] = {}
    for row in wanted:
        shard = _shard_holding(row, by_stratum.get(row.stratum, []))
        selected.setdefault(shard.filename, []).append(row)
    for rows in selected.values():
        rows.sort(key=lambda row: row.rank)
    return selected


def _shard_holding(row: MetadataRow, candidates: Sequence[ShardInfo]) -> ShardInfo:
    for shard in candidates:
        if shard.covers(row.rank):
            return shard
    raise SourceError(
        f"no shard in the manifest holds rank {row.rank} of stratum "
        f"{row.stratum!r} (document {row.id!r}), so the manifest does not describe "
        f"this archive"
    )


def resolve_columns(config: CorpusConfig, available: set[str]) -> list[str]:
    """The configured output columns the archive actually holds.

    The rest — a rendered PDF, the converter version that made it — are added by later
    steps, so their absence here is expected rather than an error. The id and stratum
    columns are not optional: without them a written row cannot be attributed.
    """
    columns = [column for column in config.columns if column in available]
    for required in (config.id_column, config.stratum_column):
        if required not in columns:
            raise SourceError(
                f"the archive has no {required!r} column, so its rows cannot be "
                f"attributed; found {', '.join(sorted(available))}"
            )
    return columns


def iter_document_batches(
    source: ArchiveSource,
    selected: Mapping[str, Sequence[MetadataRow]],
    config: CorpusConfig,
) -> Iterator[pa.Table]:
    """Yield one table per shard, holding only the wanted rows of that shard.

    Per shard rather than one table for everything, so a caller can write as it reads:
    single documents in this archive reach 100 MiB.
    """
    for filename in sorted(selected):
        wanted = selected[filename]
        LOGGER.info("Reading %d document(s) from %s", len(wanted), filename)
        yield _read_shard_rows(source, filename, wanted, config)


def _read_shard_rows(
    source: ArchiveSource,
    filename: str,
    wanted: Sequence[MetadataRow],
    config: CorpusConfig,
) -> pa.Table:
    parquet_file = source.open_parquet(filename)
    id_column = config.id_column
    columns = resolve_columns(config, set(parquet_file.schema_arrow.names))

    # The id column is small and read first, both to locate rows and to confirm this
    # shard really holds them. Row-group statistics would be cheaper but would not
    # catch an archive whose rows have moved.
    ids = parquet_file.read(columns=[id_column]).column(id_column).to_pylist()
    row_of_id = {str(value): index for index, value in enumerate(ids)}

    wanted_indices: list[int] = []
    for row in wanted:
        index = row_of_id.get(row.id)
        if index is None:
            raise SourceError(
                f"document {row.id!r} is not in {filename}, which the shard manifest "
                f"says holds ranks {row.rank} of stratum {row.stratum!r}. The archive "
                f"appears to have been re-sorted or re-sharded."
            )
        wanted_indices.append(index)

    group_indices = _row_groups_for_rows(parquet_file, wanted_indices)
    LOGGER.debug(
        "%s: %d of %d row group(s) hold the %d wanted row(s)",
        filename,
        len(group_indices),
        parquet_file.num_row_groups,
        len(wanted_indices),
    )
    table = parquet_file.read_row_groups(group_indices, columns=list(columns))
    return _select_ids_in_order(table, id_column, [row.id for row in wanted])


def _row_groups_for_rows(
    parquet_file: pq.ParquetFile, row_indices: Sequence[int]
) -> list[int]:
    """Return the row groups covering the given row offsets, in order."""
    boundaries: list[tuple[int, int, int]] = []
    start = 0
    for group_index in range(parquet_file.num_row_groups):
        rows = parquet_file.metadata.row_group(group_index).num_rows
        boundaries.append((start, start + rows, group_index))
        start += rows

    needed: set[int] = set()
    for row_index in row_indices:
        for group_start, group_end, group_index in boundaries:
            if group_start <= row_index < group_end:
                needed.add(group_index)
                break
    return sorted(needed)


def _select_ids_in_order(
    table: pa.Table, id_column: str, wanted_ids: Sequence[str]
) -> pa.Table:
    positions = {
        str(value): index
        for index, value in enumerate(table.column(id_column).to_pylist())
    }
    return table.take([positions[paper_id] for paper_id in wanted_ids])


def describe_read_cost(
    selected: Mapping[str, Sequence[MetadataRow]], shards: Sequence[ShardInfo]
) -> str:
    """Summarise how much of the archive a cut touches, for the plan output."""
    by_filename = {shard.filename: shard for shard in shards}
    selected_bytes = sum(
        by_filename[filename].file_bytes or 0
        for filename in selected
        if filename in by_filename
    )
    total_bytes = sum(shard.file_bytes or 0 for shard in shards)
    share = f"{100 * selected_bytes / total_bytes:.1f}%" if total_bytes else "unknown"
    # An upper bound: these are whole-shard sizes, and a ranged read fetches only the
    # row groups holding wanted rows.
    return (
        f"{len(selected)} of {len(shards)} shard(s) hold the documents to read, "
        f"at most {selected_bytes / 1024**2:.1f} MiB of "
        f"{total_bytes / 1024**2:.1f} MiB ({share} of the archive)"
    )
