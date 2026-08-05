"""Writing parquet whose row groups are sized by bytes rather than by row count.

pyarrow's default is a row count (about a million), which is meaningless where a single
row carries a whole document: 331 rows landed in one row group of 729 MiB. That defeats
the two things row groups are for — a reader had to materialise the whole group to read
one document, and no filter could skip anything, because the group's statistics spanned
every value in the file.

The archive build reached the same conclusion for the same reason: boundaries must follow
payload bytes, with a row cap as a backstop.
"""

import logging
from collections.abc import Sequence
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

LOGGER = logging.getLogger(__name__)

# Small on purpose. The Hub's 100-300 MB guidance exists so its viewer need not re-convert
# files, and a private repo has no viewer — while large groups are exactly what makes
# reading a few documents expensive.
DEFAULT_ROW_GROUP_BYTES = 16 * 1024**2
DEFAULT_ROW_GROUP_MAX_ROWS = 256


def row_payload_sizes(table: pa.Table) -> list[int]:
    """Bytes of variable-width data per row, which is what row group size should follow."""
    sizes = [0] * table.num_rows
    for column in table.columns:
        if not pa.types.is_binary(column.type) and not pa.types.is_string(column.type):
            continue
        lengths = pc.binary_length(column.combine_chunks()).to_pylist()
        for index, length in enumerate(lengths):
            sizes[index] += length or 0
    return sizes


def row_group_boundaries(
    sizes: Sequence[int],
    target_bytes: int = DEFAULT_ROW_GROUP_BYTES,
    max_rows: int = DEFAULT_ROW_GROUP_MAX_ROWS,
) -> list[tuple[int, int]]:
    """(offset, length) per row group, flushing on accumulated bytes or a row cap.

    A single row larger than the target becomes its own group rather than being split:
    parquet rows are atomic, and this archive holds documents over 100 MiB.
    """
    groups: list[tuple[int, int]] = []
    start = 0
    accumulated = 0
    for index, size in enumerate(sizes):
        accumulated += size
        rows = index - start + 1
        if accumulated >= target_bytes or rows >= max_rows:
            groups.append((start, rows))
            start = index + 1
            accumulated = 0
    if start < len(sizes):
        groups.append((start, len(sizes) - start))
    return groups


def write_table_with_byte_sized_row_groups(
    table: pa.Table,
    path: Path,
    target_bytes: int = DEFAULT_ROW_GROUP_BYTES,
    max_rows: int = DEFAULT_ROW_GROUP_MAX_ROWS,
    compression: str = "zstd",
) -> int:
    """Write `table`, returning the number of row groups. Row order is preserved."""
    path.parent.mkdir(parents=True, exist_ok=True)
    groups = row_group_boundaries(
        row_payload_sizes(table), target_bytes=target_bytes, max_rows=max_rows
    )
    with pq.ParquetWriter(path, table.schema, compression=compression) as writer:
        for offset, length in groups:
            writer.write_table(table.slice(offset, length))
    LOGGER.debug("%s: %d row group(s) for %d row(s)", path, len(groups), table.num_rows)
    return len(groups)
