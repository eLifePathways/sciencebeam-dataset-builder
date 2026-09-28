"""Writing shards so the Hub stores only what actually changed.

Parquet normally defeats Xet's deduplication: compression is applied per data page, so one
changed row shifts every byte after it and almost every chunk hashes differently. Writing
with content-defined chunking makes the writer pick page boundaries by content, so
unchanged rows land in byte-identical pages. It has to be on from the first upload,
because turning it on later re-encodes everything.
"""

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# Between the 300 and 500 MB the Hub's shard guidance suggests, so a stalled transfer
# costs one shard rather than the corpus.
DEFAULT_SHARD_BYTES = 400 * 1024 * 1024

COMPRESSION = "snappy"

# An upgrade that changes encoding, default page sizing or compression re-encodes every
# row, which costs a full copy of the dataset against quota with no visible symptom. So
# the version is pinned here and a mismatch fails rather than silently rewriting.
PINNED_PYARROW = "23.0.1"

PAYLOAD_COLUMNS = ("pdf", "xml")


class ParquetIOError(ValueError):
    """A shard that cannot be written or read as asked."""


def check_writer_version(pinned: str = PINNED_PYARROW) -> None:
    """Refuse to write with a pyarrow this dataset was not encoded by."""
    if pa.__version__ != pinned:
        raise ParquetIOError(
            f"pyarrow is {pa.__version__}, pinned at {pinned}. An encoder change "
            f"rewrites every row and costs a full copy against quota. Update "
            f"PINNED_PYARROW deliberately, accepting that rewrite."
        )


def write_shard(table: pa.Table, path: str, check_version: bool = True) -> None:
    """Write one shard, with content-defined chunking on."""
    if check_version:
        check_writer_version()
    pq.write_table(
        table,
        path,
        compression=COMPRESSION,
        use_content_defined_chunking=True,
    )


def read_shard(path: str) -> pa.Table:
    return pq.read_table(path)


def row_payload_bytes(table: pa.Table) -> list[int]:
    """Payload size per row. The metadata columns are noise beside `pdf` and `xml`."""
    totals = [0] * table.num_rows
    for name in PAYLOAD_COLUMNS:
        if name not in table.column_names:
            continue
        lengths = pc.fill_null(pc.binary_length(table.column(name)), 0).to_pylist()
        for position, length in enumerate(lengths):
            totals[position] += int(length)
    return totals


def shard_tables(
    table: pa.Table, max_bytes: int = DEFAULT_SHARD_BYTES
) -> list[pa.Table]:
    """Cut `table` into shards no larger than `max_bytes` of payload.

    Row order is preserved, so a shard boundary never reorders a corpus. A single row
    larger than the limit gets a shard of its own rather than being split.
    """
    if max_bytes <= 0:
        raise ParquetIOError(f"max_bytes must be positive, got {max_bytes}")
    if table.num_rows == 0:
        return []

    shards: list[pa.Table] = []
    start = 0
    running = 0
    for position, size in enumerate(row_payload_bytes(table)):
        if running and running + size > max_bytes:
            shards.append(table.slice(start, position - start))
            start = position
            running = 0
        running += size
    shards.append(table.slice(start, table.num_rows - start))
    return shards
