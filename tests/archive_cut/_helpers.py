"""Synthetic fixtures for the corpus tests.

Strata are named alpha/beta/gamma and ids are generated, so nothing here reveals or
depends on the private corpus this tool is first used on.
"""

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from sciencebeam_dataset_builder.archive_cut.allocate import MetadataRow
from sciencebeam_dataset_builder.archive_cut.config import (
    CorpusConfig,
    config_from_dict,
    config_to_dict,
)
from sciencebeam_dataset_builder.archive_cut.manifest import ManifestRow


def paper_id(stratum: str, rank: int) -> str:
    return f"{stratum}-{rank:03d}"


def metadata(**sizes: int) -> list[MetadataRow]:
    """Metadata for the given strata, ranked contiguously from 0 within each."""
    return [
        MetadataRow(id=paper_id(stratum, rank), stratum=stratum, rank=rank)
        for stratum, size in sizes.items()
        for rank in range(size)
    ]


def config(
    *,
    splits: list[str],
    name: str = "sample",
    default: dict[str, int],
    overrides: dict[str, dict[str, int]] | None = None,
    exclude: list[str] | None = None,
    version: int = 1,
) -> CorpusConfig:
    data: dict[str, Any] = {
        "name": name,
        "version": version,
        "splits": splits,
        "columns": ["id", "stratum", "xml"],
        "source": {
            "metadata_file": "metadata.jsonl",
            "shard_manifest_file": "shards.jsonl",
        },
        "allocation": {"default": default, "overrides": overrides or {}},
    }
    if exclude:
        data["exclude"] = exclude
    return config_from_dict(data)


def ranks_in(rows: list[ManifestRow], stratum: str, split: str) -> list[int]:
    """The ranks assigned to one stratum-split, ascending."""
    return sorted(
        row.rank for row in rows if row.stratum == stratum and row.split == split
    )


ARCHIVE_SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        ("stratum", pa.string()),
        ("doc", pa.binary()),
        ("doc_ext", pa.string()),
        ("xml", pa.string()),
    ]
)


def document_bytes(stratum: str, rank: int) -> bytes:
    return f"doc:{stratum}:{rank}".encode()


def document_xml(stratum: str, rank: int) -> str:
    return f"<article>{stratum}-{rank}</article>"


def write_archive(
    directory: Path,
    sizes: dict[str, int],
    rows_per_shard: int = 4,
    rows_per_row_group: int = 2,
) -> None:
    """Write a synthetic archive: shards in (stratum, rank) order, plus both sidecars.

    Small shards and small row groups on purpose, so tests can tell selective reading
    from reading everything.
    """
    directory.mkdir(parents=True, exist_ok=True)
    metadata_lines: list[str] = []
    shard_lines: list[str] = []

    for stratum, size in sizes.items():
        ranks = list(range(size))
        chunks = [
            ranks[start : start + rows_per_shard]
            for start in range(0, len(ranks), rows_per_shard)
        ]
        for shard_index, chunk in enumerate(chunks):
            filename = f"{stratum}-{shard_index:05d}-of-{len(chunks):05d}.parquet"
            _write_shard(directory / filename, stratum, chunk, rows_per_row_group)
            shard_lines.append(
                json.dumps(
                    {
                        "filename": filename,
                        "stratum": stratum,
                        "rank_from": chunk[0],
                        "rank_to": chunk[-1],
                        "rows": len(chunk),
                        "file_bytes": (directory / filename).stat().st_size,
                    }
                )
            )
        for rank in ranks:
            metadata_lines.append(
                json.dumps(
                    {
                        "id": paper_id(stratum, rank),
                        "stratum": stratum,
                        "rank": rank,
                        "doc_ext": "docx",
                    }
                )
            )

    (directory / "metadata.jsonl").write_text(
        "\n".join(metadata_lines) + "\n", encoding="utf-8"
    )
    (directory / "shards.jsonl").write_text(
        "\n".join(shard_lines) + "\n", encoding="utf-8"
    )


def _write_shard(
    path: Path, stratum: str, ranks: list[int], rows_per_row_group: int
) -> None:
    with pq.ParquetWriter(path, ARCHIVE_SCHEMA) as writer:
        for start in range(0, len(ranks), rows_per_row_group):
            group = ranks[start : start + rows_per_row_group]
            writer.write_table(
                pa.table(
                    {
                        "id": [paper_id(stratum, rank) for rank in group],
                        "stratum": [stratum] * len(group),
                        "doc": [document_bytes(stratum, rank) for rank in group],
                        "doc_ext": ["docx"] * len(group),
                        "xml": [document_xml(stratum, rank) for rank in group],
                    },
                    schema=ARCHIVE_SCHEMA,
                )
            )


def write_config(path: Path, cfg: CorpusConfig) -> Path:
    """Write a config as YAML, for tests that drive the CLI."""
    import yaml

    path.write_text(
        yaml.safe_dump(config_to_dict(cfg), sort_keys=False), encoding="utf-8"
    )
    return path


def archive_config(
    *,
    splits: list[str],
    name: str = "sample",
    default: dict[str, int],
    overrides: dict[str, dict[str, int]] | None = None,
    exclude: list[str] | None = None,
    version: int = 1,
) -> CorpusConfig:
    """A config matching the synthetic archive written by `write_archive`."""
    cfg = config(
        name=name,
        splits=splits,
        default=default,
        overrides=overrides,
        exclude=exclude,
        version=version,
    )
    data = config_to_dict(cfg)
    data["columns"] = ["id", "stratum", "doc", "doc_ext", "xml", "pdf"]
    return config_from_dict(data)
