"""Synthetic fixtures for the benchmark tests.

Strata are named alpha/beta/gamma and ids are generated, so nothing here reveals or
depends on the private corpus this tool is first used on.
"""

from typing import Any

from sciencebeam_dataset_builder.benchmark.allocate import MetadataRow
from sciencebeam_dataset_builder.benchmark.config import (
    BenchmarkConfig,
    config_from_dict,
)
from sciencebeam_dataset_builder.benchmark.manifest import ManifestRow


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
    default: dict[str, int],
    overrides: dict[str, dict[str, int]] | None = None,
    exclude: list[str] | None = None,
    version: int = 1,
) -> BenchmarkConfig:
    data: dict[str, Any] = {
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
