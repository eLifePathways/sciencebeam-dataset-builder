"""What things are called and where they live, locally and in a published repo.

Collected in one module because three steps and both ends of the pipeline need the same
answers: the cut writes a version, rendering adds to it, publishing puts it in a repo, and
the next cut reads the previous version back out. Spread across those modules, the naming
was also what coupled them to each other.
"""

import re
from collections.abc import Iterable
from pathlib import Path

from sciencebeam_dataset_builder.archive_cut.config import CorpusConfig

# Within a local version directory. Documents are held one file per (split, shard)
# rather than one per split, so that a run interrupted part way keeps the shards it
# finished: reading a shard costs real bytes and rendering one costs real minutes.
ADDED_DIRECTORY = "added"
RENDERED_DIRECTORY = "rendered"
PUBLISHED_DIRECTORY = "published"
FAILURES_FILENAME = "render-failures.csv"
# Appended only once a shard's files are written and closed, so it never records work
# that is not on disk -- the same discipline the archive build used to be resumable.
COMPLETED_FILENAME = "completed.jsonl"

# Within the published repo.
SPLITS_DIRECTORY = "splits"

# Three digits so a directory of versions keeps sorting correctly past nine.
_VERSION_DIGITS = 3
_VERSION_FILE = re.compile(
    rf"^{SPLITS_DIRECTORY}/(?P<name>.+)-v(?P<version>\d{{{_VERSION_DIGITS},}})\.yml$"
)


def version_stem(name: str, version: int) -> str:
    return f"{name}-v{version:0{_VERSION_DIGITS}d}"


def version_name(config: CorpusConfig) -> str:
    """The corpus's name and version, as its manifest and config are named."""
    return version_stem(config.name, config.version)


def split_path_in_repo(split: str) -> str:
    """One file per split at the repo root, which is what readers expect to name."""
    return f"{split}.parquet"


def manifest_path_in_repo(name: str, version: int) -> str:
    return f"{SPLITS_DIRECTORY}/{version_stem(name, version)}.csv"


def config_path_in_repo(name: str, version: int) -> str:
    return f"{SPLITS_DIRECTORY}/{version_stem(name, version)}.yml"


def published_versions(files: Iterable[str], name: str) -> list[int]:
    """Versions of this corpus already published, ascending.

    Matched on the corpus name, so a repo holding several corpora reports each one's
    versions rather than the highest number in the repo.
    """
    versions: list[int] = []
    for path in files:
        match = _VERSION_FILE.match(path)
        if match and match.group("name") == name:
            versions.append(int(match.group("version")))
    return sorted(versions)


def latest_published_version(files: Iterable[str], name: str) -> int | None:
    versions = published_versions(files, name)
    return versions[-1] if versions else None


def shard_stem(shard_filename: str) -> str:
    """A shard's name without its extension, used to name the files cut from it."""
    return Path(shard_filename).stem


def split_directory(version_dir: Path, stage: str, split: str) -> Path:
    return version_dir / stage / split


def shard_output_path(
    version_dir: Path, stage: str, split: str, shard_filename: str
) -> Path:
    return (
        split_directory(version_dir, stage, split)
        / f"{shard_stem(shard_filename)}.parquet"
    )


def files_by_split(version_dir: Path, stage: str) -> dict[str, list[Path]]:
    """Every written file, grouped by the split whose directory it sits in."""
    root = version_dir / stage
    if not root.is_dir():
        return {}
    return {
        directory.name: sorted(directory.glob("*.parquet"))
        for directory in sorted(root.iterdir())
        if directory.is_dir() and any(directory.glob("*.parquet"))
    }


def completed_path(version_dir: Path, stage: str) -> Path:
    return version_dir / stage / COMPLETED_FILENAME
