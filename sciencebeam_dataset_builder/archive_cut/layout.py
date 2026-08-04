"""What things are called and where they live, locally and in a published repo.

Collected in one module because three steps and both ends of the pipeline need the same
answers: the cut writes a version, rendering adds to it, publishing puts it in a repo, and
the next cut reads the previous version back out. Spread across those modules, the naming
was also what coupled them to each other.
"""

import re
from collections.abc import Iterable

from sciencebeam_dataset_builder.archive_cut.config import CorpusConfig

# Within a local version directory.
ADDED_DIRECTORY = "added"
RENDERED_DIRECTORY = "rendered"
PUBLISHED_DIRECTORY = "published"
FAILURES_FILENAME = "render-failures.csv"

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
