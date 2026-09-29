"""What things are called and where they live, in a published corpus repo.

Collected in one module because the builder, the card, the ledger and the verifier all need
the same answers, and spread across them the naming is what couples them to each other.
"""

import re
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum

# The split policy is shared with the live dataset and does not change here. It moves into
# this package when `dataset/` is retired.
from sciencebeam_dataset_builder.dataset.split import SPLIT_NAMES

ORG = "elifepathways"
REPO_PREFIX = "sciencebeam-dataset"

# The pairing is (input format, target format). It is a directory rather than a column, so a
# future `docx-jats` is a new folder instead of a move of every existing file.
PAIRING_PDF_JATS = "pdf-jats"
PAIRING_PDF_DUBLIN_CORE = "pdf-dublin-core"

RELEASES_DIRECTORY = "releases"
CHANGES_DIRECTORY = "changes"
CORRECTIONS_DIRECTORY = f"{CHANGES_DIRECTORY}/corrections"
REMOVALS_DIRECTORY = f"{CHANGES_DIRECTORY}/removals"

LEDGER_PATH = f"{RELEASES_DIRECTORY}/document-changes.csv"
CARD_PATH = "README.md"

REMOVALS_FILENAME = "removals.yml"
REMOVALS_PATH = f"{REMOVALS_DIRECTORY}/{REMOVALS_FILENAME}"

# No `upstream.xml`: the first commit of `corrected.xml` is upstream, so `git log -p` on
# that one file shows every fix as a readable diff without a second file to keep in sync.
CORRECTED_FILENAME = "corrected.xml"
CORRECTION_METADATA_FILENAME = "correction.yml"

# Five digits, so a split's shards keep sorting correctly well past any plausible count.
_SHARD_DIGITS = 5
_SHARD_FILE = re.compile(
    rf"^(?P<split>[a-z]+)-(?P<index>\d{{{_SHARD_DIGITS},}})\.parquet$"
)
_VERSION = re.compile(r"^v(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)$")
_RELEASE_FILE = re.compile(rf"^{RELEASES_DIRECTORY}/(?P<version>v\d+\.\d+\.\d+)\.csv$")


class LayoutError(ValueError):
    """A name or path that cannot be used as written."""


class Tier(Enum):
    """What may be done with a repo's contents, which is also who may see it."""

    OPEN = "open"
    RESTRICTED = "restricted"

    @property
    def public(self) -> bool:
        return self is Tier.OPEN


@dataclass(frozen=True)
class CorrectionPaths:
    """Where one document's correction lives in the repo."""

    directory: str
    corrected: str
    metadata: str


def repo_name(corpus: str, tier: Tier) -> str:
    """`sciencebeam-dataset-ore` public, `sciencebeam-dataset-biorxiv-restricted` private."""
    _check_names_a_path_component(corpus, "corpus")
    suffix = "" if tier is Tier.OPEN else f"-{tier.value}"
    return f"{REPO_PREFIX}-{corpus}{suffix}"


def repo_id(corpus: str, tier: Tier) -> str:
    """The Hub id, org included."""
    return f"{ORG}/{repo_name(corpus, tier)}"


def split_directory(pairing: str, split: str) -> str:
    _check_names_a_path_component(pairing, "pairing")
    _check_split(split)
    return f"{pairing}/{split}"


def shard_path(pairing: str, split: str, index: int) -> str:
    """`pdf-jats/train/train-00000.parquet`.

    The split names both the directory and the file, so a shard written into the wrong
    directory does not match its config glob and is simply not loaded.
    """
    if index < 0:
        raise LayoutError(f"Shard index must not be negative, got {index}")
    return (
        f"{split_directory(pairing, split)}/{split}-{index:0{_SHARD_DIGITS}d}.parquet"
    )


def split_glob(pairing: str, split: str) -> str:
    """The config's `data_files` pattern for one split."""
    return f"{split_directory(pairing, split)}/{split}-*.parquet"


def config_name(corpus: str, pairing: str) -> str:
    """What `load_dataset(repo, ...)` takes. Matches the pairing folder, plus the corpus."""
    _check_names_a_path_component(corpus, "corpus")
    _check_names_a_path_component(pairing, "pairing")
    return f"{corpus}-{pairing}"


def data_files(pairing: str) -> list[dict[str, str]]:
    """The card's `data_files` entries for one pairing, one per split."""
    return [
        {"split": split, "path": split_glob(pairing, split)} for split in SPLIT_NAMES
    ]


def shard_index(path: str) -> int | None:
    """The number of a shard file, or None if `path` does not name one.

    The directory has to agree with the filename, so a file in the wrong split directory
    reads as not a shard rather than as a shard of the directory it sits in.
    """
    parts = path.split("/")
    if len(parts) != 3:
        return None
    _, split, filename = parts
    match = _SHARD_FILE.match(filename)
    if not match or match.group("split") != split:
        return None
    return int(match.group("index"))


def split_shards(files: Iterable[str], pairing: str, split: str) -> list[str]:
    """Every shard belonging to one split, ascending."""
    prefix = f"{split_directory(pairing, split)}/"
    matched = [f for f in files if f.startswith(prefix) and shard_index(f) is not None]
    return sorted(matched, key=lambda f: shard_index(f) or 0)


def next_shard_index(files: Iterable[str], pairing: str, split: str) -> int:
    """The number a new shard takes.

    One past the highest in use, never the lowest free one: a number is never reused, so a
    shard emptied by removals leaves a gap rather than inviting a later file to take its
    place and its published identity.
    """
    used = [shard_index(f) for f in split_shards(files, pairing, split)]
    return max((i for i in used if i is not None), default=-1) + 1


def is_release_version(version: str) -> bool:
    return _VERSION.match(version) is not None


def version_key(version: str) -> tuple[int, int, int]:
    """Sort key for a release name. `v1.10.0` follows `v1.9.0`, which strings do not."""
    match = _VERSION.match(version)
    if not match:
        raise LayoutError(f"{version!r} is not a vMAJOR.MINOR.PATCH release name")
    return int(match["major"]), int(match["minor"]), int(match["patch"])


def release_manifest_path(version: str) -> str:
    """`releases/v1.0.0.csv`."""
    version_key(version)
    return f"{RELEASES_DIRECTORY}/{version}.csv"


def published_releases(files: Iterable[str]) -> list[str]:
    """Every release manifest in the repo, oldest first, ordered numerically."""
    versions = [m["version"] for f in files if (m := _RELEASE_FILE.match(f))]
    return sorted(versions, key=version_key)


def latest_release(files: Iterable[str]) -> str | None:
    releases = published_releases(files)
    return releases[-1] if releases else None


def correction_paths(id_value: str) -> CorrectionPaths:
    """Where a document's correction lives. Flat by `id`, which is already path-safe."""
    _check_names_a_path_component(id_value, "id")
    directory = f"{CORRECTIONS_DIRECTORY}/{id_value}"
    return CorrectionPaths(
        directory=directory,
        corrected=f"{directory}/{CORRECTED_FILENAME}",
        metadata=f"{directory}/{CORRECTION_METADATA_FILENAME}",
    )


def corrected_ids(files: Iterable[str]) -> list[str]:
    """Every document with a correction directory, by `id`."""
    prefix = f"{CORRECTIONS_DIRECTORY}/"
    suffix = f"/{CORRECTED_FILENAME}"
    ids = []
    for f in files:
        if f.startswith(prefix) and f.endswith(suffix):
            middle = f[len(prefix) : -len(suffix)]
            if middle and "/" not in middle:
                ids.append(middle)
    return sorted(ids)


def _check_split(split: str) -> None:
    if split not in SPLIT_NAMES:
        raise LayoutError(f"{split!r} is not one of {SPLIT_NAMES}")


def _check_names_a_path_component(value: str, what: str) -> None:
    if not value or "/" in value or value.startswith(".") or value != value.strip():
        raise LayoutError(
            f"{value!r} cannot be a {what}: it has to name a path component"
        )
