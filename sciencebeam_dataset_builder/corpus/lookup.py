"""Finding documents in the corpus repos' latest releases, by `id` or `<source>__<id>`."""

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

import pyarrow as pa

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.layout import LayoutError, Tier
from sciencebeam_dataset_builder.corpus.ledger import (
    LedgerError,
    Membership,
    read_membership,
)
from sciencebeam_dataset_builder.corpus.parquet_io import PAYLOAD_COLUMNS
from sciencebeam_dataset_builder.corpus.registry import Corpus, corpus_for_source, repos
from sciencebeam_dataset_builder.corpus.repo_reader import (
    HubRepo,
    LocalRepo,
    RepoReader,
    RepoReadError,
)
from sciencebeam_dataset_builder.corpus.schema import FIELD_NAMES

METADATA_COLUMNS = tuple(name for name in FIELD_NAMES if name not in PAYLOAD_COLUMNS)

_READ_ERRORS = (OSError, pa.ArrowException, LedgerError, LayoutError)

# id -> (pairing, shard path, metadata) for every row of one split's shards.
_SplitRows = dict[str, list[tuple[str, str, dict[str, Any]]]]


class QueryError(ValueError):
    """Something typed that cannot name a document."""


class Status(Enum):
    FOUND = "found"
    MISSING = "missing"
    AMBIGUOUS = "ambiguous"
    ERROR = "error"


@dataclass(frozen=True)
class Repo:
    """One corpus repo, and how to read it."""

    corpus: Corpus
    tier: Tier
    reader: RepoReader

    @property
    def repo_id(self) -> str:
        return self.corpus.repo_id(self.tier)


@dataclass(frozen=True)
class Location:
    """Where one document sits in a repo's latest release."""

    repo: Repo
    release: str
    id: str
    split: str
    file_type_pair: str
    shard: str
    metadata: Mapping[str, Any]

    @property
    def parquet_file_name(self) -> str:
        return self.shard.rsplit("/", 1)[-1]

    @property
    def uid(self) -> str:
        return f"{self.repo.corpus.source}__{self.id}"


@dataclass(frozen=True)
class Result:
    """What one query found."""

    query: str
    status: Status
    locations: tuple[Location, ...] = ()
    detail: str = ""


@dataclass
class _Loaded:
    repo: Repo
    files: list[str] = field(default_factory=list)
    release: str = ""
    members: dict[str, Membership] = field(default_factory=dict)
    error: str = ""


def local_repos(output_dir: Path) -> list[Repo]:
    """Every repo in the registry, read from the tree `build_cli` writes."""
    return [
        Repo(corpus, tier, LocalRepo(output_dir / layout.repo_name(corpus.name, tier)))
        for corpus, tier in repos()
    ]


def hub_repos(token: str | None = None) -> list[Repo]:
    """Every repo in the registry, read from the Hub. Nothing is fetched until used."""
    return [
        Repo(corpus, tier, HubRepo(corpus.repo_id(tier), token=token))
        for corpus, tier in repos()
    ]


def parse_query(query: str) -> tuple[Corpus | None, str]:
    """`<source>__<id>` names its corpus; a bare `id` could be in any of them."""
    text = query.strip()
    if not text:
        raise QueryError("blank id")
    source, separator, id_value = text.partition("__")
    if not separator:
        return None, text
    if not source or not id_value:
        raise QueryError(f"{text!r} is not <source>__<id>")
    try:
        return corpus_for_source(source), id_value
    except LayoutError as exc:
        raise QueryError(str(exc)) from exc


def lookup(queries: Sequence[str], repos_to_search: Sequence[Repo]) -> list[Result]:
    """One result per query, in order.

    A repo that cannot be read turns every query that could be in it into an error, never
    a miss.
    """
    loaded = [_load(repo) for repo in repos_to_search]
    cache: dict[tuple[str, str], _SplitRows | str] = {}
    return [_resolve(query, loaded, cache) for query in queries]


def _load(repo: Repo) -> _Loaded:
    try:
        files = repo.reader.files()
        release = layout.latest_release(files) or ""
        manifest = (
            repo.reader.fetch(layout.release_manifest_path(release))
            if release
            else None
        )
        members = {m.id: m for m in read_membership(manifest)} if manifest else {}
    except _READ_ERRORS as exc:
        return _Loaded(repo, error=f"{repo.repo_id}: {exc}")
    return _Loaded(repo, files=files, release=release, members=members)


def _resolve(
    query: str,
    loaded: Sequence[_Loaded],
    cache: dict[tuple[str, str], _SplitRows | str],
) -> Result:
    try:
        corpus, id_value = parse_query(query)
    except QueryError as exc:
        return Result(query, Status.ERROR, detail=str(exc))

    candidates = [
        each for each in loaded if corpus is None or each.repo.corpus == corpus
    ]
    if failed := [each.error for each in candidates if each.error]:
        return Result(query, Status.ERROR, detail="; ".join(failed))

    locations: list[Location] = []
    for each in candidates:
        member = each.members.get(id_value)
        if member is None:
            continue
        try:
            rows = _split_rows(each, member.split, cache).get(id_value, [])
        except _READ_ERRORS as exc:
            return Result(query, Status.ERROR, detail=str(exc))
        if len(rows) != 1:
            return Result(
                query,
                Status.ERROR,
                detail=f"{each.repo.repo_id}: {id_value} is in {each.release} but in "
                f"{len(rows)} {member.split} shards",
            )
        pairing, shard, metadata = rows[0]
        locations.append(
            Location(
                each.repo,
                each.release,
                id_value,
                member.split,
                pairing,
                shard,
                metadata,
            )
        )

    if not locations:
        where = f"{corpus.name}'s repos" if corpus else "any corpus repo"
        return Result(
            query, Status.MISSING, detail=f"not in the latest release of {where}"
        )
    if len(locations) == 1:
        return Result(query, Status.FOUND, tuple(locations))
    names = " and ".join(loc.repo.repo_id for loc in locations)
    if len({loc.repo.corpus.name for loc in locations}) == 1:
        return Result(
            query,
            Status.ERROR,
            tuple(locations),
            detail=f"in both {names}; a document cannot be in two repos",
        )
    return Result(
        query,
        Status.AMBIGUOUS,
        tuple(locations),
        detail=f"in {names}; give it as <source>__{id_value}",
    )


def _split_rows(
    loaded: _Loaded, split: str, cache: dict[tuple[str, str], _SplitRows | str]
) -> _SplitRows:
    """Every row of one split, read once per lookup however many queries need it."""
    key = (loaded.repo.repo_id, split)
    if key not in cache:
        try:
            cache[key] = _read_split(loaded, split)
        except _READ_ERRORS as exc:
            cache[key] = f"{loaded.repo.repo_id}: {exc}"
    hit = cache[key]
    if isinstance(hit, str):
        raise RepoReadError(hit)
    return hit


def _read_split(loaded: _Loaded, split: str) -> _SplitRows:
    rows: _SplitRows = defaultdict(list)
    for pairing in loaded.repo.corpus.pairings:
        for shard in layout.split_shards(loaded.files, pairing, split):
            table = loaded.repo.reader.read_columns(shard, METADATA_COLUMNS)
            for row in table.to_pylist():
                rows[row.pop("id")].append((pairing, shard, row))
    return rows
