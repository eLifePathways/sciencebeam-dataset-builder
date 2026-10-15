"""Checking documents to add, and saying how `build_cli` adds them.

Adding opens no pull request: a document arrives from the source snapshot through
`build_cli --base-release`, which appends it in a new shard after the existing ones.
What is checked here is whether that build would add it, and where it would land.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.apply_changes import MINOR, next_version
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.ledger import (
    ChangeType,
    read_ledger,
    read_membership,
)
from sciencebeam_dataset_builder.corpus.lookup import (
    QueryError,
    Repo,
    Status,
    lookup,
    parse_query,
)
from sciencebeam_dataset_builder.corpus.registry import Corpus
from sciencebeam_dataset_builder.corpus.routing import (
    NEVER_RECORDED,
    read_licences,
    route,
)
from sciencebeam_dataset_builder.dataset.license.audit_cli import (
    AuditError,
    latest_snapshot,
)
from sciencebeam_dataset_builder.dataset.license.classify_license import classify
from sciencebeam_dataset_builder.dataset.license.extract_license import Licence
from sciencebeam_dataset_builder.dataset.split import SPLIT_NAMES


class AdditionError(ValueError):
    """The inputs an addition needs cannot be read."""


@dataclass(frozen=True)
class BuildInputs:
    """What `build_cli` adds from: the dated snapshots and the licence report."""

    data_dir: Path
    licence_report: Path


@dataclass(frozen=True)
class AdditionCheck:
    """One document asked to be added, and where it would land."""

    query: str
    uid: str
    split: str
    licence: str
    repo_id: str
    errors: tuple[str, ...]
    warnings: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.errors


@dataclass(frozen=True)
class AdditionPlan:
    """The build that adds one repo's documents, and everything else it would add."""

    corpus: Corpus
    tier: Tier
    repo_id: str
    base_release: str
    version: str
    requested: tuple[str, ...]
    also_added: tuple[str, ...]

    @property
    def command(self) -> str:
        return (
            f"make corpus-build CORPUS={self.corpus.name} TIER={self.tier.value} "
            f'RUN_ARGS="--base-release {self.base_release} --version {self.version}"'
        )


def check_additions(
    queries: Sequence[str], repos: Sequence[Repo], inputs: BuildInputs
) -> tuple[list[AdditionCheck], list[AdditionPlan]]:
    """Check each `<source>__<id>`; plan one build per repo the ones that pass land in."""
    try:
        snapshot = latest_snapshot(inputs.data_dir)
        licences = read_licences(inputs.licence_report)
    except (AuditError, OSError, ValueError) as exc:
        raise AdditionError(str(exc)) from exc
    by_repo = {repo.repo_id: repo for repo in repos}
    in_snapshot: dict[str, dict[str, str]] = {}
    members: dict[str, set[str]] = {}
    seen: set[str] = set()
    checks = []
    wanted: dict[str, list[str]] = {}

    for query, result in zip(queries, lookup(queries, repos), strict=True):
        try:
            corpus, id_value = parse_query(query)
        except QueryError as exc:
            checks.append(AdditionCheck(query, "", "", "", "", (str(exc),)))
            continue
        if corpus is None:
            checks.append(
                AdditionCheck(
                    query,
                    "",
                    "",
                    "",
                    "",
                    (
                        "give it as <source>__<id>: a document not yet added has no repo "
                        "to tell its corpus from",
                    ),
                )
            )
            continue
        uid = f"{corpus.source}__{id_value}"
        errors: list[str] = []
        warnings: list[str] = []
        if uid in seen:
            errors.append("listed more than once")
        seen.add(uid)
        if result.status is Status.FOUND:
            where = result.locations[0]
            errors.append(
                f"already in {where.repo.repo_id} ({where.split}); to change it, "
                f"correct it instead"
            )
        elif result.status is not Status.MISSING:
            errors.append(f"{result.status.value}: {result.detail}")

        if corpus.name not in in_snapshot:
            in_snapshot[corpus.name] = _snapshot_splits(snapshot, corpus)
        split = in_snapshot[corpus.name].get(id_value, "")
        if not split:
            errors.append(
                f"not in the snapshot {snapshot.name}; it has to be harvested before it "
                f"can be added"
            )

        verdict = route(uid, licences)
        repo_id = ""
        if verdict.tier not in corpus.tiers:
            errors.append(
                f"its licence routes it to {verdict.tier.value} ({verdict.reason}), and "
                f"{corpus.name} has no {verdict.tier.value} repo"
            )
        else:
            repo_id = corpus.repo_id(verdict.tier)
            if repo_id not in by_repo:
                errors.append(f"{repo_id} cannot be read here")
        if verdict.licence == NEVER_RECORDED:
            warnings.append(
                "its licence was never recorded, so it can only go to a restricted repo"
            )
        elif classify(Licence(verdict.licence, "")).excluded:
            warnings.append(
                f"{verdict.licence} does not allow training under the current policy; "
                f"it is for evaluation only"
            )
        if repo_id in by_repo and not errors:
            repo = by_repo[repo_id]
            if repo_id not in members:
                members[repo_id] = _members(repo)
            warnings += _earlier_removals(repo, id_value)
            wanted.setdefault(repo_id, []).append(id_value)

        checks.append(
            AdditionCheck(
                query,
                uid,
                split,
                verdict.licence,
                repo_id,
                tuple(errors),
                tuple(warnings),
            )
        )

    plans = []
    for repo_id, ids in wanted.items():
        repo = by_repo[repo_id]
        files = repo.reader.files()
        base = layout.latest_release(files) or ""
        if not base:
            raise AdditionError(f"{repo_id} has no release to add to")
        routed = {
            id_value
            for id_value in in_snapshot[repo.corpus.name]
            if route(f"{repo.corpus.source}__{id_value}", licences).tier is repo.tier
        }
        also = sorted(routed - members[repo_id] - set(ids))
        plans.append(
            AdditionPlan(
                repo.corpus,
                repo.tier,
                repo_id,
                base,
                next_version(base, MINOR),
                tuple(ids),
                tuple(also),
            )
        )
    return checks, plans


def _snapshot_splits(snapshot: Path, corpus: Corpus) -> dict[str, str]:
    """id -> split for one corpus in the snapshot, reading only the `id` column."""
    splits: dict[str, str] = {}
    folder = snapshot / corpus.legacy_config
    for split in SPLIT_NAMES:
        for path in sorted(folder.glob(f"{split}-*.parquet")):
            for id_value in (
                pq.read_table(path, columns=["id"]).column("id").to_pylist()
            ):
                splits[id_value] = split
    return splits


def _members(repo: Repo) -> set[str]:
    files = repo.reader.files()
    release = layout.latest_release(files)
    if release is None:
        return set()
    manifest = repo.reader.fetch(layout.release_manifest_path(release))
    return {m.id for m in read_membership(manifest)}


def _earlier_removals(repo: Repo, id_value: str) -> list[str]:
    """Adding a document back silently undoes its removal, so say so."""
    files = repo.reader.files()
    if layout.LEDGER_PATH not in files:
        return []
    return [
        f"it was removed from {repo.repo_id} on top of {c.base_release or 'the first release'}: "
        f"{c.change_reason}; adding it back undoes that"
        for c in read_ledger(repo.reader.fetch(layout.LEDGER_PATH))
        if c.id == id_value and c.change_type is ChangeType.REMOVED
    ]
