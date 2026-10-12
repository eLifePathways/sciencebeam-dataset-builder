"""Checking requested changes document by document, before anything is proposed."""

from collections.abc import Sequence
from dataclasses import dataclass, field

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.change_files import (
    ChangeFileError,
    Correction,
    Removal,
    append_removals,
    next_local_change_id,
    read_correction,
    read_removals,
)
from sciencebeam_dataset_builder.corpus.ledger import Change, LedgerError, read_ledger
from sciencebeam_dataset_builder.corpus.lookup import Location, Repo, Status, lookup

_READ_ERRORS = (OSError, ChangeFileError, LedgerError)


@dataclass(frozen=True)
class RemovalCheck:
    """One requested removal, and whether it may be proposed."""

    query: str
    reason: str
    location: Location | None
    removal: Removal | None
    errors: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.errors


@dataclass(frozen=True)
class RemovalProposal:
    """What one repo's `removals.yml` becomes."""

    repo: Repo
    removals: tuple[Removal, ...]
    before: str | None
    after: str


@dataclass
class _Changes:
    """A repo's change files and ledger, as they stand on `main`."""

    files: set[str] = field(default_factory=set)
    removals_text: str | None = None
    removals: list[Removal] = field(default_factory=list)
    ledger: list[Change] = field(default_factory=list)
    error: str = ""

    @property
    def recorded(self) -> set[tuple[str, int]]:
        return {
            (c.id, c.local_change_id)
            for c in self.ledger
            if c.local_change_id is not None
        }


@dataclass
class _Draft:
    query: str
    reason: str
    location: Location | None
    removal: Removal | None = None
    errors: list[str] = field(default_factory=list)


def check_removals(
    requests: Sequence[tuple[str, str]], repos: Sequence[Repo], changed_by: str
) -> tuple[list[RemovalCheck], list[RemovalProposal]]:
    """Check each `(query, reason)`, and propose the ones that pass, one file per repo.

    A document that fails never changes what another one proposes.
    """
    if not changed_by.strip():
        raise ValueError("changed_by is blank")
    results = lookup([query for query, _ in requests], repos)
    changes: dict[str, _Changes] = {}
    seen: set[tuple[str, str]] = set()
    drafts: list[_Draft] = []

    for (query, reason), result in zip(requests, results, strict=True):
        location = result.locations[0] if result.status is Status.FOUND else None
        draft = _Draft(query, reason.strip(), location)
        drafts.append(draft)
        if location is None:
            draft.errors.append(f"{result.status.value}: {result.detail}")
            continue
        key = (location.repo.repo_id, location.id)
        if key in seen:
            draft.errors.append("listed more than once")
        seen.add(key)
        if not draft.reason:
            draft.errors.append("a reason is required")
        if key[0] not in changes:
            changes[key[0]] = _load(location.repo)
        conflicts, corrections = _pending(location, changes[key[0]])
        draft.errors.extend(conflicts)
        if not draft.errors:
            repo_changes = changes[key[0]]
            local_change_id = next_local_change_id(
                location.id, repo_changes.removals, corrections, repo_changes.ledger
            )
            draft.removal = Removal(
                location.id, local_change_id, draft.reason, changed_by
            )

    proposals = []
    for repo in repos:
        mine = [
            d
            for d in drafts
            if d.removal and d.location and d.location.repo.repo_id == repo.repo_id
        ]
        if not mine:
            continue
        before = changes[repo.repo_id].removals_text
        entries = [d.removal for d in mine if d.removal]
        try:
            after = append_removals(before, entries)
        except ChangeFileError as exc:
            for draft in mine:
                draft.errors.append(str(exc))
                draft.removal = None
            continue
        proposals.append(RemovalProposal(repo, tuple(entries), before, after))

    checks = [
        RemovalCheck(d.query, d.reason, d.location, d.removal, tuple(d.errors))
        for d in drafts
    ]
    return checks, proposals


def _load(repo: Repo) -> _Changes:
    try:
        files = set(repo.reader.files())
        text = (
            _read_text(repo, layout.REMOVALS_PATH)
            if layout.REMOVALS_PATH in files
            else None
        )
        removals = read_removals(text)
        ledger = (
            read_ledger(repo.reader.fetch(layout.LEDGER_PATH))
            if layout.LEDGER_PATH in files
            else []
        )
    except _READ_ERRORS as exc:
        return _Changes(error=f"{repo.repo_id}: {exc}")
    return _Changes(files, text, removals, ledger)


def _pending(
    location: Location, changes: _Changes
) -> tuple[list[str], list[Correction]]:
    """Why the document cannot be removed now, and the corrections it has had."""
    if changes.error:
        return [changes.error], []
    recorded = changes.recorded
    errors = [
        f"a removal is already pending (local_change_id {r.local_change_id})"
        for r in changes.removals
        if r.id == location.id and (r.id, r.local_change_id) not in recorded
    ]
    path = layout.correction_paths(location.id).metadata
    if path not in changes.files:
        return errors, []
    try:
        corrections = read_correction(_read_text(location.repo, path), location.id)
    except _READ_ERRORS as exc:
        return [*errors, str(exc)], []
    errors += [
        f"a correction is pending (local_change_id {c.local_change_id})"
        for c in corrections
        if (location.id, c.local_change_id) not in recorded
    ]
    return errors, corrections


def _read_text(repo: Repo, path: str) -> str:
    return repo.reader.fetch(path).read_text(encoding="utf-8")
