"""Checking additions before they are proposed: where each document lands, and what
stops it.

A document's licence, as its own XML states it, picks the repo. It shares the repo state
`validate` reads for removals and corrections.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.addition_rows import (
    AdditionRowsError,
    available,
)
from sciencebeam_dataset_builder.corpus.change_files import (
    Addition,
    ChangeFileError,
    append_additions,
    next_local_change_id,
    read_correction,
)
from sciencebeam_dataset_builder.corpus.ledger import (
    ChangeType,
    read_ledger,
    read_membership,
)
from sciencebeam_dataset_builder.corpus.lookup import QueryError, Repo, parse_query
from sciencebeam_dataset_builder.corpus.registry import Corpus
from sciencebeam_dataset_builder.corpus.repo_reader import RepoReader
from sciencebeam_dataset_builder.corpus.routing import route_licence
from sciencebeam_dataset_builder.corpus.validate import (
    _READ_ERRORS,
    _Changes,
    _load,
    _read_text,
)
from sciencebeam_dataset_builder.dataset.license.classify_license import classify
from sciencebeam_dataset_builder.dataset.license.extract_license import Licence


@dataclass(frozen=True)
class AdditionCheck:
    """One requested addition: where it would land, and whether it may be proposed."""

    query: str
    reason: str
    id: str
    split: str
    licence: str
    repo_id: str
    addition: Addition | None
    errors: tuple[str, ...]
    warnings: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.errors


@dataclass(frozen=True)
class AdditionProposal:
    """What one repo's `additions.yml` becomes."""

    repo: Repo
    additions: tuple[Addition, ...]
    before: str | None
    after: str


@dataclass
class _AdditionDraft:
    query: str
    reason: str
    id: str = ""
    split: str = ""
    licence: str = ""
    repo_id: str = ""
    addition: Addition | None = None
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def check_additions(
    corpus: Corpus,
    from_repo: str,
    from_commit: str,
    from_reader: RepoReader,
    requests: Sequence[tuple[str, str]],
    repos: Sequence[Repo],
    changed_by: str,
) -> tuple[list[AdditionCheck], list[AdditionProposal]]:
    """Check each `(query, reason)` against what `from_repo` holds at `from_commit`, and
    propose the ones that pass, one file per repo their licence puts them in.

    Raises AdditionRowsError when `from_repo` cannot be read.
    """
    if not changed_by.strip():
        raise ValueError("changed_by is blank")
    try:
        held = available(corpus, from_repo, from_reader)
    except _READ_ERRORS as exc:
        raise AdditionRowsError(f"{from_repo} cannot be read: {exc}") from exc
    mine = {repo.repo_id: repo for repo in repos if repo.corpus == corpus}
    members: dict[str, dict[str, str]] = {}
    unreadable: list[str] = []
    for repo_id, repo in mine.items():
        try:
            members[repo_id] = _membership(repo)
        except _READ_ERRORS as exc:
            unreadable.append(f"{repo_id}: {exc}")
    changes: dict[str, _Changes] = {}
    seen: set[str] = set()
    drafts: list[_AdditionDraft] = []

    for query, reason in requests:
        draft = _AdditionDraft(query, reason.strip())
        drafts.append(draft)
        draft.id, error = _id_in_corpus(corpus, query)
        if error:
            draft.errors.append(error)
            continue
        if draft.id in seen:
            draft.errors.append("listed more than once")
        seen.add(draft.id)
        if not draft.reason:
            draft.errors.append("a reason is required")
        document = held.get(draft.id)
        if document is None:
            draft.errors.append(f"not in {from_repo}")
            continue
        draft.split, draft.licence = document.split, document.licence
        draft.repo_id = _destination(corpus, draft, from_repo, mine)
        if not draft.repo_id:
            continue
        draft.errors += unreadable
        for repo_id, there in members.items():
            if draft.id not in there:
                continue
            if repo_id == draft.repo_id:
                draft.errors.append(f"already in {repo_id} ({there[draft.id]})")
            elif repo_id == from_repo:
                draft.warnings.append(
                    f"still in {from_repo}'s latest release: this can be applied only "
                    f"once its removal there is released"
                )
            else:
                draft.errors.append(
                    f"already in {repo_id}; to move it, add it from there"
                )
        if draft.repo_id not in changes:
            changes[draft.repo_id] = _load(mine[draft.repo_id])
        _check_addition(draft, mine[draft.repo_id], changes[draft.repo_id])
        if not draft.errors:
            draft.addition = _addition(
                draft,
                mine[draft.repo_id],
                changes[draft.repo_id],
                Addition(draft.id, 0, draft.reason, changed_by, from_repo, from_commit),
            )

    proposals = []
    for repo_id, repo in mine.items():
        entries = [d.addition for d in drafts if d.addition and d.repo_id == repo_id]
        if not entries:
            continue
        before = changes[repo_id].additions_text
        try:
            after = append_additions(before, entries)
        except ChangeFileError as exc:
            for draft in drafts:
                if draft.addition and draft.repo_id == repo_id:
                    draft.errors.append(str(exc))
                    draft.addition = None
            continue
        proposals.append(AdditionProposal(repo, tuple(entries), before, after))

    checks = [
        AdditionCheck(
            d.query,
            d.reason,
            d.id,
            d.split,
            d.licence,
            d.repo_id,
            d.addition,
            tuple(d.errors),
            tuple(d.warnings),
        )
        for d in drafts
    ]
    return checks, proposals


def new_in(
    corpus: Corpus, from_repo: str, from_reader: RepoReader, repos: Sequence[Repo]
) -> list[str]:
    """What `from_repo` holds that routes to a repo of `corpus` and that no repo of it
    holds or has removed: a harvest's new documents, in the order `from_repo` has them.

    A removed document comes back only when it is asked for by name.
    """
    try:
        held = available(corpus, from_repo, from_reader)
        taken: set[str] = set()
        for repo in repos:
            if repo.corpus != corpus:
                continue
            taken |= set(_membership(repo))
            if layout.LEDGER_PATH in repo.reader.files():
                ledger = read_ledger(repo.reader.fetch(layout.LEDGER_PATH))
                taken |= {c.id for c in ledger if c.change_type is ChangeType.REMOVED}
    except _READ_ERRORS as exc:
        raise AdditionRowsError(str(exc)) from exc
    return [
        id_value
        for id_value, document in held.items()
        if id_value not in taken
        and route_licence(f"{corpus.source}__{id_value}", document.licence).tier
        in corpus.tiers
    ]


def _id_in_corpus(corpus: Corpus, query: str) -> tuple[str, str]:
    """The id, bare or as `<source>__<id>`, or why `query` names none in `corpus`."""
    try:
        named, id_value = parse_query(query)
    except QueryError as exc:
        return "", str(exc)
    if named is not None and named != corpus:
        return "", f"names the {named.name} corpus, not {corpus.name}"
    return id_value, ""


def _membership(repo: Repo) -> dict[str, str]:
    """id -> split in the repo's latest release; empty before its first."""
    release = layout.latest_release(repo.reader.files())
    if release is None:
        return {}
    manifest = repo.reader.fetch(layout.release_manifest_path(release))
    return {m.id: m.split for m in read_membership(manifest)}


def _destination(
    corpus: Corpus, draft: _AdditionDraft, from_repo: str, mine: Mapping[str, Repo]
) -> str:
    """The repo the document's licence puts it in, or "" with the reason in `draft`."""
    verdict = route_licence(f"{corpus.source}__{draft.id}", draft.licence)
    tier = verdict.tier.value
    if verdict.tier not in corpus.tiers:
        draft.errors.append(
            f"its licence routes it to {tier} ({verdict.reason}), and {corpus.name} "
            f"has no {tier} repo"
        )
        return ""
    repo_id = corpus.repo_id(verdict.tier)
    if repo_id == from_repo:
        draft.errors.append(f"its licence keeps it in {from_repo}, where it already is")
        return ""
    if repo_id not in mine:
        draft.errors.append(f"{repo_id} cannot be read here")
        return ""
    if classify(Licence(draft.licence, "")).excluded:
        draft.warnings.append(
            f"{draft.licence} does not allow training under the current policy; "
            f"it is for evaluation only"
        )
    return repo_id


def _check_addition(draft: _AdditionDraft, repo: Repo, changes: _Changes) -> None:
    """What `repo`'s change files, ledger and open pull requests say about adding it."""
    if changes.error:
        draft.errors.append(changes.error)
        return
    recorded = changes.recorded
    draft.errors += [
        f"an addition is already pending (local_change_id {a.local_change_id})"
        for a in changes.additions
        if a.id == draft.id and (a.id, a.local_change_id) not in recorded
    ]
    draft.errors += [
        f"a removal is already pending (local_change_id {r.local_change_id})"
        for r in changes.removals
        if r.id == draft.id and (r.id, r.local_change_id) not in recorded
    ]
    for pr in changes.pull_requests:
        if layout.ADDITIONS_PATH not in pr.files:
            continue
        where = f"open pull request #{pr.num} ({pr.url})"
        if pr.addition_ids is None:
            draft.errors.append(
                f"{where} changes additions.yml in a way that cannot be read"
            )
        elif draft.id in pr.addition_ids:
            draft.errors.append(f"already proposed for addition in {where}")
        else:
            draft.errors.append(
                f"{where} already proposes additions here; it has to be merged or "
                f"closed first"
            )
    draft.warnings += [
        f"removed from {repo.repo_id} on {c.change_timestamp[:10]}: {c.change_reason}; "
        f"adding it back undoes that"
        for c in changes.ledger
        if c.id == draft.id and c.change_type is ChangeType.REMOVED
    ]


def _addition(
    draft: _AdditionDraft, repo: Repo, changes: _Changes, entry: Addition
) -> Addition | None:
    """`entry`, numbered after everything the document has had in `repo`."""
    path = layout.correction_paths(draft.id).metadata
    try:
        corrections = (
            read_correction(_read_text(repo, path), draft.id)
            if path in changes.files
            else []
        )
    except _READ_ERRORS as exc:
        draft.errors.append(str(exc))
        return None
    local_change_id = next_local_change_id(
        draft.id, changes.removals, changes.additions, corrections, changes.ledger
    )
    return replace(entry, local_change_id=local_change_id)
