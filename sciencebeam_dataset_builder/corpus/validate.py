"""Checking requested changes document by document, before anything is proposed."""

import html
import re
import xml.etree.ElementTree as ET
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime
from urllib.parse import urlparse

import pyarrow as pa

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.addition_rows import (
    AdditionRowsError,
    available,
)
from sciencebeam_dataset_builder.corpus.change_files import (
    EDITABLE_FIELDS,
    Addition,
    ChangeFileError,
    Correction,
    Removal,
    append_additions,
    append_correction,
    append_removals,
    next_local_change_id,
    read_additions,
    read_correction,
    read_removals,
)
from sciencebeam_dataset_builder.corpus.ledger import (
    Change,
    ChangeType,
    LedgerError,
    read_ledger,
    read_membership,
)
from sciencebeam_dataset_builder.corpus.lookup import (
    Location,
    QueryError,
    Repo,
    Status,
    lookup,
    parse_query,
)
from sciencebeam_dataset_builder.corpus.registry import Corpus
from sciencebeam_dataset_builder.corpus.repo_reader import (
    HubRepo,
    PullRequest,
    RepoReader,
    RepoReadError,
)
from sciencebeam_dataset_builder.corpus.routing import route_licence
from sciencebeam_dataset_builder.dataset.license.classify_license import classify
from sciencebeam_dataset_builder.dataset.license.extract_license import (
    Licence,
    extract_licence,
    parse_xml,
)

MAX_XML_BYTES = 20_000_000

_READ_ERRORS = (OSError, pa.ArrowException, ChangeFileError, LedgerError)
_ENTITY = re.compile(r"&(#[0-9]+|#x[0-9a-fA-F]+|[A-Za-z][A-Za-z0-9]*);")
_XML_ENTITIES = frozenset({"amp", "lt", "gt", "quot", "apos"})


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


@dataclass(frozen=True)
class CorrectionProposal:
    """What one correction writes into its document's `changes/corrections/<id>/`.

    `corrected_after` is None for a metadata correction, which leaves `corrected.xml`
    alone. `seed` is set only for a document's first content correction: `corrected.xml`
    is committed as it was harvested before the fix is, so the pull request's diff is
    exactly the fix.
    """

    repo: Repo
    id: str
    metadata_before: str | None
    metadata_after: str
    corrected_before: str | None = None
    corrected_after: str | None = None
    seed: str | None = None


@dataclass(frozen=True)
class MetadataCorrectionCheck:
    """One requested metadata correction, whether it may be proposed, and what it does.

    `current` is each editable field as it reads now, with any correction merged but not
    yet released applied; `changes` holds only the fields asked to differ from it.
    """

    query: str
    reason: str
    location: Location | None
    correction: Correction | None
    proposal: CorrectionProposal | None
    current: Mapping[str, str | None]
    changes: Mapping[str, str | None]
    errors: tuple[str, ...]
    warnings: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.errors


@dataclass(frozen=True)
class ContentCorrectionCheck:
    """One requested content correction, whether it may be proposed, and what it does.

    `current` is what the document reads as now: its last proposed correction, or the
    XML the dataset ships.
    """

    query: str
    reason: str
    location: Location | None
    correction: Correction | None
    proposal: CorrectionProposal | None
    current: str
    errors: tuple[str, ...]
    warnings: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.errors


@dataclass
class _Changes:
    """A repo's change files and ledger on `main`, and its open pull requests."""

    files: set[str] = field(default_factory=set)
    removals_text: str | None = None
    removals: list[Removal] = field(default_factory=list)
    additions_text: str | None = None
    additions: list[Addition] = field(default_factory=list)
    ledger: list[Change] = field(default_factory=list)
    pull_requests: list[PullRequest] = field(default_factory=list)
    error: str = ""

    @property
    def recorded(self) -> set[tuple[str, int]]:
        return {
            (c.id, c.local_change_id)
            for c in self.ledger
            if c.local_change_id is not None
        }


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
        conflicts, corrections = _removal_conflicts(location, changes[key[0]])
        draft.errors.extend(conflicts)
        if not draft.errors:
            repo_changes = changes[key[0]]
            local_change_id = next_local_change_id(
                location.id,
                repo_changes.removals,
                repo_changes.additions,
                corrections,
                repo_changes.ledger,
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


def check_content_correction(
    query: str, reason: str, xml: bytes, repos: Sequence[Repo], changed_by: str
) -> ContentCorrectionCheck:
    """Check one corrected XML against the document's current version.

    A correction already merged but not yet released does not block another: the new
    one is checked against it and replaces it.
    """
    if not changed_by.strip():
        raise ValueError("changed_by is blank")
    reason = reason.strip()
    [result] = lookup([query], repos)
    if result.status is not Status.FOUND:
        return ContentCorrectionCheck(
            query,
            reason,
            None,
            None,
            None,
            "",
            (f"{result.status.value}: {result.detail}",),
        )
    location = result.locations[0]
    errors = [] if reason else ["a reason is required"]
    changes = _load(location.repo)
    paths = layout.correction_paths(location.id)
    try:
        if changes.error:
            raise RepoReadError(changes.error)
        metadata_before, corrections = _correction_file(location, changes)
        corrected_before = (
            _read_text(location.repo, paths.corrected)
            if paths.corrected in changes.files
            else None
        )
        shipped, upstream = _row_xml(location)
    except _READ_ERRORS as exc:
        return ContentCorrectionCheck(
            query, reason, location, None, None, "", (*errors, str(exc))
        )

    errors += _pending_removals(location, changes)
    errors += open_conflicts(location.id, changes.pull_requests, removing=False)
    if corrected_before is not None and metadata_before is None:
        errors.append(f"{paths.corrected} has no {paths.metadata} beside it")
    if corrected_before is None and upstream is not None:
        errors.append(
            f"the dataset holds a correction of this document but {paths.corrected} "
            f"is missing"
        )
    current = shipped if corrected_before is None else corrected_before
    text, xml_errors, warnings = _check_xml(xml, current, location.metadata.get("doi"))
    errors += xml_errors

    correction = proposal = None
    if not errors:
        correction = Correction(
            next_local_change_id(
                location.id,
                changes.removals,
                changes.additions,
                corrections,
                changes.ledger,
            ),
            ChangeType.CONTENT_CORRECTED,
            reason,
            changed_by,
        )
        try:
            metadata_after = append_correction(metadata_before, location.id, correction)
        except ChangeFileError as exc:
            errors.append(str(exc))
            correction = None
        else:
            proposal = CorrectionProposal(
                location.repo,
                location.id,
                metadata_before,
                metadata_after,
                corrected_before,
                text,
                seed=(upstream or shipped) if corrected_before is None else None,
            )
    return ContentCorrectionCheck(
        query,
        reason,
        location,
        correction,
        proposal,
        current,
        tuple(errors),
        tuple(warnings),
    )


def check_metadata_correction(
    query: str,
    reason: str,
    values: Mapping[str, str],
    repos: Sequence[Repo],
    changed_by: str,
) -> MetadataCorrectionCheck:
    """Check new values for some of a document's editable fields.

    A field whose value is unchanged is left out, so `values` may carry every field. An
    empty value clears the field.
    """
    if not changed_by.strip():
        raise ValueError("changed_by is blank")
    reason = reason.strip()
    [result] = lookup([query], repos)
    if result.status is not Status.FOUND:
        return MetadataCorrectionCheck(
            query,
            reason,
            None,
            None,
            None,
            {},
            {},
            (f"{result.status.value}: {result.detail}",),
        )
    location = result.locations[0]
    errors = [] if reason else ["a reason is required"]
    if unknown := sorted(set(values) - set(EDITABLE_FIELDS)):
        errors.append(f"{', '.join(unknown)} cannot be corrected here")
    changes = _load(location.repo)
    try:
        if changes.error:
            raise RepoReadError(changes.error)
        metadata_before, corrections = _correction_file(location, changes)
    except _READ_ERRORS as exc:
        return MetadataCorrectionCheck(
            query, reason, location, None, None, {}, {}, (*errors, str(exc))
        )

    current = _effective_metadata(location, corrections, changes.recorded)
    requested: dict[str, str | None] = {}
    warnings = []
    for name in EDITABLE_FIELDS:
        if name not in values:
            continue
        value = _normal_value(name, values[name])
        if value == current[name]:
            continue
        requested[name] = value
        if value is None:
            warnings.append(f"it clears {name}, which is {current[name]!r} now")
        elif problem := _field_problem(name, value):
            errors.append(f"{name} {problem}")
    if not requested:
        errors.append("nothing changes: every value is the same as now")
    errors += _pending_removals(location, changes)
    errors += open_conflicts(location.id, changes.pull_requests, removing=False)

    correction = proposal = None
    if not errors:
        correction = Correction(
            next_local_change_id(
                location.id,
                changes.removals,
                changes.additions,
                corrections,
                changes.ledger,
            ),
            ChangeType.METADATA_CORRECTED,
            reason,
            changed_by,
            fields=requested,
        )
        try:
            metadata_after = append_correction(metadata_before, location.id, correction)
        except ChangeFileError as exc:
            errors.append(str(exc))
            correction = None
        else:
            proposal = CorrectionProposal(
                location.repo, location.id, metadata_before, metadata_after
            )
    return MetadataCorrectionCheck(
        query,
        reason,
        location,
        correction,
        proposal,
        current,
        requested,
        tuple(errors),
        tuple(warnings),
    )


def current_metadata(location: Location) -> dict[str, str | None]:
    """Each editable field as it reads now, with any unreleased correction applied."""
    changes = _load(location.repo)
    if changes.error:
        raise RepoReadError(changes.error)
    _, corrections = _correction_file(location, changes)
    return _effective_metadata(location, corrections, changes.recorded)


def current_xml(location: Location) -> str:
    """What the document reads as now: its last correction on `main`, else the XML the
    dataset ships. The text a correction should start from."""
    path = layout.correction_paths(location.id).corrected
    if path in location.repo.reader.files():
        return _read_text(location.repo, path)
    shipped, _ = _row_xml(location)
    return shipped


def _load(repo: Repo) -> _Changes:
    try:
        files = set(repo.reader.files())
        text = (
            _read_text(repo, layout.REMOVALS_PATH)
            if layout.REMOVALS_PATH in files
            else None
        )
        removals = read_removals(text)
        additions_text = (
            _read_text(repo, layout.ADDITIONS_PATH)
            if layout.ADDITIONS_PATH in files
            else None
        )
        additions = read_additions(additions_text)
        ledger = (
            read_ledger(repo.reader.fetch(layout.LEDGER_PATH))
            if layout.LEDGER_PATH in files
            else []
        )
        pull_requests = (
            repo.reader.open_pull_requests() if isinstance(repo.reader, HubRepo) else []
        )
    except _READ_ERRORS as exc:
        return _Changes(error=f"{repo.repo_id}: {exc}")
    return _Changes(
        files, text, removals, additions_text, additions, ledger, pull_requests
    )


def _removal_conflicts(
    location: Location, changes: _Changes
) -> tuple[list[str], list[Correction]]:
    """Why the document cannot be removed now, and the corrections it has had."""
    if changes.error:
        return [changes.error], []
    errors = _pending_removals(location, changes)
    errors += open_conflicts(location.id, changes.pull_requests, removing=True)
    try:
        _, corrections = _correction_file(location, changes)
    except _READ_ERRORS as exc:
        return [*errors, str(exc)], []
    recorded = changes.recorded
    errors += [
        f"a correction is pending (local_change_id {c.local_change_id})"
        for c in corrections
        if (location.id, c.local_change_id) not in recorded
    ]
    return errors, corrections


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


def _pending_removals(location: Location, changes: _Changes) -> list[str]:
    recorded = changes.recorded
    return [
        f"a removal is already pending (local_change_id {r.local_change_id})"
        for r in changes.removals
        if r.id == location.id and (r.id, r.local_change_id) not in recorded
    ]


def _correction_file(
    location: Location, changes: _Changes
) -> tuple[str | None, list[Correction]]:
    """The document's `correction.yml` on `main`, as text and as entries."""
    path = layout.correction_paths(location.id).metadata
    if path not in changes.files:
        return None, []
    text = _read_text(location.repo, path)
    return text, read_correction(text, location.id)


def open_conflicts(
    id_value: str, pull_requests: Sequence[PullRequest], *, removing: bool
) -> list[str]:
    """Open PRs on this document's correction, or proposing its removal.

    When removing, any open removal PR blocks: both would edit the one `removals.yml`.
    """
    directory = layout.correction_paths(id_value).directory + "/"
    errors = []
    for pr in pull_requests:
        where = f"open pull request #{pr.num} ({pr.url})"
        if any(path.startswith(directory) for path in pr.files):
            errors.append(f"{where} changes its correction")
        if layout.REMOVALS_PATH in pr.files:
            if pr.removal_ids is None:
                errors.append(
                    f"{where} changes removals.yml in a way that cannot be read"
                )
            elif id_value in pr.removal_ids:
                errors.append(f"already proposed for removal in {where}")
            elif removing:
                errors.append(
                    f"{where} already proposes removals here; it has to be merged "
                    f"or closed first"
                )
    return errors


def _row_xml(location: Location) -> tuple[str, str | None]:
    """The document's `xml` and `xml_upstream`, read from its shard without its PDF."""
    table = location.repo.reader.read_columns(
        location.shard, ("id", "xml", "xml_upstream")
    )
    for id_value, xml, upstream in zip(
        table.column("id").to_pylist(),
        table.column("xml").to_pylist(),
        table.column("xml_upstream").to_pylist(),
        strict=True,
    ):
        if id_value == location.id:
            return xml, upstream
    raise RepoReadError(f"{location.id} is not in {location.shard}")


def _check_xml(
    data: bytes, current: str, doi: str | None
) -> tuple[str, list[str], list[str]]:
    """The uploaded text, what stops it, and what only warns."""
    if len(data) > MAX_XML_BYTES:
        return "", [f"the XML is larger than {MAX_XML_BYTES // 1_000_000} MB"], []
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        return "", [f"the XML is not UTF-8 text: {exc}"], []
    errors: list[str] = []
    warnings: list[str] = []
    try:
        root = ET.fromstring(text)
    except ET.ParseError as exc:
        # An entity XML does not define, such as `&deg;`, may stay where the current
        # version already has it: a fix should not have to repair what it did not break.
        repaired = parse_xml(text)
        if repaired is None or _parses_strictly(current):
            return text, [f"the XML is not well-formed: {exc}"], []
        root = repaired
        warnings.append(
            f"the XML is not well-formed ({exc}), as the current version is not either"
        )

    if text == current:
        errors.append("the XML is the same as the current version")
    elif _characters(text) == _characters(current):
        errors.append(
            "the only change is how characters are written, such as &deg; for °; "
            "that is not a correction"
        )
    was = parse_xml(current)
    if was is not None and _local_name(root) != _local_name(was):
        errors.append(
            f"its root element is <{_local_name(root)}>, not <{_local_name(was)}>"
        )
    found = _article_doi(root)
    if doi and found and _plain_doi(found) != _plain_doi(doi):
        errors.append(f"its DOI is {found}, not the document's {doi}")
    elif doi and not found:
        warnings.append(
            "it carries no DOI, so it cannot be checked to be the same article"
        )
    before, after = extract_licence(current).label, extract_licence(text).label
    if before != after:
        errors.append(
            f"its licence statement reads {after!r} where the current one reads "
            f"{before!r}; a licence change moves a document between repos, it is not "
            f"a correction"
        )
    return text, errors, warnings


def _effective_metadata(
    location: Location,
    corrections: Sequence[Correction],
    recorded: set[tuple[str, int]],
) -> dict[str, str | None]:
    """The row's values, then each metadata correction not yet released, in order."""
    values = {name: location.metadata.get(name) for name in EDITABLE_FIELDS}
    for correction in corrections:
        if (
            correction.change_type is ChangeType.METADATA_CORRECTED
            and (location.id, correction.local_change_id) not in recorded
        ):
            values.update(correction.fields)
    return values


def _normal_value(name: str, raw: str) -> str | None:
    """Trimmed, empty as None, and a DOI without the resolver in front of it."""
    value = raw.strip()
    if name == "doi":
        value = re.sub(r"^(https?://(dx\.)?doi\.org/|doi:)", "", value, flags=re.I)
    return value or None


def _field_problem(name: str, value: str) -> str:
    """Why a value does not fit its field, or nothing."""
    if name == "doi" and not re.fullmatch(r"10\.\d{4,9}/\S+", value):
        return "is not a DOI like 10.1101/2022.01.01.000000"
    if name == "version" and (len(value) > 32 or re.search(r"\s", value)):
        return "has to be one short word, like 2"
    if name == "pub_date" and not _is_date(value):
        return "has to be a date like 2022-10-31, 2022-10 or 2022"
    if name.endswith("_url"):
        url = urlparse(value)
        if url.scheme not in ("http", "https") or not url.netloc or " " in value:
            return "has to be an http or https address"
    if name.endswith("_downloaded_at") and not _is_timestamp(value):
        return "has to be a time with its time zone, like 2026-10-01T09:14:22+00:00"
    return ""


def _is_date(value: str) -> bool:
    """A real date, as precise as the source is: year, month or day."""
    forms = {r"\d{4}": "%Y", r"\d{4}-\d{2}": "%Y-%m", r"\d{4}-\d{2}-\d{2}": "%Y-%m-%d"}
    for pattern, form in forms.items():
        if re.fullmatch(pattern, value):
            try:
                datetime.strptime(value, form)
            except ValueError:
                return False
            return True
    return False


def _is_timestamp(value: str) -> bool:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return False
    return "T" in value and parsed.tzinfo is not None


def _parses_strictly(xml: str) -> bool:
    try:
        ET.fromstring(xml)
    except ET.ParseError:
        return False
    return True


def _characters(xml: str) -> str:
    """The text with every entity but XML's own five written as its character."""
    return _ENTITY.sub(
        lambda m: m[0] if m[1] in _XML_ENTITIES else html.unescape(m[0]), xml
    )


def _local_name(element: ET.Element) -> str:
    return element.tag.rsplit("}", 1)[-1]


def _article_doi(root: ET.Element) -> str:
    for element in root.iter():
        if _local_name(element) == "article-id" and element.get("pub-id-type") == "doi":
            return (element.text or "").strip()
    return ""


def _plain_doi(doi: str) -> str:
    return re.sub(r"^https?://(dx\.)?doi\.org/", "", doi.strip().lower())


def _read_text(repo: Repo, path: str) -> str:
    return repo.reader.fetch(path).read_text(encoding="utf-8")
