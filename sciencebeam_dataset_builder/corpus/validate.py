"""Checking requested changes document by document, before anything is proposed."""

import re
import xml.etree.ElementTree as ET
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field

import pyarrow as pa
import yaml

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.change_files import (
    ChangeFileError,
    Correction,
    Removal,
    append_correction,
    append_removals,
    next_local_change_id,
    read_correction,
    read_removals,
)
from sciencebeam_dataset_builder.corpus.ledger import (
    Change,
    ChangeType,
    LedgerError,
    read_ledger,
)
from sciencebeam_dataset_builder.corpus.lookup import Location, Repo, Status, lookup
from sciencebeam_dataset_builder.corpus.repo_reader import (
    HubRepo,
    PullRequest,
    RepoReadError,
)
from sciencebeam_dataset_builder.dataset.license.extract_license import (
    extract_licence,
    parse_xml,
)

MAX_XML_BYTES = 20_000_000

_READ_ERRORS = (OSError, pa.ArrowException, ChangeFileError, LedgerError)
_ID_LINE = re.compile(r"^\s*(?:-\s+)?id:\s*(?P<value>.+?)\s*$")


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
class CorrectionProposal:
    """What one correction writes into its document's `changes/corrections/<id>/`.

    `seed` is set only for a document's first content correction: `corrected.xml` is
    committed as it was harvested before the fix is, so the pull request's diff is
    exactly the fix.
    """

    repo: Repo
    id: str
    metadata_before: str | None
    metadata_after: str
    corrected_before: str | None
    corrected_after: str
    seed: str | None


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
    errors += _open_conflicts(location, changes.pull_requests, removing=False)
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
                location.id, changes.removals, corrections, changes.ledger
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
        pull_requests = (
            repo.reader.open_pull_requests() if isinstance(repo.reader, HubRepo) else []
        )
    except _READ_ERRORS as exc:
        return _Changes(error=f"{repo.repo_id}: {exc}")
    return _Changes(files, text, removals, ledger, pull_requests)


def _removal_conflicts(
    location: Location, changes: _Changes
) -> tuple[list[str], list[Correction]]:
    """Why the document cannot be removed now, and the corrections it has had."""
    if changes.error:
        return [changes.error], []
    errors = _pending_removals(location, changes)
    errors += _open_conflicts(location, changes.pull_requests, removing=True)
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


def _open_conflicts(
    location: Location, pull_requests: Sequence[PullRequest], *, removing: bool
) -> list[str]:
    """Open PRs on this document's correction, or proposing its removal.

    When removing, any open removal PR blocks: both would edit the one `removals.yml`.
    """
    directory = layout.correction_paths(location.id).directory + "/"
    errors = []
    for pr in pull_requests:
        where = f"open pull request #{pr.num} ({pr.url})"
        if any(path.startswith(directory) for path in pr.files):
            errors.append(f"{where} changes its correction")
        if layout.REMOVALS_PATH in pr.files:
            if location.id in _ids_added(pr.added[layout.REMOVALS_PATH]):
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
    try:
        root = ET.fromstring(text)
    except ET.ParseError as exc:
        return text, [f"the XML is not well-formed: {exc}"], []

    errors: list[str] = []
    warnings: list[str] = []
    if text == current:
        errors.append("the XML is the same as the current version")
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


def _local_name(element: ET.Element) -> str:
    return element.tag.rsplit("}", 1)[-1]


def _article_doi(root: ET.Element) -> str:
    for element in root.iter():
        if _local_name(element) == "article-id" and element.get("pub-id-type") == "doi":
            return (element.text or "").strip()
    return ""


def _plain_doi(doi: str) -> str:
    return re.sub(r"^https?://(dx\.)?doi\.org/", "", doi.strip().lower())


def _ids_added(lines: Iterable[str]) -> set[str]:
    ids = set()
    for line in lines:
        if match := _ID_LINE.match(line):
            raw = match["value"]
            try:
                value = yaml.safe_load(raw)
            except yaml.YAMLError:
                value = raw
            ids.add(value if isinstance(value, str) else raw)
    return ids


def _read_text(repo: Repo, path: str) -> str:
    return repo.reader.fetch(path).read_text(encoding="utf-8")
