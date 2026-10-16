"""Opening checked changes as a pull request on their dataset repo.

A pull request only adds to `changes/`; nothing published changes until a release
applies it.
"""

import difflib
import re
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.repo_reader import (
    HubRepo,
    PullRequest,
    RepoReadError,
)
from sciencebeam_dataset_builder.corpus.validate import (
    ContentCorrectionCheck,
    MetadataCorrectionCheck,
    RemovalCheck,
    RemovalProposal,
    open_conflicts,
)
from sciencebeam_dataset_builder.corpus.validate_additions import (
    AdditionCheck,
    AdditionProposal,
)

if TYPE_CHECKING:
    from huggingface_hub import HfApi

TOOL_NOTE = "Proposed with the corpus change tool."

CorrectionCheck = ContentCorrectionCheck | MetadataCorrectionCheck

# Enough for a typical fix in a pull request description, well under what the Hub takes.
MAX_DESCRIPTION_DIFF = 20_000

_BACKTICKS = re.compile(r"`+")


class ProposalError(RuntimeError):
    """A pull request that could not be opened, or not verified once opened."""


@dataclass(frozen=True)
class Submission:
    """The pull request a proposal became."""

    repo_id: str
    pr_num: int
    pr_url: str
    title: str


def submitter(token: str | None = None) -> str:
    """Whose token this is: what `changed_by` records."""
    from huggingface_hub import HfApi

    try:
        name = HfApi(token=token).whoami().get("name")
    except Exception as exc:  # noqa: BLE001 - Hub calls fail in varied ways
        raise ProposalError(f"cannot tell who is submitting: {exc}") from exc
    if not name:
        raise ProposalError("the Hub token names no user")
    return str(name)


def submit_removals(
    proposal: RemovalProposal, checks: Sequence[RemovalCheck]
) -> Submission:
    """Open one pull request adding `proposal`'s entries to `removals.yml`."""
    reader = _hub_reader(proposal.repo.reader, proposal.repo.repo_id)
    _recheck(
        reader,
        lambda prs: [
            f"pull request #{pr.num} ({pr.url}) now proposes removals here"
            for pr in prs
            if layout.REMOVALS_PATH in pr.files
        ],
    )
    api = _api(reader)
    title = removal_title(proposal)
    files = {layout.REMOVALS_PATH: proposal.after.encode("utf-8")}
    info = _open(api, reader, files, title, removal_description(proposal, checks))
    _verify(api, reader.repo_id, info, files)
    return Submission(reader.repo_id, info.pr_num, info.pr_url, title)


def submit_additions(
    proposal: AdditionProposal, checks: Sequence[AdditionCheck]
) -> Submission:
    """Open one pull request adding `proposal`'s entries to `additions.yml`."""
    reader = _hub_reader(proposal.repo.reader, proposal.repo.repo_id)
    _recheck(
        reader,
        lambda prs: [
            f"pull request #{pr.num} ({pr.url}) now proposes additions here"
            for pr in prs
            if layout.ADDITIONS_PATH in pr.files
        ],
    )
    api = _api(reader)
    title = addition_title(proposal)
    files = {layout.ADDITIONS_PATH: proposal.after.encode("utf-8")}
    info = _open(api, reader, files, title, addition_description(proposal, checks))
    _verify(api, reader.repo_id, info, files)
    return Submission(reader.repo_id, info.pr_num, info.pr_url, title)


def submit_correction(check: CorrectionCheck) -> Submission:
    """Open one pull request for a checked content or metadata correction.

    A document's first content correction is two commits on the one pull request - the
    XML as harvested, then the fix - so its diff is exactly the correction.
    """
    proposal = check.proposal
    if not check.ok or proposal is None:
        raise ProposalError("only a correction that passed its checks can be proposed")
    reader = _hub_reader(proposal.repo.reader, proposal.repo.repo_id)
    _recheck(reader, lambda prs: open_conflicts(proposal.id, prs, removing=False))
    paths = layout.correction_paths(proposal.id)
    fix = {paths.metadata: proposal.metadata_after.encode("utf-8")}
    if proposal.corrected_after is not None:
        fix[paths.corrected] = proposal.corrected_after.encode("utf-8")
    api = _api(reader)
    title = correction_title(check)
    description = correction_description(check)
    if proposal.seed is None:
        info = _open(api, reader, fix, title, description)
    else:
        seed = {paths.corrected: proposal.seed.encode("utf-8")}
        message = f"Start {paths.corrected} from the XML as harvested"
        info = _open(api, reader, seed, message, description)
        try:
            api.create_commit(
                reader.repo_id,
                _operations(fix),
                commit_message=title,
                repo_type="dataset",
                revision=f"refs/pr/{info.pr_num}",
                parent_commit=info.oid,
            )
        except Exception as exc:  # noqa: BLE001 - Hub calls fail in varied ways
            raise ProposalError(
                f"opened {info.pr_url} with the original XML only; adding the fix "
                f"failed: {exc}. Close it and submit again."
            ) from exc
    _verify(api, reader.repo_id, info, fix)
    return Submission(reader.repo_id, info.pr_num, info.pr_url, title)


def removal_title(proposal: RemovalProposal) -> str:
    name = _repo_name(proposal.repo.repo_id)
    if len(proposal.removals) == 1:
        return f"Remove {proposal.removals[0].id} from {name}"
    return f"Remove {len(proposal.removals)} documents from {name}"


def removal_description(
    proposal: RemovalProposal, checks: Sequence[RemovalCheck]
) -> str:
    """What a reviewer needs: what, where, why, by whom, and what was checked."""
    repo = proposal.repo
    located = {
        c.removal.id: c.location
        for c in checks
        if c.removal and c.location and c.location.repo.repo_id == repo.repo_id
    }
    release = next(iter(located.values())).release if located else ""
    commit = repo.reader.commit if isinstance(repo.reader, HubRepo) else ""
    rows = []
    for removal in proposal.removals:
        where = located.get(removal.id)
        split, shard = (where.split, where.parquet_file_name) if where else ("", "")
        rows.append(
            f"| {_cell(removal.id)} | {split} | {shard} | {removal.local_change_id} "
            f"| {_cell(removal.change_reason)} |"
        )
    return "\n".join(
        [
            f"{TOOL_NOTE} Merging records these removals on `main`; the documents "
            "stay in the dataset until a release applies them.",
            "",
            "- **Change type:** removed",
            f"- **Repo:** `{repo.repo_id}` (corpus `{repo.corpus.name}`, "
            f"{repo.tier.value})",
            f"- **Checked against:** release `{release}`, commit `{commit[:12]}`",
            f"- **Submitted by:** {proposal.removals[0].changed_by}",
            f"- **Files changed:** `{layout.REMOVALS_PATH}`",
            "",
            "| id | split | shard | local_change_id | reason |",
            "| --- | --- | --- | ---: | --- |",
            *rows,
            "",
            f"**Validation:** all {len(rows)} passed. Each is in release `{release}` "
            "and has no other change pending on `main` or in an open pull request.",
        ]
    )


def addition_title(proposal: AdditionProposal) -> str:
    name = _repo_name(proposal.repo.repo_id)
    if len(proposal.additions) == 1:
        return f"Add {proposal.additions[0].id} to {name}"
    return f"Add {len(proposal.additions)} documents to {name}"


def addition_description(
    proposal: AdditionProposal, checks: Sequence[AdditionCheck]
) -> str:
    """What a reviewer needs: counts first, so a large harvest can be read, then every
    document with what was said about it."""
    repo = proposal.repo
    first = proposal.additions[0]
    mine = {
        c.addition.id: c for c in checks if c.addition and c.repo_id == repo.repo_id
    }
    counts = Counter((mine[a.id].split, mine[a.id].licence) for a in proposal.additions)
    rows = [
        f"| {_cell(a.id)} | {mine[a.id].split} | {_cell(mine[a.id].licence)} "
        f"| {a.local_change_id} | {_cell(a.change_reason)} |"
        for a in proposal.additions
    ]
    warnings = [
        f"- `{_cell(a.id)}`: {_cell(w)}"
        for a in proposal.additions
        for w in mine[a.id].warnings
    ]
    return "\n".join(
        [
            f"{TOOL_NOTE} Merging records these additions on `main`; the documents "
            "join the dataset when a release applies them, after everything already "
            "published.",
            "",
            "- **Change type:** added",
            f"- **Repo:** `{repo.repo_id}` (corpus `{repo.corpus.name}`, "
            f"{repo.tier.value})",
            f"- **From:** `{first.from_repo}` at commit `{first.from_commit[:12]}`",
            f"- **Submitted by:** {first.changed_by}",
            f"- **Files changed:** `{layout.ADDITIONS_PATH}`",
            "",
            "| split | licence | documents |",
            "| --- | --- | ---: |",
            *(
                f"| {split} | {_cell(licence)} | {n} |"
                for (split, licence), n in sorted(counts.items())
            ),
            "",
            "| id | split | licence | local_change_id | reason |",
            "| --- | --- | --- | ---: | --- |",
            *rows,
            *(["", "**Warnings:**", "", *warnings] if warnings else []),
            "",
            f"**Validation:** all {len(rows)} passed. Each is in `{first.from_repo}` at that "
            "commit, the licence its XML states puts it in this repo, this repo does not "
            "hold it yet, and nothing else is pending for it on `main` or in an open pull "
            "request.",
        ]
    )


def correction_title(check: CorrectionCheck) -> str:
    assert check.proposal is not None
    what = " metadata" if isinstance(check, MetadataCorrectionCheck) else ""
    name = _repo_name(check.proposal.repo.repo_id)
    return f"Correct {check.proposal.id}{what} in {name}"


def correction_description(check: CorrectionCheck) -> str:
    """What a reviewer needs, and how to read the change."""
    proposal, correction, where = check.proposal, check.correction, check.location
    assert proposal is not None and correction is not None and where is not None
    repo = proposal.repo
    commit = repo.reader.commit if isinstance(repo.reader, HubRepo) else ""
    paths = layout.correction_paths(proposal.id)
    if isinstance(check, MetadataCorrectionCheck):
        kept, files = "metadata", f"`{paths.metadata}`"
        body = [
            "| field | now | corrected |",
            "| --- | --- | --- |",
            *(
                f"| {name} | {_value(check.current.get(name))} | {_value(value)} |"
                for name, value in check.changes.items()
            ),
            "",
            "**Validation:** each value has its field's form, and nothing else is "
            "pending on `main` or open for this document.",
        ]
    else:
        kept, files = "XML", f"`{paths.corrected}`, `{paths.metadata}`"
        before = (
            proposal.seed if proposal.seed is not None else proposal.corrected_before
        )
        body = [
            "The first commit is the XML as harvested and the second is the fix, so the "
            "second commit's diff is exactly the correction. The pull request's own "
            "diff shows the whole file as new; the correction is below."
            if proposal.seed is not None
            else "This document has been corrected before, so the diff is against its "
            "last proposed correction.",
            "",
            *_diff_block(before or "", proposal.corrected_after or ""),
            "",
            "**Validation:** the XML parses, and keeps the document's root element, DOI "
            "and licence statement. Nothing else is pending on `main` or open for this "
            "document.",
        ]
    return "\n".join(
        [
            f"{TOOL_NOTE} Merging records this correction on `main`; the dataset "
            f"keeps the current {kept} until a release applies it.",
            "",
            f"- **Change type:** {correction.change_type.value}",
            f"- **Document:** `{proposal.id}` ({where.split}, `{where.parquet_file_name}`)",
            f"- **Repo:** `{repo.repo_id}` (corpus `{repo.corpus.name}`, "
            f"{repo.tier.value})",
            f"- **Checked against:** release `{where.release}`, commit `{commit[:12]}`",
            f"- **Submitted by:** {correction.changed_by}",
            f"- **local_change_id:** {correction.local_change_id}",
            f"- **Files changed:** {files}",
            "",
            "**Reason:**",
            "",
            *(f"> {line}" for line in correction.change_reason.splitlines()),
            "",
            *body,
            *(f"\n**Warning:** {warning}" for warning in check.warnings),
        ]
    )


def xml_diff(before: str, after: str) -> list[str]:
    """Unified diff lines, with lines broken between adjacent tags first, so a JATS
    file held on one line still shows what moved rather than one line for another."""
    return [
        line
        for line in difflib.unified_diff(
            _tag_lines(before), _tag_lines(after), "current", "corrected", lineterm=""
        )
        if not line.startswith(("---", "+++"))
    ]


def _tag_lines(xml: str) -> list[str]:
    return xml.replace("><", ">\n<").splitlines()


def _diff_block(before: str, after: str) -> list[str]:
    """The XML diff as a fenced block, cut short where it would crowd the page."""
    lines, size = [], 0
    diff = xml_diff(before, after)
    for line in diff:
        size += len(line) + 1
        if size > MAX_DESCRIPTION_DIFF:
            break
        lines.append(line)
    text = "\n".join(lines)
    fence = "`" * max([3, *(len(m) + 1 for m in _BACKTICKS.findall(text))])
    cut = len(diff) - len(lines)
    return [
        "**What changes in the XML** (lines broken between tags):",
        "",
        f"{fence}diff",
        text,
        fence,
        *(
            [
                f"*{cut} more line{'' if cut == 1 else 's'} not shown: see the commit's diff.*"
            ]
            if cut
            else []
        ),
    ]


def _value(value: str | None) -> str:
    return "*empty*" if value is None else f"`{_cell(value)}`"


def _hub_reader(reader: Any, repo_id: str) -> HubRepo:
    if not isinstance(reader, HubRepo):
        raise ProposalError(f"{repo_id} is not read from the Hub")
    return reader


def _recheck(
    reader: HubRepo, conflicts: Callable[[list[PullRequest]], list[str]]
) -> None:
    """Confirm the state the change was checked against, immediately before opening.

    `parent_commit` only sets where a pull request branches from; it does not refuse a
    `main` that has moved.
    """
    now = HubRepo(reader.repo_id, reader.revision, reader.token)
    try:
        moved = now.commit != reader.commit
        found = conflicts(now.open_pull_requests())
    except RepoReadError as exc:
        raise ProposalError(str(exc)) from exc
    if moved:
        raise ProposalError(f"{reader.revision} has moved since the check; check again")
    if found:
        raise ProposalError(f"{found[0]}; check again")


def _api(reader: HubRepo) -> "HfApi":
    from huggingface_hub import HfApi

    return HfApi(token=reader.token)


def _operations(files: Mapping[str, bytes]) -> list[Any]:
    from huggingface_hub import CommitOperationAdd

    return [CommitOperationAdd(path, content) for path, content in files.items()]


def _open(
    api: "HfApi",
    reader: HubRepo,
    files: Mapping[str, bytes],
    message: str,
    description: str,
) -> Any:
    """The pull request's first commit, branched from the commit that was checked."""
    try:
        info = api.create_commit(
            reader.repo_id,
            _operations(files),
            commit_message=message,
            commit_description=description,
            repo_type="dataset",
            create_pr=True,
            parent_commit=reader.commit,
        )
    except Exception as exc:  # noqa: BLE001 - Hub calls fail in varied ways
        raise ProposalError(f"could not open the pull request: {exc}") from exc
    if info.pr_num is None or not info.pr_url:
        raise ProposalError("the Hub made a commit but opened no pull request")
    return info


def _verify(api: "HfApi", repo_id: str, info: Any, sent: Mapping[str, bytes]) -> None:
    """Read each file back from the pull request: Hub transfers have truncated before."""
    for path, content in sent.items():
        try:
            local = api.hf_hub_download(
                repo_id, path, repo_type="dataset", revision=f"refs/pr/{info.pr_num}"
            )
            received = Path(str(local)).read_bytes()
        except Exception as exc:  # noqa: BLE001 - Hub calls fail in varied ways
            raise ProposalError(
                f"opened {info.pr_url} but could not read {path} back: {exc}"
            ) from exc
        if received != content:
            raise ProposalError(
                f"opened {info.pr_url} but its {path} is not what was checked; review "
                f"it before merging"
            )


def _repo_name(repo_id: str) -> str:
    return repo_id.rsplit("/", 1)[-1]


def _cell(text: str) -> str:
    return " ".join(text.split()).replace("|", "\\|")
