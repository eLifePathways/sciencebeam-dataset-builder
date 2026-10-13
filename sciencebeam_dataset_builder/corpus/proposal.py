"""Opening checked removals as a pull request on their dataset repo.

A pull request only adds entries to `changes/`; nothing published changes until a
release applies them.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.repo_reader import HubRepo, RepoReadError
from sciencebeam_dataset_builder.corpus.validate import RemovalCheck, RemovalProposal

if TYPE_CHECKING:
    from huggingface_hub import HfApi


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
    """Open one pull request adding `proposal`'s entries to `removals.yml`.

    `parent_commit` only sets where a pull request branches from, so the state it was
    checked against is confirmed again here, immediately before opening it.
    """
    reader = proposal.repo.reader
    if not isinstance(reader, HubRepo):
        raise ProposalError(f"{proposal.repo.repo_id} is not read from the Hub")
    now = HubRepo(reader.repo_id, reader.revision, reader.token)
    try:
        moved = now.commit != reader.commit
        busy = [
            pr for pr in now.open_pull_requests() if layout.REMOVALS_PATH in pr.files
        ]
    except RepoReadError as exc:
        raise ProposalError(str(exc)) from exc
    if moved:
        raise ProposalError(f"{reader.revision} has moved since the check; check again")
    if busy:
        raise ProposalError(
            f"pull request #{busy[0].num} ({busy[0].url}) now proposes removals here; "
            f"check again"
        )

    from huggingface_hub import CommitOperationAdd, HfApi

    api = HfApi(token=reader.token)
    title = removal_title(proposal)
    content = proposal.after.encode("utf-8")
    try:
        info = api.create_commit(
            reader.repo_id,
            [CommitOperationAdd(layout.REMOVALS_PATH, content)],
            commit_message=title,
            commit_description=removal_description(proposal, checks),
            repo_type="dataset",
            create_pr=True,
            parent_commit=reader.commit,
        )
    except Exception as exc:  # noqa: BLE001 - Hub calls fail in varied ways
        raise ProposalError(f"could not open the pull request: {exc}") from exc
    if info.pr_num is None or not info.pr_url:
        raise ProposalError("the Hub made a commit but opened no pull request")
    _verify(api, reader.repo_id, info.pr_num, info.pr_url, content)
    return Submission(reader.repo_id, info.pr_num, info.pr_url, title)


def removal_title(proposal: RemovalProposal) -> str:
    name = proposal.repo.repo_id.rsplit("/", 1)[-1]
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
            "Proposed with the corpus change tool. Merging records these removals on "
            "`main`; the documents stay in the dataset until a release applies them.",
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


def _cell(text: str) -> str:
    return " ".join(text.split()).replace("|", "\\|")


def _verify(api: "HfApi", repo_id: str, pr_num: int, pr_url: str, sent: bytes) -> None:
    """Read the file back from the pull request: Hub transfers have truncated before."""
    try:
        local = api.hf_hub_download(
            repo_id,
            layout.REMOVALS_PATH,
            repo_type="dataset",
            revision=f"refs/pr/{pr_num}",
        )
        if not isinstance(local, str):
            raise ProposalError(f"{layout.REMOVALS_PATH} was not downloaded")
        received = Path(local).read_bytes()
    except Exception as exc:  # noqa: BLE001 - Hub calls fail in varied ways
        raise ProposalError(
            f"opened {pr_url} but could not read it back: {exc}"
        ) from exc
    if received != sent:
        raise ProposalError(
            f"opened {pr_url} but its {layout.REMOVALS_PATH} is not what was checked; "
            f"review it before merging"
        )
