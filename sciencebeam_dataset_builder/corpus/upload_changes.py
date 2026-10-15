"""Uploading a release that `apply_changes` wrote and someone checked: one commit, or none.

The commit names the commit the release was made from as its parent, so it fails if
`main` has moved since rather than writing over what moved it. Every file is read back
by sha256 before the release is tagged, so a tag never points at an unchecked release.
"""

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sciencebeam_dataset_builder.corpus.apply_changes import (
    ApplyError,
    ReleaseRecord,
    record_path,
)
from sciencebeam_dataset_builder.corpus.repo_reader import HubRepo, RepoReadError

if TYPE_CHECKING:
    from huggingface_hub import HfApi

MAX_ATTEMPTS = 3


class UploadError(RuntimeError):
    """A release that was not uploaded, or not verified once it was."""


@dataclass(frozen=True)
class Uploaded:
    repo_id: str
    version: str
    commit: str


def upload_release(root: Path, token: str | None = None) -> Uploaded:
    """Commit the release written under `root` onto the commit it was made from, check
    every file, then tag it."""
    try:
        record = ReleaseRecord.read(record_path(root))
    except ApplyError as exc:
        raise UploadError(f"{exc}; write the release first, without --upload") from exc
    if not record.commit:
        raise UploadError(
            "this release was made from a local directory, so the commit it applies to "
            "is unknown; apply it again from the Hub to upload it"
        )
    _check_unchanged(root, record)
    api = _api(token)
    if _head(record, token) != record.commit:
        raise UploadError(_moved(record))
    if record.version in {
        t.name for t in api.list_repo_refs(record.repo_id, repo_type="dataset").tags
    }:
        raise UploadError(f"{record.version} is already a tag of {record.repo_id}")

    commit = _commit(root, record, token)
    _verify(api, record, commit)
    api.create_tag(
        record.repo_id,
        tag=record.version,
        revision=commit,
        repo_type="dataset",
        tag_message=f"Release {record.version}",
    )
    return Uploaded(record.repo_id, record.version, commit)


def _check_unchanged(root: Path, record: ReleaseRecord) -> None:
    for path, expected in record.files.items():
        local = root / path
        if not local.is_file() or _sha256(local.read_bytes()) != expected:
            raise UploadError(
                f"{path} changed after the release was written; apply it again"
            )


def _commit(root: Path, record: ReleaseRecord, token: str | None) -> str:
    """One commit with every file. Transfers fail part way, so a failure is retried -
    unless an earlier attempt in fact landed, which counts as done."""
    from huggingface_hub import CommitOperationAdd

    operations = [CommitOperationAdd(path, str(root / path)) for path in record.files]
    last: Exception | None = None
    for _ in range(MAX_ATTEMPTS):
        try:
            info = _api(token).create_commit(
                record.repo_id,
                operations,
                commit_message=f"Release {record.version}",
                commit_description=_description(record),
                repo_type="dataset",
                parent_commit=record.commit,
            )
            return str(info.oid)
        except Exception as exc:  # noqa: BLE001 - Hub transfers fail in varied ways
            last = exc
        head = _head(record, token)
        if head == record.commit:
            continue
        if _holds(_api(token), record, head):
            return head
        raise UploadError(_moved(record)) from last
    raise UploadError(f"the commit failed {MAX_ATTEMPTS} times: {last}") from last


def _verify(api: "HfApi", record: ReleaseRecord, commit: str) -> None:
    if not _holds(api, record, commit):
        raise UploadError(
            f"commit {commit[:12]} does not hold the files as written, so it is not "
            f"tagged; check it on the Hub before anything else"
        )


def _holds(api: "HfApi", record: ReleaseRecord, revision: str) -> bool:
    """Whether every file at `revision` has the sha256 it was written with.

    Large files carry their sha256 in the repo's metadata; the rest are read back.
    """
    try:
        infos = api.get_paths_info(
            record.repo_id, list(record.files), repo_type="dataset", revision=revision
        )
        remote: dict[str, str] = {}
        for info in infos:
            lfs: Any = getattr(info, "lfs", None)
            if lfs is not None:
                remote[info.path] = lfs.sha256
            else:
                local = api.hf_hub_download(
                    record.repo_id, info.path, repo_type="dataset", revision=revision
                )
                remote[info.path] = _sha256(Path(str(local)).read_bytes())
    except Exception:  # noqa: BLE001 - an unreadable file is an unverified one
        return False
    return remote == dict(record.files)


def _head(record: ReleaseRecord, token: str | None) -> str:
    try:
        return HubRepo(record.repo_id, token=token).commit
    except RepoReadError as exc:
        raise UploadError(str(exc)) from exc


def _moved(record: ReleaseRecord) -> str:
    return (
        f"main has moved since {record.version} was made from {record.commit[:12]}; "
        f"remove the written release and apply it again"
    )


def _description(record: ReleaseRecord) -> str:
    counts = ", ".join(f"{n} {kind}" for kind, n in record.changes.items())
    return (
        f"Applies the changes merged since {record.base_release}: {counts}. Written by "
        f"the corpus change tool from commit {record.commit[:12]}."
    )


def _api(token: str | None) -> "HfApi":
    from huggingface_hub import HfApi

    return HfApi(token=token)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
