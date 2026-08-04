"""Publishing files to a dataset repo, in few commits, with retries.

Two things learned building the archive this cuts from shape this module:

- **many files per commit.** One commit per file hit HTTP 429 partway through a
  151-shard upload, and the Hub's own guidance is to batch.
- **the manifest goes last.** Data files are committed first and the manifest and config
  only once they are all there, so an interrupted run leaves unreferenced data — harmless
  and squashable — rather than a manifest describing files that are not present.
"""

import dataclasses
import logging
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Protocol

LOGGER = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 16
DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BACKOFF_SECONDS = 5.0


class UploadError(RuntimeError):
    """Publishing failed after exhausting retries."""


@dataclasses.dataclass(frozen=True)
class FileToPublish:
    """A local file and where it belongs in the repo."""

    local_path: Path
    path_in_repo: str


class PublishTarget(Protocol):
    """Where a version is published."""

    def publish(self, files: Sequence[FileToPublish], message: str) -> None:
        """Commit these files together."""

    def tag(self, name: str, message: str) -> None:
        """Mark the published state, so a reader can pin it immutably."""

    def fetch(self, path_in_repo: str, into: Path) -> Path | None:
        """Retrieve an already-published file, or None if there is none."""

    def list_files(self) -> list[str]:
        """Every path already published, so the latest version can be found."""


class LocalPublishTarget:
    """Publish into a directory — a dry run, and what the tests use."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.commits: list[tuple[list[str], str]] = []
        self.tags: list[str] = []

    def publish(self, files: Sequence[FileToPublish], message: str) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        for item in files:
            target = self.directory / item.path_in_repo
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(item.local_path.read_bytes())
        self.commits.append(([item.path_in_repo for item in files], message))
        LOGGER.info("Wrote %d file(s) to %s (%s)", len(files), self.directory, message)

    def tag(self, name: str, message: str) -> None:
        self.tags.append(name)
        LOGGER.info("Tagged %s (%s)", name, message)

    def fetch(self, path_in_repo: str, into: Path) -> Path | None:
        candidate = self.directory / path_in_repo
        return candidate if candidate.exists() else None

    def list_files(self) -> list[str]:
        if not self.directory.is_dir():
            return []
        return sorted(
            str(path.relative_to(self.directory))
            for path in self.directory.rglob("*")
            if path.is_file()
        )


class HfPublishTarget:
    """Publish to a dataset repo on the Hub."""

    def __init__(
        self,
        repo_id: str,
        revision: str | None = None,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
        create: bool = False,
    ) -> None:
        from huggingface_hub import HfApi

        self.repo_id = repo_id
        self.revision = revision
        self._api = HfApi()
        self._max_attempts = max_attempts
        self._backoff_seconds = backoff_seconds
        self._create = create
        self._created = False

    def _ensure_repo(self) -> None:
        """Create the repo, only when asked to.

        Off by default: creating a repo is not something a publish should do as a side
        effect. Private, because a corpus cut from a non-redistributable archive must not
        default to being world-readable.
        """
        if not self._create or self._created:
            return
        self._with_retries(
            lambda: self._api.create_repo(
                repo_id=self.repo_id,
                repo_type="dataset",
                private=True,
                exist_ok=True,
            ),
            what=f"creating {self.repo_id}",
        )
        self._created = True

    def publish(self, files: Sequence[FileToPublish], message: str) -> None:
        from huggingface_hub import CommitOperationAdd

        self._ensure_repo()

        operations = [
            # The repo path comes first and the local path second; they differ here, so
            # naming them is worth the noise.
            CommitOperationAdd(
                path_in_repo=item.path_in_repo, path_or_fileobj=str(item.local_path)
            )
            for item in files
        ]
        self._with_retries(
            lambda: self._api.create_commit(
                repo_id=self.repo_id,
                repo_type="dataset",
                revision=self.revision,
                operations=operations,
                commit_message=message,
            ),
            what=f"commit of {len(files)} file(s)",
        )

    def tag(self, name: str, message: str) -> None:
        self._with_retries(
            lambda: self._api.create_tag(
                repo_id=self.repo_id,
                repo_type="dataset",
                tag=name,
                tag_message=message,
                revision=self.revision,
            ),
            what=f"tag {name}",
        )

    def fetch(self, path_in_repo: str, into: Path) -> Path | None:
        from huggingface_hub import hf_hub_download
        from huggingface_hub.errors import EntryNotFoundError

        try:
            downloaded = hf_hub_download(
                repo_id=self.repo_id,
                filename=path_in_repo,
                repo_type="dataset",
                revision=self.revision,
                local_dir=str(into),
            )
        except EntryNotFoundError:
            return None
        return Path(downloaded)

    def list_files(self) -> list[str]:
        """Every published path, or nothing if the repo does not exist yet.

        Only a missing repo counts as "nothing published" — the normal state before a
        first version. Any other failure propagates, because a run that cannot see the
        previous version must not proceed as though there were none: it would produce a
        version that silently fails to contain its predecessor.
        """
        from huggingface_hub.errors import RepositoryNotFoundError

        files: list[str] = []

        def collect() -> None:
            files.clear()
            files.extend(
                self._api.list_repo_files(
                    self.repo_id, repo_type="dataset", revision=self.revision
                )
            )

        try:
            self._with_retries(collect, what=f"listing {self.repo_id}")
        except UploadError as exc:
            if isinstance(exc.__cause__, RepositoryNotFoundError):
                LOGGER.info("%s does not exist yet; nothing published", self.repo_id)
                return []
            raise
        return files

    def _with_retries(self, action: Callable[[], object], what: str) -> None:
        last_error: Exception | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                action()
                return
            except Exception as exc:  # noqa: BLE001 — retry policy is decided below
                if not _is_retryable(exc) or attempt == self._max_attempts:
                    raise UploadError(f"{what} failed: {exc}") from exc
                last_error = exc
                delay = _retry_after(exc) or self._backoff_seconds * attempt
                LOGGER.warning(
                    "%s failed (attempt %d/%d): %s — retrying in %.0fs",
                    what,
                    attempt,
                    self._max_attempts,
                    exc,
                    delay,
                )
                time.sleep(delay)
        raise UploadError(f"{what} failed: {last_error}")


def _is_retryable(exc: Exception) -> bool:
    """Rate limiting and server errors are worth retrying; a 403 is not."""
    status = _status_code(exc)
    return status == 429 or (status is not None and 500 <= status < 600)


def _status_code(exc: Exception) -> int | None:
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    return int(status) if isinstance(status, int) else None


def _retry_after(exc: Exception) -> float | None:
    """Honour the server's own Retry-After rather than guessing."""
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None) or {}
    value = headers.get("Retry-After") if hasattr(headers, "get") else None
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def batched(
    files: Sequence[FileToPublish], size: int = DEFAULT_BATCH_SIZE
) -> list[list[FileToPublish]]:
    if size < 1:
        raise ValueError("batch size must be at least 1")
    return [list(files[start : start + size]) for start in range(0, len(files), size)]
