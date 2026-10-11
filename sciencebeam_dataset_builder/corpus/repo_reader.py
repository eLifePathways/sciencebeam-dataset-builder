"""Reading a corpus repo's files, wherever the repo is."""

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

import pyarrow as pa
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from huggingface_hub import HfApi, HfFileSystem


class RepoReadError(OSError):
    """A repo, or a file in it, that cannot be read."""


class RepoReader(Protocol):
    def files(self) -> list[str]:
        """Every file in the repo, as repo-relative paths."""

    def fetch(self, path: str) -> Path:
        """A local copy of one file."""

    def read_columns(self, path: str, columns: Sequence[str]) -> pa.Table:
        """Some columns of one parquet file, without reading the others."""


@dataclass(frozen=True)
class LocalRepo:
    """A repo on disk, such as the release `build_cli` writes."""

    root: Path

    def files(self) -> list[str]:
        if not self.root.is_dir():
            raise RepoReadError(f"{self.root} does not exist")
        return sorted(
            p.relative_to(self.root).as_posix()
            for p in self.root.rglob("*")
            if p.is_file()
        )

    def fetch(self, path: str) -> Path:
        local = self.root / path
        if not local.is_file():
            raise RepoReadError(f"{local} does not exist")
        return local

    def read_columns(self, path: str, columns: Sequence[str]) -> pa.Table:
        return pq.read_table(self.fetch(path), columns=list(columns))


@dataclass(frozen=True)
class HubRepo:
    """A dataset repo on the Hub. `token=None` uses `HF_TOKEN` or the saved login."""

    repo_id: str
    revision: str = "main"
    token: str | None = None

    @cached_property
    def commit(self) -> str:
        """The commit `revision` named on first use: every read sees that one state."""
        with self._errors():
            sha = self._api().dataset_info(self.repo_id, revision=self.revision).sha
        if not sha:
            raise RepoReadError(f"{self.revision} names no commit")
        return sha

    def files(self) -> list[str]:
        with self._errors():
            return sorted(
                self._api().list_repo_files(
                    self.repo_id, repo_type="dataset", revision=self.commit
                )
            )

    def fetch(self, path: str) -> Path:
        with self._errors():
            local = self._api().hf_hub_download(
                self.repo_id, path, repo_type="dataset", revision=self.commit
            )
        if not isinstance(local, str):
            raise RepoReadError(f"{path} was not downloaded")
        return Path(local)

    def read_columns(self, path: str, columns: Sequence[str]) -> pa.Table:
        """Range reads: the footer and the asked columns' chunks, not the whole shard."""
        remote = f"datasets/{self.repo_id}@{self.commit}/{path}"
        with self._errors(), self._fs().open(remote, "rb") as f:
            return pq.read_table(f, columns=list(columns))

    def _api(self) -> "HfApi":
        from huggingface_hub import HfApi

        return HfApi(token=self.token)

    def _fs(self) -> "HfFileSystem":
        from huggingface_hub import HfFileSystem

        return HfFileSystem(token=self.token)

    @contextmanager
    def _errors(self) -> Iterator[None]:
        """Any Hub failure reads as unreadable, so a lookup fails closed."""
        try:
            yield
        except RepoReadError:
            raise
        except Exception as exc:  # noqa: BLE001 - Hub reads fail in varied ways
            raise RepoReadError(f"{type(exc).__name__}: {exc}") from exc
