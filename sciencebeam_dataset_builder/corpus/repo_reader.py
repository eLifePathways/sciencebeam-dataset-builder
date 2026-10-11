"""Reading a corpus repo's files, wherever the repo is."""

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import pyarrow as pa
import pyarrow.parquet as pq


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
