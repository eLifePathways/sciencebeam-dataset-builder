"""A stand-in for the Hub: each repo id is a local directory, `main` a list of commits."""

from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any


@dataclass
class FakeHub:
    roots: dict[str, Path]
    # What `main` resolves to on each successive ask; the last answer repeats.
    commits: list[str | None] = field(default_factory=lambda: ["c1"])
    fail: Exception | None = None
    calls: list[tuple[str, str, str]] = field(default_factory=list)

    def install(self, monkeypatch: Any) -> None:
        monkeypatch.setattr("huggingface_hub.HfApi", lambda token=None: _Api(self))
        monkeypatch.setattr(
            "huggingface_hub.HfFileSystem", lambda token=None: _Fs(self)
        )

    def record(self, call: str, repo_id: str, revision: str) -> None:
        if self.fail is not None:
            raise self.fail
        self.calls.append((call, repo_id, revision))


@dataclass
class _Api:
    hub: FakeHub

    def dataset_info(self, repo_id: str, revision: str) -> SimpleNamespace:
        self.hub.record("dataset_info", repo_id, revision)
        sha = (
            self.hub.commits.pop(0)
            if len(self.hub.commits) > 1
            else self.hub.commits[0]
        )
        return SimpleNamespace(sha=sha)

    def list_repo_files(self, repo_id: str, repo_type: str, revision: str) -> list[str]:
        self.hub.record("list_repo_files", repo_id, revision)
        root = self.hub.roots[repo_id]
        found = [p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()]
        return sorted(found, reverse=True)

    def hf_hub_download(
        self, repo_id: str, filename: str, repo_type: str, revision: str
    ) -> str:
        self.hub.record("hf_hub_download", repo_id, revision)
        return str(self.hub.roots[repo_id] / filename)


@dataclass
class _Fs:
    hub: FakeHub

    def open(self, path: str, mode: str) -> Any:
        """`datasets/<org>/<name>@<revision>/<path in repo>`."""
        assert path.startswith("datasets/")
        repo_id, _, rest = path.removeprefix("datasets/").partition("@")
        revision, _, in_repo = rest.partition("/")
        self.hub.record("open", repo_id, revision)
        return (self.hub.roots[repo_id] / in_repo).open(mode)
