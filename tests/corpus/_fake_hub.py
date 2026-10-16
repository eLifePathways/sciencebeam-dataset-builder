"""A stand-in for the Hub: each repo id is a local directory, `main` a list of commits."""

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from huggingface_hub.errors import EntryNotFoundError


@dataclass
class FakePullRequest:
    """An open pull request: `main` with these files written over it."""

    num: int
    title: str
    files: dict[str, bytes]


@dataclass
class FakeHub:
    roots: dict[str, Path]
    # What `main` resolves to on each successive ask; the last answer repeats.
    commits: list[str | None] = field(default_factory=lambda: ["c1"])
    fail: Exception | None = None
    # The one call, and the one repo, that fail; every one when blank.
    fail_on: str = ""
    fail_repo: str = ""
    pull_requests: list[FakePullRequest] = field(default_factory=list)
    opened: list[dict[str, Any]] = field(default_factory=list)
    # Commits made straight to `main`, and the tags put on them.
    landed: list[dict[str, Any]] = field(default_factory=list)
    tags: dict[str, str] = field(default_factory=dict)
    # A commit to `main` fails this many times before one goes through.
    flaky: int = 0
    # A commit to `main` lands, but the client is told it failed.
    lost_reply: bool = False
    # A pull request, or a landed commit, serves back something other than was sent.
    tamper: bool = False
    user: str | None = "hazal"
    calls: list[tuple[str, str, str]] = field(default_factory=list)

    def install(self, monkeypatch: Any) -> None:
        monkeypatch.setattr("huggingface_hub.HfApi", lambda token=None: _Api(self))
        monkeypatch.setattr(
            "huggingface_hub.HfFileSystem", lambda token=None: _Fs(self)
        )

    def record(self, call: str, repo_id: str, revision: str) -> None:
        if (
            self.fail is not None
            and self.fail_on in ("", call)
            and self.fail_repo in ("", repo_id)
        ):
            raise self.fail
        self.calls.append((call, repo_id, revision))


def removals_change(*ids: str) -> dict[str, bytes]:
    """A pull request's files when it adds these ids to a new `removals.yml`."""
    from sciencebeam_dataset_builder.corpus.change_files import Removal, append_removals

    text = append_removals(None, [Removal(i, 1, "proposed", "someone") for i in ids])
    return {"changes/removals/removals.yml": text.encode()}


def additions_change(*ids: str) -> dict[str, bytes]:
    """A pull request's files when it adds these ids to a new `additions.yml`."""
    from sciencebeam_dataset_builder.corpus.change_files import (
        Addition,
        append_additions,
    )

    text = append_additions(
        None,
        [Addition(i, 1, "proposed", "someone", "o/source", "0" * 40) for i in ids],
    )
    return {"changes/additions/additions.yml": text.encode()}


def _url(repo_id: str, num: int) -> str:
    return f"https://huggingface.co/datasets/{repo_id}/discussions/{num}"


def _content(operation: Any) -> bytes:
    source = operation.path_or_fileobj
    return Path(source).read_bytes() if isinstance(source, str) else source


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
        if revision.startswith("refs/pr/"):
            files = self._files_at(repo_id, revision, prefix="")
            if filename not in files:
                raise EntryNotFoundError(f"{filename} is not at {revision}")
            sent = files[filename]
        elif landed := self._landed(revision):
            sent = landed["files"][filename]
        else:
            path = self.hub.roots[repo_id] / filename
            if not path.is_file():
                raise EntryNotFoundError(f"{filename} is not at {revision}")
            return str(path)
        local = self.hub.roots[repo_id].parent / f"served-{revision}" / filename
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(sent[:-1] if self.hub.tamper else sent)
        return str(local)

    def list_repo_commits(
        self, repo_id: str, repo_type: str, revision: str
    ) -> list[SimpleNamespace]:
        """`main`'s commits, newest first; a pull request adds its own on top."""
        self.hub.record("list_repo_commits", repo_id, revision)
        head = self.hub.commits[0]
        main = [SimpleNamespace(commit_id=c) for c in dict.fromkeys([head, "c1"]) if c]
        if revision.startswith("refs/pr/"):
            return [SimpleNamespace(commit_id=f"{revision}-head"), *main]
        return main

    def list_repo_tree(
        self,
        repo_id: str,
        path_in_repo: str,
        recursive: bool,
        revision: str,
        repo_type: str,
    ) -> list[SimpleNamespace]:
        self.hub.record("list_repo_tree", repo_id, revision)
        files = self._files_at(repo_id, revision, prefix=f"{path_in_repo}/")
        if not files:
            raise EntryNotFoundError(f"{path_in_repo} is not at {revision}")
        return [
            SimpleNamespace(path=path, blob_id=hashlib.sha1(content).hexdigest())
            for path, content in sorted(files.items())
        ]

    def get_paths_info(
        self, repo_id: str, paths: list[str], repo_type: str, revision: str
    ) -> list[SimpleNamespace]:
        """Parquet files carry their sha256, as LFS ones do; the rest have to be read."""
        self.hub.record("get_paths_info", repo_id, revision)
        landed = self._landed(revision)
        assert landed is not None, f"no commit {revision}"
        infos = []
        for path in paths:
            sha = hashlib.sha256(landed["files"][path]).hexdigest()
            if self.hub.tamper:
                sha = "0" * 64
            lfs = SimpleNamespace(sha256=sha) if path.endswith(".parquet") else None
            infos.append(SimpleNamespace(path=path, lfs=lfs))
        return infos

    def list_repo_refs(self, repo_id: str, repo_type: str) -> SimpleNamespace:
        self.hub.record("list_repo_refs", repo_id, "")
        return SimpleNamespace(tags=[SimpleNamespace(name=t) for t in self.hub.tags])

    def create_tag(
        self,
        repo_id: str,
        *,
        tag: str,
        revision: str,
        repo_type: str,
        tag_message: str,
    ) -> None:
        self.hub.record("create_tag", repo_id, revision)
        assert tag not in self.hub.tags
        self.hub.tags[tag] = revision

    def get_repo_discussions(
        self,
        repo_id: str,
        repo_type: str,
        discussion_type: str,
        discussion_status: str,
    ) -> Any:
        self.hub.record("get_repo_discussions", repo_id, discussion_status)
        return iter(
            SimpleNamespace(num=pr.num, title=pr.title, url=_url(repo_id, pr.num))
            for pr in self.hub.pull_requests
        )

    def create_commit(
        self,
        repo_id: str,
        operations: list[Any],
        *,
        commit_message: str,
        repo_type: str,
        commit_description: str | None = None,
        create_pr: bool = False,
        parent_commit: str | None = None,
        revision: str | None = None,
    ) -> SimpleNamespace:
        """A new pull request; with `revision` a commit added to an open one; with
        neither, a commit to `main`.

        Recorded as `create_commit`, `add_commit` and `main_commit`, so a test can fail
        just one.
        """
        if not create_pr and revision is None:
            return self._commit_to_main(
                repo_id, operations, commit_message, commit_description, parent_commit
            )
        if create_pr:
            self.hub.record("create_commit", repo_id, parent_commit or "")
            num = 100 + sum(o["create_pr"] for o in self.hub.opened)
        else:
            assert revision is not None and revision.startswith("refs/pr/")
            self.hub.record("add_commit", repo_id, parent_commit or "")
            num = int(revision.rsplit("/", 1)[1])
            last = [o for o in self.hub.opened if o["num"] == num][-1]
            assert parent_commit == last["oid"], "the pull request moved"
        oid = f"pr{num}-{len(self.hub.opened)}"
        self.hub.opened.append(
            {
                "num": num,
                "oid": oid,
                "files": {op.path_in_repo: op.path_or_fileobj for op in operations},
                "message": commit_message,
                "description": commit_description,
                "create_pr": create_pr,
                "parent_commit": parent_commit,
            }
        )
        return SimpleNamespace(pr_num=num, pr_url=_url(repo_id, num), oid=oid)

    def whoami(self) -> dict[str, Any]:
        self.hub.record("whoami", "", "")
        return {"name": self.hub.user}

    def _commit_to_main(
        self,
        repo_id: str,
        operations: list[Any],
        message: str,
        description: str | None,
        parent_commit: str | None,
    ) -> SimpleNamespace:
        self.hub.record("main_commit", repo_id, parent_commit or "")
        if self.hub.flaky:
            self.hub.flaky -= 1
            raise RuntimeError("connection reset")
        head = self.hub.commits[0]
        if parent_commit != head:
            raise RuntimeError(f"412: parent {parent_commit} is not the head {head}")
        oid = f"m{len(self.hub.landed) + 1}"
        self.hub.landed.append(
            {
                "oid": oid,
                "parent": parent_commit,
                "files": {op.path_in_repo: _content(op) for op in operations},
                "message": message,
                "description": description,
            }
        )
        self.hub.commits = [oid]
        if self.hub.lost_reply:
            self.hub.lost_reply = False
            raise RuntimeError("timed out waiting for the reply")
        return SimpleNamespace(oid=oid, pr_num=None, pr_url=None)

    def _files_at(self, repo_id: str, revision: str, prefix: str) -> dict[str, bytes]:
        """`main`'s files under `prefix`, with a pull request's written over them."""
        root = self.hub.roots[repo_id]
        files = {
            p.relative_to(root).as_posix(): p.read_bytes()
            for p in root.rglob("*")
            if p.is_file() and p.relative_to(root).as_posix().startswith(prefix)
        }
        if revision.startswith("refs/pr/"):
            num = int(revision.rsplit("/", 1)[1])
            fake = next((p for p in self.hub.pull_requests if p.num == num), None)
            overlay: dict[str, bytes] = dict(fake.files) if fake else {}
            for opened in (o for o in self.hub.opened if o["num"] == num):
                overlay.update(opened["files"])
            files.update({k: v for k, v in overlay.items() if k.startswith(prefix)})
        return files

    def _landed(self, revision: str) -> dict[str, Any] | None:
        return next((c for c in self.hub.landed if c["oid"] == revision), None)


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
