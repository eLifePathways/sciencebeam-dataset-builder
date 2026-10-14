"""A stand-in for the Hub: each repo id is a local directory, `main` a list of commits."""

from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any


@dataclass
class FakePullRequest:
    num: int
    title: str
    diff: str | None


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
    # A pull request serves back something other than what was sent.
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


def removals_diff(*ids: str) -> str:
    """What the Hub shows for a pull request adding these ids to `removals.yml`."""
    path = "changes/removals/removals.yml"
    added = "".join(f"+  - id: '{i}'\n+    local_change_id: 1\n" for i in ids)
    return (
        f"diff --git a/{path} b/{path}\n--- /dev/null\n+++ b/{path}\n"
        f"@@ -0,0 +1,{1 + 2 * len(ids)} @@\n+removals:\n{added}"
    )


def _url(repo_id: str, num: int) -> str:
    return f"https://huggingface.co/datasets/{repo_id}/discussions/{num}"


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
        if not revision.startswith("refs/pr/"):
            return str(self.hub.roots[repo_id] / filename)
        num = int(revision.rsplit("/", 1)[1])
        sent = next(
            o["files"][filename]
            for o in reversed(self.hub.opened)
            if o["num"] == num and filename in o["files"]
        )
        local = self.hub.roots[repo_id].parent / f"pr-{num}" / filename
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(sent[:-1] if self.hub.tamper else sent)
        return str(local)

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

    def get_discussion_details(
        self, repo_id: str, discussion_num: int, repo_type: str
    ) -> SimpleNamespace:
        self.hub.record("get_discussion_details", repo_id, str(discussion_num))
        pr = next(p for p in self.hub.pull_requests if p.num == discussion_num)
        return SimpleNamespace(diff=pr.diff)

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
        """A new pull request, or with `revision` a commit added to an open one.

        Recorded as `create_commit` and `add_commit`, so a test can fail just one.
        """
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
