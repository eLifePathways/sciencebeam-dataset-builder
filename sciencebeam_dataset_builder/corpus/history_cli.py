"""Remove an open corpus repo's history, keeping its files exactly as they are.

Older commits, their tags and pull request refs still hold documents removed since, so an
open repo goes public only once `main` is one commit carrying its release's tag.

    # the history the repo holds, and the pull request refs to delete first
    python -m sciencebeam_dataset_builder.corpus.history_cli --corpus ore --tier open

    # squash main to one commit and move the release's tag onto it
    python -m sciencebeam_dataset_builder.corpus.history_cli --corpus ore --tier open \\
        --version v2.0.0 --squash

A pull request ref can only be deleted on the website: open the pull request and press
"Delete ref" at the bottom. Squashing cannot be undone.
"""

import argparse
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.apply_changes import ApplyError, read_pending
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from sciencebeam_dataset_builder.corpus.repo_reader import HubRepo

if TYPE_CHECKING:
    from huggingface_hub import HfApi

HUB_DATASETS_URL = "https://huggingface.co/datasets"


class HistoryError(RuntimeError):
    pass


@dataclass(frozen=True)
class History:
    head: str
    commits: int
    # Tag -> the commit it resolves to.
    tags: dict[str, str]
    # The website page of each pull request whose ref remains.
    pull_requests: list[str]
    # Shard versions in main's history that are not the ones at its head.
    older_shards: int


def read_history(api: "HfApi", repo_id: str) -> History:
    refs = api.list_repo_refs(repo_id, repo_type="dataset", include_pull_requests=True)
    commits = [c.commit_id for c in api.list_repo_commits(repo_id, repo_type="dataset")]
    at_head = _tree(api, repo_id, commits[0])
    older = {
        (path, blob)
        for commit in commits[1:]
        for path, blob in _tree(api, repo_id, commit).items()
        if path.endswith(".parquet") and at_head.get(path) != blob
    }
    return History(
        head=commits[0],
        commits=len(commits),
        tags={t.name: _resolve(api, repo_id, t.name) for t in refs.tags},
        pull_requests=[
            f"{HUB_DATASETS_URL}/{repo_id}/discussions/{p.ref.rsplit('/', 1)[1]}"
            for p in refs.pull_requests or []
        ],
        older_shards=len(older),
    )


def squash(api: "HfApi", repo: Repo, version: str) -> History:
    """Squash `main` to one commit, tagged `version`, once nothing else can reach the
    older commits and `main` holds exactly that release."""
    repo_id = repo.repo_id
    if not repo.tier.public:
        raise HistoryError(f"{repo_id} is not an open repo, and only those go public")
    history = read_history(api, repo_id)
    if history.pull_requests:
        raise HistoryError(
            "delete these pull request refs on the website first: "
            + " ".join(history.pull_requests)
        )
    try:
        pending = read_pending(repo)
    except ApplyError as exc:
        raise HistoryError(str(exc)) from exc
    if pending.base_release != version:
        raise HistoryError(
            f"its latest release is {pending.base_release}, not {version}"
        )
    if not pending.empty:
        raise HistoryError("main has changes not yet released; release them first")

    before = _tree(api, repo_id, history.head)
    api.super_squash_history(
        repo_id, repo_type="dataset", commit_message=f"Release {version}"
    )
    for tag in history.tags:
        api.delete_tag(repo_id, tag=tag, repo_type="dataset")
    head = api.list_repo_commits(repo_id, repo_type="dataset")[0].commit_id
    api.create_tag(
        repo_id,
        tag=version,
        revision=head,
        repo_type="dataset",
        tag_message=f"Release {version}",
    )

    after = read_history(api, repo_id)
    problems = []
    if _tree(api, repo_id, after.head) != before:
        problems.append("the files at main changed")
    if after.commits != 1:
        problems.append(f"main has {after.commits} commits")
    if after.tags != {version: after.head}:
        problems.append(f"its tags are {after.tags}")
    if after.pull_requests:
        problems.append(f"pull request refs appeared: {' '.join(after.pull_requests)}")
    if problems:
        raise HistoryError("squashed, but " + "; ".join(problems))
    return after


def _tree(api: "HfApi", repo_id: str, revision: str) -> dict[str, str]:
    return {
        entry.path: entry.blob_id
        for entry in api.list_repo_tree(
            repo_id, repo_type="dataset", revision=revision, recursive=True
        )
        if hasattr(entry, "blob_id")
    }


def _resolve(api: "HfApi", repo_id: str, tag: str) -> str:
    """The commit a tag names; the Hub's tags are annotated, so `target_commit` is not it."""
    return api.list_repo_commits(repo_id, repo_type="dataset", revision=tag)[
        0
    ].commit_id


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--corpus", required=True, choices=sorted(CORPORA))
    parser.add_argument("--tier", required=True, choices=[t.value for t in Tier])
    parser.add_argument("--version", default="", help="The repo's latest release.")
    parser.add_argument(
        "--squash",
        action="store_true",
        help="Squash main to one commit tagged --version. Cannot be undone.",
    )
    args = parser.parse_args(argv)
    if args.squash != bool(args.version):
        parser.error("--squash and --version go together")
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    corpus = CORPORA[args.corpus]
    tier = Tier(args.tier)
    if tier not in corpus.tiers:
        raise SystemExit(f"{corpus.name!r} has no {tier.value} repo")
    from huggingface_hub import HfApi

    api = HfApi()
    reader = HubRepo(corpus.repo_id(tier))
    repo = Repo(corpus, tier, reader)
    if args.squash:
        try:
            after = squash(api, repo, args.version)
        except (HistoryError, OSError) as exc:
            raise SystemExit(f"{repo.repo_id}: {exc}") from exc
        print(
            f"Squashed {repo.repo_id} to one commit, {after.head[:12]}, tagged "
            f"{args.version}; its files are unchanged. Check Settings > List Large "
            f"Files on the website shows only the current shards."
        )
        return

    history = read_history(api, repo.repo_id)
    _report(repo.repo_id, history)
    if history.older_shards or history.pull_requests:
        latest = layout.latest_release(reader.files())
        then = "Then pass" if history.pull_requests else "Pass"
        print(f'{then} RUN_ARGS="--version {latest} --squash" to remove the history.')


def _report(repo_id: str, history: History) -> None:
    print(f"{repo_id}:")
    print(
        f"  {history.commits} commits on main, holding {history.older_shards} "
        f"older shard versions"
    )
    print(f"  tags: {', '.join(history.tags) or 'none'}")
    if history.pull_requests:
        print(
            f"  {len(history.pull_requests)} pull request refs; delete each on the "
            f'website with "Delete ref" at the bottom of its page:'
        )
        for url in history.pull_requests:
            print(f"    {url}")
    elif not history.older_shards:
        print("  nothing older to remove")


if __name__ == "__main__":
    main()
