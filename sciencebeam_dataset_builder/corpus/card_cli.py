"""Rewrite a corpus repo's generated files from its latest release, without a release.

They are the card and, for an open repo, `attributions.csv`. A release rewrites both, so a
change to how they are made otherwise waits for one.

    # how the files on the Hub differ from the ones its latest release gives
    python -m sciencebeam_dataset_builder.corpus.card_cli --corpus ore --tier open

    # commit the files that differ to main, then read them back to check them
    python -m sciencebeam_dataset_builder.corpus.card_cli --corpus ore --tier open --upload
"""

import argparse
import csv
import difflib
import hashlib
from collections import Counter
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.attributions import (
    ATTRIBUTION_COLUMNS,
    render_attributions,
)
from sciencebeam_dataset_builder.corpus.card import render_card
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.ledger import read_membership
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from sciencebeam_dataset_builder.corpus.repo_reader import HubRepo
from sciencebeam_dataset_builder.dataset.split import SPLIT_NAMES

if TYPE_CHECKING:
    from huggingface_hub import HfApi


class CardError(RuntimeError):
    pass


def release_files(repo: Repo) -> tuple[str, dict[str, str]]:
    """The latest release, and the generated files it gives, made as a release makes
    them."""
    files = repo.reader.files()
    latest = layout.latest_release(files)
    if latest is None:
        raise CardError("it has no release")
    members = read_membership(repo.reader.fetch(layout.release_manifest_path(latest)))
    in_split = Counter(m.split for m in members)
    counts = {repo.corpus.pairings[0]: {s: in_split.get(s, 0) for s in SPLIT_NAMES}}
    generated = {layout.CARD_PATH: render_card(repo.corpus, repo.tier, counts)}
    if repo.tier.public:
        tables = (
            repo.reader.read_columns(shard, ATTRIBUTION_COLUMNS)
            for shard in files
            if shard.endswith(".parquet")
        )
        generated[layout.ATTRIBUTIONS_PATH] = render_attributions(
            [m.id for m in members], tables
        )
    return latest, generated


def published(repo: Repo, path: str) -> str:
    if path not in repo.reader.files():
        return ""
    return repo.reader.fetch(path).read_text(encoding="utf-8")


def attributions_summary(current: str, new: str) -> str:
    old, now = _rows(current), _rows(new)
    if not old:
        return f"new, {len(now)} documents"
    added = len(now.keys() - old.keys())
    removed = len(old.keys() - now.keys())
    changed = sum(1 for i in now.keys() & old.keys() if now[i] != old[i])
    if not (added or removed or changed):
        return "the same rows, in a different order"
    return f"{added} added, {removed} removed, {changed} changed"


def upload_files(
    api: "HfApi", repo_id: str, files: Mapping[str, str], parent: str
) -> str:
    """Commit `files` onto `parent`, so a `main` that moved since is refused, and read
    each back."""
    from huggingface_hub import CommitOperationAdd

    sent = {path: text.encode("utf-8") for path, text in files.items()}
    info = api.create_commit(
        repo_id,
        [CommitOperationAdd(path, data) for path, data in sent.items()],
        commit_message="Rewrite the generated files from the latest release",
        repo_type="dataset",
        parent_commit=parent,
    )
    commit = str(info.oid)
    for path, data in sent.items():
        local = api.hf_hub_download(
            repo_id, path, repo_type="dataset", revision=commit, force_download=True
        )
        if _sha256(Path(local).read_bytes()) != _sha256(data):
            raise CardError(f"{path} read back from {commit[:12]} is not the one sent")
    return commit


def _rows(text: str) -> dict[str, dict[str, str]]:
    return {row["id"]: row for row in csv.DictReader(text.splitlines())}


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--corpus", required=True, choices=sorted(CORPORA))
    parser.add_argument("--tier", required=True, choices=[t.value for t in Tier])
    parser.add_argument(
        "--upload", action="store_true", help="Commit the files that differ to main."
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    corpus = CORPORA[args.corpus]
    tier = Tier(args.tier)
    if tier not in corpus.tiers:
        raise SystemExit(f"{corpus.name!r} has no {tier.value} repo")
    reader = HubRepo(corpus.repo_id(tier))
    repo = Repo(corpus, tier, reader)
    try:
        latest, generated = release_files(repo)
        current = {path: published(repo, path) for path in generated}
    except (CardError, OSError, ValueError) as exc:
        raise SystemExit(f"{repo.repo_id}: {exc}") from exc

    differ = {path: text for path, text in generated.items() if current[path] != text}
    for path in generated:
        if path not in differ:
            print(f"{repo.repo_id}: {path} matches release {latest}")
    if layout.CARD_PATH in differ:
        print(f"{repo.repo_id}: {layout.CARD_PATH} differs from release {latest}'s:")
        diff = difflib.unified_diff(
            current[layout.CARD_PATH].splitlines(keepends=True),
            differ[layout.CARD_PATH].splitlines(keepends=True),
            "on the Hub",
            f"from {latest}",
            n=0,
        )
        print("".join(diff), end="\n")
    if layout.ATTRIBUTIONS_PATH in differ:
        summary = attributions_summary(
            current[layout.ATTRIBUTIONS_PATH], differ[layout.ATTRIBUTIONS_PATH]
        )
        print(
            f"{repo.repo_id}: {layout.ATTRIBUTIONS_PATH} differs from release "
            f"{latest}'s: {summary}"
        )
    if not differ:
        return
    if not args.upload:
        print('Pass RUN_ARGS="--upload" to commit them to main.')
        return

    from huggingface_hub import HfApi

    try:
        commit = upload_files(HfApi(), repo.repo_id, differ, reader.commit)
    except CardError as exc:
        raise SystemExit(f"{repo.repo_id}: {exc}") from exc
    print(
        f"Committed {', '.join(differ)} to {repo.repo_id} as {commit[:12]}, read back "
        f"unchanged."
    )


if __name__ == "__main__":
    main()
