"""Rewrite a corpus repo's card from its latest release, without making a release.

A release rewrites the card, so a change to the card's text otherwise waits for one.

    # whether the card on the Hub matches the one its latest release gives
    python -m sciencebeam_dataset_builder.corpus.card_cli --corpus ore --tier open

    # commit that card to main, then read it back to check it
    python -m sciencebeam_dataset_builder.corpus.card_cli --corpus ore --tier open --upload
"""

import argparse
import difflib
import hashlib
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.card import render_card
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.ledger import LedgerError, read_membership
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from sciencebeam_dataset_builder.corpus.repo_reader import HubRepo
from sciencebeam_dataset_builder.dataset.split import SPLIT_NAMES

if TYPE_CHECKING:
    from huggingface_hub import HfApi


class CardError(RuntimeError):
    pass


def release_card(repo: Repo) -> tuple[str, str]:
    """The latest release, and the card it gives, counted as a release counts it."""
    latest = layout.latest_release(repo.reader.files())
    if latest is None:
        raise CardError("it has no release")
    manifest = repo.reader.fetch(layout.release_manifest_path(latest))
    in_split = Counter(m.split for m in read_membership(manifest))
    counts = {repo.corpus.pairings[0]: {s: in_split.get(s, 0) for s in SPLIT_NAMES}}
    return latest, render_card(repo.corpus, repo.tier, counts)


def published_card(repo: Repo) -> str:
    if layout.CARD_PATH not in repo.reader.files():
        return ""
    return repo.reader.fetch(layout.CARD_PATH).read_text(encoding="utf-8")


def upload_card(api: "HfApi", repo_id: str, card: str, parent: str) -> str:
    """Commit `card` onto `parent`, so a `main` that moved since is refused, and read it
    back."""
    from huggingface_hub import CommitOperationAdd

    data = card.encode("utf-8")
    info = api.create_commit(
        repo_id,
        [CommitOperationAdd(layout.CARD_PATH, data)],
        commit_message="Rewrite the card from the latest release",
        repo_type="dataset",
        parent_commit=parent,
    )
    commit = str(info.oid)
    path = api.hf_hub_download(
        repo_id,
        layout.CARD_PATH,
        repo_type="dataset",
        revision=commit,
        force_download=True,
    )
    if _sha256(Path(path).read_bytes()) != _sha256(data):
        raise CardError(f"the card read back from {commit[:12]} is not the one sent")
    return commit


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--corpus", required=True, choices=sorted(CORPORA))
    parser.add_argument("--tier", required=True, choices=[t.value for t in Tier])
    parser.add_argument(
        "--upload", action="store_true", help="Commit the card to main."
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
        latest, card = release_card(repo)
        current = published_card(repo)
    except (CardError, LedgerError, OSError) as exc:
        raise SystemExit(f"{repo.repo_id}: {exc}") from exc
    if current == card:
        print(f"{repo.repo_id}: the card matches release {latest}")
        return
    print(f"{repo.repo_id}: the card differs from the one release {latest} gives:")
    diff = difflib.unified_diff(
        current.splitlines(keepends=True),
        card.splitlines(keepends=True),
        "on the Hub",
        f"from {latest}",
        n=0,
    )
    print("".join(diff), end="\n")
    if not args.upload:
        print('Pass RUN_ARGS="--upload" to commit it to main.')
        return

    from huggingface_hub import HfApi

    try:
        commit = upload_card(HfApi(), repo.repo_id, card, reader.commit)
    except CardError as exc:
        raise SystemExit(f"{repo.repo_id}: {exc}") from exc
    print(
        f"Committed the card to {repo.repo_id} as {commit[:12]}, read back unchanged."
    )


if __name__ == "__main__":
    main()
