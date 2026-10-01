"""Create a corpus repo on the Hub, private, and upload its generated card.

Creating the repo is cheap and reversible - it is a private, empty-but-for-its-card
placeholder, not a publication decision. Nothing here ever makes a repo public; that
stays a deliberate, separate step.

    # one repo
    python -m sciencebeam_dataset_builder.corpus.create_repo_cli --corpus biorxiv --tier open

    # every repo the registry declares
    python -m sciencebeam_dataset_builder.corpus.create_repo_cli --all
"""

import argparse
import logging
import os
import sys

from sciencebeam_dataset_builder.corpus.card import render_card
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.registry import CORPORA, Corpus, repos

LOGGER = logging.getLogger(__name__)

# Real counts, read from the licence audit. Placeholder zeros for a corpus/tier this
# has not been computed for yet - a card with a wrong real-looking number is worse than
# one that visibly says the build has not run.
KNOWN_COUNTS: dict[tuple[str, Tier], dict[str, dict[str, int]]] = {
    ("biorxiv", Tier.OPEN): {"pdf-jats": {"train": 11, "validation": 15, "test": 27}},
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--corpus", choices=sorted(CORPORA), help="One corpus, e.g. biorxiv.")
    group.add_argument(
        "--all", action="store_true", help="Every repo the registry declares."
    )
    parser.add_argument(
        "--tier",
        choices=[t.value for t in Tier],
        help="Required with --corpus: which of that corpus's repos to create.",
    )
    parser.add_argument(
        "--no-card",
        action="store_true",
        help="Create the repo only; skip uploading the generated README.",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")
    args = parser.parse_args(argv)
    if args.corpus and not args.tier:
        parser.error("--corpus requires --tier")
    return args


def _targets(args: argparse.Namespace) -> list[tuple[Corpus, Tier]]:
    if args.all:
        return repos()
    corpus = CORPORA[args.corpus]
    tier = Tier(args.tier)
    if tier not in corpus.tiers:
        raise SystemExit(
            f"{args.corpus!r} has no {tier.value} repo; its tiers are "
            f"{[t.value for t in corpus.tiers]}"
        )
    return [(corpus, tier)]


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        stream=sys.stderr,
    )

    token = os.environ.get("HF_TOKEN")
    if not token:
        print("HF_TOKEN is not set; cannot create or upload to a Hub repo.", file=sys.stderr)
        sys.exit(1)

    from huggingface_hub import HfApi

    api = HfApi(token=token)

    for corpus, tier in _targets(args):
        repo_id = corpus.repo_id(tier)
        url = api.create_repo(
            repo_id=repo_id, repo_type="dataset", private=True, exist_ok=True
        )
        print(f"Created (or already present): {url}")

        if args.no_card:
            continue

        counts = KNOWN_COUNTS.get((corpus.name, tier))
        if counts is None:
            print(
                f"  no real counts recorded yet for {corpus.name}/{tier.value}; "
                f"skipping the card, since a guessed count would be worse than none."
            )
            continue

        card = render_card(corpus, tier, counts)
        api.upload_file(
            path_or_fileobj=card.encode("utf-8"),
            path_in_repo="README.md",
            repo_id=repo_id,
            repo_type="dataset",
            commit_message="Generate the card from the schema and the registry",
        )
        print(f"  uploaded README.md ({len(card.splitlines())} lines)")


if __name__ == "__main__":
    main()
