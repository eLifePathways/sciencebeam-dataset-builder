"""Generate the Hub dataset card, and optionally upload it.

The card's front matter and schema tables are generated from
:mod:`sciencebeam_dataset_builder.dataset.schema`; the prose comes from
`docs/dataset-card-body.md`. Regenerating after a schema change keeps the published
documentation and the Parquet files in step, and registers every source as a loadable
config.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from sciencebeam_dataset_builder.dataset.card import read_body, render_card
from sciencebeam_dataset_builder.dataset.migrate_cli import DEFAULT_REPO_ID

LOGGER = logging.getLogger(__name__)

DEFAULT_BODY_PATH = Path("docs/dataset-card-body.md")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate the Hub dataset card.")
    parser.add_argument(
        "output_path",
        type=Path,
        nargs="?",
        default=Path("output/README.md"),
        help="Where to write the rendered card (default: output/README.md).",
    )
    parser.add_argument(
        "--body",
        type=Path,
        default=DEFAULT_BODY_PATH,
        help=f"Hand-written portion of the card (default: {DEFAULT_BODY_PATH}).",
    )
    parser.add_argument(
        "--repo-id",
        default=DEFAULT_REPO_ID,
        help=f"Hub dataset repo (default: {DEFAULT_REPO_ID}).",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload the rendered card to the Hub as README.md.",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        stream=sys.stderr,
    )

    if not args.body.exists():
        print(f"Card body not found: {args.body}", file=sys.stderr)
        sys.exit(1)

    card = render_card(read_body(args.body))
    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    args.output_path.write_text(card, encoding="utf-8")
    print(f"Wrote {args.output_path} ({len(card.splitlines())} lines)")

    if not args.upload:
        return

    token = os.environ.get("HF_TOKEN")
    if not token:
        print("HF_TOKEN is not set; cannot upload.", file=sys.stderr)
        sys.exit(1)

    from huggingface_hub import HfApi

    HfApi(token=token).upload_file(
        path_or_fileobj=str(args.output_path),
        path_in_repo="README.md",
        repo_id=args.repo_id,
        repo_type="dataset",
        commit_message="Regenerate dataset card from the canonical schema",
    )
    print(f"Uploaded README.md to {args.repo_id}")


if __name__ == "__main__":
    main()
