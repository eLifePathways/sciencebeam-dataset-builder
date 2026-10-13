"""Run the change request page on this machine.

    # against the Hub, as the HF_TOKEN or saved login's user
    python -m sciencebeam_dataset_builder.corpus.change_ui

    # a preview against the local build, with submitting switched off
    python -m sciencebeam_dataset_builder.corpus.change_ui \\
        --local-dir data/output/corpus-build
"""

import argparse
import getpass
from functools import cache
from pathlib import Path

from sciencebeam_dataset_builder.corpus.change_ui.app import create_app
from sciencebeam_dataset_builder.corpus.lookup import hub_repos, local_repos
from sciencebeam_dataset_builder.corpus.proposal import submitter

HOST = "127.0.0.1"
DEFAULT_PORT = 8765


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument(
        "--local-dir",
        type=Path,
        help="Read the repos from this directory, as build_cli writes them, instead "
        "of the Hub. Nothing can be submitted.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    import uvicorn

    if args.local_dir:
        local_dir: Path = args.local_dir
        app = create_app(
            lambda: local_repos(local_dir), getpass.getuser, can_submit=False
        )
    else:
        app = create_app(hub_repos, cache(submitter), can_submit=True)
    print(f"Open http://{HOST}:{args.port}/")
    uvicorn.run(app, host=HOST, port=args.port)


if __name__ == "__main__":
    main()
