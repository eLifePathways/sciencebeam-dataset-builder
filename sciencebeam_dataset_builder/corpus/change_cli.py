"""Apply a corpus repo's merged changes as a new release: written locally, then uploaded.

    # what is pending on main, and the version it needs
    python -m sciencebeam_dataset_builder.corpus.change_cli --corpus biorxiv --tier open

    # write that release locally, to check before anything is uploaded
    python -m sciencebeam_dataset_builder.corpus.change_cli --corpus biorxiv --tier open \\
        --version v2.0.0

    # then upload exactly what was written and checked, and tag it
    python -m sciencebeam_dataset_builder.corpus.change_cli --corpus biorxiv --tier open \\
        --version v2.0.0 --upload

The repo is read from the Hub at the commit `main` names when it is first read, with
HF_TOKEN or the saved login; `--local-dir` reads the tree `build_cli` writes instead,
and what is made from it cannot be uploaded.
"""

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.apply_changes import (
    ApplyError,
    Pending,
    apply,
    next_version,
    read_pending,
    release_root,
)
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from sciencebeam_dataset_builder.corpus.repo_reader import (
    HubRepo,
    LocalRepo,
    RepoReader,
)
from sciencebeam_dataset_builder.corpus.upload_changes import (
    UploadError,
    upload_release,
)

DEFAULT_OUTPUT_DIR = Path("data/output/corpus-change")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--corpus", required=True, choices=sorted(CORPORA))
    parser.add_argument("--tier", required=True, choices=[t.value for t in Tier])
    parser.add_argument(
        "--version",
        default="",
        help="The release to write. Without it, only report what is pending.",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload the release already written for --version, and tag it. A "
        "separate, deliberate step - never implied by writing it.",
    )
    parser.add_argument(
        "--local-dir",
        type=Path,
        help="Read the repo from this directory, as build_cli writes it, not the Hub.",
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args(argv)
    if args.upload and not args.version:
        parser.error("--upload needs the --version that was written")
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    corpus = CORPORA[args.corpus]
    tier = Tier(args.tier)
    if tier not in corpus.tiers:
        raise SystemExit(f"{corpus.name!r} has no {tier.value} repo")
    if args.upload:
        _upload(release_root(args.output_dir, corpus.repo_id(tier), args.version))
        return

    reader: RepoReader = (
        LocalRepo(args.local_dir / layout.repo_name(corpus.name, tier))
        if args.local_dir
        else HubRepo(corpus.repo_id(tier))
    )
    repo = Repo(corpus, tier, reader)
    try:
        pending = read_pending(repo)
    except (ApplyError, OSError, ValueError) as exc:
        raise SystemExit(f"{repo.repo_id}: {exc}") from exc
    _report(repo, pending)
    if pending.empty:
        return
    if not args.version:
        print(
            f"Pass --version {next_version(pending.base_release, pending.bump)} "
            f"(or later) to write the release locally."
        )
        return

    try:
        release = apply(repo, pending, args.version, args.output_dir, datetime.now(UTC))
    except (ApplyError, OSError, ValueError) as exc:
        raise SystemExit(f"{repo.repo_id}: {exc}") from exc
    print(f"Wrote {release.version} to {release.root}:")
    for path in release.paths:
        print(f"  {path}")
    print("Local only. Check it, then pass --upload to upload exactly this.")


def _upload(root: Path) -> None:
    try:
        uploaded = upload_release(root)
    except UploadError as exc:
        raise SystemExit(f"Not uploaded: {exc}") from exc
    print(
        f"Uploaded {uploaded.version} to {uploaded.repo_id} as commit "
        f"{uploaded.commit[:12]}, verified, and tagged {uploaded.version}."
    )


def _report(repo: Repo, pending: Pending) -> None:
    print(f"{repo.repo_id}, release {pending.base_release}:")
    if pending.empty:
        print("  nothing pending")
        return
    counts = {
        t: pending.change_types.count(t) for t in dict.fromkeys(pending.change_types)
    }
    for change_type, count in counts.items():
        print(f"  {count} {change_type.value}")
    print(
        f"  needs a {pending.bump} version, {next_version(pending.base_release, pending.bump)} "
        f"or later"
    )
    sys.stdout.flush()


if __name__ == "__main__":
    main()
