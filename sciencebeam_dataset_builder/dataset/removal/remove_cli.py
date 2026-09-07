"""Delete manually reviewed documents from the published Hub subsets.

Reads each config's current Parquet files from the Hub, drops the rows named by the
removal lists under `data/removals/` and writes the result locally, mirroring the Hub layout
so it can be uploaded config by config.

Downloads are kept as a pristine backup under `--input-dir`. That directory is *not*
`data/input`: that one holds the copies taken before the schema migration, which still
carry the legacy columns and no `uid`. This tool needs the Hub as it stands now, so it
keeps its own snapshot and never confuses the two.

Nothing is uploaded until `--upload`, and `--upload` only pushes files that are already
on disk - so the local result can be inspected and verified first::

    # what would be removed, per config and split; downloads, writes nothing
    python -m sciencebeam_dataset_builder.dataset.removal.remove_cli --dry-run

    # snapshot the Hub to data/input-current, filter, write data/output/removed
    python -m sciencebeam_dataset_builder.dataset.removal.remove_cli

    # prove the result before it goes anywhere
    python -m sciencebeam_dataset_builder.dataset.removal.verify_cli

    # then, once the local result has been checked
    python -m sciencebeam_dataset_builder.dataset.removal.remove_cli --upload
"""

import argparse
import csv
import logging
import sys
from collections import Counter
from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from sciencebeam_dataset_builder.dataset.migrate_cli import (
    DEFAULT_REPO_ID,
    SPLIT_FILENAME,
    _hub_path,
    _token,
    read_hub_table,
)
from sciencebeam_dataset_builder.dataset.removal.companions import (
    JATS_SOURCE,
    METADATA_SOURCE,
    Companion,
    resolve_companions,
)
from sciencebeam_dataset_builder.dataset.removal.filter import remove_rows
from sciencebeam_dataset_builder.dataset.removal.removal_list import (
    Removal,
    RemovalListError,
    read_removal_lists,
)
from sciencebeam_dataset_builder.dataset.schema import (
    CANONICAL_SOURCES,
    SOURCES,
    Source,
)
from sciencebeam_dataset_builder.dataset.split import SPLIT_NAMES

LOGGER = logging.getLogger(__name__)

# The verdicts name identifiable third-party articles and judge them unfit for a
# benchmark, so they live under the gitignored data/ tree with the dataset itself,
# not in this public repository.
DEFAULT_REMOVALS_DIR = Path("data/removals")

# A snapshot of what the Hub holds *now*, kept apart from `data/input`, which is the
# pre-migration backup and carries a different schema for the same config and split.
DEFAULT_INPUT_DIR = Path("data/input-current")
DEFAULT_OUTPUT_DIR = Path("data/output/removed")

# Written next to the filtered Parquet: the audit trail of what this run deleted.
MANIFEST_FILENAME = "removed-manifest.csv"
MANIFEST_HEADER = ("uid", "source", "config", "split", "reason", "basis")


# Removal runs over every source, not only the canonical ones: a row is deleted by
# identity, and `scielo-preprints-metadata` being on an older schema is no reason to
# leave the Dublin Core rendition of a rejected document in place. It is skipped when
# nothing in it needs removing, so the usual run does not pay for its 2 GB.
DEFAULT_SOURCE_NAMES = tuple(sorted(CANONICAL_SOURCES)) + (METADATA_SOURCE.name,)


def default_removal_lists(removals_dir: Path) -> list[Path]:
    """Return every removal list in `removals_dir`, in a stable order."""
    return sorted(removals_dir.glob("*.csv"))


def remove_from_source(
    source: Source,
    reasons: dict[str, str],
    repo_id: str,
    input_dir: Path,
    output_dir: Path,
    dry_run: bool,
) -> list[tuple[str, str]]:
    """Filter every split of `source`. Returns (uid, split) for each row dropped."""
    config_dir = output_dir / source.config
    if not dry_run:
        config_dir.mkdir(parents=True, exist_ok=True)

    dropped: list[tuple[str, str]] = []
    uids = set(reasons)

    for split in SPLIT_NAMES:
        label = f"{source.config}/{split}"
        print(f"  {label}: fetching...", flush=True)
        table = read_hub_table(repo_id, _hub_path(source, split), input_dir)

        filtered, split_dropped = remove_rows(table, uids, source)
        dropped.extend((uid, split) for uid in split_dropped)

        if dry_run:
            print(
                f"  {label}: {table.num_rows} rows, would remove "
                f"{len(split_dropped)} -> {filtered.num_rows}"
            )
            continue

        out_path = config_dir / SPLIT_FILENAME.format(split=split)
        pq.write_table(filtered, out_path, compression="snappy")
        size_mb = out_path.stat().st_size / 1024 / 1024
        print(
            f"  {label}: {table.num_rows} rows, removed {len(split_dropped)} -> "
            f"{filtered.num_rows} -> {out_path} ({size_mb:.1f} MB)"
        )

    return dropped


def resolve_removal_set(
    removals: dict[str, Removal],
    load_split: Callable[[Source, str], pa.Table],
) -> tuple[dict[str, str], dict[str, Companion]]:
    """Return every uid to remove mapped to its reason, plus the companions among them.

    Reads the `scielo-preprints-jats` splits through `load_split` to follow each removed
    preprint into its Dublin Core rendition; see :mod:`.companions`. The loader is
    injected so the removal can fetch from the Hub while verification reads the snapshot
    it already has, and stays offline.
    """
    reasons = {uid: removal.reason for uid, removal in removals.items()}
    companions: dict[str, Companion] = {}

    listed_jats = {uid for uid in removals if uid.startswith(f"{JATS_SOURCE.name}__")}
    if not listed_jats:
        return reasons, companions

    unresolved: list[str] = []
    for split in SPLIT_NAMES:
        table = load_split(JATS_SOURCE, split)
        found, missing = resolve_companions(table, listed_jats)
        unresolved.extend(missing)
        for companion in found:
            companions[companion.metadata_uid] = companion
            reasons[companion.metadata_uid] = removals[companion.jats_uid].reason

    print(
        f"{METADATA_SOURCE.config}: {len(companions)} companion row(s) of "
        f"{len(listed_jats)} removed preprint(s) resolved via the OAI preprint number"
    )
    if unresolved:
        print(
            f"  warning: {len(unresolved)} removed preprint(s) carry no SciELO "
            "Preprints DOI, so their Dublin Core rendition cannot be located:"
        )
        for uid in unresolved:
            print(f"    - {uid}")
    print()

    return reasons, companions


def write_manifest(
    path: Path,
    reasons: dict[str, str],
    companions: dict[str, Companion],
    located: dict[str, tuple[Source, str]],
) -> None:
    """Write the audit trail of what was removed, from where, and on what grounds."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(MANIFEST_HEADER)
        for uid in sorted(located):
            source, split = located[uid]
            companion = companions.get(uid)
            basis = (
                f"companion of {companion.jats_uid}"
                if companion is not None
                else "reviewed"
            )
            writer.writerow(
                [uid, source.name, source.config, split, reasons[uid], basis]
            )


def upload_source(source: Source, repo_id: str, output_dir: Path) -> None:
    """Upload a filtered config directory to the Hub."""
    from huggingface_hub import HfApi

    config_dir = output_dir / source.config
    missing = [
        split
        for split in SPLIT_NAMES
        if not (config_dir / SPLIT_FILENAME.format(split=split)).exists()
    ]
    if missing:
        raise SystemExit(
            f"{source.config}: cannot upload, missing local split(s): {missing}. "
            "Run the removal without --upload first."
        )

    api = HfApi(token=_token())
    print(f"  {source.config}: uploading {config_dir}...", flush=True)
    api.upload_folder(
        repo_id=repo_id,
        repo_type="dataset",
        folder_path=str(config_dir),
        path_in_repo=source.config,
        commit_message=f"Remove manually reviewed documents from {source.config}",
    )
    print(f"  {source.config}: uploaded")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Remove manually reviewed documents from the published subsets."
    )
    parser.add_argument(
        "output_dir",
        type=Path,
        nargs="?",
        default=DEFAULT_OUTPUT_DIR,
        help=(
            "Directory to write filtered Parquet files into, mirroring the Hub layout "
            f"(default: {DEFAULT_OUTPUT_DIR})."
        ),
    )
    parser.add_argument(
        "--removals-dir",
        type=Path,
        default=DEFAULT_REMOVALS_DIR,
        help=(
            "Directory holding the removal lists, every *.csv of which is read "
            f"(default: {DEFAULT_REMOVALS_DIR})."
        ),
    )
    parser.add_argument(
        "--removal-list",
        type=Path,
        action="append",
        metavar="CSV",
        help="A `uid,reason` CSV of documents to remove; repeatable. "
        "Overrides --removals-dir.",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help=(
            "Directory holding a pristine snapshot of the Hub as it stands before "
            f"removal, reused on re-runs (default: {DEFAULT_INPUT_DIR}). Deliberately "
            "not data/input, which is the pre-migration backup."
        ),
    )
    parser.add_argument(
        "--repo-id",
        default=DEFAULT_REPO_ID,
        help=f"Hub dataset repo (default: {DEFAULT_REPO_ID}).",
    )
    parser.add_argument(
        "--source",
        action="append",
        choices=sorted(SOURCES),
        metavar="NAME",
        help=(
            "Process only this source; repeatable. Default: every source on the "
            "canonical schema."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Report what would be removed without writing anything. Still downloads: "
            "matching uids needs row data, not just the Parquet footer."
        ),
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload already-filtered local files to the Hub. Does not filter.",
    )
    parser.add_argument(
        "--allow-missing",
        action="store_true",
        help=(
            "Continue when a listed uid is not found in any processed source. Needed to "
            "re-run over subsets already filtered, where the rows are gone by design."
        ),
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

    if args.dry_run and args.upload:
        raise SystemExit("--dry-run and --upload are mutually exclusive")

    selected = [SOURCES[name] for name in (args.source or DEFAULT_SOURCE_NAMES)]

    if args.upload:
        if not _token():
            print("HF_TOKEN is not set; cannot upload.", file=sys.stderr)
            sys.exit(1)
        for source in selected:
            print(f"{source.config} ({source.name}):")
            upload_source(source, args.repo_id, args.output_dir)
        return

    lists = args.removal_list or default_removal_lists(args.removals_dir)
    if not lists:
        raise SystemExit(
            f"No removal lists given and none found in {args.removals_dir}/."
        )
    missing_lists = [str(path) for path in lists if not path.exists()]
    if missing_lists:
        raise SystemExit(f"Removal list(s) not found: {missing_lists}")

    try:
        removals = read_removal_lists(lists)
    except RemovalListError as error:
        raise SystemExit(str(error)) from error

    print(f"Removal lists: {', '.join(str(path) for path in lists)}")
    print(f"Documents listed for removal: {len(removals)}\n")

    if not _token():
        print(
            "HF_TOKEN is not set; the dataset is private and cannot be read.",
            file=sys.stderr,
        )
        sys.exit(1)

    def fetch(source: Source, split: str) -> pa.Table:
        return read_hub_table(args.repo_id, _hub_path(source, split), args.input_dir)

    reasons, companions = resolve_removal_set(removals, fetch)

    # The Dublin Core config is 2 GB and is usually untouched by a review of rendered
    # PDFs, so fetch it only when it actually has rows to lose - unless it was asked for
    # by name, where the user's intent outranks the shortcut.
    if not args.source and not companions and METADATA_SOURCE in selected:
        selected.remove(METADATA_SOURCE)
        print(
            f"{METADATA_SOURCE.config}: nothing to remove, skipping "
            "(name it with --source to process it anyway)\n"
        )

    located: dict[str, tuple[Source, str]] = {}
    for source in selected:
        print(f"{source.config} ({source.name}):")
        dropped = remove_from_source(
            source,
            reasons,
            args.repo_id,
            args.input_dir,
            args.output_dir,
            args.dry_run,
        )
        for uid, split in dropped:
            located[uid] = (source, split)

    print()
    tally = Counter(reasons[uid] for uid in located)
    for reason, count in tally.most_common():
        print(f"  {count:5d}  {reason}")
    print(f"  {len(located):5d}  total removed", end="")
    if companions:
        listed_removed = sum(1 for uid in located if uid not in companions)
        companions_removed = len(located) - listed_removed
        print(f" ({listed_removed} reviewed, {companions_removed} companion)")
    else:
        print()

    unmatched = sorted(set(reasons) - set(located))
    if unmatched:
        print(
            f"\n{len(unmatched)} listed uid(s) matched no row in the processed sources:"
        )
        for uid in unmatched[:20]:
            print(f"  - {uid}")
        if len(unmatched) > 20:
            print(f"  ... and {len(unmatched) - 20} more")
        if not args.allow_missing:
            raise SystemExit(
                "Refusing to continue: every listed uid must match a row, unless this "
                "is a re-run over already-filtered subsets (--allow-missing). Note that "
                "restricting the run with --source also leaves other sources' uids "
                "unmatched."
            )

    if args.dry_run:
        print("\nDry run only; nothing written.")
        return

    manifest_path = args.output_dir / MANIFEST_FILENAME
    write_manifest(manifest_path, reasons, companions, located)
    print(f"\nManifest of removed rows: {manifest_path}")
    print("Verify before uploading: make verify-removal")


if __name__ == "__main__":
    main()
