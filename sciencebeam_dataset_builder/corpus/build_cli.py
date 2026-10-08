"""Build one corpus repo's release from the real benchmark-v002 snapshot.

Reads a corpus's legacy split files (the old 23-column canonical schema), maps each row
onto the 15-column corpus schema, routes it by its audited licence, and writes the
requested tier locally via `prepare_release` - sharded, with its ledger, manifest and
card. Nothing here uploads anything; that is `--upload`, a separate, deliberate step.

    # build biorxiv's open tier locally, from the newest snapshot under ./data
    python -m sciencebeam_dataset_builder.corpus.build_cli --corpus biorxiv --tier open

    # then, once the local output has been checked
    python -m sciencebeam_dataset_builder.corpus.build_cli --corpus biorxiv --tier open --upload

Rerunning the same command is safe: the whole population is re-derived from the
snapshot every time and `prepare_release` overwrites the same shards, never appending
duplicates. `--base-release` is the other case - growing an *already published*
release without rewriting what is already there:

    python -m sciencebeam_dataset_builder.corpus.build_cli --corpus biorxiv --tier open \\
        --version v1.1.0 --base-release v1.0.0

This only adds. A document that no longer routes to this tier, or whose split would
disagree with what was already published, stops the build rather than silently
dropping or reshuffling it - that needs its own reviewed workflow.
"""

import argparse
import hashlib
import logging
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.ledger import (
    Change,
    ChangeType,
    PendingChange,
    read_ledger,
    read_membership,
)
from sciencebeam_dataset_builder.corpus.publish import PreparedRelease, prepare_release
from sciencebeam_dataset_builder.corpus.registry import CORPORA, Corpus
from sciencebeam_dataset_builder.corpus.routing import read_licences, route
from sciencebeam_dataset_builder.corpus.schema import CORPUS_SCHEMA
from sciencebeam_dataset_builder.corpus.verify import licences_are_publishable
from sciencebeam_dataset_builder.dataset.license.audit_cli import latest_snapshot
from sciencebeam_dataset_builder.dataset.split import SPLIT_NAMES

if TYPE_CHECKING:
    from huggingface_hub import HfApi

LOGGER = logging.getLogger(__name__)

DEFAULT_DATA_DIR = Path("data")
DEFAULT_LICENCE_REPORT = Path("data/license_report_data/licence-per-document.csv")
DEFAULT_OUTPUT_DIR = Path("data/output/corpus-build")


class BuildError(ValueError):
    """The snapshot cannot be built as asked."""


def read_legacy_rows(snapshot_dir: Path, legacy_config: str) -> list[dict[str, Any]]:
    """Every row of one legacy config's split files, each tagged with its split.

    The split is carried from the file it was read from, never recomputed - the same
    discipline the estate's invariants assume.
    """
    config_dir = snapshot_dir / legacy_config
    if not config_dir.is_dir():
        raise BuildError(f"{config_dir} does not exist")
    rows: list[dict[str, Any]] = []
    for split in SPLIT_NAMES:
        matches = sorted(config_dir.glob(f"{split}-*.parquet"))
        if not matches:
            continue
        for match in matches:
            for row in pq.read_table(match).to_pylist():
                row["_split"] = split
                rows.append(row)
    if not rows:
        raise BuildError(f"No split files found under {config_dir}")
    return rows


def route_rows(
    rows: list[dict[str, Any]], licences: dict[str, str], tier: Tier
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """The rows routed to `tier`, and their id -> split map.

    Routing keys on the legacy `source__id` uid, reconstructed here rather than stored -
    the only place that shape is still needed is this one join against the audit report.
    """
    kept: list[dict[str, Any]] = []
    splits: dict[str, str] = {}
    for row in rows:
        uid = f"{row['source']}__{row['id']}"
        verdict = route(uid, licences)
        if verdict.tier is not tier:
            continue
        kept.append({**row, "_licence": verdict.licence})
        splits[row["id"]] = row["_split"]
    return kept, splits


def build_table(rows: list[dict[str, Any]], row_updated_at: str) -> pa.Table:
    """Map legacy rows onto the 15-column corpus schema.

    `xml_upstream` is null and `xml_upstream_sha` hashes `xml` directly: nothing has been
    corrected yet, so there is no second text to hash and no original to keep separate
    from what is shipped.
    """
    columns: dict[str, list[Any]] = {name: [] for name in CORPUS_SCHEMA.names}
    for row in rows:
        columns["id"].append(row["id"])
        columns["doi"].append(row["doi"] or None)
        columns["version"].append(row["version"] or None)
        columns["pub_date"].append(row["pub_date"] or None)
        columns["licence"].append(row["_licence"])
        columns["xml_ftfy_applied"].append(row["xml_ftfy_applied"])
        columns["xml_upstream_sha"].append(
            hashlib.sha256(row["xml"].encode("utf-8")).hexdigest()
        )
        columns["xml_source_url"].append(row["xml_source_url"] or None)
        columns["xml_downloaded_at"].append(row["xml_downloaded_at"] or None)
        columns["pdf_source_url"].append(row["pdf_source_url"] or None)
        columns["pdf_downloaded_at"].append(row["pdf_downloaded_at"] or None)
        columns["row_updated_at"].append(row_updated_at)
        columns["pdf"].append(row["pdf"])
        columns["xml"].append(row["xml"])
        columns["xml_upstream"].append(None)
    return pa.table(columns, schema=CORPUS_SCHEMA)


def existing_repo_files(root: Path) -> list[str]:
    """Every file already published under `root`, as repo-relative paths."""
    if not root.is_dir():
        return []
    return [str(p.relative_to(root)) for p in root.rglob("*") if p.is_file()]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", required=True, choices=sorted(CORPORA))
    parser.add_argument("--tier", required=True, choices=[t.value for t in Tier])
    parser.add_argument(
        "--version", default="v1.0.0", help="The release tag this build produces."
    )
    parser.add_argument(
        "--base-release",
        default="",
        help="Grow an already published release instead of building from scratch. "
        "Only adds: a removal or a split disagreement stops the build.",
    )
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--licence-report", type=Path, default=DEFAULT_LICENCE_REPORT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload the built, verified local release to the Hub. A separate, "
        "deliberate step - never implied by building.",
    )
    parser.add_argument("--debug", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        stream=sys.stderr,
    )

    corpus: Corpus = CORPORA[args.corpus]
    tier = Tier(args.tier)
    if tier not in corpus.tiers:
        raise SystemExit(
            f"{corpus.name!r} has no {tier.value} repo; its tiers are "
            f"{[t.value for t in corpus.tiers]}"
        )

    snapshot_dir = latest_snapshot(args.data_dir)
    print(f"Reading {corpus.legacy_config!r} from {snapshot_dir}")
    rows = read_legacy_rows(snapshot_dir, corpus.legacy_config)

    licences = read_licences(args.licence_report)
    kept, splits = route_rows(rows, licences, tier)
    print(f"{len(kept)} of {len(rows)} rows route to {tier.value}")

    violations = (
        licences_are_publishable(r["_licence"] for r in kept) if tier.public else []
    )
    if violations:
        for v in violations:
            print(f"  REFUSED: {v}", file=sys.stderr)
        raise SystemExit(
            f"{len(violations)} row(s) failed the licence check; not building"
        )

    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    table = build_table(kept, now)

    pairing = corpus.pairings[0]
    root = args.output_dir / layout.repo_name(corpus.name, tier)
    pending: list[PendingChange] = []
    already_published: set[str] = set()
    existing_files: list[str] = []
    existing_changes: list[Change] = []

    if args.base_release:
        manifest_path = root / layout.release_manifest_path(args.base_release)
        if not manifest_path.exists():
            raise SystemExit(
                f"{manifest_path} does not exist; build {args.base_release} first"
            )
        prior = read_membership(manifest_path)
        prior_ids = {m.id for m in prior}
        carried_splits = {m.id: m.split for m in prior}

        current_ids = set(splits)
        removed = prior_ids - current_ids
        if removed:
            raise SystemExit(
                f"{len(removed)} previously published id(s) no longer route to "
                f"{tier.value}, e.g. {sorted(removed)[:3]}. This only adds; a removal "
                f"needs its own reviewed removals.yml workflow, not an automatic diff."
            )
        drifted = sorted(i for i in prior_ids if splits[i] != carried_splits[i])
        if drifted:
            raise SystemExit(
                f"{len(drifted)} id(s) would change split, e.g. {drifted[:3]}. A "
                f"split is carried forever once published, never recomputed."
            )

        new_ids = sorted(current_ids - prior_ids)
        already_published = prior_ids
        existing_files = existing_repo_files(root)
        ledger_path = root / layout.LEDGER_PATH
        existing_changes = read_ledger(ledger_path) if ledger_path.exists() else []
        pending = [
            PendingChange(
                corpus=corpus.name,
                id=id_value,
                split=splits[id_value],
                file_type_pair=pairing,
                change_type=ChangeType.ADDED,
                change_reason=f"present in {snapshot_dir.name}",
                changed_by=os.environ.get("USER", "unknown"),
            )
            for id_value in new_ids
        ]
        print(f"{len(new_ids)} new id(s) since {args.base_release}: {new_ids[:5]}")

    release = prepare_release(
        corpus=corpus,
        tier=tier,
        pairing=pairing,
        table=table,
        splits=splits,
        version=args.version,
        output_dir=args.output_dir,
        pending=pending,
        timestamp=datetime.now(UTC),
        base_release=args.base_release,
        already_published=already_published,
        existing_files=existing_files,
        existing_changes=existing_changes,
    )

    print(f"Wrote {release.root}")
    for split in SPLIT_NAMES:
        print(f"  {split}: {release.counts[pairing].get(split, 0)}")

    if not args.upload:
        print("Local build only. Pass --upload once this has been checked.")
        return

    upload_release(release)


MAX_UPLOAD_ATTEMPTS = 3


def upload_release(release: PreparedRelease) -> None:
    token = os.environ.get("HF_TOKEN")
    if not token:
        raise SystemExit("HF_TOKEN is not set; cannot upload.")

    from huggingface_hub import HfApi

    HfApi(token=token).create_repo(
        repo_id=release.repo_id, repo_type="dataset", private=True, exist_ok=True
    )

    for path in release.paths:
        local_path = release.root / path
        local_sha = hashlib.sha256(local_path.read_bytes()).hexdigest()
        _upload_with_retry(token, release.repo_id, release.version, local_path, path)
        _verify_uploaded(HfApi(token=token), release.repo_id, path, local_sha)
        print(f"  uploaded + verified: {path}")

    print(f"Uploaded {len(release.paths)} file(s) to {release.repo_id}")


def _upload_with_retry(
    token: str, repo_id: str, version: str, local_path: Path, path: str
) -> None:
    """Hub transfers truncate and stall rather than failing cleanly - a client that
    raises mid-transfer may be in a bad state, so each attempt gets a fresh one rather
    than reusing whichever client just failed.
    """
    from huggingface_hub import HfApi

    last_error: Exception | None = None
    for attempt in range(1, MAX_UPLOAD_ATTEMPTS + 1):
        try:
            HfApi(token=token).upload_file(
                path_or_fileobj=str(local_path),
                path_in_repo=path,
                repo_id=repo_id,
                repo_type="dataset",
                commit_message=f"Build {version}: {path}",
            )
            return
        except Exception as exc:  # noqa: BLE001 - Hub transfers fail in varied ways
            last_error = exc
            print(
                f"  upload of {path} failed on attempt {attempt}/{MAX_UPLOAD_ATTEMPTS}: "
                f"{exc}",
                file=sys.stderr,
            )
    raise SystemExit(
        f"Upload of {path} failed after {MAX_UPLOAD_ATTEMPTS} attempts: {last_error}"
    )


def _verify_uploaded(api: "HfApi", repo_id: str, path: str, local_sha: str) -> None:
    """Verify by content hash, never by the upload call's exit code.

    LFS-tracked files (the parquet shards) carry their sha256 in the repo's file
    metadata without a re-download; everything else is small enough to fetch back.
    """
    from huggingface_hub.hf_api import RepoFile

    info = api.get_paths_info(repo_id, [path], repo_type="dataset")[0]
    assert isinstance(info, RepoFile), f"{path} is a folder, not a file"
    if info.lfs is not None:
        remote_sha = info.lfs.sha256
    else:
        downloaded = api.hf_hub_download(repo_id, path, repo_type="dataset")
        remote_sha = hashlib.sha256(Path(downloaded).read_bytes()).hexdigest()
    if remote_sha != local_sha:
        raise SystemExit(
            f"Upload of {path} did not verify: local {local_sha[:12]} != "
            f"remote {remote_sha[:12]}. Hub transfers have truncated before; re-run."
        )


if __name__ == "__main__":
    main()
