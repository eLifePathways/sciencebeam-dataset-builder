"""Assembling one repo's release on disk, before anything is uploaded.

Nothing here talks to the Hub. A release is written locally, verified, and only then
handed to the uploader, so the checks cannot be skipped by a network call happening
first.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.card import render_card
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.ledger import (
    Change,
    Membership,
    PendingChange,
    assign_change_ids,
    write_ledger,
    write_membership,
)
from sciencebeam_dataset_builder.corpus.parquet_io import (
    DEFAULT_SHARD_BYTES,
    shard_tables,
    write_shard,
)
from sciencebeam_dataset_builder.corpus.registry import Corpus
from sciencebeam_dataset_builder.corpus.verify import Violation, verify_corpus
from sciencebeam_dataset_builder.dataset.split import SPLIT_NAMES


class PublishError(ValueError):
    """A release that cannot be assembled as asked."""


@dataclass(frozen=True)
class PreparedRelease:
    """One repo's release, written locally and not yet uploaded."""

    corpus: Corpus
    tier: Tier
    version: str
    root: Path
    paths: tuple[str, ...]
    membership: tuple[Membership, ...]
    changes: tuple[Change, ...]
    counts: Mapping[str, Mapping[str, int]]

    @property
    def repo_id(self) -> str:
        return self.corpus.repo_id(self.tier)


def rows_for_split(table: pa.Table, splits: Mapping[str, str], split: str) -> pa.Table:
    """The rows labelled `split`, in the order they arrived."""
    wanted = [id_value for id_value, name in splits.items() if name == split]
    mask = pc.is_in(table.column("id"), value_set=pa.array(wanted, type=pa.string()))
    return table.filter(mask)


def prepare_release(
    corpus: Corpus,
    tier: Tier,
    pairing: str,
    table: pa.Table,
    splits: Mapping[str, str],
    version: str,
    output_dir: Path,
    pending: Sequence[PendingChange],
    timestamp: datetime,
    existing_files: Sequence[str] = (),
    existing_changes: Sequence[Change] = (),
    max_shard_bytes: int = DEFAULT_SHARD_BYTES,
) -> PreparedRelease:
    """Write one repo's release under `output_dir`, ready to upload."""
    if pairing not in corpus.pairings:
        raise PublishError(f"{corpus.name!r} does not publish {pairing!r}")
    missing = [
        id_value
        for id_value in table.column("id").to_pylist()
        if id_value not in splits
    ]
    if missing:
        raise PublishError(
            f"{len(missing)} row(s) have no split label, e.g. {missing[:3]}. A label is "
            f"carried from the source release, never invented here."
        )

    root = output_dir / layout.repo_name(corpus.name, tier)
    paths: list[str] = []
    counts: dict[str, int] = {}
    membership: list[Membership] = []

    for split in SPLIT_NAMES:
        rows = rows_for_split(table, splits, split)
        counts[split] = rows.num_rows
        membership.extend(
            # Membership still has (uid, source, split): its shape is due for the same
            # rework as the rest of the ledger, deferred to keep this change small. The
            # bare id is stored under `uid` here as an interim, acknowledged mismatch.
            Membership(uid=id_value, source=corpus.source, split=split)
            for id_value in rows.column("id").to_pylist()
        )
        index = layout.next_shard_index(existing_files, pairing, split)
        for shard in shard_tables(rows, max_bytes=max_shard_bytes):
            path = layout.shard_path(pairing, split, index)
            write_shard(shard, str(_local(root, path)))
            paths.append(path)
            index += 1

    changes = assign_change_ids(pending, version, timestamp, existing_changes)
    all_changes = [*existing_changes, *changes]
    by_pairing = {pairing: counts}

    write_ledger(_local(root, layout.LEDGER_PATH), all_changes)
    paths.append(layout.LEDGER_PATH)

    manifest_path = layout.release_manifest_path(version)
    write_membership(_local(root, manifest_path), membership)
    paths.append(manifest_path)

    _local(root, layout.CARD_PATH).write_text(
        render_card(corpus, tier, by_pairing), encoding="utf-8"
    )
    paths.append(layout.CARD_PATH)

    return PreparedRelease(
        corpus=corpus,
        tier=tier,
        version=version,
        root=root,
        paths=tuple(paths),
        membership=tuple(membership),
        changes=tuple(changes),
        counts=by_pairing,
    )


def verify_release(
    source: Sequence[Membership], prepared: Mapping[Tier, PreparedRelease]
) -> list[Violation]:
    """Check a corpus's two halves against the release they were cut from.

    Takes both tiers because that is the grain the invariants hold at: a half on its own
    cannot show that the union reconstructs, nor that the halves are disjoint.
    """
    open_rows = list(prepared[Tier.OPEN].membership) if Tier.OPEN in prepared else []
    restricted = (
        list(prepared[Tier.RESTRICTED].membership)
        if Tier.RESTRICTED in prepared
        else []
    )
    return verify_corpus(source, open_rows, restricted)


def _local(root: Path, path: str) -> Path:
    """The local file a repo path is written to, its directory made."""
    local = root / path
    local.parent.mkdir(parents=True, exist_ok=True)
    return local
