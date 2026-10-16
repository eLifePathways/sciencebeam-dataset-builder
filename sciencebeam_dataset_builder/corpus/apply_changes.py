"""Applying merged changes: the release that turns `changes/` into the dataset.

Reads a repo as it stands, finds what `main` holds that the ledger does not yet record,
rewrites each affected shard once, puts added documents in new shards after the existing
ones, and writes the new release locally - its shards, ledger, manifest and card - for a
separate, deliberate upload.
"""

import hashlib
import json
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.addition_rows import (
    HARVEST_REPO_ID,
    AdditionRowsError,
    read_rows,
)
from sciencebeam_dataset_builder.corpus.card import render_card
from sciencebeam_dataset_builder.corpus.change_files import (
    EDITABLE_FIELDS,
    Addition,
    Correction,
    Removal,
    read_additions,
    read_correction,
    read_removals,
)
from sciencebeam_dataset_builder.corpus.ledger import (
    Change,
    ChangeType,
    Membership,
    PendingChange,
    assign_change_ids,
    read_ledger,
    read_membership,
    write_ledger,
    write_membership,
)
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.parquet_io import (
    DEFAULT_SHARD_BYTES,
    shard_tables,
    write_shard,
)
from sciencebeam_dataset_builder.corpus.repo_reader import HubRepo, RepoReader
from sciencebeam_dataset_builder.corpus.routing import route_licence
from sciencebeam_dataset_builder.corpus.schema import CORPUS_SCHEMA
from sciencebeam_dataset_builder.dataset.split import SPLIT_NAMES

MAJOR, MINOR, PATCH = "major", "minor", "patch"

# What each change does to a number published against the release before it.
BUMPS = {
    ChangeType.REMOVED: MAJOR,
    ChangeType.CONTENT_CORRECTED: MAJOR,
    ChangeType.ADDED: MINOR,
    ChangeType.METADATA_CORRECTED: PATCH,
}

_CORRECTION_FILE = f"/{layout.CORRECTION_METADATA_FILENAME}"

# Reads a repo at a revision: where an addition's rows come from.
OpenRepo = Callable[[str, str], RepoReader]


class ApplyError(ValueError):
    """Changes that cannot be applied as they stand."""


@dataclass(frozen=True)
class Pending:
    """What `main` holds that the dataset does not yet: the next release's changes."""

    base_release: str
    files: tuple[str, ...]
    membership: tuple[Membership, ...]
    ledger: tuple[Change, ...]
    removals: tuple[Removal, ...]
    corrections: Mapping[str, tuple[Correction, ...]]
    corrected_xml: Mapping[str, str]
    additions: tuple[Addition, ...] = ()

    @property
    def change_types(self) -> list[ChangeType]:
        types = [ChangeType.REMOVED for _ in self.removals]
        types += [ChangeType.ADDED for _ in self.additions]
        for entries in self.corrections.values():
            types.extend(entry.change_type for entry in entries)
        return types

    @property
    def empty(self) -> bool:
        return not self.change_types

    @property
    def bump(self) -> str:
        """The smallest version step these changes allow; empty when nothing is pending."""
        bumps = {BUMPS[t] for t in self.change_types}
        return next((b for b in (MAJOR, MINOR, PATCH) if b in bumps), "")


@dataclass(frozen=True)
class AppliedRelease:
    """A release written locally under `root`, holding only the files it changes."""

    repo: Repo
    base_release: str
    version: str
    commit: str
    root: Path
    paths: tuple[str, ...]
    membership: tuple[Membership, ...]
    changes: tuple[Change, ...]
    counts: Mapping[str, Mapping[str, int]]


def read_pending(repo: Repo) -> Pending:
    """The changes on `main` the ledger does not record, checked to apply cleanly."""
    reader = repo.reader
    files = reader.files()
    base = layout.latest_release(files)
    if base is None:
        raise ApplyError(f"{repo.repo_id} has no release to apply changes to")
    membership = read_membership(reader.fetch(layout.release_manifest_path(base)))
    ledger = (
        read_ledger(reader.fetch(layout.LEDGER_PATH))
        if layout.LEDGER_PATH in files
        else []
    )
    recorded = {(c.id, c.local_change_id) for c in ledger if c.local_change_id}

    removals_text = (
        _text(repo, layout.REMOVALS_PATH) if layout.REMOVALS_PATH in files else None
    )
    removals = [
        r
        for r in read_removals(removals_text)
        if (r.id, r.local_change_id) not in recorded
    ]
    additions = [
        a
        for a in read_additions(
            _text(repo, layout.ADDITIONS_PATH)
            if layout.ADDITIONS_PATH in files
            else None
        )
        if (a.id, a.local_change_id) not in recorded
    ]
    corrections: dict[str, tuple[Correction, ...]] = {}
    corrected_xml: dict[str, str] = {}
    for id_value in _correction_ids(files):
        paths = layout.correction_paths(id_value)
        entries = sorted(
            (
                c
                for c in read_correction(_text(repo, paths.metadata), id_value)
                if (id_value, c.local_change_id) not in recorded
            ),
            key=lambda c: c.local_change_id,
        )
        if not entries:
            continue
        corrections[id_value] = tuple(entries)
        if any(c.change_type is ChangeType.CONTENT_CORRECTED for c in entries):
            if paths.corrected not in files:
                raise ApplyError(
                    f"{id_value} has a pending content correction but no {paths.corrected}"
                )
            corrected_xml[id_value] = _text(repo, paths.corrected)

    _check_applicable(membership, removals, corrections, additions)
    return Pending(
        base,
        tuple(files),
        tuple(membership),
        tuple(ledger),
        tuple(removals),
        corrections,
        corrected_xml,
        tuple(additions),
    )


def next_version(base: str, bump: str) -> str:
    major, minor, patch = layout.version_key(base)
    if bump == MAJOR:
        return f"v{major + 1}.0.0"
    if bump == MINOR:
        return f"v{major}.{minor + 1}.0"
    return f"v{major}.{minor}.{patch + 1}"


def check_version(base: str, version: str, bump: str) -> None:
    """`version` has to follow `base` by at least the step the changes need."""
    old, new = layout.version_key(base), layout.version_key(version)
    if new <= old:
        raise ApplyError(f"{version} does not follow {base}")
    if bump == MAJOR and new[0] == old[0]:
        raise ApplyError(
            f"these changes need a major version, {next_version(base, MAJOR)} or later"
        )
    if bump == MINOR and new[:2] == old[:2]:
        raise ApplyError(
            f"these changes need a minor version, {next_version(base, MINOR)} or later"
        )


def open_hub(repo_id: str, revision: str) -> RepoReader:
    return HubRepo(repo_id, revision)


def apply(
    repo: Repo,
    pending: Pending,
    version: str,
    output_dir: Path,
    timestamp: datetime,
    open_repo: OpenRepo = open_hub,
    max_shard_bytes: int = DEFAULT_SHARD_BYTES,
) -> AppliedRelease:
    """Write release `version` under `output_dir`: only the files it changes."""
    if pending.empty:
        raise ApplyError("nothing is pending, so there is nothing to release")
    check_version(pending.base_release, version, pending.bump)
    manifest = layout.release_manifest_path(version)
    if manifest in pending.files:
        raise ApplyError(f"{version} is already released")
    root = release_root(output_dir, repo.repo_id, version)
    if root.exists() or record_path(root).exists():
        raise ApplyError(f"{root} already exists; remove it to apply again")

    stamp = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    added, added_splits = _added_rows(repo, pending.additions, open_repo, stamp)
    removed = {r.id for r in pending.removals}
    located = _locate(repo, pending, removed | set(pending.corrections))
    paths: list[str] = []
    for shard in sorted({shard for _, shard in located.values()}):
        table = pq.read_table(repo.reader.fetch(shard))
        if not table.schema.equals(CORPUS_SCHEMA):
            raise ApplyError(f"{shard} is not on the corpus schema")
        table = _rewrite(table, removed, pending, stamp)
        write_shard(table, str(_local(root, shard)))
        paths.append(shard)
    added_to = _write_added(
        repo, pending.files, added, added_splits, root, max_shard_bytes
    )
    paths += sorted(set(added_to.values()))

    changes = assign_change_ids(
        _ledger_rows(repo, pending, located, added_splits, added_to),
        pending.base_release,
        timestamp,
        pending.ledger,
    )
    write_ledger(_local(root, layout.LEDGER_PATH), [*pending.ledger, *changes])
    membership = (
        *(m for m in pending.membership if m.id not in removed),
        *(
            Membership(repo.corpus.name, a.id, added_splits[a.id])
            for a in pending.additions
        ),
    )
    write_membership(_local(root, manifest), membership)
    in_split = Counter(m.split for m in membership)
    counts = {repo.corpus.pairings[0]: {s: in_split.get(s, 0) for s in SPLIT_NAMES}}
    _local(root, layout.CARD_PATH).write_text(
        render_card(repo.corpus, repo.tier, counts), encoding="utf-8"
    )
    paths += [layout.LEDGER_PATH, manifest, layout.CARD_PATH]

    commit = repo.reader.commit if isinstance(repo.reader, HubRepo) else ""
    ReleaseRecord(
        repo.repo_id,
        pending.base_release,
        version,
        commit,
        dict(Counter(c.change_type.value for c in changes)),
        {path: _sha256(root / path) for path in paths},
    ).write(record_path(root))
    return AppliedRelease(
        repo,
        pending.base_release,
        version,
        commit,
        root,
        tuple(paths),
        membership,
        tuple(changes),
        counts,
    )


def release_root(output_dir: Path, repo_id: str, version: str) -> Path:
    """Where a release is written: `<output>/<repo name>/<version>/`."""
    layout.version_key(version)
    return output_dir / repo_id.rsplit("/", 1)[-1] / version


def record_path(root: Path) -> Path:
    """Beside the release, so it is never mistaken for one of the repo's files."""
    return root.with_name(f"{root.name}.json")


@dataclass(frozen=True)
class ReleaseRecord:
    """What a release holds and the commit it was made from, so an upload sends
    exactly what was checked, onto exactly what it was made from."""

    repo_id: str
    base_release: str
    version: str
    commit: str
    changes: Mapping[str, int]
    files: Mapping[str, str]

    def write(self, path: Path) -> None:
        path.write_text(json.dumps(asdict(self), indent=2) + "\n", encoding="utf-8")

    @classmethod
    def read(cls, path: Path) -> "ReleaseRecord":
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return cls(**data)
        except (OSError, ValueError, TypeError) as exc:
            raise ApplyError(f"{path} cannot be read: {exc}") from exc


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _check_applicable(
    membership: Sequence[Membership],
    removals: Sequence[Removal],
    corrections: Mapping[str, Sequence[Correction]],
    additions: Sequence[Addition] = (),
) -> None:
    """Refuse what the proposal checks should have stopped, rather than guess."""
    removed = [r.id for r in removals]
    if repeated := sorted({i for i in removed if removed.count(i) > 1}):
        raise ApplyError(f"more than one pending removal for {repeated[:5]}")
    if both := sorted(set(removed) & set(corrections)):
        raise ApplyError(f"both removed and corrected: {both[:5]}")
    members = {m.id for m in membership}
    if unknown := sorted((set(removed) | set(corrections)) - members):
        raise ApplyError(f"not in the release the changes apply to: {unknown[:5]}")
    added = [a.id for a in additions]
    if repeated := sorted({i for i in added if added.count(i) > 1}):
        raise ApplyError(f"more than one pending addition for {repeated[:5]}")
    if present := sorted(set(added) & members):
        raise ApplyError(f"added but already in the release: {present[:5]}")
    if both := sorted(set(added) & (set(removed) | set(corrections))):
        raise ApplyError(f"both added and removed or corrected: {both[:5]}")


def _added_rows(
    repo: Repo, additions: Sequence[Addition], open_repo: OpenRepo, stamp: str
) -> tuple[pa.Table, dict[str, str]]:
    """The added documents' rows, in the order they were declared, and their splits.

    A move waits until the losing repo has released its removal, so a document is never
    in both halves of a corpus; and each licence is read again, since the policy or the
    extractor may have changed since the proposal.
    """
    if not additions:
        return CORPUS_SCHEMA.empty_table(), {}
    tables: list[pa.Table] = []
    splits: dict[str, str] = {}
    by_from: dict[tuple[str, str], list[str]] = {}
    for addition in additions:
        by_from.setdefault((addition.from_repo, addition.from_commit), []).append(
            addition.id
        )
    try:
        for (from_repo, commit), ids in by_from.items():
            if from_repo != HARVEST_REPO_ID:
                losing = open_repo(from_repo, "main")
                release = layout.latest_release(losing.files()) or ""
                still = (
                    {
                        m.id
                        for m in read_membership(
                            losing.fetch(layout.release_manifest_path(release))
                        )
                    }
                    if release
                    else set()
                )
                if waiting := [i for i in ids if i in still]:
                    raise ApplyError(
                        f"still in {from_repo}'s latest release, {release}: "
                        f"{waiting[:5]}; release their removal there first"
                    )
            table, found = read_rows(
                repo.corpus, from_repo, open_repo(from_repo, commit), ids, stamp
            )
            tables.append(table)
            splits.update(found)
    except AdditionRowsError as exc:
        raise ApplyError(str(exc)) from exc
    rows = pa.concat_tables(tables)
    order = {id_value: n for n, id_value in enumerate(rows.column("id").to_pylist())}
    rows = rows.take(pa.array([order[a.id] for a in additions]))
    for id_value, licence in zip(
        rows.column("id").to_pylist(), rows.column("licence").to_pylist(), strict=True
    ):
        tier = route_licence(f"{repo.corpus.source}__{id_value}", licence).tier
        if tier is not repo.tier:
            raise ApplyError(
                f"{id_value}: its licence, {licence or 'none'}, now puts it in the "
                f"{tier.value} repo, not this one"
            )
    return rows, splits


def _write_added(
    repo: Repo,
    files: Sequence[str],
    added: pa.Table,
    splits: Mapping[str, str],
    root: Path,
    max_shard_bytes: int,
) -> dict[str, str]:
    """Write the added rows in new shards after each split's last; id -> shard path."""
    pairing = repo.corpus.pairings[0]
    shard_of: dict[str, str] = {}
    for split in SPLIT_NAMES:
        ids = [i for i in added.column("id").to_pylist() if splits[i] == split]
        if not ids:
            continue
        rows = added.filter(pc.is_in(added.column("id"), pa.array(ids)))
        index = layout.next_shard_index(files, pairing, split)
        for shard in shard_tables(rows, max_bytes=max_shard_bytes):
            path = layout.shard_path(pairing, split, index)
            write_shard(shard, str(_local(root, path)))
            for id_value in shard.column("id").to_pylist():
                shard_of[id_value] = path
            index += 1
    return shard_of


def _locate(repo: Repo, pending: Pending, ids: set[str]) -> dict[str, tuple[str, str]]:
    """id -> (pairing, shard), reading only the `id` column of the splits involved."""
    split_of = {m.id: m.split for m in pending.membership}
    located: dict[str, tuple[str, str]] = {}
    for split in sorted({split_of[i] for i in ids}):
        for pairing in repo.corpus.pairings:
            for shard in layout.split_shards(pending.files, pairing, split):
                column = repo.reader.read_columns(shard, ["id"]).column("id")
                for id_value in column.to_pylist():
                    if id_value not in ids:
                        continue
                    if id_value in located:
                        raise ApplyError(f"{id_value} is in more than one shard")
                    located[id_value] = (pairing, shard)
    if missing := sorted(ids - set(located)):
        raise ApplyError(f"in no shard of their split: {missing[:5]}")
    return located


def _rewrite(
    table: pa.Table, removed: set[str], pending: Pending, stamp: str
) -> pa.Table:
    """The shard without its removed rows and with its corrections applied.

    `pdf` is never read into Python: only the columns a correction changes are rebuilt.
    """
    keep = [id_value not in removed for id_value in table.column("id").to_pylist()]
    table = table.filter(pa.array(keep, type=pa.bool_()))
    ids = table.column("id").to_pylist()
    if not any(i in pending.corrections for i in ids):
        return table

    names = ("xml", "xml_upstream", "row_updated_at", *EDITABLE_FIELDS)
    columns = {name: table.column(name).to_pylist() for name in names}
    for row, id_value in enumerate(ids):
        entries = pending.corrections.get(id_value)
        if not entries:
            continue
        if id_value in pending.corrected_xml:
            corrected = pending.corrected_xml[id_value]
            if corrected == columns["xml"][row]:
                raise ApplyError(
                    f"{id_value}: corrected.xml is the same as the XML it would replace"
                )
            # Only the first correction keeps the original: a later one must not
            # overwrite it with an intermediate version.
            if columns["xml_upstream"][row] is None:
                columns["xml_upstream"][row] = columns["xml"][row]
            columns["xml"][row] = corrected
        for entry in entries:
            for name, value in entry.fields.items():
                columns[name][row] = value
        columns["row_updated_at"][row] = stamp

    for name, values in columns.items():
        index = table.schema.get_field_index(name)
        field = table.schema.field(name)
        table = table.set_column(index, field, pa.array(values, type=field.type))
    return table


def _ledger_rows(
    repo: Repo,
    pending: Pending,
    located: Mapping[str, tuple[str, str]],
    added_splits: Mapping[str, str],
    added_to: Mapping[str, str],
) -> list[PendingChange]:
    """One row per change: removals, then corrections, each in id order, then
    additions in the order they were declared."""
    split_of = {m.id: m.split for m in pending.membership}

    def row(
        id_value: str, entry: Removal | Correction, change_type: ChangeType
    ) -> PendingChange:
        pairing, shard = located[id_value]
        return PendingChange(
            corpus=repo.corpus.name,
            id=id_value,
            split=split_of[id_value],
            file_type_pair=pairing,
            change_type=change_type,
            change_reason=entry.change_reason,
            changed_by=entry.changed_by,
            local_change_id=entry.local_change_id,
            parquet_file_name=Path(shard).name,
        )

    rows = [
        row(r.id, r, ChangeType.REMOVED)
        for r in sorted(pending.removals, key=lambda r: r.id)
    ]
    for id_value in sorted(pending.corrections):
        rows += [row(id_value, c, c.change_type) for c in pending.corrections[id_value]]
    rows += [
        PendingChange(
            corpus=repo.corpus.name,
            id=a.id,
            split=added_splits[a.id],
            file_type_pair=repo.corpus.pairings[0],
            change_type=ChangeType.ADDED,
            change_reason=a.change_reason,
            changed_by=a.changed_by,
            local_change_id=a.local_change_id,
            parquet_file_name=Path(added_to[a.id]).name,
        )
        for a in pending.additions
    ]
    return rows


def _correction_ids(files: Sequence[str]) -> list[str]:
    """Every document with a `correction.yml`, content and metadata ones alike."""
    prefix = f"{layout.CORRECTIONS_DIRECTORY}/"
    ids = []
    for path in files:
        if path.startswith(prefix) and path.endswith(_CORRECTION_FILE):
            middle = path[len(prefix) : -len(_CORRECTION_FILE)]
            if middle and "/" not in middle:
                ids.append(middle)
    return sorted(ids)


def _text(repo: Repo, path: str) -> str:
    return repo.reader.fetch(path).read_text(encoding="utf-8")


def _local(root: Path, path: str) -> Path:
    local = root / path
    local.parent.mkdir(parents=True, exist_ok=True)
    return local
