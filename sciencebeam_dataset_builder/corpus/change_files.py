"""The declarative files under `changes/`: read strictly, appended without rewriting."""

import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import yaml

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.layout import LayoutError
from sciencebeam_dataset_builder.corpus.ledger import Change, ChangeType

# `licence` is absent on purpose: a licence change moves a document between repos.
EDITABLE_FIELDS = (
    "doi",
    "version",
    "pub_date",
    "xml_source_url",
    "xml_downloaded_at",
    "pdf_source_url",
    "pdf_downloaded_at",
)

CORRECTION_TYPES = (ChangeType.CONTENT_CORRECTED, ChangeType.METADATA_CORRECTED)

_REMOVAL_KEYS = ("id", "local_change_id", "change_reason", "changed_by")
_ADDITION_KEYS = (*_REMOVAL_KEYS, "from")
_CORRECTION_KEYS = ("local_change_id", "change_type", "change_reason", "changed_by")
_ITEM_INDENT = re.compile(r"^( *)- ", re.MULTILINE)
_NO_WRAP = 1_000_000
_REPO_ID = re.compile(r"^[\w.-]+/[\w.-]+$", re.ASCII)
_COMMIT = re.compile(r"^[0-9a-f]{40}$")


class ChangeFileError(ValueError):
    """A change file that cannot be used as written."""


@dataclass(frozen=True)
class Removal:
    """One entry of `removals.yml`."""

    id: str
    local_change_id: int
    change_reason: str
    changed_by: str


@dataclass(frozen=True)
class Addition:
    """One entry of `additions.yml`, with the repo and commit its row is read from."""

    id: str
    local_change_id: int
    change_reason: str
    changed_by: str
    from_repo: str
    from_commit: str


@dataclass(frozen=True)
class Correction:
    """One entry of a document's `correction.yml`."""

    local_change_id: int
    change_type: ChangeType
    change_reason: str
    changed_by: str
    fields: Mapping[str, str | None] = field(default_factory=dict)


class _StrictLoader(yaml.SafeLoader):  # pylint: disable=too-many-ancestors
    """A safe loader that refuses a repeated key instead of keeping the last."""


def _construct_mapping(loader: yaml.SafeLoader, node: yaml.MappingNode) -> Any:
    seen: set[Any] = set()
    for key_node, _ in node.value:
        key = loader.construct_object(key_node)
        if key in seen:
            raise yaml.constructor.ConstructorError(
                None, None, f"repeated key {key!r}", key_node.start_mark
            )
        seen.add(key)
    return loader.construct_mapping(node)


_StrictLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_mapping
)


def read_removals(text: str | None) -> list[Removal]:
    """The entries of `removals.yml`; `None` is a file that does not exist yet."""
    if text is None:
        return []
    where = layout.REMOVALS_PATH
    document = _load(text, where)
    _check_keys(document, ("removals",), (), where)
    items = document["removals"] or []
    if not isinstance(items, list):
        raise ChangeFileError(f"{where}: removals must be a list")
    removals = [
        _removal(item, f"{where} entry {n}") for n, item in enumerate(items, start=1)
    ]
    _check_unique([(r.id, r.local_change_id) for r in removals], where)
    return removals


def read_additions(text: str | None) -> list[Addition]:
    """The entries of `additions.yml`; `None` is a file that does not exist yet."""
    if text is None:
        return []
    where = layout.ADDITIONS_PATH
    document = _load(text, where)
    _check_keys(document, ("additions",), (), where)
    items = document["additions"] or []
    if not isinstance(items, list):
        raise ChangeFileError(f"{where}: additions must be a list")
    additions = [
        _addition(item, f"{where} entry {n}") for n, item in enumerate(items, start=1)
    ]
    _check_unique([(a.id, a.local_change_id) for a in additions], where)
    return additions


def read_correction(text: str | None, id_value: str) -> list[Correction]:
    """The entries of one document's `correction.yml`; `None` is no file yet."""
    if text is None:
        return []
    where = layout.correction_paths(id_value).metadata
    document = _load(text, where)
    _check_keys(document, ("id", "corrections"), (), where)
    if _text(document["id"], "id", where) != id_value:
        raise ChangeFileError(f"{where} names id {document['id']!r}")
    items = document["corrections"]
    if not isinstance(items, list) or not items:
        raise ChangeFileError(f"{where}: corrections must be a non-empty list")
    corrections = [
        _correction(item, f"{where} entry {n}") for n, item in enumerate(items, start=1)
    ]
    _check_unique([c.local_change_id for c in corrections], where)
    return corrections


def append_removals(text: str | None, new: Sequence[Removal]) -> str:
    """`removals.yml` with `new` after the existing entries, which stay byte for byte."""
    if not new:
        raise ChangeFileError("no removals to append")
    existing = read_removals(text)
    appended = _append_items(
        "removals:\n" if text is None else text, [_removal_item(r) for r in new]
    )
    _check_appended(
        lambda: read_removals(appended), [*existing, *new], layout.REMOVALS_PATH
    )
    return appended


def append_additions(text: str | None, new: Sequence[Addition]) -> str:
    """`additions.yml` with `new` after the existing entries, which stay byte for byte."""
    if not new:
        raise ChangeFileError("no additions to append")
    existing = read_additions(text)
    appended = _append_items(
        "additions:\n" if text is None else text, [_addition_item(a) for a in new]
    )
    _check_appended(
        lambda: read_additions(appended), [*existing, *new], layout.ADDITIONS_PATH
    )
    return appended


def append_correction(text: str | None, id_value: str, new: Correction) -> str:
    """`correction.yml` with `new` after the existing entries, which stay byte for byte."""
    existing = read_correction(text, id_value)
    header = _dump({"id": id_value}) + "corrections:\n"
    appended = _append_items(header if text is None else text, [_correction_item(new)])
    _check_appended(
        lambda: read_correction(appended, id_value),
        [*existing, new],
        layout.correction_paths(id_value).metadata,
    )
    return appended


def next_local_change_id(
    id_value: str,
    removals: Iterable[Removal],
    additions: Iterable[Addition],
    corrections: Iterable[Correction],
    ledger: Iterable[Change],
) -> int:
    """One past the highest the document has used anywhere, since the ledger refuses
    a repeated `(id, local_change_id)`. `corrections` are the document's own."""
    used = [r.local_change_id for r in removals if r.id == id_value]
    used += [a.local_change_id for a in additions if a.id == id_value]
    used += [c.local_change_id for c in corrections]
    used += [
        c.local_change_id
        for c in ledger
        if c.id == id_value and c.local_change_id is not None
    ]
    return max(used, default=0) + 1


def _load(text: str, where: str) -> dict[Any, Any]:
    try:
        document = yaml.load(text, Loader=_StrictLoader)
    except yaml.YAMLError as exc:
        raise ChangeFileError(f"{where} is not valid YAML: {exc}") from exc
    if not isinstance(document, dict):
        raise ChangeFileError(f"{where} must be a mapping at the top level")
    return document


def _check_keys(
    mapping: Any, required: Sequence[str], optional: Sequence[str], where: str
) -> None:
    if not isinstance(mapping, dict):
        raise ChangeFileError(f"{where} must be a mapping")
    if missing := [k for k in required if k not in mapping]:
        raise ChangeFileError(f"{where} is missing {', '.join(missing)}")
    if unknown := sorted(str(k) for k in mapping if k not in (*required, *optional)):
        raise ChangeFileError(f"{where} has unknown key(s) {', '.join(unknown)}")


def _text(value: Any, name: str, where: str) -> str:
    """Unquoted YAML turns `10.1101_000000` into a float and a date into a date."""
    if not isinstance(value, str):
        raise ChangeFileError(f"{where}: {name} must be text, got {value!r}; quote it")
    if not value.strip():
        raise ChangeFileError(f"{where}: {name} is blank")
    return value


def _document_id(value: Any, where: str) -> str:
    """An id that could also name the document's correction directory."""
    id_value = _text(value, "id", where)
    try:
        layout.correction_paths(id_value)
    except LayoutError as exc:
        raise ChangeFileError(f"{where}: {exc}") from exc
    return id_value


def _local_change_id(value: Any, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ChangeFileError(
            f"{where}: local_change_id must be a whole number from 1, got {value!r}"
        )
    return int(value)


def _removal(item: Any, where: str) -> Removal:
    _check_keys(item, _REMOVAL_KEYS, (), where)
    return Removal(
        id=_document_id(item["id"], where),
        local_change_id=_local_change_id(item["local_change_id"], where),
        change_reason=_text(item["change_reason"], "change_reason", where),
        changed_by=_text(item["changed_by"], "changed_by", where),
    )


def _addition(item: Any, where: str) -> Addition:
    _check_keys(item, _ADDITION_KEYS, (), where)
    from_repo, from_commit = _from(item["from"], where)
    return Addition(
        id=_document_id(item["id"], where),
        local_change_id=_local_change_id(item["local_change_id"], where),
        change_reason=_text(item["change_reason"], "change_reason", where),
        changed_by=_text(item["changed_by"], "changed_by", where),
        from_repo=from_repo,
        from_commit=from_commit,
    )


def _from(value: Any, where: str) -> tuple[str, str]:
    """`<repo id>@<commit>`: a full commit, since a branch or a tag can move."""
    repo, _, commit = _text(value, "from", where).rpartition("@")
    if not _REPO_ID.match(repo) or not _COMMIT.match(commit):
        raise ChangeFileError(
            f"{where}: from must be <repo id>@<full 40-character commit>, got {value!r}"
        )
    return repo, commit


def _correction(item: Any, where: str) -> Correction:
    _check_keys(item, _CORRECTION_KEYS, ("fields",), where)
    by_value = {t.value: t for t in CORRECTION_TYPES}
    raw_type = item["change_type"]
    change_type = by_value.get(raw_type) if isinstance(raw_type, str) else None
    if change_type is None:
        raise ChangeFileError(
            f"{where}: change_type must be one of {', '.join(by_value)}, got {raw_type!r}"
        )
    if change_type is ChangeType.METADATA_CORRECTED:
        fields = _fields(item.get("fields"), where)
    elif "fields" in item:
        raise ChangeFileError(f"{where}: only a metadata correction has fields")
    else:
        fields = {}
    return Correction(
        local_change_id=_local_change_id(item["local_change_id"], where),
        change_type=change_type,
        change_reason=_text(item["change_reason"], "change_reason", where),
        changed_by=_text(item["changed_by"], "changed_by", where),
        fields=fields,
    )


def _fields(value: Any, where: str) -> dict[str, str | None]:
    if not isinstance(value, dict) or not value:
        raise ChangeFileError(f"{where}: a metadata correction needs fields")
    fields: dict[str, str | None] = {}
    for name, new in value.items():
        if name not in EDITABLE_FIELDS:
            raise ChangeFileError(
                f"{where}: {name!r} is not editable; editable are "
                f"{', '.join(EDITABLE_FIELDS)}"
            )
        if new is not None and not isinstance(new, str):
            raise ChangeFileError(
                f"{where}: {name} must be text or null, got {new!r}; quote it"
            )
        fields[name] = new
    return fields


def _check_unique(keys: Sequence[object], where: str) -> None:
    repeated = sorted({repr(k) for k in keys if keys.count(k) > 1})
    if repeated:
        raise ChangeFileError(f"{where}: {', '.join(repeated)} appear more than once")


def _removal_item(removal: Removal) -> dict[str, Any]:
    return {
        "id": removal.id,
        "local_change_id": removal.local_change_id,
        "change_reason": removal.change_reason,
        "changed_by": removal.changed_by,
    }


def _addition_item(addition: Addition) -> dict[str, Any]:
    return {
        "id": addition.id,
        "local_change_id": addition.local_change_id,
        "change_reason": addition.change_reason,
        "changed_by": addition.changed_by,
        "from": f"{addition.from_repo}@{addition.from_commit}",
    }


def _correction_item(correction: Correction) -> dict[str, Any]:
    item: dict[str, Any] = {
        "local_change_id": correction.local_change_id,
        "change_type": correction.change_type.value,
        "change_reason": correction.change_reason,
        "changed_by": correction.changed_by,
    }
    if correction.fields:
        item["fields"] = dict(correction.fields)
    return item


def _dump(value: Any) -> str:
    return yaml.safe_dump(
        value,
        sort_keys=False,
        allow_unicode=True,
        default_flow_style=False,
        width=_NO_WRAP,
    )


def _append_items(text: str, items: Sequence[dict[str, Any]]) -> str:
    """Add list items to the end of `text`, at the indent its items already use."""
    match = _ITEM_INDENT.search(text)
    indent = match.group(1) if match else "  "
    block = "".join(
        indent + line if line.strip() else line
        for line in _dump(list(items)).splitlines(keepends=True)
    )
    return text + ("" if text.endswith("\n") else "\n") + block


def _check_appended(
    read: Callable[[], Sequence[object]], expected: Sequence[object], where: str
) -> None:
    """The appended text has to read back as exactly the old entries plus the new."""
    try:
        result = read()
    except ChangeFileError as exc:
        raise ChangeFileError(f"cannot append to {where}: {exc}") from exc
    if result != list(expected):
        raise ChangeFileError(
            f"cannot append to {where}: its list has to be the last thing in the file"
        )
