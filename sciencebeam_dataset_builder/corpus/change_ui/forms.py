"""What a person types or uploads on the change request pages, read strictly."""

import csv
import io
import re
from dataclasses import dataclass, field, replace
from typing import Any

from starlette.datastructures import FormData

MAX_DOCUMENTS = 500
MAX_CSV_BYTES = 1_000_000
MAX_REASON = 2000

_SEPARATORS = re.compile(r"[\s,;]+")


class FormError(ValueError):
    """Input that cannot be used as given. The message is shown to the person."""


def parse_ids(text: str) -> list[str]:
    """Ids separated by new lines, spaces, tabs, commas or semicolons."""
    return [part for part in _SEPARATORS.split(text) if part]


def read_csv(data: bytes) -> tuple[list[str], dict[str, str]]:
    """The ids in a CSV's `uid` or `id` column, and the reasons any row gives."""
    if len(data) > MAX_CSV_BYTES:
        raise FormError("The CSV is larger than 1 MB.")
    try:
        text = data.decode("utf-8-sig")
        rows = [r for r in csv.reader(io.StringIO(text)) if any(c.strip() for c in r)]
    except (UnicodeDecodeError, csv.Error) as exc:
        raise FormError(f"The CSV cannot be read: {exc}") from exc
    if not rows:
        raise FormError("The CSV is empty.")
    header = [cell.strip().lower() for cell in rows[0]]
    id_column = _column(header, "uid", "id")
    if id_column is None:
        raise FormError("The CSV needs a header row with an id or uid column.")
    reason_column = _column(header, "reason", "change_reason")
    ids: list[str] = []
    reasons: dict[str, str] = {}
    for row in rows[1:]:
        id_value = _cell(row, id_column)
        if not id_value:
            continue
        ids.append(id_value)
        if reason_column is not None and (reason := _cell(row, reason_column)):
            reasons[id_value] = reason
    return ids, reasons


def check_count(queries: list[str]) -> None:
    if not queries:
        raise FormError("Enter at least one document id.")
    if len(queries) > MAX_DOCUMENTS:
        raise FormError(
            f"That is {len(queries)} ids. Send at most {MAX_DOCUMENTS} at a time."
        )


@dataclass(frozen=True)
class Draft:
    """The request so far, carried from page to page in hidden fields."""

    ids: str = ""
    uids: tuple[str, ...] = ()
    shared: str = ""
    own: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_form(cls, form: FormData) -> "Draft":
        uids = tuple(dict.fromkeys(u for u in map(_text, form.getlist("uid")) if u))
        if len(uids) > MAX_DOCUMENTS:
            raise FormError(f"Send at most {MAX_DOCUMENTS} documents at a time.")
        draft = cls(
            ids=_text(form.get("ids")),
            uids=uids,
            shared=_text(form.get("shared")),
            own={u: _text(form.get(f"own:{u}")) for u in uids},
        )
        if any(len(draft.reason(u)) > MAX_REASON for u in uids):
            raise FormError(f"A reason can be at most {MAX_REASON} characters.")
        return draft

    def reason(self, uid: str) -> str:
        """The document's own reason, or the one given for all of them."""
        return self.own.get(uid, "").strip() or self.shared.strip()

    def requests(self) -> list[tuple[str, str]]:
        return [(uid, self.reason(uid)) for uid in self.uids]

    def without(self, uid: str) -> "Draft":
        return replace(self, uids=tuple(u for u in self.uids if u != uid))


def _column(header: list[str], *names: str) -> int | None:
    return next((header.index(n) for n in names if n in header), None)


def _cell(row: list[str], column: int) -> str:
    return row[column].strip() if column < len(row) else ""


def _text(value: Any) -> str:
    """A form field's text; an uploaded file where text was expected reads as none."""
    return value.strip() if isinstance(value, str) else ""
