"""`attributions.csv`: whom to credit for each document in an open repo.

Made from the XML each release ships, like the card, so it cannot drift from the documents.
"""

import csv
import io
import xml.etree.ElementTree as ET
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import astuple, dataclass
from typing import Any

import pyarrow as pa

from sciencebeam_dataset_builder.dataset.license.extract_license import parse_xml

ATTRIBUTION_FIELDS = ("id", "doi", "title", "authors", "licence")
# The parquet columns an attribution is made from.
ATTRIBUTION_COLUMNS = ("id", "doi", "licence", "xml")


class AttributionError(ValueError):
    pass


@dataclass(frozen=True)
class Attribution:
    id: str
    doi: str
    title: str
    authors: str
    licence: str


def attribution(row: Mapping[str, Any]) -> Attribution:
    meta = _article_meta(parse_xml(row["xml"]))
    return Attribution(
        id=row["id"],
        doi=row["doi"] or "",
        title=_title(meta),
        authors="; ".join(_authors(meta)),
        licence=row["licence"],
    )


def render_attributions(member_ids: Sequence[str], tables: Iterable[pa.Table]) -> str:
    """One row per member, in the release's order."""
    by_id = {
        row["id"]: row
        for table in tables
        for row in table.select(list(ATTRIBUTION_COLUMNS)).to_pylist()
    }
    missing = [i for i in member_ids if i not in by_id]
    if missing:
        raise AttributionError(
            f"no row for {len(missing)} member(s), e.g. {missing[:3]}"
        )
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(ATTRIBUTION_FIELDS)
    writer.writerows(astuple(attribution(by_id[i])) for i in member_ids)
    return out.getvalue()


def _local(element: ET.Element) -> str:
    return element.tag.split("}")[-1] if isinstance(element.tag, str) else ""


def _child(element: ET.Element, name: str) -> ET.Element | None:
    return next((c for c in element if _local(c) == name), None)


def _text(element: ET.Element | None, skip: str = "") -> str:
    """Its text with markup flattened and whitespace normalised, leaving out `skip`."""
    if element is None:
        return ""
    parts = [element.text or ""]
    for child in element:
        if _local(child) != skip:
            parts.append(_text(child, skip))
        parts.append(child.tail or "")
    return " ".join("".join(parts).split())


def _article_meta(root: ET.Element | None) -> ET.Element | None:
    """The main article's; a sub-article has a `front-stub` instead."""
    if root is None:
        return None
    return next((e for e in root.iter() if _local(e) == "article-meta"), None)


def _title(meta: ET.Element | None) -> str:
    group = _child(meta, "title-group") if meta is not None else None
    return _text(_child(group, "article-title")) if group is not None else ""


def _authors(meta: ET.Element | None) -> list[str]:
    if meta is None:
        return []
    contribs = (
        e
        for e in meta.iter()
        if _local(e) == "contrib" and e.get("contrib-type") == "author"
    )
    return [name for name in map(_name, contribs) if name]


def _name(contrib: ET.Element) -> str:
    """The first name, string name or group name it gives."""
    for element in contrib.iter():
        kind = _local(element)
        if kind == "name":
            given = _text(_child(element, "given-names"))
            surname = _text(_child(element, "surname"))
            order = (
                [surname, given]
                if element.get("name-style") == "eastern"
                else [given, surname]
            )
            parts = [*order, _text(_child(element, "suffix"))]
            return " ".join(p for p in parts if p)
        if kind == "string-name":
            return _text(element)
        if kind == "collab":
            return _text(element, skip="contrib-group")
    return ""
