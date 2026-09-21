"""Reading the licence a document states, from the XML the dataset stores."""

import html
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass

XLINK_HREF = "{http://www.w3.org/1999/xlink}href"

CC_URL = re.compile(
    r"creativecommons\.org/(?:licenses|publicdomain)/([a-z0-9\-]+)(?:/([0-9.]+))?",
    re.IGNORECASE,
)

# Named HTML entities are undefined in XML, so a stray `&deg;` breaks the parse.
UNDEFINED_ENTITY = re.compile(r"&(?!#|amp;|lt;|gt;|quot;|apos;)([A-Za-z][A-Za-z0-9]*);")
ILLEGAL_CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

# What PKP's JATS plugin leaves when a template variable is never substituted.
TEMPLATE_PLACEHOLDERS = frozenset(
    {
        "copyright-statement",
        "copyright-year",
        "publisher-name",
        "journal-title",
        "publisher-id",
        "journal-id",
        "0000-0000",
    }
)

ALL_RIGHTS_RESERVED = "All Rights Reserved"
NO_LICENCE_PLACEHOLDER = "no licence (template placeholder)"
NO_LICENCE_COPYRIGHT_ONLY = "no licence (copyright statement only)"
NO_LICENCE_ABSENT = "no licence (no permissions element)"
CONFLICTING_LICENCES = "conflicting licences"
UNPARSEABLE = "unparseable xml"


@dataclass(frozen=True)
class Licence:
    """What one document states about its licence, and the text that says so."""

    label: str
    evidence: str

    @property
    def is_creative_commons(self) -> bool:
        """Whether a Creative Commons licence was found."""
        return self.label.startswith("CC")

    @property
    def is_stated(self) -> bool:
        """Whether the document states a licence at all."""
        return self.is_creative_commons or self.label == ALL_RIGHTS_RESERVED


def parse_xml(xml_text: str) -> ET.Element | None:
    """Parse `xml_text`, recovering undefined entities and illegal control chars.

    Returns `None` if it is malformed beyond those repairs. Nothing is written back.
    """
    try:
        return ET.fromstring(xml_text)
    except ET.ParseError:
        pass

    def _resolve(match: re.Match[str]) -> str:
        entity = match.group(0)
        resolved = html.unescape(entity)
        return "" if resolved == entity else resolved

    repaired = ILLEGAL_CONTROL.sub("", UNDEFINED_ENTITY.sub(_resolve, xml_text))
    try:
        return ET.fromstring(repaired)
    except ET.ParseError:
        return None


def creative_commons_label(text: str) -> str | None:
    """Return the short CC label in `text`, or `None` if it holds no CC URL."""
    match = CC_URL.search(text or "")
    if match is None:
        return None
    code = match.group(1).lower()
    version = match.group(2) or ""
    if code in ("zero", "mark"):
        return f"CC0 {version}".strip()
    return f"CC {code.upper()} {version}".strip()


def _local_name(element: ET.Element) -> str:
    return element.tag.split("}")[-1].lower()


def _statement_text(element: ET.Element) -> str:
    return " ".join(" ".join(element.itertext()).split())


def _is_placeholder(statement: str) -> bool:
    """Whether a copyright statement is just an unsubstituted template variable."""
    stripped = re.sub(r"©|\b(19|20)\d{2}\b|[.,;:()]", " ", statement)
    return " ".join(stripped.split()).lower() in TEMPLATE_PLACEHOLDERS


def _licence_element_text(element: ET.Element) -> str:
    """Return everything in a licence element that might hold the licence."""
    parts = [
        element.get(XLINK_HREF) or "",
        element.get("href") or "",
        element.get("license-type") or "",
        *(child.get(XLINK_HREF) or "" for child in element.iter()),
        " ".join(element.itertext()),
    ]
    return " ".join(part for part in parts if part)


def _scan_licence_elements(root: ET.Element) -> tuple[set[str], list[str]]:
    """Return the CC labels and the non-CC text found in the licence elements."""
    found: set[str] = set()
    non_cc_text: list[str] = []
    for element in root.iter():
        if _local_name(element) not in ("license", "license_ref", "rights"):
            continue
        blob = _licence_element_text(element)
        label = creative_commons_label(blob)
        if label is not None:
            found.add(label)
        elif blob.strip():
            non_cc_text.append(" ".join(blob.split()))
    return found, non_cc_text


def _read_silence(root: ET.Element) -> Licence:
    """Return which kind of silence a document with no licence element carries."""
    for element in root.iter():
        if _local_name(element) != "copyright-statement":
            continue
        statement = _statement_text(element)
        if not statement:
            continue
        if _is_placeholder(statement):
            return Licence(NO_LICENCE_PLACEHOLDER, statement)
        return Licence(NO_LICENCE_COPYRIGHT_ONLY, statement)

    for element in root.iter():
        if _local_name(element) == "permissions":
            statement = _statement_text(element)
            if statement:
                return Licence(NO_LICENCE_COPYRIGHT_ONLY, statement)

    return Licence(NO_LICENCE_ABSENT, "no permissions or rights element")


def extract_licence(xml_text: str) -> Licence:
    """Return the licence one document states.

    Agreeing statements collapse to one label; genuinely different CC licences are
    reported as a conflict rather than resolved by document order.
    """
    root = parse_xml(xml_text)
    if root is None:
        return Licence(UNPARSEABLE, "not well-formed, and not recoverable")

    found, non_cc_text = _scan_licence_elements(root)
    if len(found) == 1:
        return Licence(found.pop(), "Creative Commons URL")
    if len(found) > 1:
        return Licence(CONFLICTING_LICENCES, " | ".join(sorted(found)))

    for text in non_cc_text:
        if "all rights reserved" in text.lower():
            return Licence(ALL_RIGHTS_RESERVED, text)
    if non_cc_text:
        return Licence(NO_LICENCE_COPYRIGHT_ONLY, non_cc_text[0])

    return _read_silence(root)
