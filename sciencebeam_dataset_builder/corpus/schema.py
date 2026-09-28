"""The row schema every published corpus repo conforms to.

15 columns. Everything the older 23-column schema carried and this one does not is still
derivable from `xml`, offline, so the dropped columns are a convenience layer rather than
data. See `.project-notes/specs/corpus-repos.md`.
"""

import pyarrow as pa

# Field name -> description, rendered into each repo's card so the published documentation
# and the Parquet files cannot drift apart.
FIELD_DESCRIPTIONS: dict[str, str] = {
    "source": "Corpus the row came from. Frozen: it feeds `uid`, which the split hashes.",
    "id": "Source-native identifier, unique within `source`. Opaque - do not parse it.",
    "uid": "`{source}__{id}`, unique across every repo.",
    "doi": "DOI, where the source provides one. Null otherwise.",
    "version": "Version of the record, where the source distinguishes versions.",
    "pub_date": "Publication date, ISO 8601. May be year-only where the source is no more precise.",
    "xml_ftfy_applied": "Whether `ftfy` mojibake repair was applied when the XML was harvested.",
    "xml_upstream_sha": (
        "SHA-256 of the XML as harvested, before any correction. Equals `corrections/"
        "<id>/upstream.xml` where a correction exists, and the stored `xml` where none does."
    ),
    "xml_source_url": "URL the XML was retrieved from.",
    "xml_downloaded_at": "Retrieval timestamp for the XML, ISO 8601.",
    "pdf_source_url": "URL the PDF was retrieved from.",
    "pdf_downloaded_at": "Retrieval timestamp for the PDF, ISO 8601.",
    "row_updated_at": "When this row last changed, for any reason. ISO 8601.",
    "xml": "XML content, decoded to text. This is what is shipped, so a corrected document holds the correction.",
    "pdf": "PDF bytes.",
}

CORPUS_SCHEMA = pa.schema(
    [
        # Identity.
        pa.field("source", pa.string(), nullable=False),
        pa.field("id", pa.string(), nullable=False),
        pa.field("uid", pa.string(), nullable=False),
        pa.field("doi", pa.string()),
        pa.field("version", pa.string()),
        # Metadata.
        pa.field("pub_date", pa.string()),
        # Provenance.
        pa.field("xml_ftfy_applied", pa.bool_()),
        pa.field("xml_upstream_sha", pa.string(), nullable=False),
        pa.field("xml_source_url", pa.string()),
        pa.field("xml_downloaded_at", pa.string()),
        pa.field("pdf_source_url", pa.string()),
        pa.field("pdf_downloaded_at", pa.string()),
        pa.field("row_updated_at", pa.string(), nullable=False),
        # Payload. `pdf` first: parquet writes columns in schema order, and putting the
        # large, stable column ahead of the one corrections churn helps content-defined
        # chunking keep unchanged pages byte-identical.
        pa.field("pdf", pa.binary(), nullable=False),
        pa.field("xml", pa.string(), nullable=False),
    ]
)

FIELD_NAMES = tuple(CORPUS_SCHEMA.names)

UID_SEPARATOR = "__"


def make_uid(source: str, id_value: str) -> str:
    """Return the globally unique row identifier for a source-native id."""
    return f"{source}{UID_SEPARATOR}{id_value}"


def split_uid(uid: str) -> tuple[str, str]:
    """Return the `(source, id)` a uid was built from.

    No source-native id contains the separator, checked across all known documents, so the
    split is unambiguous. A new source whose ids contain it would break this.
    """
    source, separator, id_value = uid.partition(UID_SEPARATOR)
    if not separator:
        raise ValueError(f"{uid!r} is not a uid: no {UID_SEPARATOR!r} separator")
    return source, id_value
