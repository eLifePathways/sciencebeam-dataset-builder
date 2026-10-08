"""The row schema every published corpus repo conforms to.

16 columns. Everything the older 23-column schema carried and this one does not is still
derivable from `xml`, offline, so the dropped columns are a convenience layer rather than
data. No `source` or `uid`: the repo already is one corpus, and `id` alone identifies a
row within it. See `.project-notes/specs/corpus-repos.md`.
"""

import pyarrow as pa

# The literal value for a document whose licence could never be recovered - `pkp`'s JATS
# carries only unsubstituted template placeholders. Distinguishes "checked, and there is
# nothing to find" from a licence that was simply never looked up.
LICENCE_NOT_AVAILABLE = "N/A"

# Field name -> description, rendered into each repo's card so the published documentation
# and the Parquet files cannot drift apart.
FIELD_DESCRIPTIONS: dict[str, str] = {
    "id": "Source-native identifier, unique within this corpus. Opaque - do not parse it.",
    "doi": "DOI, where the source provides one. Null otherwise.",
    "version": "Version of the record, where the source distinguishes versions.",
    "pub_date": "Publication date, ISO 8601. May be year-only where the source is no more precise.",
    "licence": f'The document\'s licence, or `"{LICENCE_NOT_AVAILABLE}"` where none is recoverable.',
    "xml_ftfy_applied": "Whether `ftfy` mojibake repair was applied when the XML was harvested.",
    "xml_upstream_sha": (
        "SHA-256 of `xml_upstream` where it is populated, otherwise of `xml`: the hash of "
        "the XML exactly as harvested, before any correction."
    ),
    "xml_source_url": "URL the XML was retrieved from.",
    "xml_downloaded_at": "Retrieval timestamp for the XML, ISO 8601.",
    "pdf_source_url": "URL the PDF was retrieved from.",
    "pdf_downloaded_at": "Retrieval timestamp for the PDF, ISO 8601.",
    "row_updated_at": "When this row last changed, for any reason. ISO 8601.",
    "pdf": "PDF bytes.",
    "xml": "XML content, decoded to text. This is what is shipped, so a corrected document holds the correction.",
    "xml_upstream": (
        "The XML as originally harvested, before correction. Null unless `xml` holds a "
        "correction, in which case this is what the publisher provided - present so a "
        "benchmark can still be run against the unmodified original."
    ),
}

CORPUS_SCHEMA = pa.schema(
    [
        # Identity.
        pa.field("id", pa.string(), nullable=False),
        pa.field("doi", pa.string()),
        pa.field("version", pa.string()),
        # Metadata.
        pa.field("pub_date", pa.string()),
        pa.field("licence", pa.string(), nullable=False),
        # Provenance.
        pa.field("xml_ftfy_applied", pa.bool_()),
        pa.field("xml_upstream_sha", pa.string(), nullable=False),
        pa.field("xml_source_url", pa.string()),
        pa.field("xml_downloaded_at", pa.string()),
        pa.field("pdf_source_url", pa.string()),
        pa.field("pdf_downloaded_at", pa.string()),
        pa.field("row_updated_at", pa.string(), nullable=False),
        # Payload. `pdf` first: parquet writes columns in schema order, and putting the
        # large, stable column ahead of the ones corrections churn helps content-defined
        # chunking keep unchanged pages byte-identical.
        pa.field("pdf", pa.binary(), nullable=False),
        pa.field("xml", pa.string(), nullable=False),
        pa.field("xml_upstream", pa.string()),
    ]
)

FIELD_NAMES = tuple(CORPUS_SCHEMA.names)
