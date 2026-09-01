"""The canonical row schema shared by every source in the benchmarking dataset.

This module is the single source of truth for the dataset contract. Source-specific
builders map their metadata onto :data:`CANONICAL_SCHEMA`; the migration and dataset
card tooling read the same definitions, so the Hub repo, the Parquet files and the
documentation cannot drift apart.

Identity is carried by three columns rather than one:

``source``
    Which corpus the row came from. Always populated, so rows stay attributable
    after subsets are concatenated.
``id``
    The source-native identifier, an opaque string that is unique within a source
    but has a different form in each one (see :data:`SOURCES`). Previously named
    ``ppr_id``, which only ever described SciELO Preprints, where the value is a
    EuropePMC accession.
``uid``
    ``f"{source}__{id}"`` - unique across the whole dataset, and the join key used
    by downstream analysis manifests and the ``{source}__{id}.xml`` file naming.
    The separator is a double underscore, which no source-native ``id`` contains
    (checked across all 2060 known documents), so a uid splits back into its parts
    unambiguously. A new source whose ids contain ``__`` would break that.

``doi`` is kept separate because it is absent for some sources (OJS/PKP and ORE
records carry no DOI) and, for bioRxiv, ``id`` is a filesystem-safe mangling of the
DOI rather than the DOI itself.
"""

from dataclasses import dataclass

import pyarrow as pa

XML_FORMAT_JATS = "jats"
XML_FORMAT_DUBLIN_CORE = "dublin_core"

AUTHOR_TYPE = pa.struct(
    [
        pa.field("name", pa.string()),
        pa.field("orcid", pa.string()),
        pa.field("affiliations", pa.list_(pa.string())),
    ]
)

# Field name -> human-readable description, rendered into the dataset card so the
# published documentation is generated from the same definitions as the Parquet files.
FIELD_DESCRIPTIONS: dict[str, str] = {
    "source": "Corpus the row came from; one of the keys of `SOURCES`.",
    "id": "Source-native identifier, unique within `source`. Opaque - the form differs per source.",
    "uid": "`{source}__{id}`, unique across the whole dataset.",
    "doi": "DOI, where the source provides one. Null otherwise.",
    "version": "Version of the record, where the source distinguishes versions.",
    "title": "Article title.",
    "authors": "Author list: `name`, `orcid`, `affiliations`.",
    "pub_date": "Publication date, ISO 8601. May be year-only or year-month where the source is no more precise.",
    "license": "Licence URL.",
    "keywords": "Author-supplied keywords.",
    "subject_heading": 'JATS `<subj-group subj-group-type="heading">`.',
    "subject_europepmc_category": (
        'JATS `<subj-group subj-group-type="EuropePMC-category">`. '
        "Only EuropePMC-sourced records carry this; null elsewhere."
    ),
    "article_type": "JATS `article/@article-type`.",
    "language": "Language, normalised to ISO 639-1.",
    "language_raw": "Language exactly as reported by the source.",
    "xml_format": f"Format of the `xml` column: `{XML_FORMAT_JATS}` or `{XML_FORMAT_DUBLIN_CORE}`.",
    "xml_source_url": "URL the XML was retrieved from.",
    "xml_downloaded_at": "Retrieval timestamp for the XML, ISO 8601.",
    "xml_ftfy_applied": "Whether `ftfy` mojibake repair was applied to the XML.",
    "pdf_source_url": "URL the PDF was retrieved from.",
    "pdf_downloaded_at": "Retrieval timestamp for the PDF, ISO 8601.",
    "xml": "XML content, decoded to text. See `xml_format`.",
    "pdf": "PDF bytes.",
}

CANONICAL_SCHEMA = pa.schema(
    [
        # Identity.
        pa.field("source", pa.string(), nullable=False),
        pa.field("id", pa.string(), nullable=False),
        pa.field("uid", pa.string(), nullable=False),
        pa.field("doi", pa.string()),
        pa.field("version", pa.string()),
        # Bibliographic metadata, all derived from the XML.
        pa.field("title", pa.string()),
        pa.field("authors", pa.list_(AUTHOR_TYPE)),
        pa.field("pub_date", pa.string()),
        pa.field("license", pa.string()),
        pa.field("keywords", pa.list_(pa.string())),
        pa.field("subject_heading", pa.string()),
        pa.field("subject_europepmc_category", pa.string()),
        pa.field("article_type", pa.string()),
        pa.field("language", pa.string()),
        pa.field("language_raw", pa.string()),
        # Provenance.
        pa.field("xml_format", pa.string(), nullable=False),
        pa.field("xml_source_url", pa.string()),
        pa.field("xml_downloaded_at", pa.string()),
        pa.field("xml_ftfy_applied", pa.bool_()),
        pa.field("pdf_source_url", pa.string()),
        pa.field("pdf_downloaded_at", pa.string()),
        # Payload.
        pa.field("xml", pa.string(), nullable=False),
        pa.field("pdf", pa.binary(), nullable=False),
    ]
)

CANONICAL_FIELD_NAMES = tuple(CANONICAL_SCHEMA.names)

# Identifier columns used before the schema was unified. Migration renames whichever
# of these a table carries to `id`; the values themselves are left untouched.
LEGACY_ID_COLUMNS = ("ppr_id", "article_id", "id")


@dataclass(frozen=True)
class Source:
    """A corpus in the dataset, and how its rows are addressed on the Hub."""

    name: str
    """Value of the `source` column."""

    config: str
    """Hub config name, which is also the directory holding its Parquet files."""

    xml_format: str
    """Value of the `xml_format` column for every row of this source."""

    id_description: str
    """What the source-native `id` looks like, for the dataset card."""

    id_example: str
    """A real `id` value, for the dataset card."""

    description: str
    """One-line description of the corpus, for the dataset card."""


SOURCES: dict[str, Source] = {
    "biorxiv": Source(
        name="biorxiv",
        config="biorxiv-jats",
        xml_format=XML_FORMAT_JATS,
        id_description="bioRxiv DOI with `/` replaced by `_`, so it is filesystem-safe",
        id_example="10.1101_2021.04.23.440814",
        description="bioRxiv preprints with publisher JATS full text.",
    ),
    "ore": Source(
        name="ore",
        config="ore-jats",
        xml_format=XML_FORMAT_JATS,
        id_description="ORE submission and galley ids with the version suffix",
        id_example="4-121_v2",
        description="Open Research Europe articles.",
    ),
    "pkp": Source(
        name="pkp",
        config="pkp-jats",
        xml_format=XML_FORMAT_JATS,
        id_description="OJS submission, file and galley ids with the galley label",
        id_example="20092-79318-5-ED",
        description="Articles from OJS journals using the PKP JATS plugin.",
    ),
    "scielo_br": Source(
        name="scielo_br",
        config="scielo_br-jats",
        xml_format=XML_FORMAT_JATS,
        id_description="SciELO PID",
        id_example="S0100-72032011000200007",
        description="SciELO Brazil articles.",
    ),
    "scielo_mx": Source(
        name="scielo_mx",
        config="scielo_mx-jats",
        xml_format=XML_FORMAT_JATS,
        id_description="SciELO PID",
        id_example="S2007-09342011000600002",
        description="SciELO Mexico articles.",
    ),
    "scielo_preprints": Source(
        name="scielo_preprints",
        config="scielo-preprints-jats",
        xml_format=XML_FORMAT_JATS,
        id_description="EuropePMC preprint accession, since the JATS comes from EuropePMC",
        id_example="PPR459180",
        description=(
            "SciELO Preprints for which a EuropePMC JATS full text exists. "
            "Metadata fields are fully populated for this source."
        ),
    ),
    "scielo_preprints_dublin_core": Source(
        name="scielo_preprints_dublin_core",
        config="scielo-preprints-metadata",
        xml_format=XML_FORMAT_DUBLIN_CORE,
        id_description="SciELO Preprints OAI identifier",
        id_example="oai_ops.preprints.scielo.org_preprint_100",
        description=(
            "SciELO Preprints OAI Dublin Core records (`<oai_dc:dc>`, no body). "
            "The wider corpus; `scielo-preprints-jats` is the subset with JATS full text."
        ),
    ),
}

SOURCES_BY_CONFIG: dict[str, Source] = {s.config: s for s in SOURCES.values()}


def make_uid(source: str, id_value: str) -> str:
    """Return the globally unique row identifier for a source-native id."""
    return f"{source}__{id_value}"
