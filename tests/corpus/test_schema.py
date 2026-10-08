"""Tests for corpus.schema — the 16-column contract every published repo conforms to."""

import pyarrow as pa

from sciencebeam_dataset_builder.corpus.schema import (
    CORPUS_SCHEMA,
    FIELD_DESCRIPTIONS,
    FIELD_NAMES,
    LICENCE_NOT_AVAILABLE,
)


class TestSchema:
    def test_it_is_the_sixteen_columns_the_spec_names_in_order(self):
        assert FIELD_NAMES == (
            "id",
            "doi",
            "version",
            "pub_date",
            "licence",
            "xml_ftfy_applied",
            "xml_upstream_sha",
            "xml_source_url",
            "xml_downloaded_at",
            "pdf_source_url",
            "pdf_downloaded_at",
            "row_updated_at",
            "pdf",
            "xml",
            "xml_upstream",
        )

    def test_there_is_no_source_or_uid_column(self):
        """The repo already is one corpus, so `id` alone identifies a row within it. A
        globally unique key, when one is needed outside the repo, is `(corpus, id)` -
        computed on demand from the registry, never stored."""
        assert "source" not in FIELD_NAMES
        assert "uid" not in FIELD_NAMES

    def test_the_large_stable_payload_column_is_written_first(self):
        """Parquet writes columns in schema order. `pdf` ahead of the columns corrections
        churn is what keeps unchanged pages byte-identical for content-defined chunking."""
        assert FIELD_NAMES.index("pdf") < FIELD_NAMES.index("xml")
        assert FIELD_NAMES.index("pdf") < FIELD_NAMES.index("xml_upstream")

    def test_identity_provenance_anchors_and_payload_are_not_nullable(self):
        required = {
            name for name in FIELD_NAMES if not CORPUS_SCHEMA.field(name).nullable
        }
        assert required == {
            "id",
            "licence",
            "xml_upstream_sha",
            "row_updated_at",
            "xml",
            "pdf",
        }

    def test_xml_upstream_is_nullable_and_only_populated_where_corrected(self):
        assert CORPUS_SCHEMA.field("xml_upstream").nullable

    def test_a_missing_licence_is_a_stated_value_not_a_null(self):
        """So the column can stay non-nullable: a repo the licence audit never reached
        and a document with genuinely no recoverable licence should not look the same as
        each other, but neither should ever be an empty string or null."""
        assert LICENCE_NOT_AVAILABLE == "N/A"

    def test_the_payload_types_survive_a_round_trip(self):
        table = pa.table(
            {
                name: pa.array([], type=CORPUS_SCHEMA.field(name).type)
                for name in FIELD_NAMES
            },
            schema=CORPUS_SCHEMA,
        )
        assert table.schema.equals(CORPUS_SCHEMA)
        assert pa.types.is_binary(CORPUS_SCHEMA.field("pdf").type)
        assert pa.types.is_boolean(CORPUS_SCHEMA.field("xml_ftfy_applied").type)

    def test_every_column_is_described_for_the_card(self):
        """The card is generated from these, so a new column without one publishes a blank
        cell rather than failing."""
        assert set(FIELD_DESCRIPTIONS) == set(FIELD_NAMES)
