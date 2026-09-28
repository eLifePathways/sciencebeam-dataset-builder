"""Tests for corpus.schema — the 15-column contract every published repo conforms to."""

import pyarrow as pa
import pytest

from sciencebeam_dataset_builder.corpus.schema import (
    CORPUS_SCHEMA,
    FIELD_DESCRIPTIONS,
    FIELD_NAMES,
    make_uid,
    split_uid,
)


class TestSchema:
    def test_it_is_the_fifteen_columns_the_spec_names_in_order(self):
        assert FIELD_NAMES == (
            "source",
            "id",
            "uid",
            "doi",
            "version",
            "pub_date",
            "xml_ftfy_applied",
            "xml_upstream_sha",
            "xml_source_url",
            "xml_downloaded_at",
            "pdf_source_url",
            "pdf_downloaded_at",
            "row_updated_at",
            "pdf",
            "xml",
        )

    def test_the_large_stable_payload_column_is_written_first(self):
        """Parquet writes columns in schema order. `pdf` ahead of the column corrections
        churn is what keeps unchanged pages byte-identical for content-defined chunking."""
        assert FIELD_NAMES.index("pdf") < FIELD_NAMES.index("xml")

    def test_identity_provenance_anchors_and_payload_are_not_nullable(self):
        required = {
            name for name in FIELD_NAMES if not CORPUS_SCHEMA.field(name).nullable
        }
        assert required == {
            "source",
            "id",
            "uid",
            "xml_upstream_sha",
            "row_updated_at",
            "xml",
            "pdf",
        }

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


class TestUid:
    def test_a_uid_is_the_source_and_the_source_native_id(self):
        assert make_uid("biorxiv", "10.1101_588491") == "biorxiv__10.1101_588491"

    def test_a_uid_splits_back_into_its_parts(self):
        assert split_uid("biorxiv__10.1101_588491") == ("biorxiv", "10.1101_588491")

    def test_an_id_containing_the_separator_would_split_at_the_first_one(self):
        """No known source-native id contains it. A new source whose ids do would break
        this, which is why `source` values are frozen and ids are checked on harvest."""
        assert split_uid(make_uid("s", "a__b")) == ("s", "a__b")

    def test_something_that_is_not_a_uid_is_refused(self):
        with pytest.raises(ValueError):
            split_uid("10.1101_588491")
