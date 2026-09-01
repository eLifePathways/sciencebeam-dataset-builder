import pyarrow as pa
import pytest

from sciencebeam_dataset_builder.dataset.normalise import (
    NormalisationError,
    describe_changes,
    find_id_column,
    normalise_table,
)
from sciencebeam_dataset_builder.dataset.schema import CANONICAL_SCHEMA, SOURCES

BIORXIV = SOURCES["biorxiv"]
PKP = SOURCES["pkp"]


def _minimal_table(id_column: str = "ppr_id", rows: int = 2, **overrides) -> pa.Table:
    """A table in the historical three-column shape, plus any overridden columns."""
    columns = {
        id_column: [f"doc{i}" for i in range(rows)],
        "xml": [f"<article>{i}</article>" for i in range(rows)],
        "pdf": [b"%PDF-" + str(i).encode() for i in range(rows)],
    }
    columns.update(overrides)
    return pa.table(columns)


class TestFindIdColumn:
    def test_finds_ppr_id(self):
        assert find_id_column(_minimal_table("ppr_id")) == "ppr_id"

    def test_finds_article_id(self):
        assert find_id_column(_minimal_table("article_id")) == "article_id"

    def test_finds_id(self):
        assert find_id_column(_minimal_table("id")) == "id"

    def test_prefers_legacy_name_over_id(self):
        table = _minimal_table("ppr_id", id=["a", "b"])
        assert find_id_column(table) == "ppr_id"

    def test_raises_when_absent(self):
        with pytest.raises(NormalisationError, match="No identifier column"):
            find_id_column(pa.table({"xml": ["<a/>"]}))


class TestNormaliseTable:
    def test_result_matches_canonical_schema(self):
        result = normalise_table(_minimal_table(), BIORXIV)
        assert result.schema.equals(CANONICAL_SCHEMA)

    def test_preserves_row_count_and_order(self):
        table = _minimal_table(rows=5)
        result = normalise_table(table, BIORXIV)
        assert result.num_rows == 5
        assert result.column("id").to_pylist() == table.column("ppr_id").to_pylist()

    def test_derives_source_uid_and_xml_format(self):
        result = normalise_table(_minimal_table(rows=1), BIORXIV)
        assert result.column("source").to_pylist() == ["biorxiv"]
        assert result.column("uid").to_pylist() == ["biorxiv__doc0"]
        assert result.column("xml_format").to_pylist() == ["jats"]

    def test_renames_article_id_without_changing_values(self):
        table = _minimal_table("article_id")
        result = normalise_table(table, PKP)
        assert result.column("id").to_pylist() == ["doc0", "doc1"]

    def test_pads_absent_columns_with_nulls(self):
        result = normalise_table(_minimal_table(rows=2), BIORXIV)
        assert result.column("doi").to_pylist() == [None, None]
        assert result.column("authors").to_pylist() == [None, None]

    def test_leaves_payload_untouched(self):
        table = _minimal_table(rows=3)
        result = normalise_table(table, BIORXIV)
        assert result.column("xml").to_pylist() == table.column("xml").to_pylist()
        assert result.column("pdf").to_pylist() == table.column("pdf").to_pylist()

    def test_narrows_large_string_to_string(self):
        table = _minimal_table(title=pa.array(["a", "b"], type=pa.large_string()))
        result = normalise_table(table, BIORXIV)
        assert result.schema.field("title").type == pa.string()
        assert result.column("title").to_pylist() == ["a", "b"]

    def test_repairs_authors_degraded_by_a_pandas_round_trip(self):
        """Field order differs and empty affiliations lost their element type."""
        degraded = pa.list_(
            pa.struct(
                [
                    pa.field("affiliations", pa.list_(pa.null())),
                    pa.field("name", pa.string()),
                    pa.field("orcid", pa.string()),
                ]
            )
        )
        table = _minimal_table(
            rows=1,
            authors=pa.array(
                [[{"affiliations": [], "name": "A. Author", "orcid": "0000"}]],
                type=degraded,
            ),
        )
        result = normalise_table(table, BIORXIV)
        assert (
            result.schema.field("authors").type
            == CANONICAL_SCHEMA.field("authors").type
        )
        assert result.column("authors").to_pylist() == [
            [{"name": "A. Author", "orcid": "0000", "affiliations": []}]
        ]

    def test_rejects_columns_outside_the_schema(self):
        table = _minimal_table(surprise=["x", "y"])
        with pytest.raises(NormalisationError, match="surprise"):
            normalise_table(table, BIORXIV)

    def test_rejects_null_identifiers(self):
        table = _minimal_table()
        table = table.set_column(
            table.schema.get_field_index("ppr_id"),
            "ppr_id",
            pa.array([None, "doc1"], type=pa.string()),
        )
        with pytest.raises(NormalisationError, match="null ppr_id"):
            normalise_table(table, BIORXIV)

    def test_nullify_empty_is_off_by_default(self):
        table = _minimal_table(rows=1, doi=[""])
        assert normalise_table(table, BIORXIV).column("doi").to_pylist() == [""]

    def test_nullify_empty_converts_empty_strings(self):
        table = _minimal_table(rows=1, doi=[""])
        result = normalise_table(table, BIORXIV, nullify_empty=True)
        assert result.column("doi").to_pylist() == [None]

    def test_nullify_empty_leaves_payload_alone(self):
        table = _minimal_table(rows=1)
        result = normalise_table(table, BIORXIV, nullify_empty=True)
        assert result.column("xml").to_pylist() == ["<article>0</article>"]


class TestDescribeChanges:
    def test_reports_the_id_rename(self):
        assert "rename ppr_id -> id" in describe_changes(_minimal_table(), BIORXIV)

    def test_reports_derived_and_padded_columns(self):
        changes = describe_changes(_minimal_table(), BIORXIV)
        assert "add source (derived)" in changes
        assert "add doi (all null)" in changes

    def test_reports_a_retype(self):
        table = _minimal_table(title=pa.array(["a", "b"], type=pa.large_string()))
        changes = describe_changes(table, BIORXIV)
        assert any(c.startswith("retype title:") for c in changes)

    def test_reports_nothing_for_a_canonical_table(self):
        canonical = pa.Table.from_pylist([], schema=CANONICAL_SCHEMA)
        assert describe_changes(canonical, BIORXIV) == ["already canonical"]
