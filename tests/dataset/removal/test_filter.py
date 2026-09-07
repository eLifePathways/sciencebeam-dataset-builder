import pyarrow as pa

from sciencebeam_dataset_builder.dataset.normalise import normalise_table
from sciencebeam_dataset_builder.dataset.removal.filter import (
    remove_rows,
    row_uids,
    verify_removal,
)
from sciencebeam_dataset_builder.dataset.schema import SOURCES

BIORXIV = SOURCES["biorxiv"]
METADATA = SOURCES["scielo_preprints_dublin_core"]


def _table(rows: int = 4) -> pa.Table:
    """A canonical-schema table whose uids are biorxiv__doc0 .. biorxiv__docN."""
    return normalise_table(
        pa.table(
            {
                "ppr_id": [f"doc{i}" for i in range(rows)],
                "title": [f"Title {i}" for i in range(rows)],
                "xml": [f"<article>{i}</article>" for i in range(rows)],
                "pdf": [b"%PDF-" + str(i).encode() for i in range(rows)],
            }
        ),
        BIORXIV,
    )


def _legacy_table(ids: list[str]) -> pa.Table:
    """The narrow `id` / `xml` / `pdf` shape scielo-preprints-metadata still carries."""
    return pa.table(
        {
            "id": ids,
            "xml": [f"<oai_dc:dc>{i}</oai_dc:dc>" for i in ids],
            "pdf": [b"%PDF-" + i.encode() for i in ids],
        }
    )


class TestRowUids:
    def test_uses_the_uid_column_when_present(self):
        assert row_uids(_table(2), BIORXIV) == ["biorxiv__doc0", "biorxiv__doc1"]

    def test_derives_the_uid_where_the_table_has_none(self):
        table = _legacy_table(["oai_a", "oai_b"])
        assert row_uids(table, METADATA) == [
            "scielo_preprints_dublin_core__oai_a",
            "scielo_preprints_dublin_core__oai_b",
        ]


class TestRemoveRows:
    def test_drops_only_the_listed_rows(self):
        filtered, dropped = remove_rows(_table(), {"biorxiv__doc1"}, BIORXIV)
        assert dropped == ["biorxiv__doc1"]
        assert filtered.column("uid").to_pylist() == [
            "biorxiv__doc0",
            "biorxiv__doc2",
            "biorxiv__doc3",
        ]

    def test_preserves_the_schema_exactly(self):
        original = _table()
        filtered, _ = remove_rows(original, {"biorxiv__doc0"}, BIORXIV)
        assert filtered.schema.equals(original.schema)

    def test_ignores_uids_that_are_not_present(self):
        original = _table()
        filtered, dropped = remove_rows(original, {"pkp__nowhere"}, BIORXIV)
        assert dropped == []
        assert filtered.num_rows == original.num_rows

    def test_removing_nothing_is_a_no_op(self):
        original = _table()
        filtered, dropped = remove_rows(original, set(), BIORXIV)
        assert dropped == []
        assert filtered.to_pylist() == original.to_pylist()

    def test_can_remove_every_row(self):
        original = _table(2)
        filtered, dropped = remove_rows(
            original, {"biorxiv__doc0", "biorxiv__doc1"}, BIORXIV
        )
        assert filtered.num_rows == 0
        assert len(dropped) == 2
        assert filtered.schema.equals(original.schema)

    def test_removes_from_a_table_that_has_no_uid_column(self):
        original = _legacy_table(["oai_a", "oai_b", "oai_c"])
        filtered, dropped = remove_rows(
            original, {"scielo_preprints_dublin_core__oai_b"}, METADATA
        )
        assert dropped == ["scielo_preprints_dublin_core__oai_b"]
        assert filtered.column("id").to_pylist() == ["oai_a", "oai_c"]
        assert filtered.schema.equals(original.schema)


class TestVerifyRemoval:
    def test_passes_for_a_faithful_removal(self):
        original = _table()
        uids = {"biorxiv__doc1", "biorxiv__doc3"}
        filtered, _ = remove_rows(original, uids, BIORXIV)
        assert verify_removal(original, filtered, uids, BIORXIV) == []

    def test_passes_for_a_table_that_has_no_uid_column(self):
        original = _legacy_table(["oai_a", "oai_b", "oai_c"])
        uids = {"scielo_preprints_dublin_core__oai_b"}
        filtered, _ = remove_rows(original, uids, METADATA)
        assert verify_removal(original, filtered, uids, METADATA) == []

    def test_detects_a_listed_row_that_survived(self):
        original = _table()
        filtered, _ = remove_rows(original, {"biorxiv__doc1"}, BIORXIV)
        failures = verify_removal(
            original, filtered, {"biorxiv__doc1", "biorxiv__doc2"}, BIORXIV
        )
        assert any("listed uid(s) survived" in f for f in failures)

    def test_detects_an_unlisted_row_that_was_dropped(self):
        original = _table()
        filtered, _ = remove_rows(original, {"biorxiv__doc1", "biorxiv__doc2"}, BIORXIV)
        failures = verify_removal(original, filtered, {"biorxiv__doc1"}, BIORXIV)
        assert any("unlisted uid(s) were dropped" in f for f in failures)

    def test_detects_reordered_survivors(self):
        original = _table()
        uids = {"biorxiv__doc1"}
        filtered, _ = remove_rows(original, uids, BIORXIV)
        reordered = filtered.take([2, 1, 0])
        assert verify_removal(original, reordered, uids, BIORXIV) == [
            "surviving rows are no longer in their original order"
        ]

    def test_detects_a_changed_payload(self):
        original = _table()
        uids = {"biorxiv__doc1"}
        filtered, _ = remove_rows(original, uids, BIORXIV)
        index = filtered.schema.get_field_index("pdf")
        tampered = filtered.set_column(
            index, "pdf", pa.array([b"%PDF-x", b"%PDF-2", b"%PDF-3"], pa.binary())
        )
        assert "pdf values changed" in verify_removal(original, tampered, uids, BIORXIV)

    def test_detects_a_changed_metadata_value(self):
        original = _table()
        uids = {"biorxiv__doc1"}
        filtered, _ = remove_rows(original, uids, BIORXIV)
        index = filtered.schema.get_field_index("title")
        tampered = filtered.set_column(
            index, "title", pa.array(["Rewritten", "Title 2", "Title 3"])
        )
        assert "title values changed" in verify_removal(
            original, tampered, uids, BIORXIV
        )

    def test_detects_a_changed_schema(self):
        original = _table()
        uids = {"biorxiv__doc1"}
        filtered, _ = remove_rows(original, uids, BIORXIV)
        assert "schema changed" in verify_removal(
            original, filtered.drop_columns(["title"]), uids, BIORXIV
        )
