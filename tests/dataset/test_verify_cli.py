import pyarrow as pa

from sciencebeam_dataset_builder.dataset.normalise import normalise_table
from sciencebeam_dataset_builder.dataset.schema import SOURCES
from sciencebeam_dataset_builder.dataset.verify_cli import verify_split

BIORXIV = SOURCES["biorxiv"]


def _original(rows: int = 3) -> pa.Table:
    return pa.table(
        {
            "ppr_id": [f"doc{i}" for i in range(rows)],
            "title": [f"Title {i}" for i in range(rows)],
            "xml": [f"<article>{i}</article>" for i in range(rows)],
            "pdf": [b"%PDF-" + str(i).encode() for i in range(rows)],
        }
    )


class TestVerifySplit:
    def test_passes_for_a_faithful_migration(self):
        original = _original()
        assert verify_split(original, normalise_table(original, BIORXIV), BIORXIV) == []

    def test_detects_a_changed_row_count(self):
        original = _original(3)
        migrated = normalise_table(_original(2), BIORXIV)
        assert verify_split(original, migrated, BIORXIV) == ["row count 3 -> 2"]

    def test_detects_a_changed_payload(self):
        original = _original()
        migrated = normalise_table(original, BIORXIV)
        index = migrated.schema.get_field_index("xml")
        tampered = migrated.set_column(
            index,
            "xml",
            pa.array(["<tampered/>", "<article>1</article>", "<article>2</article>"]),
        )
        assert "xml payload changed" in verify_split(original, tampered, BIORXIV)

    def test_detects_a_changed_carried_column(self):
        original = _original()
        migrated = normalise_table(original, BIORXIV)
        index = migrated.schema.get_field_index("title")
        tampered = migrated.set_column(
            index, "title", pa.array(["Rewritten", "Title 1", "Title 2"])
        )
        assert "title values changed" in verify_split(original, tampered, BIORXIV)

    def test_detects_reordered_ids(self):
        original = _original()
        migrated = normalise_table(original, BIORXIV)
        reordered = migrated.take([2, 1, 0])
        failures = verify_split(original, reordered, BIORXIV)
        assert "ppr_id values or their order changed" in failures

    def test_detects_a_wrong_source_value(self):
        original = _original()
        migrated = normalise_table(original, SOURCES["ore"])
        failures = verify_split(original, migrated, BIORXIV)
        assert any(f.startswith("source is") for f in failures)

    def test_detects_an_inconsistent_uid(self):
        original = _original()
        migrated = normalise_table(original, BIORXIV)
        index = migrated.schema.get_field_index("uid")
        tampered = migrated.set_column(
            index, "uid", pa.array(["wrong", "biorxiv__doc1", "biorxiv__doc2"])
        )
        assert "uid is not source__id for every row" in verify_split(
            original, tampered, BIORXIV
        )

    def test_detects_a_non_canonical_schema(self):
        original = _original()
        migrated = normalise_table(original, BIORXIV).drop_columns(["title"])
        failures = verify_split(original, migrated, BIORXIV)
        assert "schema does not match the canonical schema" in failures
        assert any("columns missing from the output" in f for f in failures)
