import pyarrow as pa

from sciencebeam_dataset_builder.dataset.removal.companions import (
    metadata_uid_for,
    preprint_number_from_doi,
    resolve_companions,
)


def _jats_table(rows: list[tuple[str, str | None]]) -> pa.Table:
    """A scielo-preprints-jats slice: (uid, doi) pairs."""
    return pa.table(
        {
            "uid": [uid for uid, _ in rows],
            "doi": pa.array([doi for _, doi in rows], pa.string()),
        }
    )


class TestPreprintNumberFromDoi:
    def test_reads_the_number_from_a_scielo_preprints_doi(self):
        assert preprint_number_from_doi("10.1590/scielopreprints.1317") == "1317"

    def test_tolerates_surrounding_whitespace(self):
        assert preprint_number_from_doi("  10.1590/scielopreprints.35 ") == "35"

    def test_matches_regardless_of_case(self):
        assert preprint_number_from_doi("10.1590/SciELOPreprints.806") == "806"

    def test_returns_none_for_a_null_doi(self):
        assert preprint_number_from_doi(None) is None

    def test_returns_none_for_an_empty_doi(self):
        assert preprint_number_from_doi("") is None

    def test_returns_none_for_a_journal_doi(self):
        assert preprint_number_from_doi("10.1590/1806-9282.20200101") is None

    def test_rejects_a_doi_with_a_non_numeric_suffix(self):
        assert preprint_number_from_doi("10.1590/scielopreprints.v2") is None

    def test_rejects_a_doi_that_merely_starts_with_the_prefix(self):
        assert preprint_number_from_doi("10.1590/scielopreprints.35.extra") is None


class TestMetadataUidFor:
    def test_builds_the_dublin_core_uid(self):
        assert metadata_uid_for("1317") == (
            "scielo_preprints_dublin_core__oai_ops.preprints.scielo.org_preprint_1317"
        )


class TestResolveCompanions:
    def test_resolves_a_removed_preprint(self):
        table = _jats_table([("scielo_preprints__PPR1", "10.1590/scielopreprints.35")])
        companions, unresolved = resolve_companions(table, {"scielo_preprints__PPR1"})
        assert unresolved == []
        assert len(companions) == 1
        assert companions[0].preprint_number == "35"
        assert companions[0].jats_uid == "scielo_preprints__PPR1"
        assert companions[0].metadata_uid == metadata_uid_for("35")

    def test_ignores_rows_that_are_not_being_removed(self):
        table = _jats_table(
            [
                ("scielo_preprints__PPR1", "10.1590/scielopreprints.35"),
                ("scielo_preprints__PPR2", "10.1590/scielopreprints.36"),
            ]
        )
        companions, _ = resolve_companions(table, {"scielo_preprints__PPR2"})
        assert [c.preprint_number for c in companions] == ["36"]

    def test_reports_a_removed_row_with_no_usable_doi(self):
        table = _jats_table([("scielo_preprints__PPR1", None)])
        companions, unresolved = resolve_companions(table, {"scielo_preprints__PPR1"})
        assert companions == []
        assert unresolved == ["scielo_preprints__PPR1"]

    def test_reports_a_removed_row_carrying_a_journal_doi(self):
        table = _jats_table([("scielo_preprints__PPR1", "10.1590/1806-9282.2020")])
        companions, unresolved = resolve_companions(table, {"scielo_preprints__PPR1"})
        assert companions == []
        assert unresolved == ["scielo_preprints__PPR1"]

    def test_resolves_nothing_when_nothing_is_removed(self):
        table = _jats_table([("scielo_preprints__PPR1", "10.1590/scielopreprints.35")])
        assert resolve_companions(table, set()) == ([], [])
