"""Tests for corpus.routing — which side of the estate a document belongs on."""

import pytest

from sciencebeam_dataset_builder.corpus.layout import LayoutError, Tier
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from sciencebeam_dataset_builder.corpus.routing import (
    PUBLISHABLE_LICENCES,
    RoutingError,
    by_tier,
    check_admissible,
    read_licences,
    route,
    route_all,
)

LICENCES = {
    "biorxiv__open": "CC BY 4.0",
    "biorxiv__nc": "CC BY-NC 4.0",
    "biorxiv__nd": "CC BY-NC-ND 4.0",
    "biorxiv__arr": "All Rights Reserved",
    "biorxiv__blank": "",
    "biorxiv__placeholder": "no licence (template placeholder)",
}


class TestRouting:
    def test_a_publishable_licence_goes_to_the_open_side(self):
        assert route("biorxiv__open", LICENCES).tier is Tier.OPEN

    def test_a_restricted_licence_goes_to_the_private_side(self):
        for uid in ("biorxiv__nc", "biorxiv__nd", "biorxiv__arr"):
            verdict = route(uid, LICENCES)
            assert verdict.tier is Tier.RESTRICTED
            assert "not publishable" in verdict.reason

    def test_a_licence_that_was_never_recorded_fails_closed(self):
        """Silence is not permission. Publishing a restricted document is a history
        rewrite, where keeping a publishable one private is a day's work."""
        for uid in ("biorxiv__blank", "biorxiv__missing"):
            verdict = route(uid, LICENCES)
            assert verdict.tier is Tier.RESTRICTED
            assert verdict.reason == "licence never recorded"

    def test_a_stated_absence_is_not_the_same_as_never_asked(self):
        """pkp states a licence block with no terms in it, which is a fact about the
        publisher rather than a gap in our pipeline. Both stay private."""
        verdict = route("biorxiv__placeholder", LICENCES)
        assert verdict.tier is Tier.RESTRICTED
        assert "not publishable" in verdict.reason

    def test_cc0_and_public_domain_are_publishable(self):
        assert route("x", {"x": "CC0 1.0"}).tier is Tier.OPEN
        assert route("x", {"x": "CC0"}).tier is Tier.OPEN

    def test_share_alike_is_not_admitted_by_the_enumeration(self):
        """It is open by the Open Definition but copyleft, so one document could put a
        share-alike obligation on anything derived from the set."""
        assert "CC BY-SA 4.0" not in PUBLISHABLE_LICENCES
        assert route("x", {"x": "CC BY-SA 4.0"}).tier is Tier.RESTRICTED

    def test_documents_are_grouped_by_the_side_they_landed_on(self):
        grouped = by_tier(route_all(LICENCES, LICENCES))
        assert [v.uid for v in grouped[Tier.OPEN]] == ["biorxiv__open"]
        assert len(grouped[Tier.RESTRICTED]) == 5


class TestAdmission:
    def test_a_corpus_may_receive_documents_for_the_tiers_it_declares(self):
        check_admissible(CORPORA["biorxiv"], route_all(LICENCES, LICENCES))

    def test_a_corpus_with_no_open_repo_refuses_a_publishable_document(self):
        """Either the licence data or the registry is wrong, and both want a person."""
        verdicts = route_all(["x"], {"x": "CC BY 4.0"})
        with pytest.raises(LayoutError, match="no repo for"):
            check_admissible(CORPORA["pkp"], verdicts)


class TestReadingTheAudit:
    def test_it_reads_uid_and_licence_from_the_report(self, tmp_path):
        path = tmp_path / "licence-per-document.csv"
        path.write_text(
            "config,split,uid,doi,licence,training_use,excluded,evidence\n"
            "biorxiv-jats,train,biorxiv__a,10.1/a,CC BY 4.0,permissive,no,url\n",
            encoding="utf-8",
        )
        assert read_licences(path) == {"biorxiv__a": "CC BY 4.0"}

    def test_the_reports_own_verdict_column_is_not_what_decides(self, tmp_path):
        """It is a predicate over the label, and the estate admits against a list."""
        path = tmp_path / "licence-per-document.csv"
        path.write_text(
            "uid,licence,training_use,excluded\n"
            "biorxiv__a,CC BY-SA 4.0,permissive,no\n",
            encoding="utf-8",
        )
        assert route("biorxiv__a", read_licences(path)).tier is Tier.RESTRICTED

    def test_a_report_without_the_columns_it_needs_is_refused(self, tmp_path):
        path = tmp_path / "licence-per-document.csv"
        path.write_text("uid,verdict\nbiorxiv__a,permissive\n", encoding="utf-8")
        with pytest.raises(RoutingError, match="licence"):
            read_licences(path)
