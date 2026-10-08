"""Tests for corpus.verify — the estate's invariants."""

import pytest

from sciencebeam_dataset_builder.corpus.ledger import Membership
from sciencebeam_dataset_builder.corpus.verify import (
    VerificationError,
    halves_are_disjoint,
    licences_are_publishable,
    new_documents_follow_the_hash,
    raise_if_violated,
    splits_are_carried,
    union_reconstructs,
    verify_corpus,
)
from sciencebeam_dataset_builder.dataset.split import DEFAULT_FRACTIONS, assign_split


def _row(id_value: str, split: str = "train") -> Membership:
    return Membership(corpus="biorxiv", id=id_value, split=split)


def _hashed(id_value: str) -> Membership:
    return Membership(
        corpus="biorxiv", id=id_value, split=assign_split(id_value, DEFAULT_FRACTIONS)
    )


class TestSplitsAreCarried:
    def test_a_document_that_keeps_its_split_passes(self):
        before = [_row("a", "test"), _row("b", "train")]
        assert splits_are_carried(before, [_row("a", "test")]) == []

    def test_a_document_that_moves_split_is_reported(self):
        before = [_row("a", "test")]
        violations = splits_are_carried(before, [_row("a", "train")])
        assert len(violations) == 1
        assert "a test -> train" in violations[0].detail

    def test_a_document_new_to_the_estate_is_not_a_move(self):
        assert splits_are_carried([_row("a")], [_row("a"), _row("b", "test")]) == []

    def test_a_removed_document_is_not_a_move(self):
        assert splits_are_carried([_row("a"), _row("b")], [_row("a")]) == []


class TestHalvesAreDisjoint:
    def test_a_document_in_one_half_only_passes(self):
        assert halves_are_disjoint([_row("a")], [_row("b")]) == []

    def test_a_document_in_both_halves_is_reported(self):
        violations = halves_are_disjoint([_row("a")], [_row("a")])
        assert len(violations) == 1
        assert "1 in both" in violations[0].detail


class TestUnionReconstructs:
    def test_the_two_halves_rebuild_the_corpus(self):
        source = [_row("a", "train"), _row("b", "test")]
        assert (
            union_reconstructs(source, [_row("a", "train")], [_row("b", "test")]) == []
        )

    def test_a_document_that_reached_neither_half_is_reported(self):
        source = [_row("a"), _row("b")]
        violations = union_reconstructs(source, [_row("a")], [])
        assert "1 lost" in violations[0].detail

    def test_a_document_that_came_from_nowhere_is_reported(self):
        violations = union_reconstructs([_row("a")], [_row("a"), _row("c")], [])
        assert "1 unexpected" in violations[0].detail

    def test_a_document_carried_with_a_different_split_is_reported(self):
        """It is both lost and unexpected, since the pair is what is compared."""
        violations = union_reconstructs([_row("a", "test")], [_row("a", "train")], [])
        assert len(violations) == 2


class TestNewDocumentsFollowTheHash:
    def test_a_new_document_placed_by_the_hash_passes(self):
        assert new_documents_follow_the_hash([], [_hashed("biorxiv__new")]) == []

    def test_a_new_document_placed_by_hand_is_reported(self):
        wrong = (
            "train"
            if assign_split("biorxiv__new", DEFAULT_FRACTIONS) != "train"
            else "test"
        )
        violations = new_documents_follow_the_hash([], [_row("biorxiv__new", wrong)])
        assert len(violations) == 1

    def test_a_document_already_in_the_estate_is_exempt(self):
        """The published labels predate this hash and are carried, not recomputed."""
        assert (
            new_documents_follow_the_hash(["biorxiv__old"], [_row("biorxiv__old")])
            == []
        )


class TestLicencesArePublishable:
    def test_only_enumerated_licences_pass(self):
        assert licences_are_publishable(["CC BY 4.0", "CC0 1.0"]) == []

    def test_anything_else_is_reported(self):
        """Checked against the actual data, not asserted as a card string: a document
        routed to the open repo on a bad licence should never go unnoticed."""
        violations = licences_are_publishable(["CC BY 4.0", "CC BY-NC 4.0"])
        assert len(violations) == 1
        assert "CC BY-NC 4.0" in violations[0].detail

    def test_an_empty_licence_is_also_not_publishable(self):
        """A licence that was never recorded fails the same way a restricted one does."""
        assert licences_are_publishable(["CC BY 4.0", ""]) != []


class TestVerifyCorpus:
    def test_a_clean_build_has_no_violations(self):
        source = [_row("a", "train"), _row("b", "test")]
        assert verify_corpus(source, [_row("a", "train")], [_row("b", "test")]) == []

    def test_every_broken_invariant_is_named_rather_than_the_first(self):
        source = [_row("a", "train"), _row("b", "test")]
        violations = verify_corpus(source, [_row("a", "test")], [_row("a", "test")])
        assert len(violations) > 1
        with pytest.raises(VerificationError, match="invariant"):
            raise_if_violated(violations)

    def test_nothing_is_raised_when_nothing_is_violated(self):
        raise_if_violated([])
