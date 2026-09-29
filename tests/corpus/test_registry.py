"""Tests for corpus.registry — the corpora, and which repos each one has."""

import pytest

from sciencebeam_dataset_builder.corpus.layout import LayoutError, Tier
from sciencebeam_dataset_builder.corpus.registry import (
    CORPORA,
    corpus_for_source,
    repo_ids,
    repos,
)


class TestTheEstate:
    def test_it_is_the_eight_repos_the_layout_note_names(self):
        assert repo_ids(Tier.OPEN) == [
            "elifepathways/sciencebeam-dataset-biorxiv",
            "elifepathways/sciencebeam-dataset-ore",
            "elifepathways/sciencebeam-dataset-scielo-br",
            "elifepathways/sciencebeam-dataset-scielo-preprints",
        ]
        assert repo_ids(Tier.RESTRICTED) == [
            "elifepathways/sciencebeam-dataset-biorxiv-restricted",
            "elifepathways/sciencebeam-dataset-pkp-restricted",
            "elifepathways/sciencebeam-dataset-scielo-br-restricted",
            "elifepathways/sciencebeam-dataset-scielo-mx-restricted",
        ]
        assert len(repos()) == 8

    def test_a_corpus_with_no_publishable_half_has_no_open_repo(self):
        """Declaring the tiers is the ceiling. `pkp` has no grant at all and `scielo_mx`
        is entirely NonCommercial, so neither can ever admit a publishable document."""
        assert CORPORA["pkp"].tiers == (Tier.RESTRICTED,)
        assert CORPORA["scielo-mx"].tiers == (Tier.RESTRICTED,)

    def test_routing_to_a_tier_a_corpus_does_not_have_fails(self):
        with pytest.raises(LayoutError, match="no open repo"):
            CORPORA["pkp"].repo_id(Tier.OPEN)


class TestIdentity:
    def test_a_row_is_routed_by_its_frozen_source_value(self):
        assert corpus_for_source("scielo_br").name == "scielo-br"
        assert corpus_for_source("scielo_preprints").name == "scielo-preprints"

    def test_an_unknown_source_names_what_is_known(self):
        with pytest.raises(LayoutError, match="known are"):
            corpus_for_source("scielo-br")

    def test_the_repo_name_is_the_source_value_hyphenated(self):
        """The current convention. `source` is frozen because it feeds the split hash;
        the name is not, so the two are declared separately rather than derived."""
        for corpus in CORPORA.values():
            assert corpus.name == corpus.source.replace("_", "-")

    def test_sources_and_names_are_each_unique(self):
        assert len({c.source for c in CORPORA.values()}) == len(CORPORA)
        assert all(name == corpus.name for name, corpus in CORPORA.items())

    def test_every_corpus_names_the_config_it_is_built_from(self):
        assert CORPORA["scielo-br"].legacy_config == "scielo_br-jats"
        assert all(c.legacy_config for c in CORPORA.values())
