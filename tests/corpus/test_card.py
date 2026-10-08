"""Tests for corpus.card — one generated card per repo."""

from sciencebeam_dataset_builder.corpus.card import (
    GENERATED_MARKER,
    render_card,
    render_front_matter,
    render_schema_table,
    size_category,
)
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from sciencebeam_dataset_builder.corpus.schema import FIELD_NAMES

BIORXIV = CORPORA["biorxiv"]
PKP = CORPORA["pkp"]
COUNTS = {"pdf-jats": {"train": 11, "validation": 15, "test": 27}}


class TestFrontMatter:
    def test_the_open_repo_declares_the_licence_its_documents_carry(self):
        assert "license: cc-by-4.0" in render_front_matter(BIORXIV, Tier.OPEN, COUNTS)

    def test_a_mixed_private_repo_can_only_say_other(self):
        """The Hub takes a single identifier, and the restricted side is not uniform."""
        assert "license: other" in render_front_matter(PKP, Tier.RESTRICTED, COUNTS)

    def test_language_is_declared_where_it_is_known(self):
        rendered = render_front_matter(CORPORA["scielo-br"], Tier.OPEN, COUNTS)
        assert "language:\n- pt\n- en\n- es" in rendered

    def test_language_is_omitted_where_nothing_is_recorded(self):
        """Declaring a language the corpus is not in is worse than declaring none."""
        assert "language:" not in render_front_matter(PKP, Tier.RESTRICTED, COUNTS)

    def test_the_config_points_at_the_pairing_directory(self):
        rendered = render_front_matter(BIORXIV, Tier.OPEN, COUNTS)
        assert "- config_name: biorxiv-pdf-jats" in rendered
        assert "{split: train, path: pdf-jats/train/train-*.parquet}" in rendered

    def test_the_size_bucket_follows_the_count(self):
        assert size_category(53) == "n<1K"
        assert size_category(5_000) == "1K<n<10K"


class TestBody:
    def test_every_column_appears_in_the_schema_table(self):
        rendered = render_schema_table()
        assert all(f"`{name}`" in rendered for name in FIELD_NAMES)

    def test_the_card_says_it_is_generated(self):
        assert GENERATED_MARKER in render_card(BIORXIV, Tier.OPEN, COUNTS)

    def test_the_split_counts_are_rendered_with_a_total(self):
        assert "| `pdf-jats` | 11 | 15 | 27 | 53 |" in render_card(
            BIORXIV, Tier.OPEN, COUNTS
        )

    def test_it_says_test_is_held_out(self):
        assert "held-out" in render_card(BIORXIV, Tier.OPEN, COUNTS)

    def test_it_indicates_that_corrected_documents_are_modified(self):
        """CC BY requires it, and the xml column is no longer verbatim upstream."""
        rendered = render_card(BIORXIV, Tier.OPEN, COUNTS)
        assert "modified" in rendered
        assert "xml_upstream_sha" in rendered

    def test_it_says_the_corpus_set_is_provisional(self):
        assert "provisional" in render_card(BIORXIV, Tier.OPEN, COUNTS)

    def test_the_loading_example_names_the_repo_and_config(self):
        rendered = render_card(BIORXIV, Tier.OPEN, COUNTS)
        assert 'load_dataset("elifepathways/sciencebeam-dataset-biorxiv"' in rendered
        assert '"biorxiv-pdf-jats"' in rendered

    def test_the_public_and_private_cards_say_different_things_about_reuse(self):
        assert "may be redistributed" in render_card(BIORXIV, Tier.OPEN, COUNTS)
        assert "may not be redistributed" in render_card(PKP, Tier.RESTRICTED, COUNTS)
