import re

import yaml

from sciencebeam_dataset_builder.dataset.card import (
    render_card,
    render_front_matter,
    render_schema_exceptions,
    render_schema_table,
    render_sources_table,
)
from sciencebeam_dataset_builder.dataset.schema import CANONICAL_SCHEMA, SOURCES
from sciencebeam_dataset_builder.dataset.split import SPLIT_NAMES


def _front_matter(text: str) -> dict:
    match = re.match(r"^---\n(.*?)\n---\n", text, re.DOTALL)
    assert match, "card does not start with YAML front matter"
    return yaml.safe_load(match.group(1))


class TestFrontMatter:
    def test_is_valid_yaml(self):
        assert _front_matter(render_front_matter(SOURCES) + "\n")

    def test_registers_every_source_as_a_config(self):
        parsed = _front_matter(render_front_matter(SOURCES) + "\n")
        registered = {c["config_name"] for c in parsed["configs"]}
        assert registered == {s.config for s in SOURCES.values()}

    def test_every_config_declares_all_three_splits(self):
        parsed = _front_matter(render_front_matter(SOURCES) + "\n")
        for config in parsed["configs"]:
            assert {f["split"] for f in config["data_files"]} == set(SPLIT_NAMES)

    def test_data_file_paths_live_under_the_config_directory(self):
        parsed = _front_matter(render_front_matter(SOURCES) + "\n")
        for config in parsed["configs"]:
            for entry in config["data_files"]:
                assert entry["path"].startswith(f"{config['config_name']}/")


class TestSchemaTable:
    def test_lists_every_canonical_column(self):
        table = render_schema_table()
        for name in CANONICAL_SCHEMA.names:
            assert f"`{name}`" in table

    def test_marks_non_nullable_columns(self):
        table = render_schema_table()
        for line in table.splitlines():
            if line.startswith("| `xml` "):
                assert "**no**" in line


class TestSourcesTable:
    def test_lists_every_source_with_its_config(self):
        table = render_sources_table(SOURCES)
        for source in SOURCES.values():
            assert f"`{source.name}`" in table
            assert f"`{source.config}`" in table
            assert f"`{source.id_example}`" in table


class TestRenderCard:
    def test_appends_the_body_verbatim(self):
        card = render_card("## Notes\n\nSomething hand-written.\n")
        assert "## Notes" in card
        assert "Something hand-written." in card

    def test_starts_with_front_matter(self):
        assert render_card("body").startswith("---\n")

    def test_marks_itself_as_generated(self):
        assert "generated-by" in render_card("body")


class TestSchemaExceptions:
    def test_flags_a_non_conforming_config_in_the_sources_table(self):
        table = render_sources_table(SOURCES)
        legacy = [s for s in SOURCES.values() if not s.canonical]
        assert legacy, "expected at least one non-canonical source to exercise this"
        for source in legacy:
            row = next(
                line for line in table.splitlines() if f"`{source.config}`" in line
            )
            assert "**legacy**" in row

    def test_marks_conforming_configs_as_canonical(self):
        table = render_sources_table(SOURCES)
        for source in SOURCES.values():
            if not source.canonical:
                continue
            row = next(
                line for line in table.splitlines() if f"`{source.config}`" in line
            )
            assert "canonical" in row and "**legacy**" not in row

    def test_card_names_every_non_conforming_config(self):
        card = render_card("body")
        assert "### Schema exceptions" in card
        for source in SOURCES.values():
            if not source.canonical:
                assert source.config in card.split("### Schema exceptions")[1]

    def test_non_conforming_configs_are_still_registered_as_loadable(self):
        parsed = _front_matter(render_front_matter(SOURCES) + "\n")
        registered = {c["config_name"] for c in parsed["configs"]}
        assert registered == {s.config for s in SOURCES.values()}

    def test_no_exceptions_section_content_when_all_conform(self):
        conforming = {k: v for k, v in SOURCES.items() if v.canonical}
        assert "no exceptions" in render_schema_exceptions(conforming)
