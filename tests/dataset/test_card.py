import re

import yaml

from sciencebeam_dataset_builder.dataset.card import (
    render_card,
    render_front_matter,
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
