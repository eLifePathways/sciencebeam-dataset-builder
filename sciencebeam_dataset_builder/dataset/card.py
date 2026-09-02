"""Render the Hub dataset card from the schema definitions.

The card's YAML front matter and its schema/source tables are generated, so the
published documentation cannot drift from :mod:`sciencebeam_dataset_builder.dataset.schema`.
Everything that needs prose lives in a body file kept in this repository and is appended
verbatim, so regenerating the card never loses hand-written notes.
"""

from pathlib import Path

import pyarrow as pa

from sciencebeam_dataset_builder.dataset.schema import (
    CANONICAL_SCHEMA,
    FIELD_DESCRIPTIONS,
    SOURCES,
    Source,
)
from sciencebeam_dataset_builder.dataset.split import SPLIT_NAMES

LICENSE = "cc0-1.0"

GENERATED_MARKER = (
    "<!-- generated-by: make dataset-card - edit docs/dataset-card-body.md instead -->"
)


def _type_label(field_type: pa.DataType) -> str:
    """Return a compact, readable label for an Arrow type."""
    if pa.types.is_list(field_type):
        return f"list<{_type_label(field_type.value_type)}>"
    if pa.types.is_struct(field_type):
        children = ", ".join(f"{f.name}: {_type_label(f.type)}" for f in field_type)
        return f"struct<{children}>"
    if pa.types.is_boolean(field_type):
        return "bool"
    if pa.types.is_binary(field_type):
        return "binary"
    return "string"


def render_front_matter(sources: dict[str, Source]) -> str:
    """Return the YAML front matter registering every source as a loadable config."""
    lines = ["---", f"license: {LICENSE}", "configs:"]
    for source in sources.values():
        lines.append(f"- config_name: {source.config}")
        lines.append("  data_files:")
        for split in SPLIT_NAMES:
            lines.append(
                f"  - {{split: {split}, path: {source.config}/{split}-*.parquet}}"
            )
    lines.append("---")
    return "\n".join(lines)


def render_schema_table() -> str:
    """Return a Markdown table of the canonical schema."""
    lines = [
        "| column | type | null | description |",
        "| --- | --- | --- | --- |",
    ]
    for field in CANONICAL_SCHEMA:
        nullable = "yes" if field.nullable else "**no**"
        lines.append(
            f"| `{field.name}` | `{_type_label(field.type)}` | {nullable} | "
            f"{FIELD_DESCRIPTIONS.get(field.name, '')} |"
        )
    return "\n".join(lines)


def render_sources_table(sources: dict[str, Source]) -> str:
    """Return a Markdown table of every source, its config and its `id` form."""
    lines = [
        "| `source` | config | `xml_format` | schema | `id` | example |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for source in sources.values():
        schema = "canonical" if source.canonical else "**legacy**"
        lines.append(
            f"| `{source.name}` | `{source.config}` | `{source.xml_format}` | {schema} | "
            f"{source.id_description} | `{source.id_example}` |"
        )
    return "\n".join(lines)


def render_source_descriptions(sources: dict[str, Source]) -> str:
    """Return a bullet list describing each source."""
    return "\n".join(
        f"- **`{source.config}`** - {source.description}" for source in sources.values()
    )


def render_schema_exceptions(sources: dict[str, Source]) -> str:
    """Return a section naming the configs that do not conform, and what they carry."""
    legacy = [source for source in sources.values() if not source.canonical]
    if not legacy:
        return "Every config conforms; there are no exceptions."

    lines = ["### Schema exceptions", ""]
    for source in legacy:
        lines.append(
            f"**`{source.config}`** does not conform. It carries only `id`, `xml` and "
            "`pdf`, and lacks `source`, `uid`, `xml_format` and the bibliographic "
            "columns. Do not concatenate it with the canonical configs."
        )
    return "\n".join(lines)


def render_card(body: str, sources: dict[str, Source] | None = None) -> str:
    """Return the full dataset card: generated front matter and tables, then `body`."""
    sources = sources or SOURCES
    return "\n".join(
        [
            render_front_matter(sources),
            "",
            GENERATED_MARKER,
            "",
            "# sciencebeam-v2-benchmarking",
            "",
            "Paired source PDF and XML for benchmarking ScienceBeam v2 document conversion.",
            "",
            "Configs marked `canonical` below share one schema, so their rows can be",
            "concatenated and stay attributable via the `source` column. Configs marked",
            "`legacy` still carry an older, narrower schema - see Schema exceptions.",
            "",
            "## Sources",
            "",
            render_source_descriptions(sources),
            "",
            render_sources_table(sources),
            "",
            "`id` is the source-native identifier and is only unique within a source; use",
            "`uid` (`{source}__{id}`) as a key across configs. `id` is opaque - do not parse it.",
            "",
            "## Schema",
            "",
            "This is the schema every `canonical` config conforms to exactly - same",
            "column order, same types, same nullability.",
            "",
            render_schema_table(),
            "",
            render_schema_exceptions(sources),
            "",
            body.strip(),
            "",
        ]
    )


def read_body(path: Path) -> str:
    """Read the hand-written portion of the card."""
    return path.read_text(encoding="utf-8")
