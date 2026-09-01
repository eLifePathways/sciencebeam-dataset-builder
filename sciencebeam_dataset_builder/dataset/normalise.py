"""Map a Parquet table from any of the dataset's historical schemas onto the canonical one.

Normalisation is deliberately conservative: it renames, adds and re-types columns, but
never reorders, drops or rewrites rows. The row count and row order of the returned
table always match the input.
"""

import logging

import pyarrow as pa
import pyarrow.compute as pc

from sciencebeam_dataset_builder.dataset.schema import (
    CANONICAL_SCHEMA,
    LEGACY_ID_COLUMNS,
    Source,
    make_uid,
)

LOGGER = logging.getLogger(__name__)


class NormalisationError(Exception):
    """Raised when a table cannot be mapped onto the canonical schema."""


def find_id_column(table: pa.Table) -> str:
    """Return the name of the column holding the source-native identifier.

    Historical subsets called it `ppr_id`, `article_id` or `id`; they are checked in
    that order so a table carrying both a legacy name and an `id` column resolves to
    the legacy one it was actually populating.
    """
    for name in LEGACY_ID_COLUMNS:
        if name in table.column_names:
            return name
    raise NormalisationError(
        f"No identifier column found; expected one of {LEGACY_ID_COLUMNS}, "
        f"got {table.column_names}"
    )


def _cast_column(column: pa.ChunkedArray, field: pa.Field) -> pa.ChunkedArray:
    """Cast a column to a canonical field's type.

    A plain cast covers the `large_string` -> `string` narrowing and the
    `list<null>` -> `list<string>` widening. Nested struct casts where the child
    fields are in a different order are not supported by every pyarrow version, so
    that case falls back to rebuilding the column from Python values.
    """
    if column.type.equals(field.type):
        return column
    try:
        return column.cast(field.type)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
        LOGGER.debug(
            "Direct cast of %r from %s to %s failed; rebuilding",
            field.name,
            column.type,
            field.type,
        )
        return pa.chunked_array([pa.array(column.to_pylist(), type=field.type)])


def _nullify_empty(column: pa.ChunkedArray) -> pa.ChunkedArray:
    """Replace empty strings with nulls, so 'missing' and 'empty' stop being the same value."""
    if not pa.types.is_string(column.type) and not pa.types.is_large_string(
        column.type
    ):
        return column
    mask = pc.equal(column, "")
    return pc.if_else(mask, pa.nulls(len(column), type=column.type), column)


def normalise_table(
    table: pa.Table,
    source: Source,
    nullify_empty: bool = False,
) -> pa.Table:
    """Return `table` conformed to :data:`CANONICAL_SCHEMA` for `source`.

    `source`, `uid` and `xml_format` are derived rather than read, since a source's
    Parquet files live in a per-source directory and so carry the source implicitly.

    Set `nullify_empty` to turn empty strings into nulls. It is off by default because
    it rewrites stored values; the historical builders wrote `""` for absent metadata.
    """
    id_column = find_id_column(table)
    id_values = table.column(id_column).cast(pa.string())

    if id_values.null_count:
        raise NormalisationError(
            f"{source.config}: {id_values.null_count} row(s) have a null {id_column}"
        )

    derived: dict[str, pa.ChunkedArray] = {
        "source": pa.chunked_array([pa.array([source.name] * len(table), pa.string())]),
        "id": id_values,
        "uid": pa.chunked_array(
            [
                pa.array(
                    [make_uid(source.name, value) for value in id_values.to_pylist()],
                    pa.string(),
                )
            ]
        ),
        "xml_format": pa.chunked_array(
            [pa.array([source.xml_format] * len(table), pa.string())]
        ),
    }

    unmapped = set(table.column_names) - set(CANONICAL_SCHEMA.names) - {id_column}
    if unmapped:
        raise NormalisationError(
            f"{source.config}: columns not present in the canonical schema would be "
            f"dropped: {sorted(unmapped)}. Add them to the schema or remove them "
            "explicitly before migrating."
        )

    columns: list[pa.ChunkedArray] = []
    added: list[str] = []
    for field in CANONICAL_SCHEMA:
        if field.name in derived:
            columns.append(derived[field.name])
            continue
        if field.name not in table.column_names:
            if not field.nullable:
                raise NormalisationError(
                    f"{source.config}: required column {field.name!r} is missing"
                )
            added.append(field.name)
            columns.append(pa.chunked_array([pa.nulls(len(table), type=field.type)]))
            continue
        column = _cast_column(table.column(field.name), field)
        if nullify_empty:
            column = _nullify_empty(column)
        columns.append(column)

    if added:
        LOGGER.info("%s: padded missing columns with nulls: %s", source.config, added)

    return pa.Table.from_arrays(columns, schema=CANONICAL_SCHEMA)


def describe_changes(table: pa.Table, source: Source) -> list[str]:
    """Return human-readable lines describing what normalising `table` would change."""
    lines: list[str] = []
    id_column = find_id_column(table)
    if id_column != "id":
        lines.append(f"rename {id_column} -> id")

    existing = set(table.column_names)
    for field in CANONICAL_SCHEMA:
        if field.name in ("source", "uid", "xml_format"):
            if field.name not in existing:
                lines.append(f"add {field.name} (derived)")
            continue
        if field.name == "id":
            continue
        if field.name not in existing:
            lines.append(f"add {field.name} (all null)")
            continue
        actual = table.schema.field(field.name)
        if not actual.type.equals(field.type):
            lines.append(f"retype {field.name}: {actual.type} -> {field.type}")
        if actual.nullable and not field.nullable:
            lines.append(f"mark {field.name} as not null")

    return lines or ["already canonical"]
