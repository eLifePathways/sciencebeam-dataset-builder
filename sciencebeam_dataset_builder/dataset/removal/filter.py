"""Deleting listed rows from a table, and proving only those rows went.

Removal only ever deletes whole rows. It does not touch the columns of the rows that
survive, and it does not re-split: split membership is a frozen hash bucket on `uid`
(see :mod:`sciencebeam_dataset_builder.dataset.split`), so deleting rows shrinks the
splits rather than reshuffling the documents that remain. A row's split is a property of
its identity, and the documents that stay keep the split they were evaluated under.

:func:`verify_removal` is the counterpart check for :func:`remove_rows`: it proves an
output table is exactly its input minus the listed rows, with every surviving value
untouched and in order.

Not every config carries a `uid` column: `scielo-preprints-metadata` still has the
narrow `id` / `xml` / `pdf` schema it was published with. :func:`row_uids` derives the
uid for those from the source and the source-native id, so removal addresses every
config by the same globally unique key regardless of what the table itself stores.
"""

import pyarrow as pa

from sciencebeam_dataset_builder.dataset.normalise import find_id_column
from sciencebeam_dataset_builder.dataset.schema import Source, make_uid


def row_uids(table: pa.Table, source: Source) -> list[str]:
    """Return the `uid` of every row, deriving it where the table has no such column."""
    if "uid" in table.column_names:
        uids: list[str] = table.column("uid").to_pylist()
        return uids
    id_column = find_id_column(table)
    return [
        make_uid(source.name, value)
        for value in table.column(id_column).cast(pa.string()).to_pylist()
    ]


def remove_rows(
    table: pa.Table, uids: set[str], source: Source
) -> tuple[pa.Table, list[str]]:
    """Return `table` without the rows whose `uid` is in `uids`, and which were dropped.

    Filtering slices the Arrow table directly, so the output carries exactly the input
    schema - including non-nullable fields and struct child order - and the surviving
    rows keep their original relative order.
    """
    present = row_uids(table, source)
    mask = [uid not in uids for uid in present]
    dropped = [uid for uid in present if uid in uids]
    return table.filter(pa.array(mask, pa.bool_())), dropped


def verify_removal(
    original: pa.Table,
    filtered: pa.Table,
    uids: set[str],
    source: Source,
) -> list[str]:
    """Return a list of failure messages; empty means the removal verified.

    Proves `filtered` is `original` minus exactly the rows named by `uids`: nothing else
    was dropped, nothing listed survived, the schema is unchanged, and every value of
    every surviving row is identical and still in its original order.
    """
    failures: list[str] = []

    if not filtered.schema.equals(original.schema):
        failures.append("schema changed")

    original_uids = row_uids(original, source)
    kept_indices = [i for i, uid in enumerate(original_uids) if uid not in uids]
    expected_uids = [original_uids[i] for i in kept_indices]

    filtered_uids = row_uids(filtered, source)
    if filtered_uids != expected_uids:
        surviving = sorted(set(filtered_uids) & uids)
        if surviving:
            failures.append(f"{len(surviving)} listed uid(s) survived: {surviving[:5]}")
        unexpected = sorted(set(expected_uids) - set(filtered_uids))
        if unexpected:
            failures.append(
                f"{len(unexpected)} unlisted uid(s) were dropped: {unexpected[:5]}"
            )
        if not surviving and not unexpected:
            failures.append("surviving rows are no longer in their original order")
        # Column comparisons below index by position, which is meaningless once the
        # rows themselves disagree.
        return failures

    # Every surviving row must be untouched, payload and metadata alike.
    kept = original.take(kept_indices)
    for column in original.column_names:
        if column not in filtered.column_names:
            failures.append(f"{column} is missing from the output")
        elif kept.column(column).to_pylist() != filtered.column(column).to_pylist():
            failures.append(f"{column} values changed")

    return failures
