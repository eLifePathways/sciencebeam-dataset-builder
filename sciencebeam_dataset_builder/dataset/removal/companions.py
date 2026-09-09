"""Following a removal from one config into the config that mirrors it.

Some documents are published twice under different identities. Removing one rendition
without the other would leave the dataset claiming a document exists in the wider corpus
while the reviewed rendition of it is gone - so a removal has to follow the link.

Only SciELO Preprints is doubled up this way. `scielo-preprints-jats` holds the EuropePMC
JATS full text, keyed by accession (`PPR459291`); `scielo-preprints-metadata` holds the
OAI Dublin Core record for the wider corpus, keyed by OAI identifier
(`oai_ops.preprints.scielo.org_preprint_1317`). Neither identifier can be derived from
the other, and the two configs share split labels deliberately.

The join is the OAI preprint number, which reaches the JATS side only through its DOI::

    scielo_preprints__PPR459291
        doi 10.1590/scielopreprints.1317
            -> scielo_preprints_dublin_core__oai_ops.preprints.scielo.org_preprint_1317

That is the join the dataset card documents. `dc:identifier` is **not** usable for it:
247 metadata records carry the journal DOI of the published version instead, and 51
carry none. So the link is resolved from the JATS table's `doi` column, at the time of
the removal, rather than being written down anywhere.

A JATS row whose DOI is absent or shaped differently has no resolvable companion. That
is reported rather than guessed at - a metadata row deleted on a bad guess is a row
deleted for no reason.

Resolution needs the JATS row to still be there. Once a preprint has been removed from
`scielo-preprints-jats`, its DOI is gone with it and its companion can no longer be
derived - so a companion must be removed in the same pass as its JATS row, or named
explicitly in a removal list afterwards. Every resolved companion is written to the run
manifest with the JATS uid it came from, which is what makes that recovery possible.
"""

import re
from dataclasses import dataclass

import pyarrow as pa

from sciencebeam_dataset_builder.dataset.schema import SOURCES, make_uid

# The DOI every SciELO Preprints record carries, whose final component is the OAI
# preprint number that keys the Dublin Core config. Matched case-insensitively: DOIs are
# case-insensitive by spec, the JATS column holds `scielopreprints` while the splits
# manifest holds `SciELOPreprints`, and the number is the only part being read.
PREPRINT_DOI_PATTERN = re.compile(r"^10\.1590/scielopreprints\.(\d+)$", re.IGNORECASE)

METADATA_ID_TEMPLATE = "oai_ops.preprints.scielo.org_preprint_{number}"

JATS_SOURCE = SOURCES["scielo_preprints"]
METADATA_SOURCE = SOURCES["scielo_preprints_dublin_core"]


@dataclass(frozen=True)
class Companion:
    """A Dublin Core row that must go because its JATS rendition is being removed."""

    jats_uid: str
    """The reviewed row, as named in a removal list."""

    metadata_uid: str
    """The row to remove alongside it."""

    preprint_number: str
    """The OAI preprint number joining the two."""


def preprint_number_from_doi(doi: str | None) -> str | None:
    """Return the OAI preprint number in a SciELO Preprints DOI, or None."""
    if not doi:
        return None
    match = PREPRINT_DOI_PATTERN.match(doi.strip())
    return match.group(1) if match else None


def metadata_uid_for(preprint_number: str) -> str:
    """Return the Dublin Core `uid` for an OAI preprint number."""
    return make_uid(
        METADATA_SOURCE.name, METADATA_ID_TEMPLATE.format(number=preprint_number)
    )


def resolve_companions(
    jats_table: pa.Table,
    removed_uids: set[str],
) -> tuple[list[Companion], list[str]]:
    """Return the companions of the removed JATS rows, and the rows with no DOI to join on.

    `jats_table` is a `scielo-preprints-jats` split. Rows that are not being removed are
    ignored; a removed row whose `doi` does not parse yields no companion and is returned
    in the second list so the caller can report it instead of silently skipping it.
    """
    companions: list[Companion] = []
    unresolved: list[str] = []

    uids = jats_table.column("uid").to_pylist()
    dois = jats_table.column("doi").to_pylist()

    for uid, doi in zip(uids, dois):
        if uid not in removed_uids:
            continue
        number = preprint_number_from_doi(doi)
        if number is None:
            unresolved.append(uid)
            continue
        companions.append(
            Companion(
                jats_uid=uid,
                metadata_uid=metadata_uid_for(number),
                preprint_number=number,
            )
        )

    return companions, unresolved
