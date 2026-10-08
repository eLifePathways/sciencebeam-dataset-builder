"""Which side of the estate a document belongs on.

Routing fails closed. A licence that was never recorded is not a licence that permits
anything: `pkp`'s empty `license` column means "never asked", and reading silence as
permission is the one mistake here that cannot be undone, because a restricted document
published is a history rewrite rather than a retraining.
"""

import csv
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from sciencebeam_dataset_builder.corpus.layout import LayoutError, Tier
from sciencebeam_dataset_builder.corpus.registry import Corpus

# An explicit enumeration, never a predicate. "Openly licensed" admits CC BY-SA, which is
# copyleft: one share-alike document in the publishable set can put that obligation on
# anything derived from it. Labels are as `extract_license.py` writes them.
PUBLISHABLE_LICENCES = frozenset(
    {
        "CC BY 4.0",
        "CC BY 3.0",
        "CC0 1.0",
        "CC0",
    }
)

NEVER_RECORDED = ""


class RoutingError(ValueError):
    """A document that cannot be routed as asked."""


@dataclass(frozen=True)
class Verdict:
    """Where one document goes, and on what evidence."""

    uid: str
    licence: str
    tier: Tier
    reason: str


def read_licences(path: Path) -> dict[str, str]:
    """Read `uid -> licence label` from the licence audit's per-document report.

    The report's own `training_use` and `excluded` columns are deliberately ignored:
    they are a predicate over the label, and the estate admits against an enumeration.
    """
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for required in ("uid", "licence"):
            if required not in (reader.fieldnames or []):
                raise RoutingError(f"{path} has no {required!r} column")
        return {row["uid"].strip(): (row["licence"] or "").strip() for row in reader}


def route(uid: str, licences: Mapping[str, str]) -> Verdict:
    """Decide one document's side."""
    licence = licences.get(uid, NEVER_RECORDED)
    if licence in PUBLISHABLE_LICENCES:
        return Verdict(uid, licence, Tier.OPEN, f"{licence} permits redistribution")
    if licence == NEVER_RECORDED:
        return Verdict(uid, licence, Tier.RESTRICTED, "licence never recorded")
    return Verdict(uid, licence, Tier.RESTRICTED, f"{licence} is not publishable")


def route_all(uids: Iterable[str], licences: Mapping[str, str]) -> list[Verdict]:
    return [route(uid, licences) for uid in uids]


def by_tier(verdicts: Sequence[Verdict]) -> dict[Tier, list[Verdict]]:
    grouped: dict[Tier, list[Verdict]] = {tier: [] for tier in Tier}
    for verdict in verdicts:
        grouped[verdict.tier].append(verdict)
    return grouped


def check_admissible(corpus: Corpus, verdicts: Sequence[Verdict]) -> None:
    """Refuse a routing that needs a repo this corpus does not have.

    The tiers a corpus declares are its ceiling. A `pkp` document turning up publishable
    means either the licence data or the registry is wrong, and both want a person.
    """
    for tier, routed in by_tier(verdicts).items():
        if routed and tier not in corpus.tiers:
            raise LayoutError(
                f"{len(routed)} {corpus.name!r} document(s) routed to {tier.value}, "
                f"which it has no repo for, e.g. {routed[0].uid}"
            )
