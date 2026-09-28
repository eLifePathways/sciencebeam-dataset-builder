"""The estate's invariants, asserted rather than believed.

Split labels published before this builder existed are not reproducible from
`assign_split`: `scielo_preprints` was stratified by language and frozen, and the other
five corpora came from a different pipeline. Their proportions are 20/30/50 but their
assignment is not this hash, so a label is **carried** from the source release and only a
document new to the estate is assigned by the hash.
"""

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from sciencebeam_dataset_builder.corpus.ledger import Membership
from sciencebeam_dataset_builder.dataset.split import DEFAULT_FRACTIONS, assign_split


class VerificationError(AssertionError):
    """An invariant that does not hold."""


@dataclass(frozen=True)
class Violation:
    """One invariant that failed, and what failed it."""

    check: str
    detail: str

    def __str__(self) -> str:
        return f"{self.check}: {self.detail}"


def splits_are_carried(
    before: Sequence[Membership], after: Sequence[Membership]
) -> list[Violation]:
    """No document already in the estate changes split.

    Removal, addition and a move between repos all keep a document where it was, so
    numbers published against an earlier release stay true of it.
    """
    was = {row.uid: row.split for row in before}
    moved = sorted(
        f"{row.uid} {was[row.uid]} -> {row.split}"
        for row in after
        if row.uid in was and was[row.uid] != row.split
    )
    if not moved:
        return []
    return [Violation("splits are carried", f"{len(moved)} moved, e.g. {moved[:3]}")]


def halves_are_disjoint(
    open_rows: Sequence[Membership], restricted_rows: Sequence[Membership]
) -> list[Violation]:
    """A document is publishable or it is not. It cannot be in both repos."""
    both = sorted({r.uid for r in open_rows} & {r.uid for r in restricted_rows})
    if not both:
        return []
    return [Violation("halves are disjoint", f"{len(both)} in both, e.g. {both[:3]}")]


def union_reconstructs(
    source: Sequence[Membership],
    open_rows: Sequence[Membership],
    restricted_rows: Sequence[Membership],
) -> list[Violation]:
    """`open + restricted` is exactly the corpus it was cut from, split for split.

    Per corpus, not in total: a corpus dropped from the estate is simply absent, and
    checking the total would report that as a loss.
    """
    expected = {(r.uid, r.split) for r in source}
    produced = {(r.uid, r.split) for r in [*open_rows, *restricted_rows]}
    violations = []
    if lost := sorted(expected - produced):
        violations.append(
            Violation("union reconstructs", f"{len(lost)} lost, e.g. {lost[:3]}")
        )
    if gained := sorted(produced - expected):
        violations.append(
            Violation(
                "union reconstructs", f"{len(gained)} unexpected, e.g. {gained[:3]}"
            )
        )
    return violations


def new_documents_follow_the_hash(
    known: Iterable[str],
    after: Sequence[Membership],
    fractions: dict[str, float] | None = None,
) -> list[Violation]:
    """A document new to the estate is assigned by `sha256(uid)`, not by hand."""
    fractions = fractions or DEFAULT_FRACTIONS
    seen = set(known)
    wrong = sorted(
        f"{row.uid} {row.split} != {assign_split(row.uid, fractions)}"
        for row in after
        if row.uid not in seen and assign_split(row.uid, fractions) != row.split
    )
    if not wrong:
        return []
    return [
        Violation("new documents follow the hash", f"{len(wrong)}, e.g. {wrong[:3]}")
    ]


def verify_corpus(
    source: Sequence[Membership],
    open_rows: Sequence[Membership],
    restricted_rows: Sequence[Membership],
) -> list[Violation]:
    """Every invariant that a freshly built corpus has to satisfy."""
    produced = [*open_rows, *restricted_rows]
    return [
        *splits_are_carried(source, produced),
        *halves_are_disjoint(open_rows, restricted_rows),
        *union_reconstructs(source, open_rows, restricted_rows),
    ]


def raise_if_violated(violations: Sequence[Violation]) -> None:
    """Fail the run, naming every invariant that broke rather than the first."""
    if violations:
        listed = "\n".join(f"  {v}" for v in violations)
        raise VerificationError(f"{len(violations)} invariant(s) failed:\n{listed}")
