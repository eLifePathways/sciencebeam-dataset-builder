"""The corpora, and which repos each one has.

Declaring the tiers states the estate's ceiling rather than describing what a repo holds
today: `pkp` has no publishable half, so routing a `pkp` document to the open side is a
failure rather than a repo that quietly appears. A corpus whose mix changes needs a
deliberate edit here, which is the point.
"""

from dataclasses import dataclass

from sciencebeam_dataset_builder.corpus.layout import (
    PAIRING_PDF_JATS,
    LayoutError,
    Tier,
    repo_id,
)


@dataclass(frozen=True)
class Corpus:
    """One corpus, and how it is addressed."""

    name: str
    """Hyphenated, and what the repo is named for. Free to change."""

    source: str
    """The `source` column value. Frozen forever: it feeds `sha256(uid)`, so changing it
    would re-split the corpus and invalidate every published number."""

    tiers: tuple[Tier, ...]
    """Which repos exist. A tier absent here has no repo and admits nothing."""

    pairings: tuple[str, ...]
    """The (input format, target format) directories this corpus publishes."""

    legacy_config: str
    """The config in `benchmark-v002` this corpus is read from when it is built."""

    languages: tuple[str, ...]
    """ISO 639-1 codes observed in the corpus, most frequent first. Empty where none are
    recorded, which is why the card omits the field rather than guessing: declaring a
    language a corpus is not in is worse than declaring none."""

    description: str

    def repo_id(self, tier: Tier) -> str:
        if tier not in self.tiers:
            raise LayoutError(
                f"{self.name!r} has no {tier.value} repo; its tiers are "
                f"{[t.value for t in self.tiers]}"
            )
        return repo_id(self.name, tier)


CORPORA: dict[str, Corpus] = {
    "biorxiv": Corpus(
        name="biorxiv",
        source="biorxiv",
        tiers=(Tier.OPEN, Tier.RESTRICTED),
        pairings=(PAIRING_PDF_JATS,),
        legacy_config="biorxiv-jats",
        languages=("en",),
        description="bioRxiv preprints with publisher JATS full text.",
    ),
    "ore": Corpus(
        name="ore",
        source="ore",
        tiers=(Tier.OPEN,),
        pairings=(PAIRING_PDF_JATS,),
        legacy_config="ore-jats",
        languages=("en",),
        description="Open Research Europe articles.",
    ),
    "pkp": Corpus(
        name="pkp",
        source="pkp",
        tiers=(Tier.RESTRICTED,),
        pairings=(PAIRING_PDF_JATS,),
        legacy_config="pkp-jats",
        languages=(),
        description="Articles from OJS journals using the PKP JATS plugin.",
    ),
    "scielo-br": Corpus(
        name="scielo-br",
        source="scielo_br",
        tiers=(Tier.OPEN, Tier.RESTRICTED),
        pairings=(PAIRING_PDF_JATS,),
        legacy_config="scielo_br-jats",
        languages=("pt", "en", "es"),
        description="SciELO Brazil articles.",
    ),
    "scielo-mx": Corpus(
        name="scielo-mx",
        source="scielo_mx",
        tiers=(Tier.RESTRICTED,),
        pairings=(PAIRING_PDF_JATS,),
        legacy_config="scielo_mx-jats",
        languages=("es", "en"),
        description="SciELO Mexico articles.",
    ),
    "scielo-preprints": Corpus(
        name="scielo-preprints",
        source="scielo_preprints",
        tiers=(Tier.OPEN,),
        pairings=(PAIRING_PDF_JATS,),
        legacy_config="scielo-preprints-jats",
        languages=("pt", "es", "fr"),
        description="SciELO Preprints for which a EuropePMC JATS full text exists.",
    ),
}

CORPORA_BY_SOURCE: dict[str, Corpus] = {c.source: c for c in CORPORA.values()}


def corpus_for_source(source: str) -> Corpus:
    """The corpus a row belongs to, by its `source` column value."""
    try:
        return CORPORA_BY_SOURCE[source]
    except KeyError:
        known = ", ".join(sorted(CORPORA_BY_SOURCE))
        raise LayoutError(
            f"{source!r} is not a known source; known are {known}"
        ) from None


def repos() -> list[tuple[Corpus, Tier]]:
    """Every repo in the estate, corpus by corpus."""
    return [(corpus, tier) for corpus in CORPORA.values() for tier in corpus.tiers]


def repo_ids(tier: Tier | None = None) -> list[str]:
    """Every repo id, optionally narrowed to one side of the estate."""
    return [
        corpus.repo_id(each) for corpus, each in repos() if tier is None or each is tier
    ]
