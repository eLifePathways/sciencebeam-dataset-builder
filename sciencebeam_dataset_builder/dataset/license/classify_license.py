"""Deciding what a stated licence permits for model training.

Policy, not fact: the two flags below are contested readings and can be changed
without re-reading any Parquet.
"""

from enum import Enum

from sciencebeam_dataset_builder.dataset.license.extract_license import (
    ALL_RIGHTS_RESERVED,
    Licence,
)

NO_DERIVATIVES_BLOCKS_TRAINING = True
NON_COMMERCIAL_BLOCKS_TRAINING = False


class TrainingUse(Enum):
    """What a licence permits for training."""

    PERMISSIVE = "permissive"
    NON_COMMERCIAL = "non-commercial"
    NO_DERIVATIVES = "no-derivatives"
    RESERVED = "all-rights-reserved"
    UNKNOWN = "unknown"

    @property
    def excluded(self) -> bool:
        """Whether this verdict keeps a document out of training, under the policy."""
        if self is TrainingUse.NO_DERIVATIVES:
            return NO_DERIVATIVES_BLOCKS_TRAINING
        if self is TrainingUse.NON_COMMERCIAL:
            return NON_COMMERCIAL_BLOCKS_TRAINING
        return self in (TrainingUse.RESERVED, TrainingUse.UNKNOWN)


def classify(licence: Licence) -> TrainingUse:
    """Return what `licence` permits for training.

    Clauses are matched as whole tokens: `NC` occurs inside the word `licence`, and a
    future `CC BY-NC-SA` must still read as non-commercial.
    """
    if licence.label == ALL_RIGHTS_RESERVED:
        return TrainingUse.RESERVED
    if not licence.is_creative_commons:
        return TrainingUse.UNKNOWN
    if licence.label.startswith("CC0"):
        return TrainingUse.PERMISSIVE

    clauses = set(licence.label.removeprefix("CC ").split(" ")[0].split("-"))
    # CC BY-NC-ND carries both; the stricter clause decides.
    if "ND" in clauses:
        return TrainingUse.NO_DERIVATIVES
    if "NC" in clauses:
        return TrainingUse.NON_COMMERCIAL
    return TrainingUse.PERMISSIVE if clauses == {"BY"} else TrainingUse.UNKNOWN
