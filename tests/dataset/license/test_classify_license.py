import pytest

from sciencebeam_dataset_builder.dataset.license import (
    classify_license as classify_module,
)
from sciencebeam_dataset_builder.dataset.license.classify_license import (
    TrainingUse,
    classify,
)
from sciencebeam_dataset_builder.dataset.license.extract_license import (
    ALL_RIGHTS_RESERVED,
    NO_LICENCE_ABSENT,
    NO_LICENCE_PLACEHOLDER,
    UNPARSEABLE,
    Licence,
)


def licence(label: str) -> Licence:
    return Licence(label, "evidence")


class TestClassify:
    @pytest.mark.parametrize("label", ["CC BY 4.0", "CC BY 3.0", "CC0 1.0"])
    def test_permissive_licences(self, label):
        assert classify(licence(label)) is TrainingUse.PERMISSIVE

    @pytest.mark.parametrize("label", ["CC BY-NC 4.0", "CC BY-NC-SA 4.0"])
    def test_non_commercial_licences(self, label):
        assert classify(licence(label)) is TrainingUse.NON_COMMERCIAL

    @pytest.mark.parametrize("label", ["CC BY-ND 4.0", "CC BY-NC-ND 4.0"])
    def test_no_derivatives_wins_over_non_commercial(self, label):
        assert classify(licence(label)) is TrainingUse.NO_DERIVATIVES

    def test_reserved_rights(self):
        assert classify(licence(ALL_RIGHTS_RESERVED)) is TrainingUse.RESERVED

    @pytest.mark.parametrize(
        "label", [NO_LICENCE_ABSENT, NO_LICENCE_PLACEHOLDER, UNPARSEABLE]
    )
    def test_no_stated_licence_is_unknown(self, label):
        assert classify(licence(label)) is TrainingUse.UNKNOWN

    def test_an_unrecognised_cc_clause_is_not_assumed_permissive(self):
        assert classify(licence("CC BY-SA 4.0")) is TrainingUse.UNKNOWN

    def test_nc_is_matched_as_a_clause_not_a_substring(self):
        # "licence" contains the letters "nc"; the word must not read as NonCommercial.
        assert classify(licence("CC BY 4.0")) is TrainingUse.PERMISSIVE


class TestExcluded:
    def test_unknown_and_reserved_always_excluded(self):
        assert TrainingUse.UNKNOWN.excluded
        assert TrainingUse.RESERVED.excluded

    def test_permissive_never_excluded(self):
        assert not TrainingUse.PERMISSIVE.excluded

    def test_no_derivatives_follows_the_policy_flag(self, monkeypatch):
        monkeypatch.setattr(classify_module, "NO_DERIVATIVES_BLOCKS_TRAINING", True)
        assert TrainingUse.NO_DERIVATIVES.excluded
        monkeypatch.setattr(classify_module, "NO_DERIVATIVES_BLOCKS_TRAINING", False)
        assert not TrainingUse.NO_DERIVATIVES.excluded

    def test_non_commercial_follows_the_policy_flag(self, monkeypatch):
        monkeypatch.setattr(classify_module, "NON_COMMERCIAL_BLOCKS_TRAINING", True)
        assert TrainingUse.NON_COMMERCIAL.excluded
        monkeypatch.setattr(classify_module, "NON_COMMERCIAL_BLOCKS_TRAINING", False)
        assert not TrainingUse.NON_COMMERCIAL.excluded
