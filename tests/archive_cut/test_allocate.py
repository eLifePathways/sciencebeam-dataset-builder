"""Tests for archive_cut.allocate — the nesting invariant and its rejection cases."""

import pytest

from sciencebeam_dataset_builder.archive_cut.allocate import (
    AllocationError,
    ArchiveChangedError,
    MetadataRow,
    MonotonicityError,
    _check_superset_of_previous,
    allocate,
)
from sciencebeam_dataset_builder.archive_cut.manifest import ManifestRow

from tests.archive_cut._helpers import config, metadata, paper_id, ranks_in

SPLITS = ["test", "validation"]


class TestRankPrefixSelection:
    def test_first_cut_takes_the_lowest_ranks(self):
        result = allocate(
            metadata(alpha=10),
            config(splits=SPLITS, default={"test": 3, "validation": 2}),
        )
        assert ranks_in(list(result.rows), "alpha", "test") == [0, 1, 2]
        assert ranks_in(list(result.rows), "alpha", "validation") == [3, 4]

    def test_selection_ignores_the_order_metadata_arrives_in(self):
        rows = metadata(alpha=6)
        shuffled = [rows[4], rows[0], rows[5], rows[2], rows[1], rows[3]]
        result = allocate(
            shuffled, config(splits=SPLITS, default={"test": 2, "validation": 1})
        )
        assert ranks_in(list(result.rows), "alpha", "test") == [0, 1]
        assert ranks_in(list(result.rows), "alpha", "validation") == [2]

    def test_zero_count_split_gets_nothing(self):
        result = allocate(
            metadata(alpha=5),
            config(splits=SPLITS, default={"test": 2, "validation": 0}),
        )
        assert ranks_in(list(result.rows), "alpha", "validation") == []
        assert len(result.rows) == 2

    def test_balanced_across_strata_regardless_of_their_size(self):
        result = allocate(
            metadata(alpha=100, beta=20, gamma=9),
            config(splits=SPLITS, default={"test": 3, "validation": 2}),
        )
        counts = result.counts()
        for stratum in ("alpha", "beta", "gamma"):
            assert counts[(stratum, "test")] == 3
            assert counts[(stratum, "validation")] == 2


class TestDisjointness:
    def test_no_document_is_in_two_splits(self):
        result = allocate(
            metadata(alpha=10, beta=10),
            config(splits=SPLITS, default={"test": 4, "validation": 3}),
        )
        ids = [row.id for row in result.rows]
        assert len(ids) == len(set(ids))


class TestNesting:
    def test_growing_a_later_split_appends_without_touching_the_earlier_one(self):
        first = allocate(
            metadata(alpha=10),
            config(splits=SPLITS, default={"test": 3, "validation": 2}),
        )
        second = allocate(
            metadata(alpha=10),
            config(splits=SPLITS, default={"test": 3, "validation": 4}, version=2),
            previous=list(first.rows),
        )
        assert ranks_in(list(second.rows), "alpha", "test") == [0, 1, 2]
        assert ranks_in(list(second.rows), "alpha", "validation") == [3, 4, 5, 6]

    def test_growing_the_first_served_split_does_not_move_published_documents(self):
        """The case contiguous rank blocks would get wrong.

        Raising the first-served split's count must append to it, not reclaim the ranks
        the next split already published.
        """
        first = allocate(
            metadata(alpha=10),
            config(splits=SPLITS, default={"test": 3, "validation": 2}),
        )
        second = allocate(
            metadata(alpha=10),
            config(splits=SPLITS, default={"test": 5, "validation": 2}, version=2),
            previous=list(first.rows),
        )
        # Ranks 3 and 4 stay in validation; test grows past them instead.
        assert ranks_in(list(second.rows), "alpha", "validation") == [3, 4]
        assert ranks_in(list(second.rows), "alpha", "test") == [0, 1, 2, 5, 6]

    def test_every_split_is_a_superset_of_its_published_self(self):
        first = allocate(
            metadata(alpha=10, beta=10),
            config(splits=SPLITS, default={"test": 3, "validation": 2}),
        )
        second = allocate(
            metadata(alpha=10, beta=10),
            config(splits=SPLITS, default={"test": 6, "validation": 3}, version=2),
            previous=list(first.rows),
        )
        published = {(row.stratum, row.split, row.id) for row in first.rows}
        current = {(row.stratum, row.split, row.id) for row in second.rows}
        assert published <= current

    def test_growth_can_be_asymmetric_per_stratum(self):
        first = allocate(
            metadata(alpha=10, beta=10),
            config(splits=SPLITS, default={"test": 2, "validation": 1}),
        )
        second = allocate(
            metadata(alpha=10, beta=10),
            config(
                splits=SPLITS,
                default={"test": 2, "validation": 1},
                overrides={"alpha": {"test": 5}},
                version=2,
            ),
            previous=list(first.rows),
        )
        counts = second.counts()
        assert counts[("alpha", "test")] == 5
        assert counts[("beta", "test")] == 2

    def test_added_ids_are_only_the_new_documents(self):
        first = allocate(
            metadata(alpha=10),
            config(splits=SPLITS, default={"test": 3, "validation": 0}),
        )
        second = allocate(
            metadata(alpha=10),
            config(splits=SPLITS, default={"test": 5, "validation": 0}, version=2),
            previous=list(first.rows),
        )
        assert second.added_ids == {paper_id("alpha", 3), paper_id("alpha", 4)}

    def test_rerunning_the_same_config_adds_nothing(self):
        first = allocate(
            metadata(alpha=10),
            config(splits=SPLITS, default={"test": 3, "validation": 2}),
        )
        again = allocate(
            metadata(alpha=10),
            config(splits=SPLITS, default={"test": 3, "validation": 2}),
            previous=list(first.rows),
        )
        assert again.added_ids == frozenset()
        assert set(again.rows) == set(first.rows)


class TestMonotonicityRejection:
    def test_lowering_a_published_count_is_rejected(self):
        first = allocate(
            metadata(alpha=10),
            config(splits=SPLITS, default={"test": 4, "validation": 2}),
        )
        with pytest.raises(MonotonicityError) as exc_info:
            allocate(
                metadata(alpha=10),
                config(splits=SPLITS, default={"test": 2, "validation": 2}, version=2),
                previous=list(first.rows),
            )
        message = str(exc_info.value)
        assert "alpha" in message
        assert "test" in message

    def test_dropping_a_published_split_is_rejected(self):
        first = allocate(
            metadata(alpha=10),
            config(splits=SPLITS, default={"test": 3, "validation": 2}),
        )
        with pytest.raises(MonotonicityError) as exc_info:
            allocate(
                metadata(alpha=10),
                config(splits=["test"], default={"test": 3}, version=2),
                previous=list(first.rows),
            )
        assert "validation" in str(exc_info.value)

    def test_the_superset_guard_rejects_a_lost_document(self):
        published = [
            ManifestRow(id="alpha-000", stratum="alpha", rank=0, split="test"),
            ManifestRow(id="alpha-001", stratum="alpha", rank=1, split="test"),
        ]
        with pytest.raises(MonotonicityError) as exc_info:
            _check_superset_of_previous(published, published[:1])
        assert "alpha-001" in str(exc_info.value)

    def test_the_superset_guard_rejects_a_document_moved_between_splits(self):
        published = [ManifestRow(id="alpha-000", stratum="alpha", rank=0, split="test")]
        moved = [
            ManifestRow(id="alpha-000", stratum="alpha", rank=0, split="validation")
        ]
        with pytest.raises(MonotonicityError):
            _check_superset_of_previous(published, moved)


class TestAvailabilityCapping:
    def test_a_stratum_that_runs_out_contributes_all_it_has(self):
        result = allocate(
            metadata(alpha=4),
            config(splits=SPLITS, default={"test": 3, "validation": 2}),
        )
        assert ranks_in(list(result.rows), "alpha", "test") == [0, 1, 2]
        assert ranks_in(list(result.rows), "alpha", "validation") == [3]

    def test_the_shortfall_is_reported(self):
        result = allocate(
            metadata(alpha=4),
            config(splits=SPLITS, default={"test": 3, "validation": 2}),
        )
        assert [
            (s.stratum, s.split, s.requested, s.allocated) for s in result.shortfalls
        ] == [("alpha", "validation", 2, 1)]

    def test_the_serve_order_decides_which_split_goes_short(self):
        reversed_order = allocate(
            metadata(alpha=4),
            config(splits=["validation", "test"], default={"test": 3, "validation": 2}),
        )
        assert ranks_in(list(reversed_order.rows), "alpha", "validation") == [0, 1]
        assert ranks_in(list(reversed_order.rows), "alpha", "test") == [2, 3]
        assert [s.split for s in reversed_order.shortfalls] == ["test"]

    def test_a_scarce_stratum_can_be_assigned_wholesale_by_override(self):
        result = allocate(
            metadata(alpha=10, gamma=4),
            config(
                splits=SPLITS,
                default={"test": 3, "validation": 2},
                overrides={"gamma": {"test": 4, "validation": 0}},
            ),
        )
        assert ranks_in(list(result.rows), "gamma", "test") == [0, 1, 2, 3]
        assert result.shortfalls == ()

    def test_capping_leaves_room_for_a_later_version_to_fill_the_gap(self):
        first = allocate(
            metadata(alpha=4),
            config(splits=SPLITS, default={"test": 3, "validation": 2}),
        )
        second = allocate(
            metadata(alpha=10),
            config(splits=SPLITS, default={"test": 3, "validation": 2}, version=2),
            previous=list(first.rows),
        )
        assert ranks_in(list(second.rows), "alpha", "validation") == [3, 4]
        assert second.shortfalls == ()


class TestExclusions:
    def test_an_excluded_document_is_not_selected(self):
        result = allocate(
            metadata(alpha=10),
            config(
                splits=SPLITS,
                default={"test": 3, "validation": 0},
                exclude=[paper_id("alpha", 1)],
            ),
        )
        assert ranks_in(list(result.rows), "alpha", "test") == [0, 2, 3]

    def test_exclusions_are_reproducible_across_reruns(self):
        excluding = config(
            splits=SPLITS,
            default={"test": 3, "validation": 2},
            exclude=[paper_id("alpha", 0), paper_id("alpha", 4)],
        )
        first = allocate(metadata(alpha=10), excluding)
        second = allocate(metadata(alpha=10), excluding)
        assert first.rows == second.rows

    def test_excluding_an_already_published_document_is_rejected(self):
        first = allocate(
            metadata(alpha=10),
            config(splits=SPLITS, default={"test": 3, "validation": 0}),
        )
        with pytest.raises(AllocationError) as exc_info:
            allocate(
                metadata(alpha=10),
                config(
                    splits=SPLITS,
                    default={"test": 3, "validation": 0},
                    exclude=[paper_id("alpha", 1)],
                    version=2,
                ),
                previous=list(first.rows),
            )
        assert paper_id("alpha", 1) in str(exc_info.value)

    def test_a_stratum_excluded_down_to_nothing_still_reports_its_shortfall(self):
        result = allocate(
            metadata(alpha=10, gamma=2),
            config(
                splits=SPLITS,
                default={"test": 2, "validation": 0},
                exclude=[paper_id("gamma", 0), paper_id("gamma", 1)],
            ),
        )
        assert [(s.stratum, s.requested, s.allocated) for s in result.shortfalls] == [
            ("gamma", 2, 0)
        ]

    def test_excluding_an_unknown_id_is_rejected(self):
        with pytest.raises(AllocationError) as exc_info:
            allocate(
                metadata(alpha=5),
                config(
                    splits=SPLITS,
                    default={"test": 1, "validation": 0},
                    exclude=["not-an-id"],
                ),
            )
        assert "not-an-id" in str(exc_info.value)


class TestArchiveChangeDetection:
    def test_a_published_document_missing_from_the_archive_is_rejected(self):
        published = [
            ManifestRow(id=paper_id("alpha", 0), stratum="alpha", rank=0, split="test")
        ]
        with pytest.raises(ArchiveChangedError) as exc_info:
            allocate(
                metadata(beta=5),
                config(splits=SPLITS, default={"test": 1, "validation": 0}, version=2),
                previous=published,
            )
        assert paper_id("alpha", 0) in str(exc_info.value)

    def test_a_changed_rank_is_rejected(self):
        published = [
            ManifestRow(id=paper_id("alpha", 0), stratum="alpha", rank=7, split="test")
        ]
        with pytest.raises(ArchiveChangedError) as exc_info:
            allocate(
                metadata(alpha=5),
                config(splits=SPLITS, default={"test": 1, "validation": 0}, version=2),
                previous=published,
            )
        assert "rank" in str(exc_info.value)

    def test_a_changed_stratum_is_rejected(self):
        published = [
            ManifestRow(id=paper_id("alpha", 0), stratum="beta", rank=0, split="test")
        ]
        with pytest.raises(ArchiveChangedError) as exc_info:
            allocate(
                metadata(alpha=5),
                config(splits=SPLITS, default={"test": 1, "validation": 0}, version=2),
                previous=published,
            )
        assert "stratum" in str(exc_info.value)


class TestInvalidInput:
    def test_duplicate_metadata_id_is_rejected(self):
        rows = metadata(alpha=2)
        with pytest.raises(AllocationError) as exc_info:
            allocate(
                [*rows, rows[0]],
                config(splits=SPLITS, default={"test": 1, "validation": 0}),
            )
        assert "duplicate id" in str(exc_info.value)

    def test_duplicate_rank_within_a_stratum_is_rejected(self):
        rows = metadata(alpha=2)
        clashing = MetadataRow(id="alpha-extra", stratum="alpha", rank=0)
        with pytest.raises(AllocationError) as exc_info:
            allocate(
                [*rows, clashing],
                config(splits=SPLITS, default={"test": 1, "validation": 0}),
            )
        assert "duplicate rank" in str(exc_info.value)

    def test_an_override_for_an_unknown_stratum_is_rejected(self):
        with pytest.raises(AllocationError) as exc_info:
            allocate(
                metadata(alpha=5),
                config(
                    splits=SPLITS,
                    default={"test": 1, "validation": 0},
                    overrides={"delta": {"test": 2}},
                ),
            )
        assert "delta" in str(exc_info.value)
