"""Tests for corpus.ledger — what is in a release, and what happened to a document."""

from datetime import UTC, datetime

import pytest

from sciencebeam_dataset_builder.corpus.layout import LayoutError
from sciencebeam_dataset_builder.corpus.ledger import (
    Change,
    ChangeType,
    LedgerError,
    Membership,
    PendingChange,
    append_to_ledger,
    assign_change_ids,
    next_change_id,
    read_ledger,
    read_membership,
    render_changelog,
    write_ledger,
    write_membership,
)

WHEN = datetime(2026, 10, 1, 9, 14, 22, tzinfo=UTC)


def _pending(
    uid: str,
    change_type: ChangeType = ChangeType.ADDED,
    reason: str = "initial release",
):
    return PendingChange(
        uid=uid,
        source="biorxiv",
        split="train",
        change_type=change_type,
        reason=reason,
        changed_by="hazal",
    )


def _change(change_id: int, uid: str = "biorxiv__a", release: str = "v1.0.0") -> Change:
    return Change(
        change_id=change_id,
        uid=uid,
        source="biorxiv",
        split="train",
        release=release,
        change_type=ChangeType.ADDED,
        reason="initial release",
        change_timestamp="2026-10-01T09:14:22Z",
        changed_by="hazal",
    )


class TestChangeIds:
    def test_the_first_id_is_one(self):
        assert next_change_id([]) == 1

    def test_a_missing_id_is_never_reissued(self):
        """The id is the stable handle for a change, so a gap stays a gap."""
        assert next_change_id([_change(1), _change(4)]) == 5

    def test_pending_changes_are_stamped_with_the_release_and_time(self):
        stamped = assign_change_ids([_pending("biorxiv__a")], "v1.0.0", WHEN)
        assert stamped[0].release == "v1.0.0"
        assert stamped[0].change_timestamp == "2026-10-01T09:14:22Z"
        assert stamped[0].change_id == 1

    def test_ids_continue_from_what_is_already_recorded(self):
        stamped = assign_change_ids(
            [_pending("biorxiv__b"), _pending("biorxiv__c")],
            "v1.1.0",
            WHEN,
            existing=[_change(1), _change(2)],
        )
        assert [c.change_id for c in stamped] == [3, 4]

    def test_a_release_that_is_not_semver_is_refused(self):
        """A change cannot be recorded against a release that could not exist."""
        with pytest.raises(LayoutError):
            assign_change_ids([_pending("biorxiv__a")], "latest", WHEN)


class TestReadingAndWriting:
    def test_a_ledger_round_trips(self, tmp_path):
        path = tmp_path / "document-changes.csv"
        changes = assign_change_ids(
            [_pending("biorxiv__a"), _pending("biorxiv__b")], "v1.0.0", WHEN
        )
        write_ledger(path, changes)
        assert read_ledger(path) == changes

    def test_a_reason_containing_a_comma_survives_the_file(self, tmp_path):
        """The usual way a hand-edited CSV breaks, which is why this file is generated."""
        path = tmp_path / "document-changes.csv"
        reason = "editorial, not a research article"
        write_ledger(
            path,
            assign_change_ids(
                [_pending("biorxiv__a", ChangeType.REMOVED, reason)], "v2.0.0", WHEN
            ),
        )
        assert f'"{reason}"' in path.read_text(encoding="utf-8")
        assert read_ledger(path)[0].reason == reason

    def test_the_same_uid_may_appear_more_than_once(self, tmp_path):
        """A document can be corrected and later removed. Only the id has to be unique."""
        path = tmp_path / "document-changes.csv"
        write_ledger(path, [_change(1, "biorxiv__a"), _change(2, "biorxiv__a")])
        assert [c.uid for c in read_ledger(path)] == ["biorxiv__a", "biorxiv__a"]

    def test_a_repeated_change_id_is_refused(self, tmp_path):
        path = tmp_path / "document-changes.csv"
        with pytest.raises(LedgerError, match="more than once"):
            write_ledger(path, [_change(1), _change(1, "biorxiv__b")])

    def test_an_unknown_change_type_is_refused(self, tmp_path):
        path = tmp_path / "document-changes.csv"
        path.write_text(
            "change_id,uid,source,split,release,change_type,reason,change_timestamp,changed_by\n"
            "1,biorxiv__a,biorxiv,train,v1.0.0,tidied,why,2026-10-01T09:14:22Z,hazal\n",
            encoding="utf-8",
        )
        with pytest.raises(LedgerError, match="is not one of"):
            read_ledger(path)

    def test_a_missing_column_names_itself(self, tmp_path):
        path = tmp_path / "document-changes.csv"
        path.write_text("change_id,uid\n1,biorxiv__a\n", encoding="utf-8")
        with pytest.raises(LedgerError, match="missing column"):
            read_ledger(path)

    def test_appending_creates_the_ledger_if_it_is_absent(self, tmp_path):
        path = tmp_path / "releases" / "document-changes.csv"
        added = append_to_ledger(path, [_pending("biorxiv__a")], "v1.0.0", WHEN)
        assert [c.change_id for c in added] == [1]
        assert len(read_ledger(path)) == 1

    def test_appending_keeps_what_was_there(self, tmp_path):
        path = tmp_path / "document-changes.csv"
        append_to_ledger(path, [_pending("biorxiv__a")], "v1.0.0", WHEN)
        append_to_ledger(path, [_pending("biorxiv__b")], "v1.1.0", WHEN)
        recorded = read_ledger(path)
        assert [c.change_id for c in recorded] == [1, 2]
        assert [c.release for c in recorded] == ["v1.0.0", "v1.1.0"]


class TestMembership:
    def test_membership_round_trips(self, tmp_path):
        path = tmp_path / "v1.0.0.csv"
        rows = [
            Membership("biorxiv__a", "biorxiv", "train"),
            Membership("biorxiv__b", "biorxiv", "test"),
        ]
        write_membership(path, rows)
        assert read_membership(path) == rows

    def test_a_document_cannot_be_in_a_release_twice(self, tmp_path):
        path = tmp_path / "v1.0.0.csv"
        path.write_text(
            "uid,source,split\nbiorxiv__a,biorxiv,train\nbiorxiv__a,biorxiv,test\n",
            encoding="utf-8",
        )
        with pytest.raises(LedgerError, match="more than once"):
            read_membership(path)


class TestChangelog:
    def test_releases_are_newest_first_and_ordered_numerically(self):
        """v1.10.0 is newer than v1.9.0, which sorting the strings would get wrong."""
        changes = [
            _change(1, release="v1.9.0"),
            _change(2, release="v1.10.0"),
            _change(3, release="v1.0.0"),
        ]
        rendered = render_changelog(changes)
        assert rendered.index("## v1.10.0") < rendered.index("## v1.9.0")
        assert rendered.index("## v1.9.0") < rendered.index("## v1.0.0")

    def test_a_release_is_summarised_by_counts_per_change_type(self):
        changes = assign_change_ids(
            [
                _pending("biorxiv__a"),
                _pending("biorxiv__b"),
                _pending("biorxiv__c", ChangeType.REMOVED, "editorial"),
            ],
            "v2.0.0",
            WHEN,
        )
        assert "added 2, removed 1" in render_changelog(changes)

    def test_it_says_it_is_generated(self):
        assert "Do not edit" in render_changelog([_change(1)])
