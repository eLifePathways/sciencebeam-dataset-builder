"""Tests for corpus.ledger — what is in a release, and what happened to a document."""

from dataclasses import replace
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
    write_ledger,
    write_membership,
)

WHEN = datetime(2026, 10, 1, 9, 14, 22, tzinfo=UTC)


def _pending(
    id_value: str,
    change_type: ChangeType = ChangeType.ADDED,
    change_reason: str = "initial release",
    local_change_id: int | None = None,
) -> PendingChange:
    return PendingChange(
        corpus="biorxiv",
        id=id_value,
        split="train",
        file_type_pair="pdf-jats",
        parquet_file_name="train-00000.parquet",
        change_type=change_type,
        change_reason=change_reason,
        changed_by="hazal",
        local_change_id=local_change_id,
    )


def _change(
    change_id: int, id_value: str = "a", base_release: str = "v1.0.0"
) -> Change:
    return Change(
        change_id=change_id,
        local_change_id=None,
        corpus="biorxiv",
        id=id_value,
        split="train",
        file_type_pair="pdf-jats",
        parquet_file_name="train-00000.parquet",
        base_release=base_release,
        change_type=ChangeType.ADDED,
        change_reason="initial release",
        change_timestamp="2026-10-01T09:14:22Z",
        changed_by="hazal",
    )


class TestChangeIds:
    def test_the_first_id_is_one(self):
        assert next_change_id([]) == 1

    def test_a_missing_id_is_never_reissued(self):
        """The id is the stable handle for a change, so a gap stays a gap."""
        assert next_change_id([_change(1), _change(4)]) == 5

    def test_pending_changes_are_stamped_with_the_base_release_and_time(self):
        stamped = assign_change_ids([_pending("a")], "v1.0.0", WHEN)
        assert stamped[0].base_release == "v1.0.0"
        assert stamped[0].change_timestamp == "2026-10-01T09:14:22Z"
        assert stamped[0].change_id == 1

    def test_ids_continue_from_what_is_already_recorded(self):
        stamped = assign_change_ids(
            [_pending("b"), _pending("c")],
            "v1.1.0",
            WHEN,
            existing=[_change(1), _change(2)],
        )
        assert [c.change_id for c in stamped] == [3, 4]

    def test_a_release_that_is_not_semver_is_refused(self):
        """A change cannot be recorded against a release that could not exist."""
        with pytest.raises(LayoutError):
            assign_change_ids([_pending("a")], "latest", WHEN)

    def test_an_empty_base_release_is_allowed(self):
        """The very first release has nothing to be based on."""
        stamped = assign_change_ids([_pending("a")], "", WHEN)
        assert stamped[0].base_release == ""

    def test_local_change_id_passes_through_untouched(self):
        """Carried from the declarative file, not allocated here."""
        stamped = assign_change_ids([_pending("a", local_change_id=2)], "v1.0.0", WHEN)
        assert stamped[0].local_change_id == 2

    def test_local_change_id_is_null_for_an_addition(self):
        """Nothing to be idempotent against: an addition has no declarative file."""
        stamped = assign_change_ids([_pending("a")], "v1.0.0", WHEN)
        assert stamped[0].local_change_id is None


class TestReadingAndWriting:
    def test_a_ledger_round_trips(self, tmp_path):
        path = tmp_path / "document-changes.csv"
        changes = assign_change_ids([_pending("a"), _pending("b")], "v1.0.0", WHEN)
        write_ledger(path, changes)
        assert read_ledger(path) == changes

    def test_a_reason_containing_a_comma_survives_the_file(self, tmp_path):
        """The usual way a hand-edited CSV breaks, which is why this file is generated."""
        path = tmp_path / "document-changes.csv"
        reason = "editorial, not a research article"
        write_ledger(
            path,
            assign_change_ids(
                [_pending("a", ChangeType.REMOVED, reason, local_change_id=1)],
                "v2.0.0",
                WHEN,
            ),
        )
        assert f'"{reason}"' in path.read_text(encoding="utf-8")
        assert read_ledger(path)[0].change_reason == reason

    def test_the_same_id_may_appear_more_than_once(self, tmp_path):
        """A document can be corrected and later removed. Only change_id is unique."""
        path = tmp_path / "document-changes.csv"
        write_ledger(path, [_change(1, "a"), _change(2, "a")])
        assert [c.id for c in read_ledger(path)] == ["a", "a"]

    def test_a_repeated_change_id_is_refused(self, tmp_path):
        path = tmp_path / "document-changes.csv"
        with pytest.raises(LedgerError, match="more than once"):
            write_ledger(path, [_change(1), _change(1, "b")])

    def test_a_repeated_local_change_id_for_the_same_document_is_refused(
        self, tmp_path
    ):
        """(id, local_change_id) is the idempotency key the apply step checks; it must
        be unique among the rows that carry one."""
        path = tmp_path / "document-changes.csv"
        dup = [
            replace(c, local_change_id=1) for c in [_change(1, "a"), _change(2, "a")]
        ]
        with pytest.raises(LedgerError, match="local_change_id"):
            write_ledger(path, dup)

    def test_local_change_id_may_repeat_across_different_documents(self, tmp_path):
        path = tmp_path / "document-changes.csv"
        rows = [
            replace(_change(1, "a"), local_change_id=1),
            replace(_change(2, "b"), local_change_id=1),
        ]
        write_ledger(path, rows)
        assert len(read_ledger(path)) == 2

    def test_an_unknown_change_type_is_refused(self, tmp_path):
        path = tmp_path / "document-changes.csv"
        path.write_text(
            "change_id,local_change_id,corpus,id,split,file_type_pair,"
            "parquet_file_name,base_release,change_type,change_reason,"
            "change_timestamp,changed_by\n"
            "1,,biorxiv,a,train,pdf-jats,train-00000.parquet,v1.0.0,tidied,why,"
            "2026-10-01T09:14:22Z,hazal\n",
            encoding="utf-8",
        )
        with pytest.raises(LedgerError, match="is not one of"):
            read_ledger(path)

    def test_a_missing_column_names_itself(self, tmp_path):
        path = tmp_path / "document-changes.csv"
        path.write_text("change_id,id\n1,a\n", encoding="utf-8")
        with pytest.raises(LedgerError, match="missing column"):
            read_ledger(path)

    def test_appending_creates_the_ledger_if_it_is_absent(self, tmp_path):
        path = tmp_path / "releases" / "document-changes.csv"
        added = append_to_ledger(path, [_pending("a")], "v1.0.0", WHEN)
        assert [c.change_id for c in added] == [1]
        assert len(read_ledger(path)) == 1

    def test_appending_keeps_what_was_there(self, tmp_path):
        path = tmp_path / "document-changes.csv"
        append_to_ledger(path, [_pending("a")], "v1.0.0", WHEN)
        append_to_ledger(path, [_pending("b")], "v1.1.0", WHEN)
        recorded = read_ledger(path)
        assert [c.change_id for c in recorded] == [1, 2]
        assert [c.base_release for c in recorded] == ["v1.0.0", "v1.1.0"]


class TestMembership:
    def test_membership_round_trips(self, tmp_path):
        path = tmp_path / "v1.0.0.csv"
        rows = [
            Membership("biorxiv", "a", "train"),
            Membership("biorxiv", "b", "test"),
        ]
        write_membership(path, rows)
        assert read_membership(path) == rows

    def test_a_document_cannot_be_in_a_release_twice(self, tmp_path):
        path = tmp_path / "v1.0.0.csv"
        path.write_text(
            "corpus,id,split\nbiorxiv,a,train\nbiorxiv,a,test\n",
            encoding="utf-8",
        )
        with pytest.raises(LedgerError, match="more than once"):
            read_membership(path)
