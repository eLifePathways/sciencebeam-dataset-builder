"""Tests for corpus.change_files — the declarative files under `changes/`."""

import pytest

from sciencebeam_dataset_builder.corpus.change_files import (
    Addition,
    ChangeFileError,
    Correction,
    Removal,
    append_additions,
    append_correction,
    append_removals,
    next_local_change_id,
    read_additions,
    read_correction,
    read_removals,
)
from sciencebeam_dataset_builder.corpus.ledger import Change, ChangeType

REMOVALS = """\
# reviewed 2026-10
removals:
  - id: a
    local_change_id: 1
    change_reason: editorial, not a research article
    changed_by: hazal
"""

SOURCE = "elifepathways/sciencebeam-v2-benchmarking"
COMMIT = "0b7d4a5c6e8f0a1b2c3d4e5f6a7b8c9d0e9f1c2e"

ADDITIONS = f"""\
additions:
  - id: '10.1101_000000'
    local_change_id: 1
    change_reason: new in the harvest
    changed_by: hazal
    from: {SOURCE}@{COMMIT}
"""

CORRECTION = """\
id: a
corrections:
  - local_change_id: 1
    change_type: content-corrected
    change_reason: surname was "Doe", should be "Smith"
    changed_by: hazal
"""


def _removal(id_value: str = "b", local_change_id: int = 1) -> Removal:
    return Removal(id_value, local_change_id, "duplicate of another record", "hazal")


def _addition(id_value: str = "b", local_change_id: int = 1) -> Addition:
    return Addition(
        id_value, local_change_id, "new in the harvest", "hazal", SOURCE, COMMIT
    )


def _correction(
    local_change_id: int = 2,
    change_type: ChangeType = ChangeType.METADATA_CORRECTED,
    fields: dict[str, str | None] | None = None,
) -> Correction:
    return Correction(
        local_change_id=local_change_id,
        change_type=change_type,
        change_reason="pub_date was the harvest year",
        changed_by="hazal",
        fields={"pub_date": "2022-10-31"} if fields is None else fields,
    )


def _ledger_row(id_value: str, local_change_id: int | None) -> Change:
    return Change(
        change_id=1,
        local_change_id=local_change_id,
        corpus="biorxiv",
        id=id_value,
        split="train",
        file_type_pair="pdf-jats",
        parquet_file_name="train-00000.parquet",
        base_release="v1.0.0",
        change_type=ChangeType.REMOVED,
        change_reason="why",
        change_timestamp="2026-10-01T09:14:22Z",
        changed_by="hazal",
    )


class TestReadRemovals:
    def test_a_file_that_does_not_exist_yet_has_no_removals(self):
        assert read_removals(None) == []

    def test_entries_are_read(self):
        assert read_removals(REMOVALS) == [
            Removal("a", 1, "editorial, not a research article", "hazal")
        ]

    def test_an_empty_list_has_no_removals(self):
        assert read_removals("removals:\n") == []

    def test_an_unquoted_id_that_reads_as_a_number_is_refused(self):
        """YAML 1.1 reads `10.1101_000000` as the float 10.1101."""
        text = REMOVALS.replace("id: a", "id: 10.1101_000000")
        with pytest.raises(ChangeFileError, match="quote it"):
            read_removals(text)

    def test_a_repeated_key_is_refused_rather_than_the_last_one_kept(self):
        text = REMOVALS + "    change_reason: something else\n"
        with pytest.raises(ChangeFileError, match="repeated key"):
            read_removals(text)

    def test_a_missing_key_is_named(self):
        with pytest.raises(ChangeFileError, match="missing changed_by"):
            read_removals(REMOVALS.replace("    changed_by: hazal\n", ""))

    def test_an_unknown_key_is_refused(self):
        with pytest.raises(ChangeFileError, match="unknown key"):
            read_removals(REMOVALS + "    uid: biorxiv__a\n")

    def test_an_unknown_top_level_key_is_refused(self):
        with pytest.raises(ChangeFileError, match="unknown key"):
            read_removals(REMOVALS + "notes: x\n")

    @pytest.mark.parametrize("value", ["0", "'1'", "true", "1.5"])
    def test_local_change_id_must_be_a_whole_number_from_one(self, value):
        text = REMOVALS.replace("local_change_id: 1", f"local_change_id: {value}")
        with pytest.raises(ChangeFileError, match="local_change_id"):
            read_removals(text)

    def test_a_blank_reason_is_refused(self):
        with pytest.raises(ChangeFileError, match="blank"):
            read_removals(REMOVALS.replace("editorial, not a research article", "' '"))

    def test_an_id_that_cannot_name_a_directory_is_refused(self):
        with pytest.raises(ChangeFileError, match="path component"):
            read_removals(REMOVALS.replace("id: a", "id: a/b"))

    def test_the_same_document_and_local_change_id_twice_is_refused(self):
        entry = REMOVALS.split("removals:\n")[1]
        with pytest.raises(ChangeFileError, match="more than once"):
            read_removals(REMOVALS + entry)

    def test_a_document_removed_again_under_a_new_local_change_id_is_allowed(self):
        """Removed, re-added, removed again."""
        entry = REMOVALS.split("removals:\n")[1].replace("change_id: 1", "change_id: 2")
        assert [r.local_change_id for r in read_removals(REMOVALS + entry)] == [1, 2]

    def test_malformed_yaml_is_a_change_file_error(self):
        with pytest.raises(ChangeFileError, match="not valid YAML"):
            read_removals("removals: [\n")

    def test_an_empty_existing_file_is_refused(self):
        with pytest.raises(ChangeFileError, match="mapping"):
            read_removals("")


class TestAppendRemovals:
    def test_the_file_is_created_when_absent(self):
        text = append_removals(None, [_removal()])
        assert read_removals(text) == [_removal()]

    def test_existing_text_is_kept_byte_for_byte(self):
        text = append_removals(REMOVALS, [_removal()])
        assert text.startswith(REMOVALS)
        assert [r.id for r in read_removals(text)] == ["a", "b"]

    def test_an_id_that_looks_like_a_number_is_written_quoted(self):
        text = append_removals(None, [_removal("10.1101_000000")])
        assert read_removals(text)[0].id == "10.1101_000000"

    def test_a_reason_with_quotes_colons_and_accents_survives(self):
        reason = 'surname was "Doe": should be "Smith", ação'
        removal = Removal("b", 1, reason, "hazal")
        assert read_removals(append_removals(REMOVALS, [removal]))[1] == removal

    def test_the_indent_already_in_use_is_followed(self):
        flush = "removals:\n- id: a\n  local_change_id: 1\n  change_reason: x\n  changed_by: hazal\n"
        text = append_removals(flush, [_removal()])
        assert "\n- id: b\n" in text
        assert len(read_removals(text)) == 2

    def test_a_file_without_a_final_newline_is_appended_to(self):
        text = append_removals(REMOVALS.rstrip("\n"), [_removal()])
        assert len(read_removals(text)) == 2

    def test_a_pair_already_present_is_refused(self):
        with pytest.raises(ChangeFileError, match="more than once"):
            append_removals(REMOVALS, [_removal("a", 1)])

    def test_a_flow_style_list_is_refused_rather_than_rewritten(self):
        with pytest.raises(ChangeFileError, match="cannot append"):
            append_removals("removals: []\n", [_removal()])

    def test_nothing_to_append_is_refused(self):
        with pytest.raises(ChangeFileError, match="no removals"):
            append_removals(REMOVALS, [])

    def test_an_invalid_new_entry_is_refused(self):
        with pytest.raises(ChangeFileError, match="blank"):
            append_removals(REMOVALS, [Removal("b", 1, " ", "hazal")])


class TestReadAdditions:
    def test_a_file_that_does_not_exist_yet_has_no_additions(self):
        assert read_additions(None) == []

    def test_entries_are_read_with_where_they_come_from(self):
        [addition] = read_additions(ADDITIONS)
        assert addition == _addition("10.1101_000000")

    def test_a_missing_from_is_named(self):
        text = ADDITIONS.replace(f"    from: {SOURCE}@{COMMIT}\n", "")
        with pytest.raises(ChangeFileError, match="missing from"):
            read_additions(text)

    @pytest.mark.parametrize(
        "value",
        [
            "main",
            f"{SOURCE}@main",
            f"{SOURCE}@v1.0.0",
            f"{SOURCE}@{COMMIT[:7]}",
            f"{SOURCE}@{COMMIT.upper()}",
            f"sciencebeam-v2-benchmarking@{COMMIT}",
            f"{SOURCE}@",
        ],
    )
    def test_from_has_to_name_a_repo_and_a_full_commit(self, value):
        """A branch or a tag can move; a commit cannot."""
        text = ADDITIONS.replace(f"{SOURCE}@{COMMIT}", value)
        with pytest.raises(ChangeFileError, match="full 40-character commit"):
            read_additions(text)

    def test_the_same_document_and_local_change_id_twice_is_refused(self):
        entry = ADDITIONS.split("additions:\n", 1)[1]
        with pytest.raises(ChangeFileError, match="more than once"):
            read_additions(ADDITIONS + entry)

    def test_an_unknown_key_is_refused(self):
        with pytest.raises(ChangeFileError, match="unknown key"):
            read_additions(ADDITIONS + "    split: train\n")


class TestAppendAdditions:
    def test_the_file_is_created_when_absent(self):
        text = append_additions(None, [_addition()])
        assert read_additions(text) == [_addition()]

    def test_existing_text_is_kept_byte_for_byte(self):
        text = append_additions(ADDITIONS, [_addition()])
        assert text.startswith(ADDITIONS)
        assert read_additions(text)[-1] == _addition()

    def test_from_is_written_as_one_value(self):
        assert f"from: {SOURCE}@{COMMIT}\n" in append_additions(None, [_addition()])

    def test_nothing_to_append_is_refused(self):
        with pytest.raises(ChangeFileError, match="no additions"):
            append_additions(ADDITIONS, [])


class TestReadCorrection:
    def test_a_file_that_does_not_exist_yet_has_no_corrections(self):
        assert read_correction(None, "a") == []

    def test_a_content_correction_has_no_fields(self):
        [correction] = read_correction(CORRECTION, "a")
        assert correction.change_type is ChangeType.CONTENT_CORRECTED
        assert correction.fields == {}

    def test_a_metadata_correction_carries_its_fields(self):
        text = append_correction(CORRECTION, "a", _correction(fields={"doi": None}))
        assert read_correction(text, "a")[1].fields == {"doi": None}

    def test_the_file_has_to_name_its_own_document(self):
        with pytest.raises(ChangeFileError, match="names id"):
            read_correction(CORRECTION, "b")

    def test_a_metadata_correction_without_fields_is_refused(self):
        text = CORRECTION.replace("content-corrected", "metadata-corrected")
        with pytest.raises(ChangeFileError, match="needs fields"):
            read_correction(text, "a")

    def test_a_content_correction_with_fields_is_refused(self):
        text = CORRECTION + "    fields:\n      doi: '10.1/x'\n"
        with pytest.raises(ChangeFileError, match="only a metadata correction"):
            read_correction(text, "a")

    def test_licence_is_not_an_editable_field(self):
        """A licence change moves a document between repos."""
        text = (
            CORRECTION.replace("content-corrected", "metadata-corrected")
            + "    fields:\n      licence: CC BY 4.0\n"
        )
        with pytest.raises(ChangeFileError, match="not editable"):
            read_correction(text, "a")

    def test_an_unquoted_date_is_refused(self):
        text = (
            CORRECTION.replace("content-corrected", "metadata-corrected")
            + "    fields:\n      pub_date: 2022-10-31\n"
        )
        with pytest.raises(ChangeFileError, match="quote it"):
            read_correction(text, "a")

    def test_removed_is_not_a_correction_type(self):
        with pytest.raises(ChangeFileError, match="change_type"):
            read_correction(CORRECTION.replace("content-corrected", "removed"), "a")

    def test_a_repeated_local_change_id_is_refused(self):
        entry = CORRECTION.split("corrections:\n")[1]
        with pytest.raises(ChangeFileError, match="more than once"):
            read_correction(CORRECTION + entry, "a")

    def test_an_empty_list_of_corrections_is_refused(self):
        with pytest.raises(ChangeFileError, match="non-empty"):
            read_correction("id: a\ncorrections: []\n", "a")


class TestAppendCorrection:
    def test_the_file_is_created_with_its_id_when_absent(self):
        text = append_correction(None, "10.1101_000000", _correction(1))
        assert text.startswith("id: '10.1101_000000'\ncorrections:\n")
        assert read_correction(text, "10.1101_000000") == [_correction(1)]

    def test_existing_text_is_kept_byte_for_byte(self):
        text = append_correction(CORRECTION, "a", _correction())
        assert text.startswith(CORRECTION)
        assert [c.local_change_id for c in read_correction(text, "a")] == [1, 2]

    def test_a_date_is_written_quoted(self):
        text = append_correction(CORRECTION, "a", _correction())
        assert "pub_date: '2022-10-31'" in text

    def test_a_repeated_local_change_id_is_refused(self):
        with pytest.raises(ChangeFileError, match="more than once"):
            append_correction(CORRECTION, "a", _correction(1))

    def test_fields_on_a_content_correction_are_refused(self):
        bad = _correction(change_type=ChangeType.CONTENT_CORRECTED)
        with pytest.raises(ChangeFileError, match="only a metadata correction"):
            append_correction(CORRECTION, "a", bad)


class TestNextLocalChangeId:
    def test_a_document_never_changed_starts_at_one(self):
        assert next_local_change_id("a", [], [], [], []) == 1

    def test_every_change_type_and_the_ledger_share_one_sequence(self):
        """The ledger refuses a repeated (id, local_change_id) across change types."""
        corrections = read_correction(CORRECTION, "a")
        removals = [Removal("a", 3, "why", "hazal")]
        ledger = [_ledger_row("a", 4)]
        assert next_local_change_id("a", removals, [], corrections, ledger) == 5
        additions = [_addition("a", 6)]
        assert next_local_change_id("a", removals, additions, corrections, ledger) == 7

    def test_other_documents_do_not_count(self):
        removals = [Removal("b", 7, "why", "hazal")]
        ledger = [_ledger_row("b", 9), _ledger_row("a", None)]
        assert next_local_change_id("a", removals, [_addition("b", 8)], [], ledger) == 1

    def test_a_gap_is_never_filled(self):
        removals = [Removal("a", 1, "why", "hazal"), Removal("a", 4, "why", "hazal")]
        assert next_local_change_id("a", removals, [], [], []) == 5
