import pytest

from sciencebeam_dataset_builder.dataset.removal.removal_list import (
    RemovalListError,
    read_removal_list,
    read_removal_lists,
)


def _write_list(tmp_path, name: str, body: str):
    path = tmp_path / name
    path.write_text(body, encoding="utf-8")
    return path


class TestReadRemovalList:
    def test_reads_uid_and_reason(self, tmp_path):
        path = _write_list(
            tmp_path,
            "part-1.csv",
            "uid,reason\npkp__a,no abstract\npkp__b,editorial content\n",
        )
        removals = read_removal_list(path)
        assert [(r.uid, r.reason) for r in removals] == [
            ("pkp__a", "no abstract"),
            ("pkp__b", "editorial content"),
        ]

    def test_strips_surrounding_whitespace(self, tmp_path):
        path = _write_list(tmp_path, "p.csv", "uid,reason\npkp__a, no abstract \n")
        assert read_removal_list(path)[0].reason == "no abstract"

    def test_skips_blank_lines(self, tmp_path):
        path = _write_list(tmp_path, "p.csv", "uid,reason\npkp__a,no abstract\n\n\n")
        assert len(read_removal_list(path)) == 1

    def test_rejects_a_headerless_file(self, tmp_path):
        path = _write_list(tmp_path, "p.csv", "pkp__a,no abstract\npkp__b,too short\n")
        with pytest.raises(RemovalListError, match="expected header"):
            read_removal_list(path)

    def test_rejects_an_empty_file(self, tmp_path):
        with pytest.raises(RemovalListError, match="file is empty"):
            read_removal_list(_write_list(tmp_path, "p.csv", ""))

    def test_rejects_a_malformed_row(self, tmp_path):
        path = _write_list(tmp_path, "p.csv", "uid,reason\npkp__a,no abstract,extra\n")
        with pytest.raises(RemovalListError, match="expected 2 fields"):
            read_removal_list(path)

    def test_rejects_a_blank_uid(self, tmp_path):
        path = _write_list(tmp_path, "p.csv", "uid,reason\n ,no abstract\n")
        with pytest.raises(RemovalListError, match="blank uid"):
            read_removal_list(path)

    def test_rejects_a_duplicate_uid(self, tmp_path):
        path = _write_list(
            tmp_path, "p.csv", "uid,reason\npkp__a,no abstract\npkp__a,too short\n"
        )
        with pytest.raises(RemovalListError, match="duplicate uid"):
            read_removal_list(path)


class TestReadRemovalLists:
    def test_merges_several_lists(self, tmp_path):
        first = _write_list(tmp_path, "part-1.csv", "uid,reason\npkp__a,no abstract\n")
        second = _write_list(tmp_path, "part-2.csv", "uid,reason\npkp__b,too short\n")
        merged = read_removal_lists([first, second])
        assert set(merged) == {"pkp__a", "pkp__b"}
        assert merged["pkp__b"].source_list == second

    def test_rejects_a_uid_listed_in_two_files(self, tmp_path):
        first = _write_list(tmp_path, "part-1.csv", "uid,reason\npkp__a,no abstract\n")
        second = _write_list(tmp_path, "part-2.csv", "uid,reason\npkp__a,too short\n")
        with pytest.raises(RemovalListError, match="also listed in"):
            read_removal_lists([first, second])
