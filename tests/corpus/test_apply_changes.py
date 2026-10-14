"""Tests for corpus.apply_changes — turning merged changes into a release."""

import shutil
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.apply_changes import (
    MAJOR,
    PATCH,
    AppliedRelease,
    ApplyError,
    apply,
    check_version,
    next_version,
    read_pending,
)
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.ledger import (
    ChangeType,
    read_ledger,
    read_membership,
)
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from tests.corpus._repos import jats, publish, root_of, write_file

BIORXIV = CORPORA["biorxiv"]
WHEN = datetime(2026, 11, 5, 10, 22, 41, tzinfo=UTC)
STAMP = "2026-11-05T10:22:41Z"
ORIGINAL = jats(title="Old title")
FIXED = jats(title="New title")


def _removals(*entries: tuple[str, int]) -> str:
    return "removals:\n" + "".join(
        f"  - id: {i}\n    local_change_id: {n}\n    change_reason: editorial, not "
        f"research\n    changed_by: hazal\n"
        for i, n in entries
    )


def _corrections(id_value: str, *entries: str) -> str:
    return f"id: {id_value}\ncorrections:\n" + "".join(entries)


def _content(n: int) -> str:
    return (
        f"  - local_change_id: {n}\n    change_type: content-corrected\n"
        f"    change_reason: fix {n}\n    changed_by: hazal\n"
    )


def _metadata(n: int, **fields: str) -> str:
    values = "".join(f"      {k}: '{v}'\n" for k, v in fields.items())
    return (
        f"  - local_change_id: {n}\n    change_type: metadata-corrected\n"
        f"    change_reason: meta {n}\n    changed_by: hazal\n    fields:\n{values}"
    )


@pytest.fixture
def repo(tmp_path):
    return publish(
        tmp_path / "hub",
        BIORXIV,
        Tier.OPEN,
        {"train": ["a", "b"], "test": ["c"]},
        xml={"a": ORIGINAL},
    )


def _shard(root: Path, split: str = "train") -> pa.Table:
    return pq.read_table(root / layout.shard_path("pdf-jats", split, 0))


def _apply(repo: Repo, tmp_path: Path, version: str = "v2.0.0") -> AppliedRelease:
    return apply(repo, read_pending(repo), version, tmp_path / "out", WHEN)


def _publish_release(release: AppliedRelease, repo: Repo) -> None:
    """What the upload does: the written files become the repo's."""
    for path in release.paths:
        target = root_of(repo) / path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(release.root / path, target)


class TestPending:
    def test_a_repo_with_no_changes_has_nothing_pending(self, repo):
        pending = read_pending(repo)
        assert pending.empty
        assert pending.bump == ""
        assert pending.base_release == "v1.0.0"

    def test_changes_the_ledger_already_records_are_not_pending(self, repo, tmp_path):
        write_file(repo, layout.REMOVALS_PATH, _removals(("b", 1)))
        _publish_release(_apply(repo, tmp_path), repo)
        assert read_pending(repo).empty

    def test_a_removal_or_content_correction_needs_a_major_version(self, repo):
        write_file(repo, layout.REMOVALS_PATH, _removals(("b", 1)))
        assert read_pending(repo).bump == MAJOR

    def test_a_metadata_correction_alone_needs_only_a_patch(self, repo):
        paths = layout.correction_paths("a")
        write_file(
            repo, paths.metadata, _corrections("a", _metadata(1, pub_date="2022"))
        )
        assert read_pending(repo).bump == PATCH

    def test_a_document_not_in_the_release_is_refused(self, repo):
        write_file(repo, layout.REMOVALS_PATH, _removals(("z", 1)))
        with pytest.raises(ApplyError, match="not in the release"):
            read_pending(repo)

    def test_removing_and_correcting_one_document_is_refused(self, repo):
        write_file(repo, layout.REMOVALS_PATH, _removals(("a", 2)))
        paths = layout.correction_paths("a")
        write_file(
            repo, paths.metadata, _corrections("a", _metadata(1, pub_date="2022"))
        )
        with pytest.raises(ApplyError, match="both removed and corrected"):
            read_pending(repo)

    def test_a_content_correction_without_its_file_is_refused(self, repo):
        write_file(
            repo, layout.correction_paths("a").metadata, _corrections("a", _content(1))
        )
        with pytest.raises(ApplyError, match="no changes/corrections/a/corrected.xml"):
            read_pending(repo)


class TestVersions:
    def test_the_next_version_for_each_step(self):
        assert next_version("v1.4.2", MAJOR) == "v2.0.0"
        assert next_version("v1.4.2", PATCH) == "v1.4.3"

    @pytest.mark.parametrize("version", ["v1.0.0", "v0.9.0", "v1.1.0"])
    def test_a_major_change_refuses_anything_less(self, version):
        with pytest.raises(ApplyError):
            check_version("v1.0.0", version, MAJOR)

    def test_a_patch_change_takes_any_later_version(self):
        check_version("v1.0.0", "v1.0.1", PATCH)
        check_version("v1.0.0", "v2.0.0", PATCH)

    def test_a_version_already_released_is_refused(self, repo, tmp_path):
        write_file(repo, layout.REMOVALS_PATH, _removals(("b", 1)))
        write_file(repo, layout.release_manifest_path("v2.0.0"), "corpus,id,split\n")
        with pytest.raises(ApplyError):
            _apply(repo, tmp_path, "v2.0.0")


class TestRemoval:
    @pytest.fixture
    def release(self, repo, tmp_path):
        write_file(repo, layout.REMOVALS_PATH, _removals(("b", 1)))
        return _apply(repo, tmp_path)

    def test_the_row_is_gone_and_the_others_are_byte_for_byte(self, repo, release):
        before, after = _shard(root_of(repo)), _shard(release.root)
        assert after.column("id").to_pylist() == ["a"]
        assert after.slice(0, 1).equals(before.slice(0, 1))

    def test_only_the_files_it_changes_are_written(self, release):
        assert release.paths == (
            "pdf-jats/train/train-00000.parquet",
            layout.LEDGER_PATH,
            "releases/v2.0.0.csv",
            layout.CARD_PATH,
        )

    def test_the_ledger_records_it_against_the_release_it_was_made_on(self, release):
        [change] = read_ledger(release.root / layout.LEDGER_PATH)
        assert (change.change_id, change.local_change_id, change.id) == (1, 1, "b")
        assert change.change_type is ChangeType.REMOVED
        assert (change.base_release, change.change_timestamp) == ("v1.0.0", STAMP)
        assert (change.split, change.parquet_file_name) == (
            "train",
            "train-00000.parquet",
        )
        assert change.change_reason == "editorial, not research"
        assert change.changed_by == "hazal"

    def test_the_manifest_is_complete_and_the_card_counts_follow(self, release):
        members = read_membership(release.root / "releases/v2.0.0.csv")
        assert [(m.id, m.split) for m in members] == [("a", "train"), ("c", "test")]
        card = (release.root / layout.CARD_PATH).read_text(encoding="utf-8")
        assert "| `pdf-jats` | 1 | 0 | 1 | 2 |" in card

    def test_a_shard_emptied_by_removals_is_kept_with_no_rows(self, repo, tmp_path):
        """Its number must never be taken by a later shard."""
        write_file(repo, layout.REMOVALS_PATH, _removals(("c", 1)))
        release = _apply(repo, tmp_path)
        assert _shard(release.root, "test").num_rows == 0


class TestCorrection:
    def test_a_first_content_correction_keeps_the_original(self, repo, tmp_path):
        paths = layout.correction_paths("a")
        write_file(repo, paths.metadata, _corrections("a", _content(1)))
        write_file(repo, paths.corrected, FIXED)
        release = _apply(repo, tmp_path)
        before = _shard(root_of(repo)).to_pylist()[0]
        row, untouched = _shard(release.root).to_pylist()
        assert (row["xml"], row["xml_upstream"]) == (FIXED, ORIGINAL)
        assert row["xml_upstream_sha"] == before["xml_upstream_sha"]
        assert row["pdf"] == before["pdf"]
        assert row["row_updated_at"] == STAMP
        assert untouched["row_updated_at"] == "2026-10-01T00:00:00Z"

    def test_a_later_correction_leaves_the_original_alone(self, tmp_path):
        repo = publish(
            tmp_path / "hub",
            BIORXIV,
            Tier.OPEN,
            {"train": ["a"]},
            xml={"a": FIXED},
            upstream={"a": ORIGINAL},
        )
        paths = layout.correction_paths("a")
        write_file(repo, paths.metadata, _corrections("a", _content(1)))
        write_file(repo, paths.corrected, jats(title="Newer title"))
        [row] = _shard(_apply(repo, tmp_path).root).to_pylist()
        assert row["xml_upstream"] == ORIGINAL

    def test_metadata_corrections_apply_in_order(self, repo, tmp_path):
        paths = layout.correction_paths("a")
        write_file(
            repo,
            paths.metadata,
            _corrections(
                "a",
                _metadata(1, pub_date="2021", doi="10.1/x"),
                _metadata(2, pub_date="2022"),
            ),
        )
        release = _apply(repo, tmp_path, "v1.0.1")
        row = _shard(release.root).to_pylist()[0]
        assert (row["pub_date"], row["doi"], row["xml"]) == ("2022", "10.1/x", ORIGINAL)
        ledger = read_ledger(release.root / layout.LEDGER_PATH)
        assert [c.local_change_id for c in ledger] == [1, 2]

    def test_a_corrected_file_the_same_as_the_xml_is_refused(self, repo, tmp_path):
        paths = layout.correction_paths("a")
        write_file(repo, paths.metadata, _corrections("a", _content(1)))
        write_file(repo, paths.corrected, ORIGINAL)
        with pytest.raises(ApplyError, match="same as the XML"):
            _apply(repo, tmp_path)


class TestOutput:
    def test_nothing_pending_writes_nothing(self, repo, tmp_path):
        with pytest.raises(ApplyError, match="nothing is pending"):
            _apply(repo, tmp_path)
        assert not (tmp_path / "out").exists()

    def test_an_earlier_run_is_never_written_over(self, repo, tmp_path):
        write_file(repo, layout.REMOVALS_PATH, _removals(("b", 1)))
        _apply(repo, tmp_path)
        with pytest.raises(ApplyError, match="already exists"):
            _apply(repo, tmp_path)

    def test_a_second_release_numbers_its_changes_after_the_first(self, repo, tmp_path):
        write_file(repo, layout.REMOVALS_PATH, _removals(("b", 1)))
        _publish_release(_apply(repo, tmp_path), repo)
        write_file(repo, layout.REMOVALS_PATH, _removals(("b", 1), ("c", 1)))
        release = _apply(repo, tmp_path, "v3.0.0")
        assert [c.change_id for c in release.changes] == [2]
        assert [c.base_release for c in release.changes] == ["v2.0.0"]
