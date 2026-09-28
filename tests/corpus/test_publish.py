"""Tests for corpus.publish — assembling a release on disk."""

from datetime import UTC, datetime

import pyarrow as pa
import pytest

from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.ledger import (
    ChangeType,
    Membership,
    PendingChange,
    read_ledger,
    read_membership,
)
from sciencebeam_dataset_builder.corpus.parquet_io import read_shard
from sciencebeam_dataset_builder.corpus.publish import (
    PublishError,
    prepare_release,
    verify_release,
)
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from sciencebeam_dataset_builder.corpus.schema import CORPUS_SCHEMA

WHEN = datetime(2026, 10, 1, 9, 14, 22, tzinfo=UTC)
BIORXIV = CORPORA["biorxiv"]


def _table(uids: list[str]) -> pa.Table:
    rows = len(uids)
    return pa.table(
        {
            "source": ["biorxiv"] * rows,
            "id": [u.split("__")[1] for u in uids],
            "uid": uids,
            "doi": [None] * rows,
            "version": [None] * rows,
            "pub_date": [None] * rows,
            "xml_ftfy_applied": [None] * rows,
            "xml_upstream_sha": ["0" * 64] * rows,
            "xml_source_url": [None] * rows,
            "xml_downloaded_at": [None] * rows,
            "pdf_source_url": [None] * rows,
            "pdf_downloaded_at": [None] * rows,
            "row_updated_at": ["2026-10-01T00:00:00Z"] * rows,
            "pdf": [b"pdf"] * rows,
            "xml": ["<article/>"] * rows,
        },
        schema=CORPUS_SCHEMA,
    )


def _pending(uid: str) -> PendingChange:
    return PendingChange(
        uid=uid,
        source="biorxiv",
        split="train",
        change_type=ChangeType.ADDED,
        reason="initial release",
        changed_by="hazal",
    )


SPLITS = {"biorxiv__a": "train", "biorxiv__b": "test", "biorxiv__c": "test"}


def _prepare(tmp_path, **kwargs):
    uids = kwargs.pop("uids", list(SPLITS))
    return prepare_release(
        corpus=BIORXIV,
        tier=Tier.OPEN,
        pairing="pdf-jats",
        table=_table(uids),
        splits=SPLITS,
        version=kwargs.pop("version", "v1.0.0"),
        output_dir=tmp_path,
        pending=[_pending(u) for u in uids],
        timestamp=WHEN,
        **kwargs,
    )


class TestAssembling:
    def test_it_writes_a_shard_per_split_that_has_rows(self, tmp_path):
        release = _prepare(tmp_path)
        assert "pdf-jats/train/train-00000.parquet" in release.paths
        assert "pdf-jats/test/test-00000.parquet" in release.paths
        assert not any("validation" in p for p in release.paths)

    def test_rows_are_placed_by_their_carried_label(self, tmp_path):
        release = _prepare(tmp_path)
        train = read_shard(str(release.root / "pdf-jats/train/train-00000.parquet"))
        assert train.column("uid").to_pylist() == ["biorxiv__a"]

    def test_it_writes_the_ledger_the_manifest_the_changelog_and_the_card(
        self, tmp_path
    ):
        release = _prepare(tmp_path)
        for path in (
            "releases/document-changes.csv",
            "releases/v1.0.0.csv",
            "releases/CHANGELOG.md",
            "README.md",
        ):
            assert path in release.paths
            assert (release.root / path).exists()

    def test_the_manifest_is_the_membership_of_this_release(self, tmp_path):
        release = _prepare(tmp_path)
        recorded = read_membership(release.root / "releases/v1.0.0.csv")
        assert {r.uid for r in recorded} == set(SPLITS)
        assert {r.split for r in recorded} == {"train", "test"}

    def test_the_ledger_records_every_change_against_this_release(self, tmp_path):
        release = _prepare(tmp_path)
        recorded = read_ledger(release.root / "releases/document-changes.csv")
        assert [c.change_id for c in recorded] == [1, 2, 3]
        assert {c.release for c in recorded} == {"v1.0.0"}

    def test_the_card_carries_this_release_counts(self, tmp_path):
        release = _prepare(tmp_path)
        card = (release.root / "README.md").read_text(encoding="utf-8")
        assert "| `pdf-jats` | 1 | 0 | 2 | 3 |" in card

    def test_new_shards_continue_from_what_the_repo_already_has(self, tmp_path):
        release = _prepare(
            tmp_path, existing_files=["pdf-jats/train/train-00000.parquet"]
        )
        assert "pdf-jats/train/train-00001.parquet" in release.paths


class TestRefusals:
    def test_a_row_with_no_carried_label_is_refused(self, tmp_path):
        """A label is carried from the source release, never invented at publish time."""
        with pytest.raises(PublishError, match="no split label"):
            _prepare(tmp_path, uids=["biorxiv__a", "biorxiv__unlabelled"])

    def test_a_pairing_the_corpus_does_not_publish_is_refused(self, tmp_path):
        with pytest.raises(PublishError, match="does not publish"):
            prepare_release(
                corpus=BIORXIV,
                tier=Tier.OPEN,
                pairing="docx-jats",
                table=_table(["biorxiv__a"]),
                splits=SPLITS,
                version="v1.0.0",
                output_dir=tmp_path,
                pending=[],
                timestamp=WHEN,
            )


class TestVerification:
    def test_a_release_matching_its_source_passes(self, tmp_path):
        release = _prepare(tmp_path)
        source = [Membership(uid, "biorxiv", split) for uid, split in SPLITS.items()]
        assert verify_release(source, {Tier.OPEN: release}) == []

    def test_a_document_that_did_not_reach_either_half_is_caught(self, tmp_path):
        release = _prepare(tmp_path, uids=["biorxiv__a"])
        source = [Membership(uid, "biorxiv", split) for uid, split in SPLITS.items()]
        violations = verify_release(source, {Tier.OPEN: release})
        assert any("lost" in v.detail for v in violations)
