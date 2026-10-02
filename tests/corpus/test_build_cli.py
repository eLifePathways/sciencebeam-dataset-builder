"""Tests for corpus.build_cli — building a release from the real legacy snapshot shape."""

import hashlib

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sciencebeam_dataset_builder.corpus.build_cli import (
    BuildError,
    build_table,
    existing_repo_files,
    main,
    read_legacy_rows,
    route_rows,
)
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.ledger import read_ledger, read_membership
from sciencebeam_dataset_builder.corpus.parquet_io import read_shard

# The old 23-column canonical schema's shape, trimmed to what build_cli actually reads.
LEGACY_COLUMNS = (
    "source",
    "id",
    "doi",
    "version",
    "pub_date",
    "xml_ftfy_applied",
    "xml_source_url",
    "xml_downloaded_at",
    "pdf_source_url",
    "pdf_downloaded_at",
    "xml",
    "pdf",
)


def _write_legacy_split(path, source: str, ids: list[str]) -> None:
    rows = len(ids)
    table = pa.table(
        {
            "source": [source] * rows,
            "id": ids,
            "doi": [f"10.1/{i}" for i in ids],
            "version": [""] * rows,
            "pub_date": ["2026"] * rows,
            "xml_ftfy_applied": [False] * rows,
            "xml_source_url": [""] * rows,
            "xml_downloaded_at": ["2026-01-01T00:00:00Z"] * rows,
            "pdf_source_url": [""] * rows,
            "pdf_downloaded_at": ["2026-01-01T00:00:00Z"] * rows,
            "xml": [f"<article>{i}</article>" for i in ids],
            "pdf": [f"pdf-{i}".encode() for i in ids],
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


def _snapshot(tmp_path, source: str, by_split: dict[str, list[str]]):
    snapshot_dir = tmp_path / "data" / "2026-01-01"
    for split, ids in by_split.items():
        _write_legacy_split(
            snapshot_dir / f"{source}-jats" / f"{split}-00000-of-00001.parquet",
            source,
            ids,
        )
    return snapshot_dir


def _licence_report(tmp_path, licences: dict[str, str]):
    path = tmp_path / "licence-per-document.csv"
    lines = ["uid,licence"] + [f"{uid},{lic}" for uid, lic in licences.items()]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


class TestReadLegacyRows:
    def test_every_split_is_tagged_with_the_folder_it_came_from(self, tmp_path):
        snapshot_dir = _snapshot(
            tmp_path, "biorxiv", {"train": ["a"], "test": ["b", "c"]}
        )
        rows = read_legacy_rows(snapshot_dir, "biorxiv-jats")
        assert {r["id"]: r["_split"] for r in rows} == {
            "a": "train",
            "b": "test",
            "c": "test",
        }

    def test_a_missing_config_directory_is_refused(self, tmp_path):
        with pytest.raises(BuildError, match="does not exist"):
            read_legacy_rows(tmp_path, "nothing-here-jats")

    def test_a_config_with_no_split_files_is_refused(self, tmp_path):
        (tmp_path / "biorxiv-jats").mkdir()
        with pytest.raises(BuildError, match="No split files"):
            read_legacy_rows(tmp_path, "biorxiv-jats")


class TestRouteRows:
    def test_rows_route_by_the_legacy_source_id_uid(self):
        rows = [
            {"source": "biorxiv", "id": "a", "_split": "train"},
            {"source": "biorxiv", "id": "b", "_split": "test"},
        ]
        licences = {"biorxiv__a": "CC BY 4.0", "biorxiv__b": "CC BY-NC 4.0"}
        kept, splits = route_rows(rows, licences, Tier.OPEN)
        assert [r["id"] for r in kept] == ["a"]
        assert kept[0]["_licence"] == "CC BY 4.0"
        assert splits == {"a": "train"}

    def test_an_unrecorded_licence_routes_restricted_not_open(self):
        rows = [{"source": "biorxiv", "id": "a", "_split": "train"}]
        kept, _ = route_rows(rows, {}, Tier.OPEN)
        assert kept == []


class TestBuildTable:
    def test_columns_map_onto_the_corpus_schema(self):
        rows = [
            {
                "id": "a",
                "doi": "10.1/a",
                "version": "",
                "pub_date": "2026",
                "_licence": "CC BY 4.0",
                "xml_ftfy_applied": False,
                "xml_source_url": "",
                "xml_downloaded_at": "2026-01-01T00:00:00Z",
                "pdf_source_url": "",
                "pdf_downloaded_at": "2026-01-01T00:00:00Z",
                "xml": "<article/>",
                "pdf": b"pdf-bytes",
            }
        ]
        table = build_table(rows, "2026-10-02T00:00:00Z")
        d = table.to_pydict()
        assert d["id"] == ["a"]
        assert d["doi"] == ["10.1/a"]
        assert d["version"] == [None]  # empty string becomes null, not ""
        assert d["licence"] == ["CC BY 4.0"]
        assert d["xml_upstream"] == [None]
        assert d["row_updated_at"] == ["2026-10-02T00:00:00Z"]

    def test_xml_upstream_sha_hashes_xml_directly(self):
        rows = [
            {
                "id": "a",
                "doi": None,
                "version": None,
                "pub_date": None,
                "_licence": "CC BY 4.0",
                "xml_ftfy_applied": False,
                "xml_source_url": None,
                "xml_downloaded_at": None,
                "pdf_source_url": None,
                "pdf_downloaded_at": None,
                "xml": "<article/>",
                "pdf": b"x",
            }
        ]
        table = build_table(rows, "2026-10-02T00:00:00Z")
        expected = hashlib.sha256(b"<article/>").hexdigest()
        assert table.to_pydict()["xml_upstream_sha"] == [expected]


class TestExistingRepoFiles:
    def test_lists_every_file_as_a_relative_path(self, tmp_path):
        (tmp_path / "pdf-jats/train").mkdir(parents=True)
        (tmp_path / "pdf-jats/train/train-00000.parquet").write_bytes(b"x")
        (tmp_path / "README.md").write_text("hi", encoding="utf-8")
        assert sorted(existing_repo_files(tmp_path)) == [
            "README.md",
            "pdf-jats/train/train-00000.parquet",
        ]

    def test_a_directory_that_does_not_exist_yet_is_empty(self, tmp_path):
        assert existing_repo_files(tmp_path / "nothing-here") == []


class TestMainEndToEnd:
    """The scenario this was built for: a second build must not duplicate, rewrite or
    reshuffle what the first one already published."""

    def _run(self, tmp_path, **extra_args):
        data_dir = tmp_path / "data"
        output_dir = tmp_path / "output"
        licence_report = _licence_report(
            tmp_path,
            {
                "biorxiv__a": "CC BY 4.0",
                "biorxiv__b": "CC BY 4.0",
                "biorxiv__c": "CC BY 4.0",
            },
        )
        argv = [
            "--corpus",
            "biorxiv",
            "--tier",
            "open",
            "--data-dir",
            str(data_dir),
            "--licence-report",
            str(licence_report),
            "--output-dir",
            str(output_dir),
        ]
        for key, value in extra_args.items():
            argv += [f"--{key.replace('_', '-')}", str(value)]
        main(argv)
        return output_dir / "sciencebeam-dataset-biorxiv"

    def test_a_second_build_from_scratch_overwrites_not_duplicates(self, tmp_path):
        _snapshot(tmp_path, "biorxiv", {"train": ["a"], "test": ["b", "c"]})
        root = self._run(tmp_path)
        first_bytes = (root / "pdf-jats/test/test-00000.parquet").read_bytes()

        root_again = self._run(tmp_path)
        assert root_again == root
        assert not (root / "pdf-jats/test/test-00001.parquet").exists()
        second_bytes = (root / "pdf-jats/test/test-00000.parquet").read_bytes()
        # Re-derived from the same source each time: same row content, even if the
        # build timestamp column makes the bytes not byte-identical.
        assert read_shard(str(root / "pdf-jats/test/test-00000.parquet")).num_rows == 2
        assert len(first_bytes) > 0 and len(second_bytes) > 0

    def test_an_incremental_build_adds_without_touching_old_shards(self, tmp_path):
        _snapshot(tmp_path, "biorxiv", {"train": ["a"], "test": ["b"]})
        first_root = self._run(tmp_path, version="v1.0.0")
        old_train = first_root / "pdf-jats/train/train-00000.parquet"
        old_mtime = old_train.stat().st_mtime_ns

        # The snapshot grows: "c" is a new test document.
        _snapshot(tmp_path, "biorxiv", {"train": ["a"], "test": ["b", "c"]})
        second_root = self._run(tmp_path, version="v1.1.0", base_release="v1.0.0")

        assert second_root == first_root
        assert old_train.stat().st_mtime_ns == old_mtime, "old shard was rewritten"
        assert not (second_root / "pdf-jats/test/test-00001.parquet").exists() or (
            read_shard(str(second_root / "pdf-jats/test/test-00001.parquet"))
            .column("id")
            .to_pylist()
            == ["c"]
        )

        manifest = read_membership(second_root / "releases/v1.1.0.csv")
        assert {m.id for m in manifest} == {"a", "b", "c"}

        ledger = read_ledger(second_root / "releases/document-changes.csv")
        assert [c.id for c in ledger] == ["c"]
        assert ledger[0].base_release == "v1.0.0"

    def test_a_removal_is_refused_rather_than_silently_dropped(self, tmp_path):
        _snapshot(tmp_path, "biorxiv", {"train": ["a"], "test": ["b"]})
        self._run(tmp_path, version="v1.0.0")

        # "b" disappears from the snapshot entirely.
        _snapshot(tmp_path, "biorxiv", {"train": ["a"], "test": []})
        with pytest.raises(SystemExit, match="no longer route"):
            self._run(tmp_path, version="v1.1.0", base_release="v1.0.0")
