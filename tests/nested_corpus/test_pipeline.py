"""Cut, render and publish chained, including growing a published version.

The invariants this exists to protect are only really demonstrated end to end: that a
later version's split contains the earlier one's, that a document which cannot be
rendered leaves the data and manifest agreeing, and that it is excluded from then on.
"""

import csv
import stat
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from sciencebeam_dataset_builder.nested_corpus.config import (
    config_from_dict,
    config_to_dict,
    load_config,
)
from sciencebeam_dataset_builder.nested_corpus.cut_cli import main as cut_main
from sciencebeam_dataset_builder.nested_corpus.publish_cli import main as publish_main
from sciencebeam_dataset_builder.nested_corpus.render_cli import main as render_main

from tests.nested_corpus._helpers import (
    archive_config,
    paper_id,
    write_archive,
    write_config,
)

FAKE_CONVERTER = Path(__file__).parent / "_fake_converter.py"
SPLITS = ["test", "validation"]


@pytest.fixture
def converter(tmp_path):
    path = tmp_path / "fake-lowriter"
    path.write_text(
        f'#!/bin/sh\nexec "{sys.executable}" "{FAKE_CONVERTER}" "$@"\n',
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return str(path)


def _run_version(tmp_path, archive, repo, cfg, converter, label, previous=None):
    """Cut, render and publish one version into the shared `repo` directory."""
    config_path = write_config(tmp_path / f"{label}.yml", cfg)
    version_dir = tmp_path / label
    args = [str(config_path), str(version_dir), "--source-dir", str(archive)]
    if previous is not None:
        args += ["--previous-manifest", str(previous)]
    cut_main(args)
    render_main([str(version_dir), "--converter", converter])
    publish_main([str(version_dir), "--target-dir", str(repo)])
    return version_dir


def _published_ids(repo, split):
    return pq.read_table(repo / f"{split}.parquet").column("id").to_pylist()


def _manifest(repo, name):
    with (repo / "splits" / f"{name}.csv").open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _bumped(cfg, **counts):
    data = config_to_dict(cfg)
    data["version"] = cfg.version + 1
    data["allocation"]["default"].update(counts)
    return config_from_dict(data)


class TestFirstVersion:
    def test_the_published_split_holds_the_selected_documents_with_pdfs(
        self, tmp_path, converter
    ):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8, "beta": 8})
        repo = tmp_path / "repo"
        cfg = archive_config(splits=SPLITS, default={"test": 2, "validation": 1})
        _run_version(tmp_path, archive, repo, cfg, converter, "v1")

        table = pq.read_table(repo / "test.parquet")
        assert table.num_rows == 4
        assert all(pdf.startswith(b"%PDF-") for pdf in table.column("pdf").to_pylist())
        assert table.column("pdf_converter_version").to_pylist() == (
            ["FakeConverter 1.2.3"] * 4
        )

    def test_the_source_document_is_kept_alongside_the_pdf(self, tmp_path, converter):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 4})
        repo = tmp_path / "repo"
        cfg = archive_config(splits=SPLITS, default={"test": 1, "validation": 0})
        _run_version(tmp_path, archive, repo, cfg, converter, "v1")

        table = pq.read_table(repo / "test.parquet")
        assert table.column("doc").to_pylist() == [b"doc:alpha:0"]
        assert table.column("doc_ext").to_pylist() == ["docx"]

    def test_the_manifest_and_config_are_published_under_splits(
        self, tmp_path, converter
    ):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 4})
        repo = tmp_path / "repo"
        cfg = archive_config(splits=SPLITS, default={"test": 1, "validation": 0})
        _run_version(tmp_path, archive, repo, cfg, converter, "v1")

        assert (repo / "splits" / "sample-v001.csv").exists()
        assert (repo / "splits" / "sample-v001.yml").exists()

    def test_the_data_is_committed_before_the_manifest(self, tmp_path, converter):
        """An interrupted publish must not leave a manifest describing absent data."""
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 4})
        repo = tmp_path / "repo"
        cfg = archive_config(splits=SPLITS, default={"test": 1, "validation": 0})
        version_dir = tmp_path / "v1"
        config_path = write_config(tmp_path / "v1.yml", cfg)
        cut_main([str(config_path), str(version_dir), "--source-dir", str(archive)])
        render_main([str(version_dir), "--converter", converter])

        from sciencebeam_dataset_builder.nested_corpus.upload import LocalPublishTarget

        recorded = LocalPublishTarget(repo)
        from sciencebeam_dataset_builder.nested_corpus import publish_cli

        original = publish_cli.build_target
        publish_cli.build_target = lambda args, config: recorded
        try:
            publish_main([str(version_dir), "--target-dir", str(repo)])
        finally:
            publish_cli.build_target = original

        paths_in_order = [path for paths, _ in recorded.commits for path in paths]
        assert paths_in_order.index("test.parquet") < paths_in_order.index(
            "splits/sample-v001.csv"
        )
        assert recorded.tags == ["sample-v001"]


class TestGrowingAPublishedVersion:
    def test_the_new_split_contains_the_old_one(self, tmp_path, converter):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8, "beta": 8})
        repo = tmp_path / "repo"
        first = archive_config(splits=SPLITS, default={"test": 2, "validation": 1})
        _run_version(tmp_path, archive, repo, first, converter, "v1")
        before = _published_ids(repo, "test")

        second = _bumped(first, test=4)
        _run_version(
            tmp_path,
            archive,
            repo,
            second,
            converter,
            "v2",
            previous=tmp_path / "v1" / "sample-v001.csv",
        )
        after = _published_ids(repo, "test")

        assert set(before) <= set(after)
        assert len(after) == 8

    def test_carried_documents_keep_their_original_pdf_bytes(self, tmp_path, converter):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8})
        repo = tmp_path / "repo"
        first = archive_config(splits=SPLITS, default={"test": 2, "validation": 0})
        _run_version(tmp_path, archive, repo, first, converter, "v1")
        before = dict(
            zip(
                _published_ids(repo, "test"),
                pq.read_table(repo / "test.parquet").column("pdf").to_pylist(),
                strict=True,
            )
        )

        second = _bumped(first, test=4)
        _run_version(
            tmp_path,
            archive,
            repo,
            second,
            converter,
            "v2",
            previous=tmp_path / "v1" / "sample-v001.csv",
        )
        after = dict(
            zip(
                _published_ids(repo, "test"),
                pq.read_table(repo / "test.parquet").column("pdf").to_pylist(),
                strict=True,
            )
        )
        for document_id, pdf in before.items():
            assert after[document_id] == pdf

    def test_no_document_moves_between_splits(self, tmp_path, converter):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8})
        repo = tmp_path / "repo"
        first = archive_config(splits=SPLITS, default={"test": 2, "validation": 2})
        _run_version(tmp_path, archive, repo, first, converter, "v1")
        before = {(row["id"], row["split"]) for row in _manifest(repo, "sample-v001")}

        second = _bumped(first, test=4)
        _run_version(
            tmp_path,
            archive,
            repo,
            second,
            converter,
            "v2",
            previous=tmp_path / "v1" / "sample-v001.csv",
        )
        after = {(row["id"], row["split"]) for row in _manifest(repo, "sample-v002")}
        assert before <= after

    def test_both_versions_manifests_remain_published(self, tmp_path, converter):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8})
        repo = tmp_path / "repo"
        first = archive_config(splits=SPLITS, default={"test": 2, "validation": 0})
        _run_version(tmp_path, archive, repo, first, converter, "v1")
        _run_version(
            tmp_path,
            archive,
            repo,
            _bumped(first, test=4),
            converter,
            "v2",
            previous=tmp_path / "v1" / "sample-v001.csv",
        )
        assert sorted(p.name for p in (repo / "splits").glob("*.csv")) == [
            "sample-v001.csv",
            "sample-v002.csv",
        ]


class TestRenderFailureThroughPublication:
    def test_a_failed_document_is_absent_from_data_manifest_and_added_to_exclusions(
        self, tmp_path, converter
    ):
        archive = tmp_path / "archive"
        # The fake converter fails on ids ending -fails, which rank 1 does here.
        write_archive(archive, {"alpha": 4})
        for path in archive.glob("*.parquet"):
            table = pq.read_table(path)
            ids = [
                f"{i}-fails" if i == paper_id("alpha", 1) else i
                for i in table.column("id").to_pylist()
            ]
            pq.write_table(table.set_column(0, "id", [ids]), path)
        metadata = (archive / "metadata.jsonl").read_text(encoding="utf-8")
        (archive / "metadata.jsonl").write_text(
            metadata.replace(
                f'"{paper_id("alpha", 1)}"', f'"{paper_id("alpha", 1)}-fails"'
            ),
            encoding="utf-8",
        )

        repo = tmp_path / "repo"
        cfg = archive_config(splits=SPLITS, default={"test": 3, "validation": 0})
        _run_version(tmp_path, archive, repo, cfg, converter, "v1")

        published = _published_ids(repo, "test")
        assert f"{paper_id('alpha', 1)}-fails" not in published
        assert len(published) == 2

        manifest_ids = {row["id"] for row in _manifest(repo, "sample-v001")}
        assert f"{paper_id('alpha', 1)}-fails" not in manifest_ids
        assert manifest_ids == set(published)

        published_config = load_config(repo / "splits" / "sample-v001.yml")
        assert published_config.exclude == (f"{paper_id('alpha', 1)}-fails",)

    def test_the_published_config_excludes_it_from_a_later_cut(
        self, tmp_path, converter
    ):
        """Requirement 12's point: the same document is not rediscovered every version."""
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8})
        repo = tmp_path / "repo"
        cfg = archive_config(
            splits=SPLITS,
            default={"test": 2, "validation": 0},
            exclude=[paper_id("alpha", 0)],
        )
        _run_version(tmp_path, archive, repo, cfg, converter, "v1")
        assert paper_id("alpha", 0) not in _published_ids(repo, "test")
        assert _published_ids(repo, "test") == [
            paper_id("alpha", 1),
            paper_id("alpha", 2),
        ]
