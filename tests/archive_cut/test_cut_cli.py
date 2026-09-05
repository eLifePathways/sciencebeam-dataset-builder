"""Tests for archive_cut.cut_cli — cutting a version end to end from a local archive."""

import csv

import pytest

from sciencebeam_dataset_builder.archive_cut.config import load_config
from sciencebeam_dataset_builder.archive_cut.cut_cli import main, parse_args
from sciencebeam_dataset_builder.archive_cut.layout import version_name

from tests.archive_cut._helpers import (
    added_table,
    archive_config,
    document_bytes,
    paper_id,
    stage_files,
    write_archive,
    write_config,
)

SPLITS = ["test", "validation"]


def _cut(tmp_path, archive_dir, cfg, *extra, output_name="v1"):
    config_path = write_config(tmp_path / f"config-{output_name}.yml", cfg)
    output_dir = tmp_path / output_name
    main([str(config_path), str(output_dir), "--source-dir", str(archive_dir), *extra])
    return output_dir


def _manifest_rows(output_dir, version=1):
    path = output_dir / f"sample-v{version:03d}.csv"
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


class TestVersionName:
    def test_names_the_corpus_and_zero_pads_the_version(self):
        counts = {"test": 1, "validation": 0}
        named = archive_config(splits=SPLITS, default=counts, name="other-corpus")
        assert version_name(named) == "other-corpus-v001"
        assert (
            version_name(archive_config(splits=SPLITS, default=counts, version=12))
            == "sample-v012"
        )
        assert (
            version_name(archive_config(splits=SPLITS, default=counts, version=120))
            == "sample-v120"
        )


class TestParseArgs:
    def test_requires_a_config_and_an_output_directory(self):
        with pytest.raises(SystemExit):
            parse_args([])

    def test_source_dir_and_source_repo_are_mutually_exclusive(self, tmp_path):
        with pytest.raises(SystemExit):
            parse_args(
                [
                    str(tmp_path / "c.yml"),
                    str(tmp_path / "out"),
                    "--source-dir",
                    str(tmp_path),
                    "--source-repo",
                    "owner/name",
                ]
            )


class TestFirstCut:
    def test_writes_the_manifest_the_config_and_the_documents(self, tmp_path):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8, "beta": 8})
        cfg = archive_config(splits=SPLITS, default={"test": 2, "validation": 1})
        output_dir = _cut(tmp_path, archive, cfg)

        assert (output_dir / "sample-v001.csv").exists()
        assert (output_dir / "sample-v001.yml").exists()
        assert stage_files(output_dir, "added", "test")
        assert stage_files(output_dir, "added", "validation")

    def test_the_manifest_records_every_selected_document(self, tmp_path):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8, "beta": 8})
        cfg = archive_config(splits=SPLITS, default={"test": 2, "validation": 1})
        rows = _manifest_rows(_cut(tmp_path, archive, cfg))
        assert len(rows) == 6
        assert {row["split"] for row in rows} == {"test", "validation"}
        assert {row["stratum"] for row in rows} == {"alpha", "beta"}

    def test_the_documents_carry_their_content(self, tmp_path):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8})
        cfg = archive_config(splits=SPLITS, default={"test": 2, "validation": 0})
        output_dir = _cut(tmp_path, archive, cfg)
        table = added_table(output_dir, "test")
        assert table.column("id").to_pylist() == [
            paper_id("alpha", 0),
            paper_id("alpha", 1),
        ]
        assert table.column("doc").to_pylist() == [
            document_bytes("alpha", 0),
            document_bytes("alpha", 1),
        ]

    def test_a_split_with_a_zero_count_gets_no_file(self, tmp_path):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8})
        cfg = archive_config(splits=SPLITS, default={"test": 2, "validation": 0})
        output_dir = _cut(tmp_path, archive, cfg)
        assert not stage_files(output_dir, "added", "validation")

    def test_a_local_archive_records_no_revision(self, tmp_path):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 4})
        cfg = archive_config(splits=SPLITS, default={"test": 1, "validation": 0})
        output_dir = _cut(tmp_path, archive, cfg)
        assert load_config(output_dir / "sample-v001.yml").source.revision is None


class TestExtending:
    def test_only_the_new_documents_are_written(self, tmp_path):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8})
        first = _cut(
            tmp_path,
            archive,
            archive_config(splits=SPLITS, default={"test": 2, "validation": 1}),
        )
        second = _cut(
            tmp_path,
            archive,
            archive_config(
                splits=SPLITS, default={"test": 4, "validation": 1}, version=2
            ),
            "--previous-manifest",
            str(first / "sample-v001.csv"),
            output_name="v2",
        )
        added = added_table(second, "test")
        # Ranks 0 and 1 are already published; rank 2 belongs to validation, so test
        # grows past it into 3 and 4.
        assert added.column("id").to_pylist() == [
            paper_id("alpha", 3),
            paper_id("alpha", 4),
        ]

    def test_the_manifest_of_the_new_version_contains_the_old_one(self, tmp_path):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8})
        first = _cut(
            tmp_path,
            archive,
            archive_config(splits=SPLITS, default={"test": 2, "validation": 1}),
        )
        second = _cut(
            tmp_path,
            archive,
            archive_config(
                splits=SPLITS, default={"test": 4, "validation": 1}, version=2
            ),
            "--previous-manifest",
            str(first / "sample-v001.csv"),
            output_name="v2",
        )
        published = {(r["id"], r["split"]) for r in _manifest_rows(first)}
        current = {(r["id"], r["split"]) for r in _manifest_rows(second, version=2)}
        assert published <= current

    def test_rerunning_the_same_config_writes_no_documents(self, tmp_path):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8})
        cfg = archive_config(splits=SPLITS, default={"test": 2, "validation": 1})
        first = _cut(tmp_path, archive, cfg)
        again = _cut(
            tmp_path,
            archive,
            cfg,
            "--previous-manifest",
            str(first / "sample-v001.csv"),
            output_name="again",
        )
        assert not (again / "added").exists()
        # The manifest is still written in full: it is the record of the version, not a
        # record of what this run happened to fetch.
        assert len(_manifest_rows(again)) == 3


class TestPlanOnly:
    def test_nothing_is_written(self, tmp_path, capsys):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8, "beta": 8})
        cfg = archive_config(splits=SPLITS, default={"test": 2, "validation": 1})
        output_dir = _cut(tmp_path, archive, cfg, "--plan-only")
        assert not output_dir.exists()
        assert "nothing written" in capsys.readouterr().out

    def test_the_read_cost_is_reported(self, tmp_path, capsys):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 16}, rows_per_shard=4)
        cfg = archive_config(splits=SPLITS, default={"test": 2, "validation": 0})
        _cut(tmp_path, archive, cfg, "--plan-only")
        assert "1 of 4 shard(s)" in capsys.readouterr().out

    def test_a_shortfall_is_reported(self, tmp_path, capsys):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8, "gamma": 2})
        cfg = archive_config(splits=SPLITS, default={"test": 2, "validation": 1})
        _cut(tmp_path, archive, cfg, "--plan-only")
        out = capsys.readouterr().out
        assert "Shortfall" in out
        assert "gamma" in out


class TestFailures:
    def test_a_lowered_count_fails_without_writing_anything(self, tmp_path, capsys):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8})
        first = _cut(
            tmp_path,
            archive,
            archive_config(splits=SPLITS, default={"test": 4, "validation": 1}),
        )
        lowered = archive_config(
            splits=SPLITS, default={"test": 2, "validation": 1}, version=2
        )
        config_path = write_config(tmp_path / "lowered.yml", lowered)
        output_dir = tmp_path / "v2"
        with pytest.raises(SystemExit) as exc_info:
            main(
                [
                    str(config_path),
                    str(output_dir),
                    "--source-dir",
                    str(archive),
                    "--previous-manifest",
                    str(first / "sample-v001.csv"),
                    "--previous-config",
                    str(first / "sample-v001.yml"),
                ]
            )
        assert exc_info.value.code == 1
        assert not output_dir.exists()
        assert "fewer documents" in capsys.readouterr().err

    def test_the_lowered_count_message_names_the_split(self, tmp_path, capsys):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8})
        first = _cut(
            tmp_path,
            archive,
            archive_config(splits=SPLITS, default={"test": 4, "validation": 1}),
        )
        config_path = write_config(
            tmp_path / "lowered.yml",
            archive_config(
                splits=SPLITS, default={"test": 2, "validation": 1}, version=2
            ),
        )
        with pytest.raises(SystemExit):
            main(
                [
                    str(config_path),
                    str(tmp_path / "v2"),
                    "--source-dir",
                    str(archive),
                    "--previous-config",
                    str(first / "sample-v001.yml"),
                ]
            )
        assert "'test'" in capsys.readouterr().err

    def test_a_missing_archive_directory_exits_non_zero(self, tmp_path, capsys):
        cfg = archive_config(splits=SPLITS, default={"test": 1, "validation": 0})
        config_path = write_config(tmp_path / "c.yml", cfg)
        with pytest.raises(SystemExit) as exc_info:
            main(
                [
                    str(config_path),
                    str(tmp_path / "out"),
                    "--source-dir",
                    str(tmp_path / "nope"),
                ]
            )
        assert exc_info.value.code == 1
        assert "SourceError" in capsys.readouterr().err

    def test_no_archive_at_all_exits_non_zero(self, tmp_path, capsys):
        cfg = archive_config(splits=SPLITS, default={"test": 1, "validation": 0})
        config_path = write_config(tmp_path / "c.yml", cfg)
        with pytest.raises(SystemExit):
            main([str(config_path), str(tmp_path / "out")])
        assert "no archive to read" in capsys.readouterr().err
