"""Extending a published corpus without being told where its previous version is.

The published corpus is the record of how every version was made, so a run should find
what it must contain rather than being handed it. These cover the discovery, and the
cases where guessing would silently break nesting.
"""

import stat
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from sciencebeam_dataset_builder.archive_cut.config import (
    config_from_dict,
    config_to_dict,
)
from sciencebeam_dataset_builder.archive_cut.cut_cli import main as cut_main
from sciencebeam_dataset_builder.archive_cut.publish_cli import main as publish_main
from sciencebeam_dataset_builder.archive_cut.render_cli import main as render_main

from tests.archive_cut._helpers import (
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


def _config(*, version=1, test=2, validation=0, name="sample"):
    return archive_config(
        splits=SPLITS,
        default={"test": test, "validation": validation},
        version=version,
        name=name,
    )


def _publish_version(tmp_path, archive, repo, cfg, converter, label, extra=()):
    config_path = write_config(tmp_path / f"{label}.yml", cfg)
    version_dir = tmp_path / label
    cut_main([str(config_path), str(version_dir), "--source-dir", str(archive), *extra])
    render_main([str(version_dir), "--converter", converter])
    publish_main([str(version_dir), "--target-dir", str(repo)])
    return version_dir


class TestDiscoveringThePreviousVersion:
    def test_the_previous_version_is_found_in_the_published_directory(
        self, tmp_path, converter
    ):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8})
        repo = tmp_path / "repo"
        _publish_version(tmp_path, archive, repo, _config(test=2), converter, "v1")

        # No --previous-manifest: it must find v001 in the repo by itself.
        second = _publish_version(
            tmp_path,
            archive,
            repo,
            _config(version=2, test=4),
            converter,
            "v2",
            extra=["--previous-dir", str(repo)],
        )
        published = pq.read_table(repo / "test.parquet").column("id").to_pylist()
        assert published == [paper_id("alpha", i) for i in range(4)]
        assert (second / "added" / "test.parquet").exists()

    def test_only_the_new_documents_are_read(self, tmp_path, converter):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8})
        repo = tmp_path / "repo"
        _publish_version(tmp_path, archive, repo, _config(test=2), converter, "v1")
        second = _publish_version(
            tmp_path,
            archive,
            repo,
            _config(version=2, test=4),
            converter,
            "v2",
            extra=["--previous-dir", str(repo)],
        )
        added = pq.read_table(second / "added" / "test.parquet")
        assert added.column("id").to_pylist() == [
            paper_id("alpha", 2),
            paper_id("alpha", 3),
        ]

    def test_a_specific_earlier_version_can_be_named(self, tmp_path, converter):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8})
        repo = tmp_path / "repo"
        _publish_version(tmp_path, archive, repo, _config(test=2), converter, "v1")
        _publish_version(
            tmp_path,
            archive,
            repo,
            _config(version=2, test=4),
            converter,
            "v2",
            extra=["--previous-dir", str(repo)],
        )
        # v003 extending v001 rather than v002 is legitimate but must be asked for.
        third = tmp_path / "v3"
        cut_main(
            [
                str(write_config(tmp_path / "v3.yml", _config(version=3, test=5))),
                str(third),
                "--source-dir",
                str(archive),
                "--previous-dir",
                str(repo),
                "--previous-version",
                "1",
            ]
        )
        added = pq.read_table(third / "added" / "test.parquet")
        assert len(added) == 3

    def test_an_empty_repo_is_treated_as_a_first_cut(self, tmp_path, converter, capsys):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8})
        cut_main(
            [
                str(write_config(tmp_path / "v1.yml", _config())),
                str(tmp_path / "v1"),
                "--source-dir",
                str(archive),
                "--previous-dir",
                str(tmp_path / "does-not-exist"),
            ]
        )
        assert (tmp_path / "v1" / "sample-v001.csv").exists()

    def test_a_local_manifest_takes_precedence_over_the_repo(self, tmp_path, converter):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8})
        repo = tmp_path / "repo"
        first = _publish_version(
            tmp_path, archive, repo, _config(test=2), converter, "v1"
        )
        second = tmp_path / "v2"
        cut_main(
            [
                str(write_config(tmp_path / "v2.yml", _config(version=2, test=4))),
                str(second),
                "--source-dir",
                str(archive),
                "--previous-manifest",
                str(first / "sample-v001.csv"),
                "--previous-dir",
                str(repo),
            ]
        )
        assert len(pq.read_table(second / "added" / "test.parquet")) == 2


class TestGuardsAgainstSilentlyBreakingNesting:
    def test_a_later_version_with_nowhere_to_look_is_refused(self, tmp_path):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8})
        with pytest.raises(SystemExit) as exc_info:
            cut_main(
                [
                    str(write_config(tmp_path / "v2.yml", _config(version=2, test=4))),
                    str(tmp_path / "v2"),
                    "--source-dir",
                    str(archive),
                ]
            )
        assert exc_info.value.code == 1

    def test_the_refusal_says_how_to_resolve_it(self, tmp_path, capsys):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8})
        with pytest.raises(SystemExit):
            cut_main(
                [
                    str(write_config(tmp_path / "v2.yml", _config(version=2, test=4))),
                    str(tmp_path / "v2"),
                    "--source-dir",
                    str(archive),
                ]
            )
        error = capsys.readouterr().err
        assert "nothing to extend" in error
        assert "--first-version" in error

    def test_version_one_needs_no_previous_version(self, tmp_path):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8})
        cut_main(
            [
                str(write_config(tmp_path / "v1.yml", _config())),
                str(tmp_path / "v1"),
                "--source-dir",
                str(archive),
            ]
        )
        assert (tmp_path / "v1" / "sample-v001.csv").exists()

    def test_first_version_is_refused_when_one_is_already_published(
        self, tmp_path, converter, capsys
    ):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8})
        repo = tmp_path / "repo"
        _publish_version(tmp_path, archive, repo, _config(test=2), converter, "v1")
        with pytest.raises(SystemExit):
            cut_main(
                [
                    str(
                        write_config(tmp_path / "again.yml", _config(version=2, test=4))
                    ),
                    str(tmp_path / "again"),
                    "--source-dir",
                    str(archive),
                    "--previous-dir",
                    str(repo),
                    "--first-version",
                ]
            )
        assert "already published" in capsys.readouterr().err

    def test_a_version_not_above_the_published_one_is_refused(
        self, tmp_path, converter, capsys
    ):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8})
        repo = tmp_path / "repo"
        _publish_version(tmp_path, archive, repo, _config(test=2), converter, "v1")
        # Rerunning version 1 against a repo that already holds version 1: the config's
        # version has to advance, or the published version would be overwritten.
        with pytest.raises(SystemExit):
            cut_main(
                [
                    str(write_config(tmp_path / "v1again.yml", _config(test=4))),
                    str(tmp_path / "v1again"),
                    "--source-dir",
                    str(archive),
                    "--previous-dir",
                    str(repo),
                ]
            )
        assert "must be higher than the published one" in capsys.readouterr().err

    def test_a_lowered_count_is_caught_against_the_published_config(
        self, tmp_path, converter, capsys
    ):
        """The published config is read automatically, so the cheap check still applies."""
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8})
        repo = tmp_path / "repo"
        _publish_version(tmp_path, archive, repo, _config(test=4), converter, "v1")
        lowered = config_from_dict(
            {**config_to_dict(_config(version=2, test=2)), "version": 2}
        )
        with pytest.raises(SystemExit):
            cut_main(
                [
                    str(write_config(tmp_path / "lowered.yml", lowered)),
                    str(tmp_path / "lowered"),
                    "--source-dir",
                    str(archive),
                    "--previous-dir",
                    str(repo),
                ]
            )
        assert "fewer documents" in capsys.readouterr().err

    def test_a_published_version_missing_its_manifest_is_refused(
        self, tmp_path, converter, capsys
    ):
        archive = tmp_path / "archive"
        write_archive(archive, {"alpha": 8})
        repo = tmp_path / "repo"
        _publish_version(tmp_path, archive, repo, _config(test=2), converter, "v1")
        (repo / "splits" / "sample-v001.csv").unlink()
        with pytest.raises(SystemExit):
            cut_main(
                [
                    str(write_config(tmp_path / "v2.yml", _config(version=2, test=4))),
                    str(tmp_path / "v2"),
                    "--source-dir",
                    str(archive),
                    "--previous-dir",
                    str(repo),
                ]
            )
        assert "could not be retrieved" in capsys.readouterr().err
