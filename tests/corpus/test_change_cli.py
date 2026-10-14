"""Tests for corpus.change_cli — applying merged changes from the command line."""

from pathlib import Path

import pytest

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.change_cli import main
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from tests.corpus._repos import publish, write_file

REMOVAL = (
    "removals:\n  - id: b\n    local_change_id: 1\n    change_reason: editorial\n"
    "    changed_by: hazal\n"
)


@pytest.fixture
def hub(tmp_path):
    repo = publish(
        tmp_path / "hub", CORPORA["biorxiv"], Tier.OPEN, {"train": ["a", "b"]}
    )
    return tmp_path / "hub", repo


def _main(tmp_path: Path, local_dir: Path, *extra: str) -> None:
    main(
        [
            "--corpus",
            "biorxiv",
            "--tier",
            "open",
            "--local-dir",
            str(local_dir),
            "--output-dir",
            str(tmp_path / "out"),
            *extra,
        ]
    )


def test_nothing_pending_says_so(tmp_path, hub, capsys):
    _main(tmp_path, hub[0])
    assert "nothing pending" in capsys.readouterr().out


def test_without_a_version_it_reports_and_writes_nothing(tmp_path, hub, capsys):
    write_file(hub[1], layout.REMOVALS_PATH, REMOVAL)
    _main(tmp_path, hub[0])
    out = capsys.readouterr().out
    assert "1 removed" in out
    assert "needs a major version, v2.0.0 or later" in out
    assert "--version v2.0.0" in out
    assert not (tmp_path / "out").exists()


def test_with_a_version_it_writes_the_release_locally(tmp_path, hub, capsys):
    write_file(hub[1], layout.REMOVALS_PATH, REMOVAL)
    _main(tmp_path, hub[0], "--version", "v2.0.0")
    out = capsys.readouterr().out
    assert "releases/v2.0.0.csv" in out
    root = tmp_path / "out" / "sciencebeam-dataset-biorxiv" / "v2.0.0"
    assert (root / "releases" / "v2.0.0.csv").exists()


def test_a_version_too_small_for_the_changes_is_refused(tmp_path, hub):
    write_file(hub[1], layout.REMOVALS_PATH, REMOVAL)
    with pytest.raises(SystemExit, match="need a major version"):
        _main(tmp_path, hub[0], "--version", "v1.0.1")


def test_changes_that_cannot_apply_are_refused(tmp_path, hub):
    write_file(hub[1], layout.REMOVALS_PATH, REMOVAL.replace("id: b", "id: z"))
    with pytest.raises(SystemExit, match="not in the release"):
        _main(tmp_path, hub[0])


def test_a_tier_the_corpus_lacks_is_refused(tmp_path):
    with pytest.raises(SystemExit, match="no restricted repo"):
        main(["--corpus", "ore", "--tier", "restricted"])
