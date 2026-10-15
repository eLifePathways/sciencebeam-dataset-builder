"""Tests for corpus.additions — checking documents to add, and the build that adds them."""

from pathlib import Path

import pytest

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.additions import (
    AdditionError,
    BuildInputs,
    check_additions,
)
from sciencebeam_dataset_builder.corpus.ledger import Change, ChangeType, write_ledger
from sciencebeam_dataset_builder.corpus.lookup import Repo
from tests.corpus._additions import addition_world
from tests.corpus._repos import root_of


@pytest.fixture
def world(tmp_path: Path) -> tuple[list[Repo], BuildInputs]:
    return addition_world(tmp_path)


@pytest.fixture
def repos(world: tuple[list[Repo], BuildInputs]) -> list[Repo]:
    return world[0]


@pytest.fixture
def inputs(world: tuple[list[Repo], BuildInputs]) -> BuildInputs:
    return world[1]


def _one(queries, repos, inputs):
    [check], plans = check_additions(queries, repos, inputs)
    return check, plans


class TestWhereADocumentLands:
    def test_a_publishable_document_goes_to_the_open_repo(self, repos, inputs):
        check, [plan] = _one(["biorxiv__n1"], repos, inputs)
        assert check.ok and check.warnings == ()
        assert (check.split, check.licence) == ("train", "CC BY 4.0")
        assert check.repo_id == "elifepathways/sciencebeam-dataset-biorxiv"
        assert (plan.base_release, plan.version) == ("v1.0.0", "v1.1.0")
        assert plan.requested == ("n1",)
        assert plan.command == (
            'make corpus-build CORPUS=biorxiv TIER=open RUN_ARGS="--base-release v1.0.0 '
            '--version v1.1.0"'
        )

    def test_the_build_says_what_else_it_would_add(self, repos, inputs):
        """build_cli adds every new document that routes to the repo, not only these."""
        _, [plan] = _one(["biorxiv__n1"], repos, inputs)
        assert plan.also_added == ("n5",)

    def test_a_non_commercial_document_goes_restricted_and_is_eval_only(
        self, repos, inputs
    ):
        check, _ = _one(["biorxiv__n2"], repos, inputs)
        assert check.repo_id == "elifepathways/sciencebeam-dataset-biorxiv-restricted"
        assert "does not allow training" in check.warnings[0]

    def test_a_licence_never_recorded_fails_closed_to_restricted(self, repos, inputs):
        check, _ = _one(["biorxiv__n3"], repos, inputs)
        assert check.ok
        assert check.repo_id.endswith("-restricted")
        assert "never recorded" in check.warnings[0]

    def test_a_corpus_without_the_repo_its_licence_needs_is_refused(
        self, repos, inputs
    ):
        check, plans = _one(["ore__o1"], repos, inputs)
        assert "ore has no restricted repo" in check.errors[0]
        assert plans == []


class TestWhatStopsIt:
    def test_a_document_already_published_is_refused(self, repos, inputs):
        check, _ = _one(["biorxiv__a"], repos, inputs)
        assert "already in elifepathways/sciencebeam-dataset-biorxiv" in check.errors[0]

    def test_a_document_not_in_the_snapshot_cannot_be_built_in(self, repos, inputs):
        check, _ = _one(["biorxiv__n4"], repos, inputs)
        assert "not in the snapshot 2026-01-01" in check.errors[0]

    def test_a_bare_id_names_no_corpus(self, repos, inputs):
        check, _ = _one(["n1"], repos, inputs)
        assert "give it as <source>__<id>" in check.errors[0]

    def test_the_same_document_twice_is_refused_the_second_time(self, repos, inputs):
        checks, [plan] = check_additions(["biorxiv__n1", "biorxiv__n1"], repos, inputs)
        assert checks[0].ok
        assert checks[1].errors == ("listed more than once",)
        assert plan.requested == ("n1",)

    def test_no_snapshot_is_an_error(self, repos, tmp_path):
        missing = BuildInputs(tmp_path / "nothing", tmp_path / "licences.csv")
        with pytest.raises(AdditionError):
            check_additions(["biorxiv__n1"], repos, missing)


def test_adding_back_a_removed_document_says_it_undoes_the_removal(repos, inputs):
    write_ledger(
        root_of(repos[1]) / layout.LEDGER_PATH,
        [
            Change(
                change_id=1,
                local_change_id=1,
                corpus="biorxiv",
                id="n2",
                split="train",
                file_type_pair="pdf-jats",
                parquet_file_name="train-00000.parquet",
                base_release="v1.0.0",
                change_type=ChangeType.REMOVED,
                change_reason="editorial",
                change_timestamp="2026-10-01T09:14:22Z",
                changed_by="hazal",
            )
        ],
    )
    check, _ = _one(["biorxiv__n2"], repos, inputs)
    assert check.ok
    assert any("adding it back undoes that" in w for w in check.warnings)
