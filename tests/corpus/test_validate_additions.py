"""Tests for corpus.validate_additions — checking additions before they are proposed."""

from pathlib import Path

import pytest

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.addition_rows import (
    HARVEST_REPO_ID,
    AdditionRowsError,
)
from sciencebeam_dataset_builder.corpus.change_files import Addition, read_additions
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.ledger import Change, ChangeType, write_ledger
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from sciencebeam_dataset_builder.corpus.repo_reader import HubRepo, LocalRepo
from sciencebeam_dataset_builder.corpus.validate_additions import check_additions
from tests.corpus._fake_hub import (
    FakeHub,
    FakePullRequest,
    additions_change,
    removals_change,
)
from tests.corpus._harvest import BY, BY_NC, COMMIT, write_harvest
from tests.corpus._repos import jats, publish, root_of, write_file

BIORXIV = CORPORA["biorxiv"]
ORE = CORPORA["ore"]
REASON = "new in the harvest"


def _additions_file(id_value: str, local_change_id: int = 1) -> str:
    return (
        "additions:\n"
        f"  - id: {id_value}\n"
        f"    local_change_id: {local_change_id}\n"
        "    change_reason: earlier\n"
        "    changed_by: someone\n"
        f"    from: {HARVEST_REPO_ID}@{COMMIT}\n"
    )


def _removed(repo: Repo, id_value: str) -> None:
    write_ledger(
        root_of(repo) / layout.LEDGER_PATH,
        [
            Change(
                change_id=1,
                local_change_id=1,
                corpus=repo.corpus.name,
                id=id_value,
                split="train",
                file_type_pair="pdf-jats",
                parquet_file_name="train-00000.parquet",
                base_release="v1.0.0",
                change_type=ChangeType.REMOVED,
                change_reason="editorial",
                change_timestamp="2026-10-01T09:14:22Z",
                changed_by="someone",
            )
        ],
    )


@pytest.fixture
def harvest(tmp_path: Path) -> LocalRepo:
    return write_harvest(tmp_path / "harvest")


@pytest.fixture
def open_repo(tmp_path: Path) -> Repo:
    return publish(tmp_path, BIORXIV, Tier.OPEN, {"train": ["a", "n2"]})


@pytest.fixture
def restricted(tmp_path: Path) -> Repo:
    """b's licence has since become publishable; c's has not."""
    return publish(
        tmp_path,
        BIORXIV,
        Tier.RESTRICTED,
        {"train": ["b", "c"]},
        xml={"b": jats(licence=BY), "c": jats(licence=BY_NC)},
    )


@pytest.fixture
def repos(tmp_path: Path, open_repo: Repo, restricted: Repo) -> list[Repo]:
    return [open_repo, restricted, publish(tmp_path, ORE, Tier.OPEN, {"train": ["o0"]})]


def _check(repos, reader, *ids, corpus=BIORXIV, from_repo=HARVEST_REPO_ID):
    return check_additions(
        corpus, from_repo, COMMIT, reader, [(i, REASON) for i in ids], repos, "hazal"
    )


class TestWhereItLands:
    def test_a_publishable_document_is_proposed_for_the_open_repo(
        self, repos, harvest, open_repo
    ):
        [check], [proposal] = _check(repos, harvest, "n1")
        assert check.ok and check.warnings == ()
        assert (check.split, check.licence, check.repo_id) == (
            "train",
            "CC BY 4.0",
            open_repo.repo_id,
        )
        assert check.addition == Addition(
            "n1", 1, REASON, "hazal", HARVEST_REPO_ID, COMMIT
        )
        assert proposal.repo is open_repo and proposal.before is None
        assert read_additions(proposal.after) == [check.addition]

    def test_a_non_commercial_one_goes_restricted_for_evaluation_only(
        self, repos, harvest, restricted
    ):
        [check], _ = _check(repos, harvest, "n3")
        assert check.repo_id == restricted.repo_id
        assert "does not allow training" in check.warnings[0]

    def test_each_repo_gets_its_own_proposal(self, repos, harvest):
        _, proposals = _check(repos, harvest, "n3", "n1")
        assert [[a.id for a in p.additions] for p in proposals] == [["n1"], ["n3"]]

    def test_a_corpus_without_the_repo_its_licence_needs_is_refused(
        self, repos, harvest
    ):
        [check], proposals = _check(repos, harvest, "o2", corpus=ORE)
        assert "ore has no restricted repo" in check.errors[0]
        assert proposals == []

    def test_a_uid_of_the_corpus_is_its_document(self, repos, harvest):
        [check], _ = _check(repos, harvest, "biorxiv__n1")
        assert check.ok and check.id == "n1"

    def test_a_uid_of_another_corpus_is_refused(self, repos, harvest):
        [check], _ = _check(repos, harvest, "ore__o1")
        assert check.errors == ("names the ore corpus, not biorxiv",)


class TestWhatStopsIt:
    def test_a_document_not_in_the_harvest(self, repos, harvest):
        [check], _ = _check(repos, harvest, "n9")
        assert check.errors == (f"not in {HARVEST_REPO_ID}",)

    def test_a_document_already_in_the_repo(self, repos, harvest, open_repo):
        [check], _ = _check(repos, harvest, "n2")
        assert check.errors == (f"already in {open_repo.repo_id} (train)",)

    def test_a_document_in_the_other_repo_has_to_be_moved(
        self, tmp_path, harvest, open_repo
    ):
        """A harvest addition would leave it in both halves of the corpus."""
        holding = publish(tmp_path / "x", BIORXIV, Tier.RESTRICTED, {"train": ["n1"]})
        [check], _ = _check([open_repo, holding], harvest, "n1")
        assert check.errors == (
            f"already in {holding.repo_id}; to move it, add it from there",
        )

    def test_the_same_document_twice(self, repos, harvest):
        checks, [proposal] = _check(repos, harvest, "n1", "biorxiv__n1")
        assert checks[1].errors == ("listed more than once",)
        assert [a.id for a in proposal.additions] == ["n1"]

    def test_a_reason_is_required(self, repos, harvest):
        checks, _ = check_additions(
            BIORXIV, HARVEST_REPO_ID, COMMIT, harvest, [("n1", " ")], repos, "hazal"
        )
        assert checks[0].errors == ("a reason is required",)

    def test_an_addition_already_pending_on_main(self, repos, harvest, open_repo):
        write_file(open_repo, layout.ADDITIONS_PATH, _additions_file("n1"))
        [check], _ = _check(repos, harvest, "n1")
        assert "an addition is already pending (local_change_id 1)" in check.errors

    def test_another_documents_pending_entry_is_kept_byte_for_byte(
        self, repos, harvest, open_repo
    ):
        existing = _additions_file("x")
        write_file(open_repo, layout.ADDITIONS_PATH, existing)
        [check], [proposal] = _check(repos, harvest, "n1")
        assert check.ok
        assert proposal.before == existing
        assert proposal.after.startswith(existing)

    def test_an_unreadable_additions_file(self, repos, harvest, open_repo):
        write_file(open_repo, layout.ADDITIONS_PATH, "additions: [\n")
        [check], _ = _check(repos, harvest, "n1")
        assert "not valid YAML" in check.errors[0]

    def test_a_repo_of_the_corpus_that_cannot_be_read_fails_closed(
        self, repos, harvest, restricted
    ):
        """It might hold the document already."""
        manifest = root_of(restricted) / layout.release_manifest_path("v1.0.0")
        manifest.write_text("not,a,manifest\n", encoding="utf-8")
        [check], _ = _check(repos, harvest, "n1")
        assert restricted.repo_id in check.errors[0]

    def test_a_from_that_is_neither_the_harvest_nor_the_corpus_is_refused(
        self, repos, harvest
    ):
        with pytest.raises(AdditionRowsError, match="neither"):
            _check(repos, harvest, "n1", from_repo="elifepathways/elsewhere")


class TestAddingBack:
    def test_a_removed_document_comes_back_saying_when_and_why_it_went(
        self, repos, harvest, open_repo
    ):
        _removed(open_repo, "n1")
        [check], _ = _check(repos, harvest, "n1")
        assert check.ok
        assert check.warnings == (
            f"removed from {open_repo.repo_id} on 2026-10-01: editorial; adding it "
            f"back undoes that",
        )
        assert check.addition is not None
        assert check.addition.local_change_id == 2


class TestAMove:
    def test_a_document_now_publishable_moves_to_the_open_repo(
        self, repos, restricted, open_repo
    ):
        [check], [proposal] = _check(
            repos, restricted.reader, "b", from_repo=restricted.repo_id
        )
        assert check.ok and proposal.repo is open_repo
        assert check.addition is not None
        assert check.addition.from_repo == restricted.repo_id
        assert (
            "can be applied only once its removal there is released"
            in (check.warnings[0])
        )

    def test_a_document_its_licence_keeps_where_it_is(self, repos, restricted):
        [check], _ = _check(repos, restricted.reader, "c", from_repo=restricted.repo_id)
        assert check.errors == (
            f"its licence keeps it in {restricted.repo_id}, where it already is",
        )


class TestOpenPullRequests:
    @pytest.fixture
    def hub(self, open_repo, monkeypatch):
        hub = FakeHub({open_repo.repo_id: root_of(open_repo)})
        hub.install(monkeypatch)
        return hub

    @pytest.fixture
    def on_hub(self, hub, open_repo, restricted):
        return [Repo(BIORXIV, Tier.OPEN, HubRepo(open_repo.repo_id)), restricted]

    def test_a_document_already_proposed_says_where(self, hub, on_hub, harvest):
        hub.pull_requests = [FakePullRequest(7, "Add n1", additions_change("n1"))]
        [check], proposals = _check(on_hub, harvest, "n1")
        assert (
            "already proposed for addition in open pull request #7" in (check.errors[0])
        )
        assert proposals == []

    def test_another_open_additions_pull_request_blocks_the_repo(
        self, hub, on_hub, harvest
    ):
        hub.pull_requests = [FakePullRequest(7, "Add z", additions_change("z"))]
        [check], _ = _check(on_hub, harvest, "n1")
        assert "has to be merged or closed first" in check.errors[0]

    def test_a_pull_request_that_cannot_be_read_fails_closed(
        self, hub, on_hub, harvest
    ):
        hub.pull_requests = [
            FakePullRequest(9, "?", {layout.ADDITIONS_PATH: b"additions: [\n"})
        ]
        [check], _ = _check(on_hub, harvest, "n1")
        assert "cannot be read" in check.errors[0]

    def test_an_open_removal_pull_request_does_not_block_an_addition(
        self, hub, on_hub, harvest
    ):
        hub.pull_requests = [FakePullRequest(7, "Remove a", removals_change("a"))]
        [check], _ = _check(on_hub, harvest, "n1")
        assert check.ok
