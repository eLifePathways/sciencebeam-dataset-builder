"""Tests for corpus.proposal — opening checked additions as a Hub pull request."""

import pytest

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.addition_rows import HARVEST_REPO_ID
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.proposal import (
    ProposalError,
    addition_description,
    addition_title,
    submit_additions,
)
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from sciencebeam_dataset_builder.corpus.repo_reader import HubRepo
from sciencebeam_dataset_builder.corpus.validate_additions import check_additions
from tests.corpus._fake_hub import FakeHub, FakePullRequest, additions_change
from tests.corpus._harvest import COMMIT, write_harvest
from tests.corpus._repos import publish, root_of

BIORXIV = CORPORA["biorxiv"]
REASON = "new in the harvest"


@pytest.fixture
def harvest(tmp_path):
    return write_harvest(tmp_path / "harvest")


@pytest.fixture
def local(tmp_path):
    return publish(tmp_path, BIORXIV, Tier.OPEN, {"train": ["a"]})


@pytest.fixture
def hub(local, monkeypatch):
    hub = FakeHub({local.repo_id: root_of(local)})
    hub.install(monkeypatch)
    return hub


@pytest.fixture
def repo(local, hub):
    return Repo(BIORXIV, Tier.OPEN, HubRepo(local.repo_id))


def _checked(repo, harvest, *ids):
    checks, proposals = check_additions(
        BIORXIV,
        HARVEST_REPO_ID,
        COMMIT,
        harvest,
        [(i, REASON) for i in ids],
        [repo],
        "hazal",
    )
    assert all(c.ok for c in checks)
    return checks, proposals[0]


class TestSubmitAdditions:
    def test_one_pull_request_adds_the_checked_file(self, hub, repo, harvest):
        checks, proposal = _checked(repo, harvest, "n1")
        submission = submit_additions(proposal, checks)
        [opened] = hub.opened
        assert opened["files"] == {layout.ADDITIONS_PATH: proposal.after.encode()}
        assert opened["create_pr"] is True
        assert opened["parent_commit"] == "c1"
        assert submission.title == "Add n1 to sciencebeam-dataset-biorxiv"

    def test_nothing_is_opened_if_main_moved_since_the_check(self, hub, repo, harvest):
        hub.commits = ["c1", "c2"]
        checks, proposal = _checked(repo, harvest, "n1")
        with pytest.raises(ProposalError, match="has moved"):
            submit_additions(proposal, checks)
        assert hub.opened == []

    def test_nothing_is_opened_if_an_additions_pull_request_opened_meanwhile(
        self, hub, repo, harvest
    ):
        checks, proposal = _checked(repo, harvest, "n1")
        hub.pull_requests = [FakePullRequest(9, "Add z", additions_change("z"))]
        with pytest.raises(ProposalError, match="#9"):
            submit_additions(proposal, checks)
        assert hub.opened == []

    def test_a_repo_not_on_the_hub_cannot_take_a_pull_request(self, local, harvest):
        checks, proposal = _checked(local, harvest, "n1")
        with pytest.raises(ProposalError, match="not read from the Hub"):
            submit_additions(proposal, checks)


class TestDescription:
    def test_a_reviewer_sees_counts_then_every_document(self, hub, repo, harvest):
        checks, proposal = _checked(repo, harvest, "n1", "n2")
        text = addition_description(proposal, checks)
        assert f"`{HARVEST_REPO_ID}` at commit `{COMMIT[:12]}`" in text
        assert "| train | CC BY 4.0 | 2 |" in text
        assert f"| n1 | train | CC BY 4.0 | 1 | {REASON} |" in text
        assert (
            addition_title(proposal) == "Add 2 documents to sciencebeam-dataset-biorxiv"
        )

    def test_warnings_are_carried_to_the_reviewer(self, tmp_path, harvest):
        repo = publish(tmp_path / "r", BIORXIV, Tier.RESTRICTED, {"train": ["b"]})
        checks, proposal = _checked(repo, harvest, "n3")
        text = addition_description(proposal, checks)
        assert "**Warnings:**" in text
        assert "- `n3`: CC BY-NC 4.0 does not allow training" in text
