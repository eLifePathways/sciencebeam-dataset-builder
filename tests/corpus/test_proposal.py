"""Tests for corpus.proposal — opening checked removals as a Hub pull request."""

import pytest

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.proposal import (
    ProposalError,
    removal_description,
    removal_title,
    submit_removals,
    submitter,
)
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from sciencebeam_dataset_builder.corpus.repo_reader import HubRepo
from sciencebeam_dataset_builder.corpus.validate import check_removals
from tests.corpus._fake_hub import FakeHub, FakePullRequest, removals_diff
from tests.corpus._repos import publish, root_of

BIORXIV = CORPORA["biorxiv"]
REASON = "editorial, not a research article"


@pytest.fixture
def local(tmp_path):
    return publish(tmp_path, BIORXIV, Tier.OPEN, {"train": ["a", "b"]})


@pytest.fixture
def hub(local, monkeypatch):
    hub = FakeHub({local.repo_id: root_of(local)})
    hub.install(monkeypatch)
    return hub


@pytest.fixture
def repo(local, hub):
    return Repo(BIORXIV, Tier.OPEN, HubRepo(local.repo_id))


def _checked(repo, *ids, reason=REASON):
    checks, proposals = check_removals([(i, reason) for i in ids], [repo], "hazal")
    assert all(c.ok for c in checks)
    return checks, proposals[0]


class TestSubmitRemovals:
    def test_one_pull_request_adds_the_checked_file(self, hub, repo):
        checks, proposal = _checked(repo, "a")
        submission = submit_removals(proposal, checks)
        [opened] = hub.opened
        assert opened["files"] == {layout.REMOVALS_PATH: proposal.after.encode()}
        assert opened["create_pr"] is True
        assert opened["parent_commit"] == "c1"
        assert (submission.pr_num, submission.pr_url) == (
            100,
            f"https://huggingface.co/datasets/{repo.repo_id}/discussions/100",
        )
        assert submission.title == "Remove a from sciencebeam-dataset-biorxiv"

    def test_nothing_is_opened_if_main_moved_since_the_check(self, hub, repo):
        hub.commits = ["c1", "c2"]
        checks, proposal = _checked(repo, "a")
        with pytest.raises(ProposalError, match="has moved"):
            submit_removals(proposal, checks)
        assert hub.opened == []

    def test_nothing_is_opened_if_a_removal_pull_request_opened_meanwhile(
        self, hub, repo
    ):
        checks, proposal = _checked(repo, "a")
        hub.pull_requests = [FakePullRequest(9, "Remove z", removals_diff("z"))]
        with pytest.raises(ProposalError, match="#9"):
            submit_removals(proposal, checks)
        assert hub.opened == []

    def test_a_submission_repeated_after_success_is_refused(self, hub, repo):
        """The first pull request is now open, so the second sees it."""
        checks, proposal = _checked(repo, "a")
        submit_removals(proposal, checks)
        hub.pull_requests = [FakePullRequest(100, "Remove a", removals_diff("a"))]
        with pytest.raises(ProposalError, match="#100"):
            submit_removals(proposal, checks)
        assert len(hub.opened) == 1

    def test_a_pull_request_that_reads_back_differently_is_reported(self, hub, repo):
        hub.tamper = True
        checks, proposal = _checked(repo, "a")
        with pytest.raises(ProposalError, match="discussions/100.*review it"):
            submit_removals(proposal, checks)

    def test_a_hub_failure_while_opening_is_reported(self, hub, repo):
        checks, proposal = _checked(repo, "a")
        hub.fail, hub.fail_on = RuntimeError("503"), "create_commit"
        with pytest.raises(ProposalError, match="could not open the pull request: 503"):
            submit_removals(proposal, checks)

    def test_a_repo_not_on_the_hub_cannot_take_a_pull_request(self, local):
        checks, proposals = check_removals([("a", REASON)], [local], "hazal")
        with pytest.raises(ProposalError, match="not read from the Hub"):
            submit_removals(proposals[0], checks)


class TestDescription:
    def test_a_reviewer_sees_what_where_why_and_who(self, hub, repo):
        checks, proposal = _checked(repo, "a", "b")
        text = removal_description(proposal, checks)
        assert "| a | train | train-00000.parquet | 1 | editorial, not a" in text
        assert "| b | train | train-00000.parquet | 1 |" in text
        assert "**Submitted by:** hazal" in text
        assert "release `v1.0.0`, commit `c1`" in text
        assert f"`{layout.REMOVALS_PATH}`" in text
        assert "all 2 passed" in text

    def test_a_reason_cannot_break_the_table(self, hub, repo):
        checks, proposal = _checked(repo, "a", reason="a | b\nc")
        assert "| a \\| b c |" in removal_description(proposal, checks)

    def test_several_documents_are_counted_in_the_title(self, hub, repo):
        _, proposal = _checked(repo, "a", "b")
        assert (
            removal_title(proposal)
            == "Remove 2 documents from sciencebeam-dataset-biorxiv"
        )


class TestSubmitter:
    def test_it_is_the_tokens_user(self, monkeypatch):
        FakeHub({}).install(monkeypatch)
        assert submitter() == "hazal"

    def test_a_token_with_no_user_is_refused(self, monkeypatch):
        FakeHub({}, user=None).install(monkeypatch)
        with pytest.raises(ProposalError, match="names no user"):
            submitter()

    def test_a_failed_lookup_is_refused(self, monkeypatch):
        FakeHub({}, fail=RuntimeError("401")).install(monkeypatch)
        with pytest.raises(ProposalError, match="401"):
            submitter()
