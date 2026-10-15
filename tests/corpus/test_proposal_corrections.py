"""Tests for corpus.proposal — opening a checked content correction as a pull request."""

import pytest

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.change_files import read_correction
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.proposal import (
    ProposalError,
    correction_description,
    submit_correction,
)
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from sciencebeam_dataset_builder.corpus.repo_reader import HubRepo
from sciencebeam_dataset_builder.corpus.validate import (
    ContentCorrectionCheck,
    check_content_correction,
    check_metadata_correction,
)
from tests.corpus._fake_hub import FakeHub, FakePullRequest
from tests.corpus._repos import jats, publish, root_of, write_file

BIORXIV = CORPORA["biorxiv"]
REASON = 'surname was "Doe", should be "Smith"'
SHIPPED = jats(title="Old title")
FIXED = jats(title="New title")
PATHS = layout.correction_paths("a")


@pytest.fixture
def local(tmp_path):
    return publish(tmp_path, BIORXIV, Tier.OPEN, {"train": ["a"]}, xml={"a": SHIPPED})


@pytest.fixture
def hub(local, monkeypatch):
    hub = FakeHub({local.repo_id: root_of(local)})
    hub.install(monkeypatch)
    return hub


@pytest.fixture
def repo(local, hub):
    return Repo(BIORXIV, Tier.OPEN, HubRepo(local.repo_id))


def _checked(repo: Repo, xml: str = FIXED) -> ContentCorrectionCheck:
    check = check_content_correction("a", REASON, xml.encode(), [repo], "hazal")
    assert check.ok, check.errors
    return check


class TestAFirstCorrection:
    def test_it_is_the_original_then_the_fix_on_one_pull_request(self, hub, repo):
        submission = submit_correction(_checked(repo))
        seed, fix = hub.opened
        assert seed["create_pr"] and seed["parent_commit"] == "c1"
        assert seed["files"] == {PATHS.corrected: SHIPPED.encode()}
        assert not fix["create_pr"]
        assert fix["num"] == seed["num"] == submission.pr_num == 100
        assert fix["parent_commit"] == seed["oid"]
        assert fix["files"][PATHS.corrected] == FIXED.encode()
        [entry] = read_correction(fix["files"][PATHS.metadata].decode(), "a")
        assert entry.change_reason == REASON
        assert submission.title == "Correct a in sciencebeam-dataset-biorxiv"

    def test_a_failed_second_commit_says_the_pull_request_holds_the_original_only(
        self, hub, repo
    ):
        hub.fail, hub.fail_on = RuntimeError("503"), "add_commit"
        with pytest.raises(
            ProposalError, match="discussions/100 with the original XML"
        ):
            submit_correction(_checked(repo))

    def test_the_description_says_how_to_read_the_diff(self, hub, repo):
        text = correction_description(_checked(repo))
        assert "second commit's diff is exactly the correction" in text
        assert '> surname was "Doe", should be "Smith"' in text
        assert "**local_change_id:** 1" in text
        assert f"`{PATHS.corrected}`, `{PATHS.metadata}`" in text


class TestALaterCorrection:
    def test_it_is_one_commit_with_both_files(self, local, hub, repo):
        write_file(
            local,
            PATHS.metadata,
            "id: a\ncorrections:\n  - local_change_id: 1\n"
            "    change_type: content-corrected\n    change_reason: x\n"
            "    changed_by: someone\n",
        )
        write_file(local, PATHS.corrected, FIXED)
        check = _checked(repo, jats(title="Newer title"))
        submit_correction(check)
        [only] = hub.opened
        assert set(only["files"]) == {PATHS.corrected, PATHS.metadata}
        assert "diff is against its last proposed correction" in only["description"]


class TestRefusals:
    def test_nothing_is_opened_if_main_moved(self, hub, repo):
        hub.commits = ["c1", "c2"]
        check = _checked(repo)
        with pytest.raises(ProposalError, match="has moved"):
            submit_correction(check)
        assert hub.opened == []

    def test_nothing_is_opened_if_the_document_got_an_open_pull_request(
        self, hub, repo
    ):
        check = _checked(repo)
        hub.pull_requests = [
            FakePullRequest(4, "Correct a", {PATHS.corrected: b"<a/>"})
        ]
        with pytest.raises(ProposalError, match="#4"):
            submit_correction(check)
        assert hub.opened == []

    def test_a_pull_request_that_reads_back_differently_is_reported(self, hub, repo):
        hub.tamper = True
        with pytest.raises(ProposalError, match="review it before merging"):
            submit_correction(_checked(repo))

    def test_a_failed_check_cannot_be_submitted(self, hub, repo):
        check = check_content_correction("a", " ", FIXED.encode(), [repo], "hazal")
        with pytest.raises(ProposalError, match="passed its checks"):
            submit_correction(check)

    def test_a_repo_not_on_the_hub_cannot_take_a_pull_request(self, local):
        check = check_content_correction("a", REASON, FIXED.encode(), [local], "hazal")
        with pytest.raises(ProposalError, match="not read from the Hub"):
            submit_correction(check)


class TestAMetadataCorrection:
    @pytest.fixture
    def check(self, repo):
        check = check_metadata_correction(
            "a", "harvest year", {"pub_date": "2022-10-31", "doi": ""}, [repo], "hazal"
        )
        assert check.ok, check.errors
        return check

    def test_it_is_one_commit_of_correction_yml_alone(self, hub, check):
        submission = submit_correction(check)
        [only] = hub.opened
        assert set(only["files"]) == {PATHS.metadata}
        [entry] = read_correction(only["files"][PATHS.metadata].decode(), "a")
        assert entry.fields == {"pub_date": "2022-10-31", "doi": None}
        assert submission.title == "Correct a metadata in sciencebeam-dataset-biorxiv"

    def test_the_description_shows_each_value_before_and_after(self, hub, check):
        text = correction_description(check)
        assert "**Change type:** metadata-corrected" in text
        assert "| pub_date | `2026` | `2022-10-31` |" in text
        assert "| doi | `10.1/a` | *empty* |" in text
        assert f"**Files changed:** `{PATHS.metadata}`" in text
        assert "keeps the current metadata" in text
