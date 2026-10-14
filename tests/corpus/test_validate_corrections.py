"""Tests for corpus.validate — checking corrections before they are proposed."""

import pytest

from sciencebeam_dataset_builder.corpus import layout, validate
from sciencebeam_dataset_builder.corpus.change_files import read_correction
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.ledger import ChangeType
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from sciencebeam_dataset_builder.corpus.repo_reader import HubRepo
from sciencebeam_dataset_builder.corpus.validate import (
    ContentCorrectionCheck,
    check_content_correction,
)
from tests.corpus._fake_hub import FakeHub, FakePullRequest, removals_diff
from tests.corpus._repos import jats, publish, root_of, write_file

BIORXIV = CORPORA["biorxiv"]
REASON = 'surname was "Doe", should be "Smith"'
SHIPPED = jats(title="Old title")
FIXED = jats(title="New title")
PATHS = layout.correction_paths("a")


def _correction_yml(
    local_change_id: int = 1, change_type: str = "content-corrected"
) -> str:
    fields = (
        "    fields:\n      pub_date: '2021'\n"
        if change_type == "metadata-corrected"
        else ""
    )
    return (
        "id: a\ncorrections:\n"
        f"  - local_change_id: {local_change_id}\n"
        f"    change_type: {change_type}\n"
        "    change_reason: earlier\n"
        f"    changed_by: someone\n{fields}"
    )


@pytest.fixture
def repo(tmp_path):
    return publish(
        tmp_path,
        BIORXIV,
        Tier.OPEN,
        {"train": ["a", "b"]},
        xml={"a": SHIPPED, "b": jats(doi="10.1/b")},
    )


def _check(
    repo: Repo, xml: str | bytes = FIXED, reason: str = REASON, query: str = "a"
) -> ContentCorrectionCheck:
    data = xml.encode() if isinstance(xml, str) else xml
    return check_content_correction(query, reason, data, [repo], "hazal")


class TestFirstCorrection:
    def test_the_original_is_seeded_before_the_fix(self, repo):
        check = _check(repo)
        assert check.ok
        assert check.warnings == ()
        assert check.current == SHIPPED
        assert check.correction is not None
        assert check.correction.local_change_id == 1
        assert check.correction.change_type is ChangeType.CONTENT_CORRECTED
        proposal = check.proposal
        assert proposal is not None
        assert (proposal.seed, proposal.corrected_before, proposal.corrected_after) == (
            SHIPPED,
            None,
            FIXED,
        )
        assert proposal.metadata_before is None
        assert read_correction(proposal.metadata_after, "a") == [check.correction]

    def test_metadata_corrections_alone_still_mean_a_first_content_correction(
        self, repo
    ):
        write_file(repo, PATHS.metadata, _correction_yml(1, "metadata-corrected"))
        check = _check(repo)
        assert check.ok
        assert check.proposal is not None
        assert check.proposal.seed == SHIPPED
        assert check.correction is not None
        assert check.correction.local_change_id == 2


class TestALaterCorrection:
    @pytest.fixture
    def pending(self, repo):
        """Merged but not yet released: `main` is ahead of the dataset."""
        write_file(repo, PATHS.metadata, _correction_yml(1))
        write_file(repo, PATHS.corrected, FIXED)
        return repo

    def test_it_is_checked_against_and_replaces_the_pending_one(self, pending):
        check = _check(pending, jats(title="Newer title"))
        assert check.ok
        assert check.current == FIXED
        assert check.proposal is not None
        assert check.proposal.corrected_before == FIXED
        assert check.proposal.seed is None
        assert check.correction is not None
        assert check.correction.local_change_id == 2

    def test_the_pending_text_again_is_no_correction(self, pending):
        assert "same as the current version" in _check(pending, FIXED).errors[0]


class TestTheUpload:
    def test_it_has_to_be_well_formed(self, repo):
        assert "not well-formed" in _check(repo, "<article>").errors[0]

    def test_it_has_to_be_utf8(self, repo):
        assert "not UTF-8" in _check(repo, b"\xff\xfe<").errors[0]

    def test_it_has_a_size_limit(self, repo, monkeypatch):
        monkeypatch.setattr(validate, "MAX_XML_BYTES", 10)
        assert "larger than" in _check(repo).errors[0]

    def test_the_same_text_is_no_correction(self, repo):
        assert "same as the current version" in _check(repo, SHIPPED).errors[0]

    def test_another_articles_doi_is_refused(self, repo):
        errors = _check(repo, jats(doi="10.1/b", title="New")).errors
        assert any("not the document's 10.1/a" in e for e in errors)

    def test_a_doi_in_url_form_is_the_same_doi(self, repo):
        assert _check(repo, jats(doi="https://doi.org/10.1/A", title="New")).ok

    def test_no_doi_only_warns(self, repo):
        check = _check(repo, jats(doi="", title="New"))
        assert check.ok
        assert "carries no DOI" in check.warnings[0]

    def test_a_changed_licence_statement_is_refused(self, repo):
        nc = jats(
            title="New", licence="https://creativecommons.org/licenses/by-nc/4.0/"
        )
        errors = _check(repo, nc).errors
        assert any("moves a document between repos" in e for e in errors)

    def test_a_different_root_element_is_refused(self, repo):
        errors = _check(repo, FIXED.replace("article", "book")).errors
        assert any("root element is <book>" in e for e in errors)


class TestWhatElseIsGoingOn:
    def test_a_reason_is_required(self, repo):
        assert _check(repo, reason=" ").errors == ("a reason is required",)

    def test_a_missing_document_says_so(self, repo):
        check = _check(repo, query="z")
        assert check.errors[0].startswith("missing:")
        assert check.proposal is None

    def test_a_pending_removal_is_refused(self, repo):
        write_file(
            repo,
            layout.REMOVALS_PATH,
            "removals:\n  - id: a\n    local_change_id: 1\n    change_reason: x\n"
            "    changed_by: someone\n",
        )
        assert "a removal is already pending" in _check(repo).errors[0]

    def test_a_corrected_file_without_its_metadata_is_refused(self, repo):
        write_file(repo, PATHS.corrected, FIXED)
        assert any("has no" in e for e in _check(repo, jats(title="Newer")).errors)

    def test_a_correction_in_the_dataset_without_its_file_is_refused(self, tmp_path):
        repo = publish(
            tmp_path,
            BIORXIV,
            Tier.OPEN,
            {"train": ["a"]},
            xml={"a": FIXED},
            upstream={"a": SHIPPED},
        )
        errors = _check(repo, jats(title="Newer")).errors
        assert any("corrected.xml is missing" in e for e in errors)

    def test_an_unreadable_change_file_is_refused(self, repo):
        write_file(repo, layout.REMOVALS_PATH, "removals: [\n")
        assert "not valid YAML" in _check(repo).errors[0]

    def test_a_blank_submitter_is_refused_outright(self, repo):
        with pytest.raises(ValueError, match="changed_by"):
            check_content_correction("a", REASON, FIXED.encode(), [repo], " ")


class TestOpenPullRequests:
    @pytest.fixture
    def hub(self, repo, monkeypatch):
        hub = FakeHub({repo.repo_id: root_of(repo)})
        hub.install(monkeypatch)
        return hub

    @pytest.fixture
    def on_hub(self, repo, hub):
        return Repo(BIORXIV, Tier.OPEN, HubRepo(repo.repo_id))

    def test_an_open_correction_of_the_document_blocks_another(self, hub, on_hub):
        diff = f"diff --git a/{PATHS.corrected} b/{PATHS.corrected}\n"
        hub.pull_requests = [FakePullRequest(4, "Correct a", diff)]
        assert "#4" in _check(on_hub).errors[0]

    def test_an_open_removal_of_the_document_blocks_it(self, hub, on_hub):
        hub.pull_requests = [FakePullRequest(5, "Remove a", removals_diff("a"))]
        assert "already proposed for removal" in _check(on_hub).errors[0]

    def test_an_open_removal_of_another_document_does_not(self, hub, on_hub):
        """Only removals share one file; a correction has its own directory."""
        hub.pull_requests = [FakePullRequest(5, "Remove b", removals_diff("b"))]
        assert _check(on_hub).ok
