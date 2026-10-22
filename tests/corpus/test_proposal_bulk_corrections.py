"""Tests for corpus.proposal — one pull request for a repo's batch of corrections."""

import pytest

from sciencebeam_dataset_builder.corpus import layout, proposal
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.proposal import (
    ProposalError,
    corrections_description,
    submit_corrections,
)
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from sciencebeam_dataset_builder.corpus.repo_reader import HubRepo
from sciencebeam_dataset_builder.corpus.validate import (
    ContentCorrectionCheck,
    check_content_corrections,
)
from tests.corpus._fake_hub import FakeHub, FakePullRequest
from tests.corpus._repos import jats, publish, root_of, write_file

BIORXIV = CORPORA["biorxiv"]
REASON = "author given-name and surname were swapped"
IDS = ("a", "b", "c")


def _old(id_value: str) -> str:
    return jats(doi=f"10.1/{id_value}", title=f"Old {id_value}")


def _new(id_value: str, title: str = "New") -> str:
    return jats(doi=f"10.1/{id_value}", title=f"{title} {id_value}")


@pytest.fixture
def local(tmp_path):
    return publish(
        tmp_path,
        BIORXIV,
        Tier.OPEN,
        {"train": list(IDS)},
        xml={i: _old(i) for i in IDS},
    )


@pytest.fixture
def hub(local, monkeypatch):
    hub = FakeHub({local.repo_id: root_of(local)})
    hub.install(monkeypatch)
    return hub


@pytest.fixture
def repo(local, hub):
    return Repo(BIORXIV, Tier.OPEN, HubRepo(local.repo_id))


def _checked(repo: Repo, *items: tuple[str, str]) -> list[ContentCorrectionCheck]:
    checks = check_content_corrections(
        [(i, REASON, xml.encode()) for i, xml in items], [repo], "hazal"
    )
    assert all(c.ok for c in checks), [c.errors for c in checks]
    return checks


def _already_corrected(local: Repo, id_value: str) -> None:
    """Merged before, so the next correction has no original to commit first."""
    paths = layout.correction_paths(id_value)
    write_file(
        local,
        paths.metadata,
        f"id: {id_value}\ncorrections:\n  - local_change_id: 1\n"
        "    change_type: content-corrected\n    change_reason: x\n"
        "    changed_by: someone\n",
    )
    write_file(local, paths.corrected, _new(id_value, "Earlier"))


class TestOnePullRequest:
    def test_first_corrections_are_the_originals_then_every_fix(self, hub, repo):
        submission = submit_corrections(
            _checked(repo, ("a", _new("a")), ("b", _new("b")))
        )
        seed, fix = hub.opened
        assert seed["create_pr"] and not fix["create_pr"]
        assert seed["files"] == {
            layout.correction_paths(i).corrected: _old(i).encode() for i in ("a", "b")
        }
        assert set(fix["files"]) == {
            path
            for i in ("a", "b")
            for path in (
                layout.correction_paths(i).corrected,
                layout.correction_paths(i).metadata,
            )
        }
        assert (
            seed["message"] == "Start 2 corrected.xml files from the XML as harvested"
        )
        title = "Correct 2 documents in sciencebeam-dataset-biorxiv"
        assert submission.title == title
        assert hub.renamed == {100: title}

    def test_only_first_corrections_have_an_original_committed(self, local, hub, repo):
        _already_corrected(local, "b")
        submit_corrections(_checked(repo, ("a", _new("a")), ("b", _new("b"))))
        seed, fix = hub.opened
        assert list(seed["files"]) == [layout.correction_paths("a").corrected]
        assert layout.correction_paths("b").corrected in fix["files"]

    def test_later_corrections_alone_are_one_commit(self, local, hub, repo):
        _already_corrected(local, "a")
        _already_corrected(local, "b")
        submit_corrections(_checked(repo, ("a", _new("a")), ("b", _new("b"))))
        [only] = hub.opened
        assert only["create_pr"] and len(only["files"]) == 4
        assert hub.renamed == {}

    def test_a_batch_of_one_is_a_single_correction(self, hub, repo):
        submission = submit_corrections(_checked(repo, ("a", _new("a"))))
        assert submission.title == "Correct a in sciencebeam-dataset-biorxiv"


class TestRefusals:
    def test_a_failed_check_is_not_proposed(self, repo):
        checks = check_content_corrections(
            [("a", REASON, _new("a").encode()), ("b", REASON, b"<article>")],
            [repo],
            "hazal",
        )
        with pytest.raises(ProposalError, match="passed their checks"):
            submit_corrections(checks)

    def test_two_repos_are_two_pull_requests(self, tmp_path, hub, repo):
        ore = publish(
            tmp_path, CORPORA["ore"], Tier.OPEN, {"train": ["o"]}, xml={"o": _old("o")}
        )
        checks = check_content_corrections(
            [("a", REASON, _new("a").encode()), ("o", REASON, _new("o").encode())],
            [repo, ore],
            "hazal",
        )
        with pytest.raises(ProposalError, match="one repo's corrections"):
            submit_corrections(checks)

    def test_a_document_opened_meanwhile_stops_the_batch(self, hub, repo):
        checks = _checked(repo, ("a", _new("a")), ("b", _new("b")))
        path = layout.correction_paths("b").metadata
        hub.pull_requests = [FakePullRequest(9, "Correct b", {path: b"id: b\n"})]
        with pytest.raises(ProposalError, match="#9"):
            submit_corrections(checks)
        assert hub.opened == []


class TestDescription:
    def test_it_lists_every_document_then_each_diff(self, hub, repo):
        text = corrections_description(
            _checked(repo, ("a", _new("a")), ("b", _new("b")))
        )
        assert f"| `a` | train | train-00000.parquet | 1 | {REASON} |" in text
        assert "2 of them are corrected for the first time" in text
        assert "### `b`" in text
        assert "\n+<article-title>New b</article-title>" in text

    def test_it_stays_under_the_hubs_limit_and_says_what_it_cut(
        self, hub, repo, monkeypatch
    ):
        monkeypatch.setattr(proposal, "MAX_DESCRIPTION", 2_500)
        long = "x" * 3_000
        text = corrections_description(
            _checked(repo, *((i, _new(i, long)) for i in IDS))
        )
        assert len(text) <= 2_500
        assert text.count("### `") == 3
        assert "not shown: see the commit's diff" in text
