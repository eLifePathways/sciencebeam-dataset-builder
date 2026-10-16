"""Tests for corpus.change_ui — the add documents pages."""

import re
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from sciencebeam_dataset_builder.corpus.addition_rows import HARVEST_REPO_ID
from sciencebeam_dataset_builder.corpus.change_ui.app import create_app
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from sciencebeam_dataset_builder.corpus.repo_reader import (
    HubRepo,
    LocalRepo,
    RepoReader,
    RepoReadError,
)
from tests.corpus._fake_hub import FakeHub
from tests.corpus._harvest import COMMIT, write_harvest
from tests.corpus._repos import publish, root_of

BIORXIV = CORPORA["biorxiv"]
REASON = "new in the harvest"
DIGEST = re.compile(r'name="digest:([^"]+)" value="([0-9a-f]{64})"')


def _post(client: TestClient, path: str, data: dict[str, Any]) -> Any:
    token = cast(FastAPI, client.app).state.csrf
    return client.post(path, data={**data, "csrf": token})


def _carried(*ids: str, shared: str = REASON) -> dict[str, Any]:
    """The hidden fields the reasons and review pages carry."""
    return {
        "corpus": "biorxiv",
        "from_repo": HARVEST_REPO_ID,
        "from_commit": COMMIT,
        "ids": "\n".join(ids),
        "uid": list(ids),
        "shared": shared,
        **{f"own:{i}": "" for i in ids},
    }


@pytest.fixture
def harvest(tmp_path: Path) -> LocalRepo:
    return write_harvest(tmp_path / "harvest")


@pytest.fixture
def open_repo(tmp_path: Path) -> Repo:
    return publish(tmp_path, BIORXIV, Tier.OPEN, {"train": ["a", "n2"]})


@pytest.fixture
def restricted(tmp_path: Path) -> Repo:
    return publish(tmp_path, BIORXIV, Tier.RESTRICTED, {"train": ["b"]})


@pytest.fixture
def open_from(harvest: LocalRepo) -> Callable[[str, str], tuple[RepoReader, str]]:
    def opened(repo_id: str, revision: str) -> tuple[RepoReader, str]:
        if repo_id != HARVEST_REPO_ID:
            raise RepoReadError(f"{repo_id} is not here")
        return harvest, COMMIT

    return opened


def _client(
    repos: Sequence[Repo],
    open_from: Callable[[str, str], tuple[RepoReader, str]],
    can_submit: bool = False,
) -> TestClient:
    app = create_app(lambda: repos, lambda: "hazal", can_submit, open_from)
    return TestClient(app, base_url="http://127.0.0.1")


@pytest.fixture
def client(open_repo, restricted, open_from):
    return _client([open_repo, restricted], open_from)


class TestDocuments:
    def test_the_first_page_starts_an_addition(self, client):
        assert 'href="/additions"' in client.get("/").text

    def test_the_harvest_at_main_is_the_default(self, client):
        page = client.get("/additions").text
        assert f'<option value="{HARVEST_REPO_ID}" selected>' in page
        assert 'name="revision" value="main"' in page

    def test_named_documents_go_on_to_their_reasons_pinned_to_a_commit(self, client):
        page = _post(
            client,
            "/additions/reasons",
            {"corpus": "biorxiv", "from_repo": HARVEST_REPO_ID, "ids": "n1 n3"},
        ).text
        assert "Why are they being added?" in page
        assert f'name="from_commit" value="{COMMIT}"' in page
        assert 'name="uid" value="n1"' in page and 'name="uid" value="n3"' in page

    def test_every_new_document_can_be_asked_for_at_once(self, client):
        """n2 is published already, so only n1 and n3 are new."""
        page = _post(
            client,
            "/additions/reasons",
            {"corpus": "biorxiv", "from_repo": HARVEST_REPO_ID, "all_new": "yes"},
        ).text
        assert re.findall(r'name="uid" value="([^"]+)"', page) == ["n1", "n3"]

    def test_no_corpus_is_refused_with_the_form_kept(self, client):
        response = _post(
            client, "/additions/reasons", {"from_repo": HARVEST_REPO_ID, "ids": "n1"}
        )
        assert response.status_code == 400
        assert "Choose the corpus" in response.text
        assert ">n1</textarea>" in response.text

    def test_a_from_that_cannot_be_read_is_said(self, client):
        response = _post(
            client,
            "/additions/reasons",
            {"corpus": "biorxiv", "from_repo": "elifepathways/x", "ids": "n1"},
        )
        assert response.status_code == 400
        assert "elifepathways/x is not here" in response.text

    def test_back_from_the_reasons_keeps_the_commit_and_ids(self, client):
        page = _post(client, "/additions/edit", _carried("n1")).text
        assert f'name="revision" value="{COMMIT}"' in page
        assert ">n1</textarea>" in page


class TestReview:
    def test_each_document_says_where_it_goes(self, client):
        page = _post(client, "/additions/check", _carried("n1", "n3")).text
        assert "All 2 documents can be" in page
        assert "sciencebeam-dataset-biorxiv-restricted" in page
        assert "does not allow training" in page
        assert "+ additions:" in page
        assert len(DIGEST.findall(page)) == 2

    def test_a_failing_document_can_be_left_out(self, client):
        page = _post(client, "/additions/check", _carried("n1", "n2")).text
        assert "1 of 2 documents can be proposed" in page
        assert 'name="leave_out" value="n2"' in page
        page = _post(
            client, "/additions/check", {**_carried("n1", "n2"), "leave_out": "n2"}
        ).text
        assert "The document can be" in page

    def test_a_preview_cannot_submit(self, client):
        response = _post(
            client, "/additions/submit", {**_carried("n1"), "confirm": "yes"}
        )
        assert response.status_code == 403
        assert "nothing can be submitted" in response.text


class TestSubmit:
    @pytest.fixture
    def hub(self, open_repo, monkeypatch):
        hub = FakeHub({open_repo.repo_id: root_of(open_repo)})
        hub.install(monkeypatch)
        return hub

    @pytest.fixture
    def on_hub(self, hub, open_repo, restricted, open_from):
        repos = [Repo(BIORXIV, Tier.OPEN, HubRepo(open_repo.repo_id)), restricted]
        return _client(repos, open_from, can_submit=True)

    def _reviewed(self, client: TestClient, data: dict[str, Any]) -> dict[str, Any]:
        page = _post(client, "/additions/check", data).text
        digests = {f"digest:{repo}": digest for repo, digest in DIGEST.findall(page)}
        return {**data, **digests, "confirm": "yes"}

    def test_a_reviewed_and_confirmed_request_opens_its_pull_request(self, hub, on_hub):
        page = _post(
            on_hub, "/additions/submit", self._reviewed(on_hub, _carried("n1"))
        )
        assert "Submitted for review" in page.text
        assert "Pull request #100" in page.text
        [opened] = hub.opened
        assert list(opened["files"]) == ["changes/additions/additions.yml"]

    def test_nothing_is_opened_without_the_confirmation(self, hub, on_hub):
        data = self._reviewed(on_hub, _carried("n1"))
        del data["confirm"]
        response = _post(on_hub, "/additions/submit", data)
        assert response.status_code == 400
        assert hub.opened == []

    def test_files_that_changed_since_the_review_are_not_submitted(self, hub, on_hub):
        data = self._reviewed(on_hub, _carried("n1"))
        stale = {
            k: ("0" * 64 if k.startswith("digest:") else v) for k, v in data.items()
        }
        response = _post(on_hub, "/additions/submit", stale)
        assert response.status_code == 409
        assert hub.opened == []
