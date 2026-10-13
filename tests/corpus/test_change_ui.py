"""Tests for corpus.change_ui — the change request page and its API."""

from collections.abc import Callable, Sequence

import pytest
from fastapi.testclient import TestClient

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.change_ui.app import MAX_DOCUMENTS, create_app
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.proposal import ProposalError
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from sciencebeam_dataset_builder.corpus.repo_reader import HubRepo
from tests.corpus._fake_hub import FakeHub, FakePullRequest, removals_diff
from tests.corpus._repos import publish, root_of, write_file

BIORXIV = CORPORA["biorxiv"]
ORE = CORPORA["ore"]
REASON = "editorial, not a research article"


def _client(
    repos: Callable[[], Sequence[Repo]],
    identity: Callable[[], str] = lambda: "hazal",
    can_submit: bool = True,
    base_url: str = "http://127.0.0.1",
) -> TestClient:
    return TestClient(create_app(repos, identity, can_submit), base_url=base_url)


@pytest.fixture
def biorxiv(tmp_path):
    return publish(tmp_path, BIORXIV, Tier.OPEN, {"train": ["a", "b"]})


@pytest.fixture
def local(biorxiv):
    return _client(lambda: [biorxiv], can_submit=False)


class TestPage:
    def test_the_page_and_its_script_are_served(self, local):
        assert "Corpus change requests" in local.get("/").text
        assert local.get("/static/app.js").status_code == 200

    def test_a_request_for_another_host_name_is_refused(self, biorxiv):
        """A page that rebinds its own name to 127.0.0.1 still sends its own Host."""
        foreign = _client(lambda: [biorxiv], base_url="http://attacker.example")
        assert foreign.get("/api/whoami").status_code == 400

    def test_a_body_that_is_not_json_is_refused(self, local):
        """A cross-site form can post text, but not JSON without asking first."""
        response = local.post(
            "/api/lookup", content="queries=a", headers={"content-type": "text/plain"}
        )
        assert response.status_code == 422


class TestWhoami:
    def test_it_names_the_user_and_whether_submitting_is_on(self, local):
        assert local.get("/api/whoami").json() == {"user": "hazal", "can_submit": False}

    def test_no_user_is_unauthorised(self, biorxiv):
        def nobody() -> str:
            raise ProposalError("the Hub token names no user")

        client = _client(lambda: [biorxiv], identity=nobody)
        response = client.get("/api/whoami")
        assert response.status_code == 401
        assert response.json()["detail"] == "the Hub token names no user"


class TestLookup:
    def test_each_query_comes_back_with_where_it_is(self, local):
        found, missing = local.post("/api/lookup", json={"queries": ["a", "z"]}).json()[
            "results"
        ]
        assert found["status"] == "found"
        [where] = found["locations"]
        assert where["uid"] == "biorxiv__a"
        assert (where["split"], where["parquet_file_name"]) == (
            "train",
            "train-00000.parquet",
        )
        assert where["metadata"]["licence"] == "CC BY 4.0"
        assert missing["status"] == "missing"

    @pytest.mark.parametrize("count", [0, MAX_DOCUMENTS + 1])
    def test_too_few_or_too_many_queries_are_refused(self, local, count):
        response = local.post("/api/lookup", json={"queries": ["a"] * count})
        assert response.status_code == 422


class TestCheck:
    def test_the_review_shows_each_document_and_each_file(self, local):
        body = {"requests": [{"query": "a", "reason": REASON}]}
        data = local.post("/api/removals/check", json=body).json()
        [check] = data["checks"]
        assert check["ok"]
        assert check["removal"]["local_change_id"] == 1
        [proposal] = data["proposals"]
        assert proposal["path"] == layout.REMOVALS_PATH
        assert proposal["before"] is None
        assert "change_reason: editorial, not a research article" in proposal["added"]
        assert len(proposal["digest"]) == 64

    def test_a_failing_document_says_why(self, local):
        body = {"requests": [{"query": "a", "reason": " "}]}
        [check] = local.post("/api/removals/check", json=body).json()["checks"]
        assert not check["ok"]
        assert check["errors"] == ["a reason is required"]


class TestSubmit:
    @pytest.fixture
    def hub(self, biorxiv, monkeypatch):
        hub = FakeHub({biorxiv.repo_id: root_of(biorxiv)})
        hub.install(monkeypatch)
        return hub

    @pytest.fixture
    def client(self, biorxiv, hub):
        return _client(lambda: [Repo(BIORXIV, Tier.OPEN, HubRepo(biorxiv.repo_id))])

    def _reviewed(self, client, requests):
        data = client.post("/api/removals/check", json={"requests": requests}).json()
        return {p["repo_id"]: p["digest"] for p in data["proposals"]}

    def test_a_reviewed_request_opens_its_pull_request(self, hub, client, biorxiv):
        requests = [{"query": "a", "reason": REASON}]
        reviewed = self._reviewed(client, requests)
        response = client.post(
            "/api/removals/submit", json={"requests": requests, "reviewed": reviewed}
        )
        [result] = response.json()["results"]
        assert result["ok"]
        assert result["repo_id"] == biorxiv.repo_id
        assert result["pr_url"].endswith("/discussions/100")
        assert len(hub.opened) == 1

    def test_a_preview_cannot_submit(self, local):
        body = {"requests": [{"query": "a", "reason": REASON}], "reviewed": {}}
        assert local.post("/api/removals/submit", json=body).status_code == 403

    def test_files_that_changed_since_the_review_are_not_submitted(self, hub, client):
        requests = [{"query": "a", "reason": REASON}]
        reviewed = {k: "0" * 64 for k in self._reviewed(client, requests)}
        response = client.post(
            "/api/removals/submit", json={"requests": requests, "reviewed": reviewed}
        )
        assert response.status_code == 409
        assert hub.opened == []

    def test_a_document_that_fails_now_stops_the_submission(self, hub, client):
        requests = [{"query": "a", "reason": REASON}]
        reviewed = self._reviewed(client, requests)
        hub.pull_requests = [FakePullRequest(7, "Remove a", removals_diff("a"))]
        response = client.post(
            "/api/removals/submit", json={"requests": requests, "reviewed": reviewed}
        )
        assert response.status_code == 409
        assert "#7" in response.json()["checks"][0]["errors"][0]
        assert hub.opened == []

    def test_one_repo_failing_leaves_the_others_opened(
        self, tmp_path, monkeypatch, biorxiv
    ):
        ore = publish(tmp_path, ORE, Tier.OPEN, {"train": ["o"]})
        hub = FakeHub(
            {biorxiv.repo_id: root_of(biorxiv), ore.repo_id: root_of(ore)},
            fail=RuntimeError("503"),
            fail_on="create_commit",
            fail_repo=biorxiv.repo_id,
        )
        hub.install(monkeypatch)
        client = _client(
            lambda: [
                Repo(BIORXIV, Tier.OPEN, HubRepo(biorxiv.repo_id)),
                Repo(ORE, Tier.OPEN, HubRepo(ore.repo_id)),
            ]
        )
        requests = [
            {"query": "a", "reason": REASON},
            {"query": "o", "reason": REASON},
        ]
        reviewed = self._reviewed(client, requests)
        results = client.post(
            "/api/removals/submit", json={"requests": requests, "reviewed": reviewed}
        ).json()["results"]
        assert [(r["repo_id"], r["ok"]) for r in results] == [
            (biorxiv.repo_id, False),
            (ore.repo_id, True),
        ]
        assert "503" in results[0]["error"]


def test_an_existing_removals_file_is_shown_as_it_is(biorxiv):
    existing = "removals:\n"
    write_file(biorxiv, layout.REMOVALS_PATH, existing)
    client = _client(lambda: [biorxiv], can_submit=False)
    body = {"requests": [{"query": "a", "reason": REASON}]}
    [proposal] = client.post("/api/removals/check", json=body).json()["proposals"]
    assert proposal["before"] == existing
    assert proposal["added"].startswith("  - id: a\n")
