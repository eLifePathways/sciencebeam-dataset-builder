"""Tests for corpus.change_ui — the content correction pages."""

import base64
import html
import re
from collections.abc import Callable, Sequence
from typing import Any, cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.change_ui.app import create_app
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from sciencebeam_dataset_builder.corpus.repo_reader import HubRepo
from tests.corpus._fake_hub import FakeHub, FakePullRequest
from tests.corpus._repos import jats, publish, root_of, write_file

BIORXIV = CORPORA["biorxiv"]
REASON = 'surname was "Doe", should be "Smith"'
SHIPPED = jats(title="Old title")
FIXED = jats(title="New title")
PATHS = layout.correction_paths("a")


def _client(repos: Callable[[], Sequence[Repo]], can_submit: bool = True) -> TestClient:
    app = create_app(repos, lambda: "hazal", can_submit)
    return TestClient(app, base_url="http://127.0.0.1")


def _post(client: TestClient, path: str, data: dict[str, Any], **kwargs: Any) -> Any:
    token = cast(FastAPI, client.app).state.csrf
    return client.post(path, data={**data, "csrf": token}, **kwargs)


def _hidden(page: str, name: str) -> str:
    match = re.search(rf'name="{name}" value="([^"]*)"', page)
    assert match, f"no hidden {name}"
    return html.unescape(match.group(1))


def _upload(client: TestClient, xml: str = FIXED, reason: str = REASON) -> Any:
    return _post(
        client,
        "/corrections/content/check",
        {"uid": "biorxiv__a", "reason": reason},
        files={"xml": ("a.xml", xml.encode(), "application/xml")},
    )


def _reviewed(page: str) -> dict[str, str]:
    """The fields the review page posts on, ticked as confirmed."""
    fields = {
        name: _hidden(page, name) for name in ("uid", "reason", "xml", "reviewed")
    }
    return {**fields, "confirm": "yes"}


@pytest.fixture
def local(tmp_path):
    return publish(tmp_path, BIORXIV, Tier.OPEN, {"train": ["a"]}, xml={"a": SHIPPED})


@pytest.fixture
def preview(local):
    return _client(lambda: [local], can_submit=False)


class TestFindingTheDocument:
    def test_the_first_page_starts_a_correction(self, preview):
        assert 'href="/corrections/content"' in preview.get("/").text

    def test_a_found_document_is_shown_with_where_to_upload(self, preview):
        page = _post(preview, "/corrections/content/find", {"uid": "a"}).text
        assert "biorxiv__a" in page and "10.1/a" in page
        assert 'name="uid" value="biorxiv__a"' in page
        assert 'type="file" id="xml" name="xml"' in page
        assert "/corrections/content/current?uid=biorxiv__a" in page

    def test_a_missing_document_says_so(self, preview):
        assert (
            "missing:" in _post(preview, "/corrections/content/find", {"uid": "z"}).text
        )

    def test_no_id_is_refused(self, preview):
        response = _post(preview, "/corrections/content/find", {"uid": " "})
        assert response.status_code == 400

    def test_the_current_xml_downloads_as_a_file(self, preview):
        response = preview.get("/corrections/content/current", params={"uid": "a"})
        assert response.text == SHIPPED
        assert response.headers["content-disposition"] == 'attachment; filename="a.xml"'

    def test_a_pending_correction_is_what_downloads(self, local, preview):
        """So a second fix starts from the first, and its diff is only itself."""
        write_file(local, PATHS.corrected, FIXED)
        response = preview.get("/corrections/content/current", params={"uid": "a"})
        assert response.text == FIXED

    def test_a_missing_document_has_nothing_to_download(self, preview):
        response = preview.get("/corrections/content/current", params={"uid": "z"})
        assert response.status_code == 404


class TestReview:
    def test_a_passing_correction_shows_its_diff_and_the_files_it_writes(self, preview):
        page = _upload(preview).text
        assert "The correction can be proposed" in page
        assert '<span class="del">-&lt;article-title&gt;Old title' in page
        assert '<span class="add">+&lt;article-title&gt;New title' in page
        assert "Committed first as the XML the dataset ships" in page
        assert "+   - local_change_id: 1" in page
        assert base64.b64decode(_hidden(page, "xml")).decode() == FIXED
        assert len(_hidden(page, "reviewed")) == 64

    def test_a_failing_correction_says_why_and_cannot_be_confirmed(self, preview):
        page = _upload(preview, "<article>").text
        assert "not well-formed" in page
        assert "What changes in the XML" not in page
        assert 'name="confirm" value="yes" disabled' in page

    def test_a_warning_is_shown_but_does_not_stop_it(self, preview):
        page = _upload(preview, jats(doi="", title="New title")).text
        assert "Worth knowing" in page and "carries no DOI" in page
        assert "The correction can be proposed" in page

    def test_no_file_goes_back_to_the_upload(self, preview):
        response = _post(
            preview,
            "/corrections/content/check",
            {"uid": "biorxiv__a", "reason": REASON},
        )
        assert response.status_code == 400
        assert "Choose the corrected XML file" in response.text

    def test_going_back_keeps_the_reason(self, preview):
        data = {"uid": "biorxiv__a", "reason": "keep me"}
        page = _post(preview, "/corrections/content/find", data).text
        assert ">keep me</textarea>" in page


class TestSubmit:
    @pytest.fixture
    def hub(self, local, monkeypatch):
        hub = FakeHub({local.repo_id: root_of(local)})
        hub.install(monkeypatch)
        return hub

    @pytest.fixture
    def client(self, local, hub):
        return _client(lambda: [Repo(BIORXIV, Tier.OPEN, HubRepo(local.repo_id))])

    def test_a_reviewed_and_confirmed_correction_opens_its_pull_request(
        self, hub, client
    ):
        page = _post(
            client, "/corrections/content/submit", _reviewed(_upload(client).text)
        )
        assert "Submitted for review" in page.text
        assert "Pull request #100" in page.text
        assert len(hub.opened) == 2

    def test_a_repeated_submission_shows_the_pull_request_already_open(
        self, hub, client
    ):
        """A second press, or Back and submit again, finds its own pull request."""
        data = _reviewed(_upload(client).text)
        _post(client, "/corrections/content/submit", data)
        hub.pull_requests = [
            FakePullRequest(100, "Correct a", dict(hub.opened[-1]["files"]))
        ]
        again = _post(client, "/corrections/content/submit", data).text
        assert "submitted already" in again and "Pull request #100" in again
        assert len(hub.opened) == 2

    def test_nothing_is_opened_without_the_confirmation(self, hub, client):
        data = _reviewed(_upload(client).text)
        del data["confirm"]
        response = _post(client, "/corrections/content/submit", data)
        assert response.status_code == 400
        assert hub.opened == []

    def test_a_preview_cannot_submit(self, preview):
        data = _reviewed(_upload(preview).text)
        response = _post(preview, "/corrections/content/submit", data)
        assert response.status_code == 403
        assert "nothing can be submitted" in response.text

    def test_a_different_file_than_the_one_reviewed_is_not_submitted(self, hub, client):
        data = _reviewed(_upload(client).text)
        data["xml"] = base64.b64encode(jats(title="Other").encode()).decode()
        response = _post(client, "/corrections/content/submit", data)
        assert response.status_code == 409
        assert "changed after you reviewed it" in response.text
        assert hub.opened == []

    def test_an_upload_that_did_not_survive_the_round_trip_is_refused(self, client):
        data = {**_reviewed(_upload(client).text), "xml": "not base64!"}
        response = _post(client, "/corrections/content/submit", data)
        assert response.status_code == 400
        assert "did not come back intact" in response.text

    def test_a_hub_failure_says_it_was_not_submitted(self, hub, client):
        data = _reviewed(_upload(client).text)
        hub.fail, hub.fail_on = RuntimeError("503"), "create_commit"
        page = _post(client, "/corrections/content/submit", data).text
        assert "Not submitted" in page and "503" in page
