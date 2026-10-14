"""Tests for corpus.change_ui — the metadata correction pages."""

import html
import re
from collections.abc import Callable, Sequence
from typing import Any, cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.change_files import EDITABLE_FIELDS
from sciencebeam_dataset_builder.corpus.change_ui.app import create_app
from sciencebeam_dataset_builder.corpus.change_ui.metadata import FIELDS
from sciencebeam_dataset_builder.corpus.layout import Tier
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.registry import CORPORA
from sciencebeam_dataset_builder.corpus.repo_reader import HubRepo
from tests.corpus._fake_hub import FakeHub
from tests.corpus._repos import publish, root_of

BIORXIV = CORPORA["biorxiv"]
REASON = "the harvest year, not the publication date"
PATHS = layout.correction_paths("a")


def _client(repos: Callable[[], Sequence[Repo]], can_submit: bool = True) -> TestClient:
    app = create_app(repos, lambda: "hazal", can_submit)
    return TestClient(app, base_url="http://127.0.0.1")


def _post(client: TestClient, path: str, data: dict[str, Any]) -> Any:
    token = cast(FastAPI, client.app).state.csrf
    return client.post(path, data={**data, "csrf": token})


def _check(client: TestClient, **values: str) -> Any:
    fields = {f"field:{name}": value for name, value in values.items()}
    return _post(
        client,
        "/corrections/metadata/check",
        {"uid": "biorxiv__a", "reason": REASON, **fields},
    )


def _field(page: str, name: str) -> str:
    """The value a field is filled in with on the page."""
    match = re.search(rf'name="field:{name}"\s+value="([^"]*)"', page)
    assert match, f"no field {name}"
    return html.unescape(match.group(1))


def _reviewed(page: str) -> dict[str, str]:
    """Every hidden field the review page posts on, ticked as confirmed."""
    hidden = re.findall(r'type="hidden" name="([^"]+)" value="([^"]*)"', page)
    return {name: html.unescape(value) for name, value in hidden} | {"confirm": "yes"}


@pytest.fixture
def local(tmp_path):
    return publish(tmp_path, BIORXIV, Tier.OPEN, {"train": ["a"]})


@pytest.fixture
def preview(local):
    return _client(lambda: [local], can_submit=False)


def test_every_editable_field_has_a_label():
    assert tuple(FIELDS) == EDITABLE_FIELDS


class TestTheValues:
    def test_the_first_page_starts_a_metadata_correction(self, preview):
        assert 'href="/corrections/metadata"' in preview.get("/").text

    def test_the_document_page_finds_into_the_metadata_flow(self, preview):
        assert (
            'action="/corrections/metadata/find"'
            in preview.get("/corrections/metadata").text
        )

    def test_each_field_is_filled_in_with_its_value_now(self, preview):
        page = _post(preview, "/corrections/metadata/find", {"uid": "a"}).text
        assert _field(page, "doi") == "10.1/a"
        assert _field(page, "pub_date") == "2026"
        assert _field(page, "version") == ""
        assert "Publication date" in page

    def test_going_back_keeps_what_was_typed(self, preview):
        data = {"uid": "a", "reason": "kept", "field:pub_date": "1999"}
        page = _post(preview, "/corrections/metadata/find", data).text
        assert _field(page, "pub_date") == "1999"
        assert ">kept</textarea>" in page

    def test_a_missing_document_says_so(self, preview):
        assert (
            "missing:"
            in _post(preview, "/corrections/metadata/find", {"uid": "z"}).text
        )


class TestReview:
    def test_only_the_changed_values_are_shown_now_and_corrected(self, preview):
        page = _check(preview, doi="10.1/a", pub_date="2022-10-31").text
        assert "The correction can be proposed" in page
        assert '<td class="mono">2026</td>' in page
        assert '<td class="mono">2022-10-31</td>' in page
        assert "<td>DOI</td>" not in page
        assert "+     fields:" in page

    def test_a_bad_value_says_why_and_cannot_be_confirmed(self, preview):
        page = _check(preview, pub_date="2022-13").text
        assert "pub_date has to be a date" in page
        assert 'name="confirm" value="yes" disabled' in page

    def test_clearing_a_value_is_shown_as_cleared(self, preview):
        page = _check(preview, doi="").text
        assert '<span class="detail">cleared</span>' in page
        assert "Worth knowing" in page


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
        data = _reviewed(_check(client, pub_date="2022-10-31").text)
        page = _post(client, "/corrections/metadata/submit", data).text
        assert "Submitted for review" in page
        assert "keeps its current\n  metadata" in page
        [only] = hub.opened
        assert set(only["files"]) == {PATHS.metadata}

    def test_a_value_changed_after_the_review_is_not_submitted(self, hub, client):
        data = _reviewed(_check(client, pub_date="2022-10-31").text)
        data["field:pub_date"] = "2021"
        response = _post(client, "/corrections/metadata/submit", data)
        assert response.status_code == 409
        assert hub.opened == []

    def test_nothing_is_opened_without_the_confirmation(self, hub, client):
        data = _reviewed(_check(client, pub_date="2022-10-31").text)
        del data["confirm"]
        assert _post(client, "/corrections/metadata/submit", data).status_code == 400
        assert hub.opened == []

    def test_a_preview_cannot_submit(self, preview):
        data = _reviewed(_check(preview, pub_date="2022-10-31").text)
        assert _post(preview, "/corrections/metadata/submit", data).status_code == 403
