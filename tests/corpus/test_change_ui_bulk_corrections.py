"""Tests for corpus.change_ui — the bulk content correction pages."""

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
from tests.corpus._fake_hub import FakeHub
from tests.corpus._repos import jats, publish, root_of

BIORXIV = CORPORA["biorxiv"]
REASON = "author given-name and surname were swapped"
CHECK = "/corrections/content/bulk/check"
SUBMIT = "/corrections/content/bulk/submit"
DIGEST = re.compile(r'name="digest:([^"]+)" value="([0-9a-f]{64})"')
HIDDEN = re.compile(r'<input type="hidden" name="([^"]+)" value="([^"]*)">')


def _new(id_value: str) -> bytes:
    return jats(doi=f"10.1/{id_value}", title=f"New {id_value}").encode()


def _client(
    repos: Callable[[], Sequence[Repo]], can_submit: bool = False
) -> TestClient:
    app = create_app(repos, lambda: "hazal", can_submit)
    return TestClient(app, base_url="http://127.0.0.1")


def _post(
    client: TestClient,
    path: str,
    data: dict[str, Any] | list[tuple[str, str]],
    files: list[Any] | None = None,
) -> Any:
    token = cast(FastAPI, client.app).state.csrf
    pairs = list(data.items()) if isinstance(data, dict) else data
    form: dict[str, list[str]] = {}
    for key, value in [*pairs, ("csrf", token)]:
        form.setdefault(key, []).append(value)
    return client.post(path, data=form, files=files)


def _upload(
    client: TestClient, *names: str, csv: str | None = None, **data: str
) -> Any:
    files = [
        ("xml", (f"{n}.xml", _new(n.split("__")[-1]), "application/xml")) for n in names
    ]
    if csv is not None:
        files.append(("csv", ("reasons.csv", csv.encode(), "text/csv")))
    return _post(client, CHECK, {"shared": REASON, **data}, files)


def _carried(page: str) -> list[tuple[str, str]]:
    """Every hidden field a page carries, so the next post sends what it would."""
    return [(k, html.unescape(v)) for k, v in HIDDEN.findall(page) if k != "csrf"]


@pytest.fixture
def local(tmp_path):
    return publish(
        tmp_path,
        BIORXIV,
        Tier.OPEN,
        {"train": ["a", "b", "c"]},
        xml={i: jats(doi=f"10.1/{i}") for i in "abc"},
    )


@pytest.fixture
def preview(local: Repo) -> TestClient:
    return _client(lambda: [local])


class TestFiles:
    def test_the_first_page_offers_bulk_corrections(self, preview):
        assert 'href="/corrections/content/bulk"' in preview.get("/").text

    def test_the_upload_takes_many_files(self, preview):
        page = preview.get("/corrections/content/bulk").text
        assert 'name="xml"' in page and "multiple" in page

    def test_each_file_is_checked_and_a_repo_is_one_pull_request(self, preview):
        page = _upload(preview, "biorxiv__a", "biorxiv__b").text
        assert "All 2 documents can be" in page
        assert len(DIGEST.findall(page)) == 1
        assert page.count("<details>") == 2

    def test_a_csv_reason_replaces_the_shared_one(self, preview):
        csv = "uid,reason\nbiorxiv__b,its own reason\nbiorxiv__z,unused\n"
        page = _upload(preview, "biorxiv__a", "biorxiv__b", csv=csv).text
        assert 'name="reason:biorxiv__b" value="its own reason"' in page
        assert f'name="reason:biorxiv__a" value="{REASON}"' in page
        assert "1 row of the CSV name no uploaded file" in page

    def test_a_file_that_is_not_xml_is_refused(self, preview):
        files = [("xml", ("a.txt", b"x", "text/plain"))]
        response = _post(preview, CHECK, {"shared": REASON}, files)
        assert response.status_code == 400
        assert "a.txt is not an .xml file" in response.text

    def test_a_failing_document_can_be_left_out(self, preview):
        page = _upload(preview, "biorxiv__a", "biorxiv__zz").text
        assert "1 of 2 documents can be proposed" in page
        again = _post(preview, CHECK, [*_carried(page), ("leave_out", "biorxiv__zz")])
        assert "The document can be" in again.text

    def test_a_preview_cannot_submit(self, preview):
        page = _upload(preview, "biorxiv__a").text
        response = _post(preview, SUBMIT, [*_carried(page), ("confirm", "yes")])
        assert response.status_code == 403


class TestSubmit:
    @pytest.fixture
    def hub(self, local, monkeypatch):
        hub = FakeHub({local.repo_id: root_of(local)})
        hub.install(monkeypatch)
        return hub

    @pytest.fixture
    def client(self, hub, local):
        return _client(
            lambda: [Repo(BIORXIV, Tier.OPEN, HubRepo(local.repo_id))], can_submit=True
        )

    def test_one_pull_request_opens_for_the_repo(self, hub, client):
        page = _upload(client, "biorxiv__a", "biorxiv__b").text
        done = _post(client, SUBMIT, [*_carried(page), ("confirm", "yes")]).text
        assert "Submitted for review" in done and "Pull request #100" in done
        seed, fix = hub.opened
        assert set(seed["files"]) == {
            layout.correction_paths(i).corrected for i in ("a", "b")
        }
        assert len(fix["files"]) == 4

    def test_what_changed_since_the_review_is_not_submitted(self, hub, client):
        page = _upload(client, "biorxiv__a").text
        stale = [
            (k, "0" * 64 if k.startswith("digest:") else v) for k, v in _carried(page)
        ]
        response = _post(client, SUBMIT, [*stale, ("confirm", "yes")])
        assert response.status_code == 409
        assert hub.opened == []
