"""Tests for corpus.change_ui — the add documents pages."""

from pathlib import Path
from typing import Any, cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from sciencebeam_dataset_builder.corpus.additions import BuildInputs
from sciencebeam_dataset_builder.corpus.change_ui.app import create_app
from tests.corpus._additions import addition_world


def _client(repos: list[Any], inputs: BuildInputs | None) -> TestClient:
    app = create_app(lambda: repos, lambda: "hazal", False, inputs)
    return TestClient(app, base_url="http://127.0.0.1")


def _post(client: TestClient, path: str, data: dict[str, Any]) -> Any:
    token = cast(FastAPI, client.app).state.csrf
    return client.post(path, data={**data, "csrf": token})


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    return _client(*addition_world(tmp_path))


def test_the_first_page_starts_an_addition(client):
    assert 'href="/additions"' in client.get("/").text


def test_without_the_snapshot_the_page_says_where_to_run_it(tmp_path):
    repos, _ = addition_world(tmp_path)
    page = _client(repos, None).get("/additions").text
    assert "only on\n  the machine that builds the corpora" in page
    assert 'action="/additions/check"' not in page


def test_the_checks_end_in_the_build_to_run(client):
    page = _post(client, "/additions/check", {"corpus": "biorxiv", "ids": "n1 a"}).text
    assert "1 of 2 documents can be added" in page
    assert "make corpus-build CORPUS=biorxiv TIER=open" in page
    assert "--base-release v1.0.0 --version v1.1.0" in page
    assert "already in elifepathways/sciencebeam-dataset-biorxiv" in page
    assert "also adds 1 other new document" in page


def test_no_ids_is_refused_with_the_form_kept(client):
    response = _post(client, "/additions/check", {"corpus": "biorxiv", "ids": " "})
    assert response.status_code == 400
    assert "Enter at least one document id" in response.text
    assert 'value="biorxiv" selected' in response.text


def test_no_corpus_is_refused_with_the_ids_kept(client):
    response = _post(client, "/additions/check", {"corpus": "", "ids": "n1"})
    assert response.status_code == 400
    assert "Choose the corpus the documents come from" in response.text
    assert ">n1</textarea>" in response.text
