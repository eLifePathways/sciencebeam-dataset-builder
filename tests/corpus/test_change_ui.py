"""Tests for corpus.change_ui — the change request pages."""

import re
from collections.abc import Callable, Sequence
from typing import Any, cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.datastructures import FormData

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.change_ui.app import create_app
from sciencebeam_dataset_builder.corpus.change_ui.forms import (
    MAX_CSV_BYTES,
    MAX_DOCUMENTS,
    MAX_REASON,
    Draft,
    FormError,
    check_count,
    parse_ids,
    read_csv,
)
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
DIGEST = re.compile(r'name="digest:([^"]+)" value="([0-9a-f]{64})"')


def _client(
    repos: Callable[[], Sequence[Repo]],
    identity: Callable[[], str] = lambda: "hazal",
    can_submit: bool = True,
    base_url: str = "http://127.0.0.1",
) -> TestClient:
    return TestClient(create_app(repos, identity, can_submit), base_url=base_url)


def _post(client: TestClient, path: str, data: dict[str, Any], **kwargs: Any) -> Any:
    token = cast(FastAPI, client.app).state.csrf
    return client.post(path, data={**data, "csrf": token}, **kwargs)


def _request(
    *uids: str, shared: str = REASON, own: dict[str, str] | None = None
) -> dict[str, Any]:
    """The hidden fields a page carries for these documents."""
    return {
        "ids": " ".join(uids),
        "uid": list(uids),
        "shared": shared,
        **{f"own:{uid}": (own or {}).get(uid, "") for uid in uids},
    }


@pytest.fixture
def biorxiv(tmp_path):
    return publish(tmp_path, BIORXIV, Tier.OPEN, {"train": ["a", "b"]})


@pytest.fixture
def local(biorxiv):
    return _client(lambda: [biorxiv], can_submit=False)


class TestForms:
    def test_ids_are_split_on_lines_spaces_commas_and_semicolons(self):
        assert parse_ids(" a\nb, c;d\t\te \n\n") == ["a", "b", "c", "d", "e"]

    def test_a_csv_gives_its_ids_and_any_reasons(self):
        data = (
            '﻿uid,reason\r\nbiorxiv__a,"editorial, not research"\r\n'
            'b,"said ""no"""\r\nc,\r\n\r\n'
        ).encode()
        assert read_csv(data) == (
            ["biorxiv__a", "b", "c"],
            {"biorxiv__a": "editorial, not research", "b": 'said "no"'},
        )

    def test_an_id_column_is_enough(self):
        assert read_csv(b"id\na\n") == (["a"], {})

    @pytest.mark.parametrize(
        ("data", "message"),
        [
            (b"", "empty"),
            (b"name,reason\na,x\n", "id or uid column"),
            (b"id\n\xff\n", "cannot be read"),
            (b"id\n" + b"a\n" * MAX_CSV_BYTES, "larger than 1 MB"),
        ],
    )
    def test_a_csv_that_cannot_be_used_says_why(self, data, message):
        with pytest.raises(FormError, match=message):
            read_csv(data)

    @pytest.mark.parametrize("count", [0, MAX_DOCUMENTS + 1])
    def test_too_few_or_too_many_ids_are_refused(self, count):
        with pytest.raises(FormError):
            check_count(["a"] * count)

    def test_a_documents_own_reason_wins_over_the_shared_one(self):
        form = FormData(
            [
                ("uid", "a"),
                ("uid", "b"),
                ("uid", "a"),
                ("shared", " all "),
                ("own:b", "b's"),
            ]
        )
        draft = Draft.from_form(form)
        assert draft.uids == ("a", "b")
        assert draft.requests() == [("a", "all"), ("b", "b's")]
        assert draft.without("a").uids == ("b",)

    def test_a_reason_that_is_too_long_is_refused(self):
        form = FormData([("uid", "a"), ("shared", "x" * (MAX_REASON + 1))])
        with pytest.raises(FormError, match="at most"):
            Draft.from_form(form)


class TestPages:
    def test_the_first_page_offers_the_four_change_types(self, local):
        page = local.get("/").text
        for title in ("Remove documents", "Add documents", "Correct content"):
            assert title in page
        assert page.count('href="/removals"') == 1

    def test_a_request_for_another_host_name_is_refused(self, biorxiv):
        """A page that rebinds its own name to 127.0.0.1 still sends its own Host."""
        foreign = _client(lambda: [biorxiv], base_url="http://attacker.example")
        assert foreign.get("/").status_code == 400

    def test_a_form_without_this_servers_token_is_refused(self, local):
        """Another site can post a form here, but cannot read the token to put in it."""
        response = local.post("/removals/find", data={"ids": "a", "csrf": "guess"})
        assert response.status_code == 403
        assert "expired" in response.text

    def test_the_user_is_named_and_a_preview_says_so(self, local):
        assert "Previewing a local build as hazal" in local.get("/").text

    def test_no_user_is_shown_rather_than_hidden(self, biorxiv):
        def nobody() -> str:
            raise ProposalError("the Hub token names no user")

        page = _client(lambda: [biorxiv], identity=nobody).get("/").text
        assert "Not signed in" in page
        assert "the Hub token names no user" in page


class TestFind:
    def test_found_documents_can_be_ticked_and_the_rest_say_why(self, local):
        page = _post(local, "/removals/find", {"ids": "a z"}).text
        assert 'name="uid" value="biorxiv__a" checked' in page
        assert "train-00000.parquet" in page
        assert "not in the latest release" in page
        assert "1 found · 1 missing" in page

    def test_a_csv_fills_in_each_documents_reason(self, local):
        csv = b"uid,reason\nbiorxiv__a,its own reason\n"
        page = _post(
            local,
            "/removals/find",
            {"ids": ""},
            files={"csv": ("r.csv", csv, "text/csv")},
        ).text
        assert 'name="own:biorxiv__a" value="its own reason"' in page

    def test_one_document_named_twice_can_be_ticked_once(self, local):
        page = _post(local, "/removals/find", {"ids": "a biorxiv__a"}).text
        assert page.count('name="uid" value="biorxiv__a"') == 1
        assert "listed above already" in page

    def test_no_ids_is_refused_with_the_form_kept(self, local):
        response = _post(local, "/removals/find", {"ids": " "})
        assert response.status_code == 400
        assert "Enter at least one document id" in response.text

    def test_a_bad_csv_says_why(self, local):
        response = _post(
            local, "/removals/find", {"ids": "a"}, files={"csv": ("r.csv", b"x\n1\n")}
        )
        assert response.status_code == 400
        assert "id or uid column" in response.text


class TestReasons:
    def test_each_document_gets_a_field_for_its_own_reason(self, local):
        data = _request("biorxiv__a", own={"biorxiv__a": "mine"})
        page = _post(local, "/removals/reasons", data).text
        assert 'name="own:biorxiv__a" value="mine"' in page
        assert 'aria-label="Reason for biorxiv__a"' in page

    def test_nothing_ticked_goes_back_to_the_documents(self, local):
        response = _post(local, "/removals/reasons", {"ids": "a"})
        assert response.status_code == 400
        assert "Tick at least one document" in response.text


class TestReview:
    def test_a_passing_document_shows_what_the_pull_request_adds(self, local):
        page = _post(local, "/removals/check", _request("biorxiv__a")).text
        assert "The document can be\n  proposed" in page
        assert "+   - id: a" in page
        assert "change_reason: editorial, not a research article" in page
        assert DIGEST.search(page)

    def test_a_failing_document_says_why_and_can_be_left_out(self, local):
        page = _post(local, "/removals/check", _request("biorxiv__a", shared=" ")).text
        assert "a reason is required" in page
        assert 'name="leave_out" value="biorxiv__a"' in page
        assert 'name="confirm" value="yes" disabled' in page

    def test_leaving_a_document_out_checks_the_rest(self, local):
        data = {**_request("biorxiv__a", "biorxiv__zz"), "leave_out": "biorxiv__zz"}
        page = _post(local, "/removals/check", data).text
        assert "The document can be\n  proposed" in page
        assert 'name="uid" value="biorxiv__zz"' not in page

    def test_leaving_out_every_document_goes_back_to_the_documents(self, local):
        data = {**_request("biorxiv__zz"), "leave_out": "biorxiv__zz"}
        assert (
            "Every document has been left out"
            in _post(local, "/removals/check", data).text
        )

    def test_what_was_typed_is_shown_as_text_never_as_markup(self, local):
        page = _post(
            local,
            "/removals/check",
            _request("biorxiv__a", shared="<script>x</script>"),
        ).text
        assert "<script>x</script>" not in page
        assert "&lt;script&gt;x&lt;/script&gt;" in page

    def test_an_existing_removals_file_is_shown_as_it_is(self, biorxiv, local):
        write_file(biorxiv, layout.REMOVALS_PATH, "removals:\n")
        page = _post(local, "/removals/check", _request("biorxiv__a")).text
        assert "The file as it is now" in page


class TestSubmit:
    @pytest.fixture
    def hub(self, biorxiv, monkeypatch):
        hub = FakeHub({biorxiv.repo_id: root_of(biorxiv)})
        hub.install(monkeypatch)
        return hub

    @pytest.fixture
    def client(self, biorxiv, hub):
        return _client(lambda: [Repo(BIORXIV, Tier.OPEN, HubRepo(biorxiv.repo_id))])

    def _reviewed(self, client: TestClient, data: dict[str, Any]) -> dict[str, Any]:
        page = _post(client, "/removals/check", data).text
        digests = {f"digest:{repo}": digest for repo, digest in DIGEST.findall(page)}
        return {**data, **digests, "confirm": "yes"}

    def test_a_reviewed_and_confirmed_request_opens_its_pull_request(self, hub, client):
        page = _post(
            client, "/removals/submit", self._reviewed(client, _request("biorxiv__a"))
        )
        assert "Submitted for review" in page.text
        assert 'href="https://huggingface.co/datasets/' in page.text
        assert "Pull request #100" in page.text
        assert len(hub.opened) == 1

    def test_nothing_is_opened_without_the_confirmation(self, hub, client):
        data = self._reviewed(client, _request("biorxiv__a"))
        del data["confirm"]
        response = _post(client, "/removals/submit", data)
        assert response.status_code == 400
        assert "Tick the box" in response.text
        assert hub.opened == []

    def test_a_preview_cannot_submit(self, local):
        data = {**_request("biorxiv__a"), "confirm": "yes"}
        response = _post(local, "/removals/submit", data)
        assert response.status_code == 403
        assert "nothing can be submitted" in response.text

    def test_files_that_changed_since_the_review_are_not_submitted(self, hub, client):
        data = self._reviewed(client, _request("biorxiv__a"))
        stale = {
            k: ("0" * 64 if k.startswith("digest:") else v) for k, v in data.items()
        }
        response = _post(client, "/removals/submit", stale)
        assert response.status_code == 409
        assert "changed after you reviewed them" in response.text
        assert hub.opened == []

    def test_a_document_that_fails_now_stops_the_submission(self, hub, client):
        data = self._reviewed(client, _request("biorxiv__a"))
        hub.pull_requests = [FakePullRequest(7, "Remove a", removals_diff("a"))]
        response = _post(client, "/removals/submit", data)
        assert response.status_code == 409
        assert "open pull request #7" in response.text
        assert hub.opened == []

    def test_one_repo_failing_leaves_the_others_opened(
        self, tmp_path, monkeypatch, biorxiv
    ):
        ore = publish(tmp_path, ORE, Tier.OPEN, {"train": ["o"]})
        FakeHub(
            {biorxiv.repo_id: root_of(biorxiv), ore.repo_id: root_of(ore)},
            fail=RuntimeError("503"),
            fail_on="create_commit",
            fail_repo=biorxiv.repo_id,
        ).install(monkeypatch)
        client = _client(
            lambda: [
                Repo(BIORXIV, Tier.OPEN, HubRepo(biorxiv.repo_id)),
                Repo(ORE, Tier.OPEN, HubRepo(ore.repo_id)),
            ]
        )
        data = self._reviewed(client, _request("biorxiv__a", "ore__o"))
        page = _post(client, "/removals/submit", data).text
        assert "Only partly submitted" in page
        assert "Not opened" in page and "503" in page
        assert "Pull request #100" in page
