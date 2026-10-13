"""The change request page and the JSON API behind it.

Every check reads the repos afresh, and a submission checks again before opening
anything, so what is opened is what the person reviewed or nothing at all.
"""

import hashlib
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from starlette.middleware.trustedhost import TrustedHostMiddleware

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.lookup import Location, Repo, Result, lookup
from sciencebeam_dataset_builder.corpus.proposal import ProposalError, submit_removals
from sciencebeam_dataset_builder.corpus.validate import (
    RemovalCheck,
    RemovalProposal,
    check_removals,
)

STATIC = Path(__file__).parent / "static"
MAX_DOCUMENTS = 500

# Only a page served from this machine may call the API: a site that rebinds its own
# name to 127.0.0.1 arrives with a different Host and is refused.
LOCAL_HOSTS = ("127.0.0.1", "localhost")


class LookupBody(BaseModel):
    queries: list[str] = Field(min_length=1, max_length=MAX_DOCUMENTS)


class RemovalBody(BaseModel):
    query: str = Field(max_length=500)
    reason: str = Field(max_length=2000)


class RemovalsBody(BaseModel):
    requests: list[RemovalBody] = Field(min_length=1, max_length=MAX_DOCUMENTS)


class SubmitBody(RemovalsBody):
    # repo id -> digest of the `removals.yml` the person reviewed.
    reviewed: dict[str, str]


def create_app(
    repos: Callable[[], Sequence[Repo]],
    identity: Callable[[], str],
    can_submit: bool,
) -> FastAPI:
    app = FastAPI(
        title="Corpus change requests",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=list(LOCAL_HOSTS))

    def who() -> str:
        try:
            name = identity()
        except ProposalError as exc:
            raise HTTPException(401, str(exc)) from exc
        if not name.strip():
            raise HTTPException(401, "no user to record the change against")
        return name

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(STATIC / "index.html")

    @app.get("/api/whoami")
    def whoami() -> dict[str, Any]:
        return {"user": who(), "can_submit": can_submit}

    @app.post("/api/lookup")
    def find(body: LookupBody) -> dict[str, Any]:
        return {"results": [_result(r) for r in lookup(body.queries, repos())]}

    @app.post("/api/removals/check")
    def check(body: RemovalsBody) -> dict[str, Any]:
        checks, proposals = check_removals(_pairs(body), repos(), who())
        return {
            "checks": [_check(c) for c in checks],
            "proposals": [_proposal(p) for p in proposals],
        }

    @app.post("/api/removals/submit", response_model=None)
    def submit(body: SubmitBody) -> dict[str, Any] | JSONResponse:
        if not can_submit:
            raise HTTPException(403, "this page is a preview; nothing can be submitted")
        checks, proposals = check_removals(_pairs(body), repos(), who())
        if not all(c.ok for c in checks):
            return JSONResponse(
                {
                    "detail": "Some documents no longer pass the checks.",
                    "checks": [_check(c) for c in checks],
                },
                status_code=409,
            )
        if {p.repo.repo_id: _digest(p.after) for p in proposals} != body.reviewed:
            return JSONResponse(
                {"detail": "The files to propose changed after you reviewed them."},
                status_code=409,
            )
        return {"results": [_submit(p, checks) for p in proposals]}

    app.mount("/static", StaticFiles(directory=STATIC), name="static")
    return app


def _pairs(body: RemovalsBody) -> list[tuple[str, str]]:
    return [(r.query, r.reason) for r in body.requests]


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _submit(
    proposal: RemovalProposal, checks: Sequence[RemovalCheck]
) -> dict[str, Any]:
    """One repo's outcome; a failure here leaves the other repos' pull requests be."""
    repo_id = proposal.repo.repo_id
    try:
        submission = submit_removals(proposal, checks)
    except ProposalError as exc:
        return {"repo_id": repo_id, "ok": False, "error": str(exc)}
    return {
        "repo_id": repo_id,
        "ok": True,
        "pr_num": submission.pr_num,
        "pr_url": submission.pr_url,
        "title": submission.title,
    }


def _location(location: Location) -> dict[str, Any]:
    return {
        "repo_id": location.repo.repo_id,
        "corpus": location.repo.corpus.name,
        "tier": location.repo.tier.value,
        "release": location.release,
        "id": location.id,
        "uid": location.uid,
        "split": location.split,
        "file_type_pair": location.file_type_pair,
        "shard": location.shard,
        "parquet_file_name": location.parquet_file_name,
        "metadata": dict(location.metadata),
    }


def _result(result: Result) -> dict[str, Any]:
    return {
        "query": result.query,
        "status": result.status.value,
        "detail": result.detail,
        "locations": [_location(loc) for loc in result.locations],
    }


def _check(check: RemovalCheck) -> dict[str, Any]:
    removal = check.removal
    return {
        "query": check.query,
        "reason": check.reason,
        "ok": check.ok,
        "errors": list(check.errors),
        "location": _location(check.location) if check.location else None,
        "removal": (
            {
                "id": removal.id,
                "local_change_id": removal.local_change_id,
                "change_reason": removal.change_reason,
                "changed_by": removal.changed_by,
            }
            if removal
            else None
        ),
    }


def _proposal(proposal: RemovalProposal) -> dict[str, Any]:
    before = proposal.before or ""
    return {
        "repo_id": proposal.repo.repo_id,
        "corpus": proposal.repo.corpus.name,
        "tier": proposal.repo.tier.value,
        "path": layout.REMOVALS_PATH,
        "ids": [r.id for r in proposal.removals],
        "before": proposal.before,
        "added": proposal.after.removeprefix(before),
        "digest": _digest(proposal.after),
    }
