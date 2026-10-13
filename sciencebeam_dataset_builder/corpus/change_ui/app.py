"""The change request pages: one form per step, rendered on the server.

Each page carries the request so far in hidden fields. Every check reads the repos
afresh, and a submission checks again before opening anything, so what is opened is
what the person reviewed or nothing at all.
"""

import hashlib
import secrets
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.concurrency import run_in_threadpool
from starlette.datastructures import FormData, UploadFile
from starlette.middleware.trustedhost import TrustedHostMiddleware

from sciencebeam_dataset_builder.corpus import layout
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
from sciencebeam_dataset_builder.corpus.lookup import (
    Location,
    Repo,
    Result,
    Status,
    lookup,
)
from sciencebeam_dataset_builder.corpus.proposal import ProposalError, submit_removals
from sciencebeam_dataset_builder.corpus.validate import (
    RemovalCheck,
    RemovalProposal,
    check_removals,
)

HERE = Path(__file__).parent

# Only a page served from this machine may post: a site that rebinds its own name to
# 127.0.0.1 arrives with a different Host and is refused.
LOCAL_HOSTS = ("127.0.0.1", "localhost")

STEPS = (
    ("type", "Change type"),
    ("documents", "Documents"),
    ("reasons", "Reasons"),
    ("review", "Check and review"),
    ("done", "Submitted"),
)

# A uid and a reason per document, plus a digest per repo and the request's own fields.
_MAX_FIELDS = 2 * MAX_DOCUMENTS + 64

Checked = tuple[list[RemovalCheck], list[RemovalProposal]]


class PageError(Exception):
    """A request that cannot go on; shown as its own page."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


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
    app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")
    # Every form carries this, so another site cannot post one on the person's behalf.
    app.state.csrf = secrets.token_urlsafe(32)
    templates = Jinja2Templates(directory=HERE / "templates")
    step_keys = [key for key, _ in STEPS]

    def who() -> tuple[str, str]:
        """The user, or why there is none."""
        try:
            name = identity()
        except ProposalError as exc:
            return "", f"{exc}. Set HF_TOKEN or run 'hf auth login', then reload."
        if not name.strip():
            return "", "There is no user to record the change against."
        return name, ""

    def page(
        request: Request,
        template: str,
        step: str,
        *,
        status_code: int = 200,
        error: str = "",
        **context: Any,
    ) -> HTMLResponse:
        user, problem = who()
        return templates.TemplateResponse(
            request,
            template,
            {
                "steps": STEPS,
                "step_index": step_keys.index(step) if step in step_keys else -1,
                "user": user,
                "can_submit": can_submit,
                "csrf": app.state.csrf,
                "error": error or problem,
                "max_documents": MAX_DOCUMENTS,
                "max_reason": MAX_REASON,
                **context,
            },
            status_code=status_code,
        )

    async def form(request: Request) -> FormData:
        data = await request.form(max_files=1, max_fields=_MAX_FIELDS)
        if not secrets.compare_digest(_text(data.get("csrf")), app.state.csrf):
            raise PageError(
                403, "This page has expired. Start again from the first page."
            )
        return data

    def draft_of(data: FormData) -> Draft:
        try:
            return Draft.from_form(data)
        except FormError as exc:
            raise PageError(400, str(exc)) from exc

    async def review(
        request: Request,
        draft: Draft,
        *,
        checked: Checked | None = None,
        error: str = "",
        status_code: int = 200,
    ) -> HTMLResponse:
        user, problem = who()
        if problem:
            return page(
                request, "reasons.html", "reasons", draft=draft, status_code=401
            )
        if checked is None:
            checked = await run_in_threadpool(
                check_removals, draft.requests(), repos(), user
            )
        checks, proposals = checked
        return page(
            request,
            "review.html",
            "review",
            draft=draft,
            checks=[_check(c) for c in checks],
            proposals=[_proposal(p) for p in proposals],
            all_pass=bool(checks) and all(c.ok for c in checks),
            passed=sum(c.ok for c in checks),
            error=error,
            status_code=status_code,
        )

    @app.exception_handler(PageError)
    async def page_error(request: Request, exc: PageError) -> HTMLResponse:
        return page(
            request, "error.html", "", error=str(exc), status_code=exc.status_code
        )

    @app.get("/")
    def start(request: Request) -> HTMLResponse:
        return page(request, "type.html", "type")

    @app.get("/removals")
    def documents(request: Request) -> HTMLResponse:
        return page(request, "documents.html", "documents", ids="")

    @app.post("/removals/find")
    async def find(request: Request) -> HTMLResponse:
        data = await form(request)
        ids = _text(data.get("ids"))
        try:
            queries = parse_ids(ids)
            csv_reasons: dict[str, str] = {}
            upload = data.get("csv")
            if isinstance(upload, UploadFile) and upload.filename:
                csv_ids, csv_reasons = read_csv(await upload.read(MAX_CSV_BYTES + 1))
                queries += csv_ids
            check_count(queries)
        except FormError as exc:
            return page(
                request,
                "documents.html",
                "documents",
                ids=ids,
                error=str(exc),
                status_code=400,
            )
        results = await run_in_threadpool(lookup, queries, repos())
        return page(
            request,
            "documents.html",
            "documents",
            ids="\n".join(queries),
            rows=_rows(results, csv_reasons),
            summary=_summary(results),
        )

    @app.post("/removals/reasons")
    async def reasons(request: Request) -> HTMLResponse:
        draft = draft_of(await form(request))
        if not draft.uids:
            return page(
                request,
                "documents.html",
                "documents",
                ids=draft.ids,
                error="Tick at least one document, then continue.",
                status_code=400,
            )
        return page(request, "reasons.html", "reasons", draft=draft)

    @app.post("/removals/check")
    async def check(request: Request) -> HTMLResponse:
        data = await form(request)
        draft = draft_of(data).without(_text(data.get("leave_out")))
        if not draft.uids:
            return page(
                request,
                "documents.html",
                "documents",
                ids=draft.ids,
                error="Every document has been left out. Find documents again.",
            )
        return await review(request, draft)

    @app.post("/removals/submit")
    async def submit(request: Request) -> HTMLResponse:
        data = await form(request)
        draft = draft_of(data)
        user, problem = who()
        if problem:
            return await review(request, draft)
        if not can_submit:
            return await review(
                request,
                draft,
                error="This is a preview against a local build, so nothing can be "
                "submitted.",
                status_code=403,
            )
        if _text(data.get("confirm")) != "yes":
            return await review(
                request,
                draft,
                error="Tick the box to confirm you have reviewed the removals.",
                status_code=400,
            )
        checks, proposals = await run_in_threadpool(
            check_removals, draft.requests(), repos(), user
        )
        if not all(c.ok for c in checks):
            return await review(
                request,
                draft,
                checked=(checks, proposals),
                error="Some documents no longer pass the checks. Review them again.",
                status_code=409,
            )
        reviewed = {
            key.removeprefix("digest:"): _text(value)
            for key, value in data.multi_items()
            if key.startswith("digest:")
        }
        if {p.repo.repo_id: _digest(p.after) for p in proposals} != reviewed:
            return await review(
                request,
                draft,
                checked=(checks, proposals),
                error="The files to propose changed after you reviewed them. Review "
                "them again.",
                status_code=409,
            )
        outcomes = [await run_in_threadpool(_submit, p, checks) for p in proposals]
        return page(
            request,
            "done.html",
            "done",
            draft=draft,
            outcomes=outcomes,
            all_ok=all(o["ok"] for o in outcomes),
        )

    return app


def _text(value: Any) -> str:
    return value if isinstance(value, str) else ""


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
    url = submission.pr_url if submission.pr_url.startswith("https://") else ""
    return {
        "repo_id": repo_id,
        "ok": True,
        "pr_num": submission.pr_num,
        "pr_url": url,
        "title": submission.title,
    }


def _location(location: Location) -> dict[str, Any]:
    metadata = location.metadata
    return {
        "repo_id": location.repo.repo_id,
        "repo_name": location.repo.repo_id.rsplit("/", 1)[-1],
        "tier": location.repo.tier.value,
        "uid": location.uid,
        "split": location.split,
        "parquet_file_name": location.parquet_file_name,
        "licence": metadata.get("licence") or "",
        "doi": metadata.get("doi") or "",
        "pub_date": metadata.get("pub_date") or "",
    }


def _rows(
    results: Sequence[Result], csv_reasons: dict[str, str]
) -> list[dict[str, Any]]:
    """One row per query. Only a document found in one repo, once, can be ticked."""
    seen: set[str] = set()
    rows = []
    for result in results:
        found = result.status is Status.FOUND
        where = _location(result.locations[0]) if found else None
        row = {
            "query": result.query,
            "status": result.status.value,
            "detail": result.detail,
            "location": where,
            "includable": False,
            "own": csv_reasons.get(result.query.strip(), ""),
        }
        if where and where["uid"] in seen:
            row.update(status="duplicate", detail="listed above already")
        elif where:
            row["includable"] = True
            seen.add(where["uid"])
        rows.append(row)
    return rows


def _summary(results: Sequence[Result]) -> str:
    counts = {s: sum(r.status is s for r in results) for s in Status}
    return " · ".join(f"{n} {s.value}" for s, n in counts.items() if n)


def _check(check: RemovalCheck) -> dict[str, Any]:
    where = _location(check.location) if check.location else None
    return {
        "uid": where["uid"] if where else check.query,
        "ok": check.ok,
        "errors": list(check.errors),
        "reason": check.reason,
        "location": where,
    }


def _proposal(proposal: RemovalProposal) -> dict[str, Any]:
    added = proposal.after.removeprefix(proposal.before or "")
    return {
        "repo_id": proposal.repo.repo_id,
        "tier": proposal.repo.tier.value,
        "path": layout.REMOVALS_PATH,
        "count": len(proposal.removals),
        "before": proposal.before,
        "added_lines": added.rstrip("\n").split("\n"),
        "digest": _digest(proposal.after),
    }
