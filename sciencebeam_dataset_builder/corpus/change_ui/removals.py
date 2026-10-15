"""The removal pages: documents, reasons, review, submitted."""

from collections.abc import Callable, Sequence
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from starlette.concurrency import run_in_threadpool
from starlette.datastructures import FormData, UploadFile

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.change_ui.forms import (
    MAX_CSV_BYTES,
    Draft,
    FormError,
    check_count,
    parse_ids,
    read_csv,
)
from sciencebeam_dataset_builder.corpus.change_ui.pages import (
    PageError,
    Pages,
    Steps,
    digest,
    location_view,
    safe_url,
    text,
)
from sciencebeam_dataset_builder.corpus.lookup import Repo, Result, Status, lookup
from sciencebeam_dataset_builder.corpus.proposal import ProposalError, submit_removals
from sciencebeam_dataset_builder.corpus.validate import (
    RemovalCheck,
    RemovalProposal,
    check_removals,
)

STEPS: Steps = (
    ("type", "Change type"),
    ("documents", "Documents"),
    ("reasons", "Reasons"),
    ("review", "Check and review"),
    ("done", "Submitted"),
)

Checked = tuple[list[RemovalCheck], list[RemovalProposal]]


def routes(pages: Pages, repos: Callable[[], Sequence[Repo]]) -> APIRouter:
    router = APIRouter(prefix="/removals")

    def draft_of(data: FormData) -> Draft:
        try:
            return Draft.from_form(data)
        except FormError as exc:
            raise PageError(400, str(exc)) from exc

    def documents_page(
        request: Request, ids: str, *, error: str = "", status_code: int = 200
    ) -> HTMLResponse:
        return pages.render(
            request,
            "documents.html",
            STEPS,
            "documents",
            ids=ids,
            error=error,
            status_code=status_code,
        )

    async def review(
        request: Request,
        draft: Draft,
        *,
        checked: Checked | None = None,
        error: str = "",
        status_code: int = 200,
    ) -> HTMLResponse:
        user, problem = pages.who()
        if problem:
            return pages.render(
                request, "reasons.html", STEPS, "reasons", draft=draft, status_code=401
            )
        if checked is None:
            checked = await run_in_threadpool(
                check_removals, draft.requests(), repos(), user
            )
        checks, proposals = checked
        return pages.render(
            request,
            "review.html",
            STEPS,
            "review",
            draft=draft,
            checks=[_check(c) for c in checks],
            proposals=[_proposal(p) for p in proposals],
            all_pass=bool(checks) and all(c.ok for c in checks),
            passed=sum(c.ok for c in checks),
            error=error,
            status_code=status_code,
        )

    @router.get("")
    def documents(request: Request) -> HTMLResponse:
        return documents_page(request, "")

    @router.post("/find")
    async def find(request: Request) -> HTMLResponse:
        data = await pages.form(request)
        ids = text(data.get("ids"))
        try:
            queries = parse_ids(ids)
            csv_reasons: dict[str, str] = {}
            upload = data.get("csv")
            if isinstance(upload, UploadFile) and upload.filename:
                csv_ids, csv_reasons = read_csv(await upload.read(MAX_CSV_BYTES + 1))
                queries += csv_ids
            check_count(queries)
        except FormError as exc:
            return documents_page(request, ids, error=str(exc), status_code=400)
        results = await run_in_threadpool(lookup, queries, repos())
        return pages.render(
            request,
            "documents.html",
            STEPS,
            "documents",
            ids="\n".join(queries),
            rows=_rows(results, csv_reasons),
            summary=_summary(results),
        )

    @router.post("/reasons")
    async def reasons(request: Request) -> HTMLResponse:
        draft = draft_of(await pages.form(request))
        if not draft.uids:
            return documents_page(
                request,
                draft.ids,
                error="Tick at least one document, then continue.",
                status_code=400,
            )
        return pages.render(request, "reasons.html", STEPS, "reasons", draft=draft)

    @router.post("/check")
    async def check(request: Request) -> HTMLResponse:
        data = await pages.form(request)
        draft = draft_of(data).without(text(data.get("leave_out")))
        if not draft.uids:
            return documents_page(
                request,
                draft.ids,
                error="Every document has been left out. Find documents again.",
            )
        return await review(request, draft)

    @router.post("/submit")
    async def submit(request: Request) -> HTMLResponse:
        data = await pages.form(request)
        draft = draft_of(data)
        user, problem = pages.who()
        if problem:
            return await review(request, draft)
        if not pages.can_submit:
            return await review(
                request,
                draft,
                error="This is a preview against a local build, so nothing can be "
                "submitted.",
                status_code=403,
            )
        if text(data.get("confirm")) != "yes":
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
            key.removeprefix("digest:"): text(value)
            for key, value in data.multi_items()
            if key.startswith("digest:")
        }
        if {p.repo.repo_id: digest(p.after) for p in proposals} != reviewed:
            return await review(
                request,
                draft,
                checked=(checks, proposals),
                error="The files to propose changed after you reviewed them. Review "
                "them again.",
                status_code=409,
            )
        outcomes = [await run_in_threadpool(_submit, p, checks) for p in proposals]
        return pages.render(
            request,
            "done.html",
            STEPS,
            "done",
            draft=draft,
            outcomes=outcomes,
            all_ok=all(o["ok"] for o in outcomes),
        )

    return router


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
        "pr_url": safe_url(submission.pr_url),
        "title": submission.title,
    }


def _rows(
    results: Sequence[Result], csv_reasons: dict[str, str]
) -> list[dict[str, Any]]:
    """One row per query. Only a document found in one repo, once, can be ticked."""
    seen: set[str] = set()
    rows = []
    for result in results:
        found = result.status is Status.FOUND
        where = location_view(result.locations[0]) if found else None
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
    where = location_view(check.location) if check.location else None
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
        "digest": digest(proposal.after),
    }
