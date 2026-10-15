"""The metadata correction pages: document, corrected values, review, submitted."""

from collections.abc import Callable, Mapping, Sequence

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from starlette.concurrency import run_in_threadpool
from starlette.datastructures import FormData

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.change_files import EDITABLE_FIELDS
from sciencebeam_dataset_builder.corpus.change_ui.pages import (
    PageError,
    Pages,
    Steps,
    location_view,
    proposal_digest,
    safe_url,
    text,
)
from sciencebeam_dataset_builder.corpus.lookup import Location, Repo, Status, lookup
from sciencebeam_dataset_builder.corpus.proposal import (
    ProposalError,
    submit_correction,
)
from sciencebeam_dataset_builder.corpus.validate import (
    MetadataCorrectionCheck,
    check_metadata_correction,
    current_metadata,
)

STEPS: Steps = (
    ("type", "Change type"),
    ("document", "Document"),
    ("fields", "Corrected values"),
    ("review", "Check and review"),
    ("done", "Submitted"),
)

_TIMESTAMP = "With its time zone, like 2026-10-01T09:14:22+00:00."

# Field -> (label, hint), in the order the fields are shown.
FIELDS: dict[str, tuple[str, str]] = {
    "doi": ("DOI", "Like 10.1101/2022.01.01.000000. A doi.org link is fine."),
    "version": ("Version", "One short word, like 2."),
    "pub_date": ("Publication date", "2022-10-31, 2022-10 or 2022."),
    "xml_source_url": ("XML source URL", "Where the XML was retrieved from."),
    "xml_downloaded_at": ("XML retrieved at", _TIMESTAMP),
    "pdf_source_url": ("PDF source URL", "Where the PDF was retrieved from."),
    "pdf_downloaded_at": ("PDF retrieved at", _TIMESTAMP),
}


def routes(pages: Pages, repos: Callable[[], Sequence[Repo]]) -> APIRouter:
    router = APIRouter(prefix="/corrections/metadata")

    async def locate(query: str) -> tuple[Location | None, str]:
        [result] = await run_in_threadpool(lookup, [query], repos())
        if result.status is Status.FOUND:
            return result.locations[0], ""
        return None, f"{result.status.value}: {result.detail}"

    def document_page(
        request: Request, uid: str, *, error: str = "", status_code: int = 200
    ) -> HTMLResponse:
        return pages.render(
            request,
            "correction_document.html",
            STEPS,
            "document",
            uid=uid,
            action="/corrections/metadata/find",
            what="metadata",
            error=error,
            status_code=status_code,
        )

    def fields_page(
        request: Request,
        location: Location,
        values: Mapping[str, str | None],
        reason: str,
        *,
        error: str = "",
        status_code: int = 200,
    ) -> HTMLResponse:
        return pages.render(
            request,
            "metadata_fields.html",
            STEPS,
            "fields",
            document=location_view(location),
            uid=location.uid,
            reason=reason,
            fields=[
                {"name": name, "label": label, "hint": hint, "value": values.get(name)}
                for name, (label, hint) in FIELDS.items()
            ],
            error=error,
            status_code=status_code,
        )

    def review(
        request: Request,
        check: MetadataCorrectionCheck,
        values: Mapping[str, str],
        *,
        error: str = "",
        status_code: int = 200,
    ) -> HTMLResponse:
        proposal = check.proposal
        added = (
            proposal.metadata_after.removeprefix(proposal.metadata_before or "")
            if proposal
            else ""
        )
        return pages.render(
            request,
            "metadata_review.html",
            STEPS,
            "review",
            check=check,
            document=location_view(check.location) if check.location else None,
            uid=check.location.uid if check.location else check.query,
            values=values,
            rows=[
                {
                    "label": FIELDS[name][0],
                    "now": check.current.get(name),
                    "corrected": value,
                }
                for name, value in check.changes.items()
            ],
            reviewed=proposal_digest(proposal),
            new_metadata=bool(proposal and proposal.metadata_before is None),
            metadata_added=added.rstrip("\n").split("\n") if added else [],
            paths=layout.correction_paths(check.location.id)
            if check.location
            else None,
            error=error,
            status_code=status_code,
        )

    @router.get("")
    def document(request: Request) -> HTMLResponse:
        return document_page(request, "")

    @router.post("/find")
    async def find(request: Request) -> HTMLResponse:
        """The values come from the dataset, or from the review when going back."""
        data = await pages.form(request)
        uid = text(data.get("uid")).strip()
        if not uid:
            return document_page(
                request, uid, error="Enter a document id.", status_code=400
            )
        location, problem = await locate(uid)
        if location is None:
            return document_page(request, uid, error=problem)
        values: Mapping[str, str | None] = _values(data)
        if not values:
            try:
                values = await run_in_threadpool(current_metadata, location)
            except (OSError, ValueError) as exc:
                raise PageError(502, f"cannot read the current values: {exc}") from exc
        return fields_page(request, location, values, text(data.get("reason")))

    @router.post("/check")
    async def check(request: Request) -> HTMLResponse:
        data = await pages.form(request)
        uid, reason, values = (
            text(data.get("uid")),
            text(data.get("reason")),
            _values(data),
        )
        user, unknown = pages.who()
        if unknown:
            location, problem = await locate(uid)
            if location is None:
                raise PageError(400, problem)
            return fields_page(request, location, values, reason, status_code=401)
        result = await run_in_threadpool(
            check_metadata_correction, uid, reason, values, repos(), user
        )
        return review(request, result, values)

    @router.post("/submit")
    async def submit(request: Request) -> HTMLResponse:
        data = await pages.form(request)
        uid, reason, values = (
            text(data.get("uid")),
            text(data.get("reason")),
            _values(data),
        )
        user, unknown = pages.who()
        if unknown:
            raise PageError(401, unknown)
        result = await run_in_threadpool(
            check_metadata_correction, uid, reason, values, repos(), user
        )
        if not pages.can_submit:
            return review(
                request,
                result,
                values,
                error="This is a preview against a local build, so nothing can be "
                "submitted.",
                status_code=403,
            )
        if text(data.get("confirm")) != "yes":
            return review(
                request,
                result,
                values,
                error="Tick the box to confirm you have reviewed the correction.",
                status_code=400,
            )
        if not result.ok:
            return review(
                request,
                result,
                values,
                error="The correction no longer passes the checks. Review it again.",
                status_code=409,
            )
        if proposal_digest(result.proposal) != text(data.get("reviewed")):
            return review(
                request,
                result,
                values,
                error="What would be proposed changed after you reviewed it. Review it "
                "again.",
                status_code=409,
            )
        try:
            submission = await run_in_threadpool(submit_correction, result)
        except ProposalError as exc:
            return pages.render(
                request,
                "correction_done.html",
                STEPS,
                "done",
                ok=False,
                kept="metadata",
                problem=str(exc),
            )
        return pages.render(
            request,
            "correction_done.html",
            STEPS,
            "done",
            ok=True,
            kept="metadata",
            repo_id=submission.repo_id,
            pr_num=submission.pr_num,
            pr_url=safe_url(submission.pr_url),
            title=submission.title,
        )

    return router


def _values(data: FormData) -> dict[str, str]:
    """The fields the form carried. A field it did not carry is left as it is."""
    return {
        name: text(data.get(f"field:{name}"))
        for name in EDITABLE_FIELDS
        if f"field:{name}" in data
    }
