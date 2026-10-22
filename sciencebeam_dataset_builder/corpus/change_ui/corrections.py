"""The content correction pages: document, corrected XML, review, submitted.

The uploaded XML travels from the review to the submission base64 encoded: a browser
rewrites the line breaks of any text field it posts, which would change the file.
"""

import base64
import binascii
from collections.abc import Callable, Sequence

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response
from starlette.concurrency import run_in_threadpool
from starlette.datastructures import UploadFile

from sciencebeam_dataset_builder.corpus import layout
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
    already_open,
    submit_correction,
    xml_diff,
)
from sciencebeam_dataset_builder.corpus.validate import (
    MAX_XML_BYTES,
    ContentCorrectionCheck,
    check_content_correction,
    current_xml,
)

STEPS: Steps = (
    ("type", "Change type"),
    ("document", "Document"),
    ("correction", "Corrected XML"),
    ("review", "Check and review"),
    ("done", "Submitted"),
)

MAX_DIFF_LINES = 2000


def routes(pages: Pages, repos: Callable[[], Sequence[Repo]]) -> APIRouter:
    router = APIRouter(prefix="/corrections/content")

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
            action="/corrections/content/find",
            what="XML",
            error=error,
            status_code=status_code,
        )

    def upload_page(
        request: Request,
        location: Location,
        reason: str,
        *,
        error: str = "",
        status_code: int = 200,
    ) -> HTMLResponse:
        return pages.render(
            request,
            "correction_upload.html",
            STEPS,
            "correction",
            document=location_view(location),
            uid=location.uid,
            reason=reason,
            error=error,
            status_code=status_code,
        )

    def review(
        request: Request,
        check: ContentCorrectionCheck,
        xml: bytes,
        *,
        error: str = "",
        status_code: int = 200,
    ) -> HTMLResponse:
        proposal = check.proposal
        lines, truncated = (
            _diff(check.current, proposal.corrected_after or "")
            if proposal
            else ([], False)
        )
        added = (
            proposal.metadata_after.removeprefix(proposal.metadata_before or "")
            if proposal
            else ""
        )
        return pages.render(
            request,
            "correction_review.html",
            STEPS,
            "review",
            check=check,
            document=location_view(check.location) if check.location else None,
            uid=check.location.uid if check.location else check.query,
            xml=base64.b64encode(xml).decode("ascii"),
            reviewed=proposal_digest(check.proposal),
            diff=lines,
            truncated=truncated,
            max_diff=MAX_DIFF_LINES,
            first=bool(proposal and proposal.seed is not None),
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

    @router.get("/current")
    async def current(uid: str) -> Response:
        """The XML to start a correction from, as a file to save."""
        location, problem = await locate(uid)
        if location is None:
            raise PageError(404, problem)
        try:
            xml = await run_in_threadpool(current_xml, location)
        except (OSError, ValueError) as exc:
            raise PageError(502, f"cannot read the current XML: {exc}") from exc
        return Response(
            xml.encode("utf-8"),
            media_type="application/xml",
            headers={
                "Content-Disposition": f'attachment; filename="{location.id}.xml"'
            },
        )

    @router.post("/find")
    async def find(request: Request) -> HTMLResponse:
        data = await pages.form(request)
        uid = text(data.get("uid")).strip()
        if not uid:
            return document_page(
                request, uid, error="Enter a document id.", status_code=400
            )
        location, problem = await locate(uid)
        if location is None:
            return document_page(request, uid, error=problem)
        return upload_page(request, location, text(data.get("reason")))

    @router.post("/check")
    async def check(request: Request) -> HTMLResponse:
        data = await pages.form(request)
        uid, reason = text(data.get("uid")), text(data.get("reason"))
        location, problem = await locate(uid)
        if location is None:
            raise PageError(400, problem)
        user, unknown = pages.who()
        if unknown:
            return upload_page(request, location, reason, status_code=401)
        upload = data.get("xml")
        if not isinstance(upload, UploadFile) or not upload.filename:
            return upload_page(
                request,
                location,
                reason,
                error="Choose the corrected XML file to upload.",
                status_code=400,
            )
        xml = await upload.read(MAX_XML_BYTES + 1)
        result = await run_in_threadpool(
            check_content_correction, location.uid, reason, xml, repos(), user
        )
        return review(request, result, xml)

    @router.post("/submit")
    async def submit(request: Request) -> HTMLResponse:
        data = await pages.form(request)
        uid, reason = text(data.get("uid")), text(data.get("reason"))
        try:
            xml = base64.b64decode(text(data.get("xml")), validate=True)
        except (binascii.Error, ValueError) as exc:
            raise PageError(
                400, "The uploaded XML did not come back intact. Upload it again."
            ) from exc
        user, unknown = pages.who()
        if unknown:
            raise PageError(401, unknown)
        result = await run_in_threadpool(
            check_content_correction, uid, reason, xml, repos(), user
        )
        if not pages.can_submit:
            return review(
                request,
                result,
                xml,
                error="This is a preview against a local build, so nothing can be "
                "submitted.",
                status_code=403,
            )
        if text(data.get("confirm")) != "yes":
            return review(
                request,
                result,
                xml,
                error="Tick the box to confirm you have reviewed the correction.",
                status_code=400,
            )
        if not result.ok:
            opened = (
                await run_in_threadpool(already_open, [(result.location, xml)])
                if result.location
                else []
            )
            if opened:
                return pages.render(
                    request,
                    "correction_done.html",
                    STEPS,
                    "done",
                    ok=True,
                    already=True,
                    kept="XML",
                    repo_id=opened[0].repo_id,
                    pr_num=opened[0].pr_num,
                    pr_url=safe_url(opened[0].pr_url),
                    title=opened[0].title,
                )
            return review(
                request,
                result,
                xml,
                error="The correction no longer passes the checks. Review it again.",
                status_code=409,
            )
        if proposal_digest(result.proposal) != text(data.get("reviewed")):
            return review(
                request,
                result,
                xml,
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
                problem=str(exc),
            )
        return pages.render(
            request,
            "correction_done.html",
            STEPS,
            "done",
            ok=True,
            kept="XML",
            repo_id=submission.repo_id,
            pr_num=submission.pr_num,
            pr_url=safe_url(submission.pr_url),
            title=submission.title,
        )

    return router


def _diff(before: str, after: str) -> tuple[list[tuple[str, str]], bool]:
    """Unified diff lines with their kind, for the page to colour."""
    lines = [
        ({"+": "add", "-": "del", "@": "hunk"}.get(line[:1], "same"), line)
        for line in xml_diff(before, after)
    ]
    return lines[:MAX_DIFF_LINES], len(lines) > MAX_DIFF_LINES
