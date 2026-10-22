"""The bulk content correction pages: files, review, submitted.

Each file is named for its document, `<uid>.xml`. The documents are checked one by one,
and those in the same repo are proposed together, as one pull request. The files travel
from page to page base64 encoded, as the single correction does.
"""

import base64
import binascii
from collections.abc import Callable, Sequence
from pathlib import PurePath
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from starlette.concurrency import run_in_threadpool
from starlette.datastructures import FormData, UploadFile

from sciencebeam_dataset_builder.corpus.change_ui.forms import (
    MAX_CSV_BYTES,
    MAX_REASON,
    FormError,
    read_csv,
)
from sciencebeam_dataset_builder.corpus.change_ui.pages import (
    PageError,
    Pages,
    Steps,
    digest,
    location_view,
    proposal_digest,
    safe_url,
    text,
)
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.proposal import (
    ProposalError,
    Submission,
    already_open,
    submit_corrections,
    xml_diff,
)
from sciencebeam_dataset_builder.corpus.validate import (
    MAX_XML_BYTES,
    ContentCorrectionCheck,
    check_content_corrections,
)

STEPS: Steps = (
    ("type", "Change type"),
    ("files", "Corrected files"),
    ("review", "Check and review"),
    ("done", "Submitted"),
)

MAX_FILES = 100
MAX_DIFF_LINES = 400


class _Batch:
    """The documents so far: each one's file and reason, in the order they came."""

    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.reasons: dict[str, str] = {}

    @classmethod
    def from_carried(cls, data: FormData) -> "_Batch":
        batch = cls()
        for uid in dict.fromkeys(text(u).strip() for u in data.getlist("uid")):
            try:
                batch.files[uid] = base64.b64decode(
                    text(data.get(f"file:{uid}")), validate=True
                )
            except (binascii.Error, ValueError) as exc:
                raise PageError(
                    400, "A file was changed in transit. Upload again."
                ) from exc
            batch.reasons[uid] = text(data.get(f"reason:{uid}")).strip()
        return batch

    def without(self, uid: str) -> "_Batch":
        batch = _Batch()
        batch.files = {u: f for u, f in self.files.items() if u != uid}
        batch.reasons = {u: r for u, r in self.reasons.items() if u != uid}
        return batch

    def items(self) -> list[tuple[str, str, bytes]]:
        return [(uid, self.reasons[uid], data) for uid, data in self.files.items()]


def routes(pages: Pages, repos: Callable[[], Sequence[Repo]]) -> APIRouter:
    router = APIRouter(prefix="/corrections/content/bulk")

    def files_page(
        request: Request, *, shared: str = "", error: str = "", status_code: int = 200
    ) -> HTMLResponse:
        return pages.render(
            request,
            "bulk_files.html",
            STEPS,
            "files",
            shared=shared,
            max_files=MAX_FILES,
            error=error,
            status_code=status_code,
        )

    async def review(
        request: Request,
        batch: _Batch,
        *,
        checks: list[ContentCorrectionCheck] | None = None,
        notice: str = "",
        error: str = "",
        status_code: int = 200,
    ) -> HTMLResponse:
        user, problem = pages.who()
        if problem:
            return files_page(request, status_code=401)
        if checks is None:
            checks = await run_in_threadpool(
                check_content_corrections, batch.items(), repos(), user
            )
        return pages.render(
            request,
            "bulk_review.html",
            STEPS,
            "review",
            carried=[
                {
                    "uid": uid,
                    "file": base64.b64encode(data).decode("ascii"),
                    "reason": batch.reasons[uid],
                }
                for uid, data in batch.files.items()
            ],
            checks=[_check_view(c) for c in checks],
            repos=_repo_views(checks),
            all_pass=bool(checks) and all(c.ok for c in checks),
            passed=sum(c.ok for c in checks),
            notice=notice,
            error=error,
            status_code=status_code,
        )

    @router.get("")
    def files(request: Request) -> HTMLResponse:
        return files_page(request)

    @router.post("/check")
    async def check(request: Request) -> HTMLResponse:
        data = await pages.form(request, max_files=MAX_FILES + 1)
        if data.getlist("uid"):
            batch = _Batch.from_carried(data).without(text(data.get("leave_out")))
            if not batch.files:
                return files_page(
                    request, error="Every document has been left out. Upload again."
                )
            return await review(request, batch)
        shared = text(data.get("shared")).strip()
        try:
            batch, unmatched = await _uploaded(data, shared)
        except FormError as exc:
            return files_page(request, shared=shared, error=str(exc), status_code=400)
        notice = (
            f"{unmatched} row{'' if unmatched == 1 else 's'} of the CSV name no uploaded "
            f"file and {'was' if unmatched == 1 else 'were'} not used."
            if unmatched
            else ""
        )
        return await review(request, batch, notice=notice)

    @router.post("/submit")
    async def submit(request: Request) -> HTMLResponse:
        data = await pages.form(request)
        batch = _Batch.from_carried(data)
        user, problem = pages.who()
        if problem:
            return await review(request, batch)
        if not pages.can_submit:
            return await review(
                request,
                batch,
                error="This is a preview against a local build, so nothing can be "
                "submitted.",
                status_code=403,
            )
        if text(data.get("confirm")) != "yes":
            return await review(
                request,
                batch,
                error="Tick the box to confirm you have reviewed the corrections.",
                status_code=400,
            )
        checks = await run_in_threadpool(
            check_content_corrections, batch.items(), repos(), user
        )
        if not all(c.ok for c in checks):
            opened = await run_in_threadpool(
                already_open,
                [(c.location, batch.files[c.query]) for c in checks if c.location],
            )
            if opened and all(c.location for c in checks):
                return pages.render(
                    request,
                    "bulk_done.html",
                    STEPS,
                    "done",
                    carried=[],
                    outcomes=[_outcome(s, already=True) for s in opened],
                    all_ok=True,
                )
            return await review(
                request,
                batch,
                checks=checks,
                error="Some documents no longer pass the checks. Review them again.",
                status_code=409,
            )
        reviewed = {
            key.removeprefix("digest:"): text(value)
            for key, value in data.multi_items()
            if key.startswith("digest:")
        }
        grouped = _by_repo(checks)
        if {r: _digest(cs) for r, cs in grouped.items()} != reviewed:
            return await review(
                request,
                batch,
                checks=checks,
                error="What would be proposed changed after you reviewed it. Review it "
                "again.",
                status_code=409,
            )
        outcomes = [
            await run_in_threadpool(_submit, repo_id, cs)
            for repo_id, cs in grouped.items()
        ]
        return pages.render(
            request,
            "bulk_done.html",
            STEPS,
            "done",
            carried=[
                {
                    "uid": uid,
                    "file": base64.b64encode(f).decode("ascii"),
                    "reason": batch.reasons[uid],
                }
                for uid, f in batch.files.items()
            ],
            outcomes=outcomes,
            all_ok=all(o["ok"] for o in outcomes),
        )

    return router


async def _uploaded(data: FormData, shared: str) -> tuple[_Batch, int]:
    """The uploaded files, each named for its document, with each one's reason; and how
    many rows of the CSV named no uploaded file."""
    uploads = [
        u for u in data.getlist("xml") if isinstance(u, UploadFile) and u.filename
    ]
    if not uploads:
        raise FormError("Choose at least one corrected XML file.")
    if len(uploads) > MAX_FILES:
        raise FormError(
            f"That is {len(uploads)} files. Send at most {MAX_FILES} at a time."
        )
    reasons: dict[str, str] = {}
    csv_upload = data.get("csv")
    if isinstance(csv_upload, UploadFile) and csv_upload.filename:
        _, reasons = read_csv(await csv_upload.read(MAX_CSV_BYTES + 1))
    batch = _Batch()
    for upload in uploads:
        name = PurePath(upload.filename or "").name
        if not name.lower().endswith(".xml"):
            raise FormError(f"{name} is not an .xml file.")
        uid = name[: -len(".xml")]
        if uid in batch.files:
            raise FormError(f"{name} is uploaded twice.")
        content = await upload.read(MAX_XML_BYTES + 1)
        if len(content) > MAX_XML_BYTES:
            raise FormError(f"{name} is larger than {MAX_XML_BYTES // 1_000_000} MB.")
        batch.files[uid] = content
        batch.reasons[uid] = reasons.get(uid, "").strip() or shared
        if len(batch.reasons[uid]) > MAX_REASON:
            raise FormError(f"A reason can be at most {MAX_REASON} characters.")
    return batch, sum(1 for uid in reasons if uid not in batch.files)


def _by_repo(
    checks: Sequence[ContentCorrectionCheck],
) -> dict[str, list[ContentCorrectionCheck]]:
    grouped: dict[str, list[ContentCorrectionCheck]] = {}
    for check in checks:
        if check.proposal is not None:
            grouped.setdefault(check.proposal.repo.repo_id, []).append(check)
    return grouped


def _digest(checks: Sequence[ContentCorrectionCheck]) -> str:
    return digest(*(proposal_digest(c.proposal) for c in checks))


def _submit(repo_id: str, checks: Sequence[ContentCorrectionCheck]) -> dict[str, Any]:
    """One repo's outcome; a failure here leaves the other repos' pull requests be."""
    try:
        submission = submit_corrections(checks)
    except ProposalError as exc:
        return {"repo_id": repo_id, "ok": False, "error": str(exc)}
    return _outcome(submission)


def _outcome(submission: Submission, *, already: bool = False) -> dict[str, Any]:
    return {
        "repo_id": submission.repo_id,
        "ok": True,
        "already": already,
        "pr_num": submission.pr_num,
        "pr_url": safe_url(submission.pr_url),
        "title": submission.title,
    }


def _check_view(check: ContentCorrectionCheck) -> dict[str, Any]:
    where = location_view(check.location) if check.location else None
    return {
        "query": check.query,
        "uid": where["uid"] if where else check.query,
        "location": where,
        "ok": check.ok,
        "errors": list(check.errors),
        "warnings": list(check.warnings),
        "reason": check.reason,
    }


def _repo_views(checks: Sequence[ContentCorrectionCheck]) -> list[dict[str, Any]]:
    views = []
    for repo_id, repo_checks in _by_repo(checks).items():
        proposals = [c.proposal for c in repo_checks if c.proposal is not None]
        documents = []
        for check, proposal in zip(repo_checks, proposals, strict=True):
            lines = [
                ({"+": "add", "-": "del", "@": "hunk"}.get(line[:1], "same"), line)
                for line in xml_diff(check.current, proposal.corrected_after or "")
            ]
            documents.append(
                {
                    "id": proposal.id,
                    "diff": lines[:MAX_DIFF_LINES],
                    "truncated": len(lines) > MAX_DIFF_LINES,
                }
            )
        views.append(
            {
                "repo_id": repo_id,
                "tier": proposals[0].repo.tier.value,
                "documents": documents,
                "first": sum(p.seed is not None for p in proposals),
                "digest": _digest(repo_checks),
            }
        )
    return views
