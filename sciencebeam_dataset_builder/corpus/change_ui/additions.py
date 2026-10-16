"""The add documents pages: documents, reasons, review, submitted.

Documents come from one commit of the harvest repo, or of another repo of the same corpus
for a move; each one's licence decides which repo it goes to.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from starlette.concurrency import run_in_threadpool
from starlette.datastructures import FormData

from sciencebeam_dataset_builder.corpus import layout
from sciencebeam_dataset_builder.corpus.addition_rows import (
    HARVEST_REPO_ID,
    AdditionRowsError,
)
from sciencebeam_dataset_builder.corpus.change_ui.forms import (
    Draft,
    FormError,
    check_count,
    parse_ids,
)
from sciencebeam_dataset_builder.corpus.change_ui.pages import (
    PageError,
    Pages,
    Steps,
    digest,
    safe_url,
    text,
)
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.proposal import ProposalError, submit_additions
from sciencebeam_dataset_builder.corpus.registry import CORPORA, Corpus
from sciencebeam_dataset_builder.corpus.repo_reader import (
    HubRepo,
    RepoReader,
    RepoReadError,
)
from sciencebeam_dataset_builder.corpus.validate_additions import (
    AdditionCheck,
    AdditionProposal,
    check_additions,
    new_in,
)

STEPS: Steps = (
    ("type", "Change type"),
    ("documents", "Documents"),
    ("reasons", "Reasons"),
    ("review", "Check and review"),
    ("done", "Submitted"),
)

# Reads a repo at a revision, and names the commit that revision is.
OpenFrom = Callable[[str, str], tuple[RepoReader, str]]

Checked = tuple[list[AdditionCheck], list[AdditionProposal]]


def hub_from(repo_id: str, revision: str) -> tuple[RepoReader, str]:
    reader = HubRepo(repo_id, revision)
    return reader, reader.commit


@dataclass(frozen=True)
class _From:
    """Where the documents come from, pinned once the documents page is sent."""

    corpus: Corpus
    repo_id: str
    commit: str

    @classmethod
    def from_form(cls, data: FormData) -> "_From":
        return cls(
            _corpus(text(data.get("corpus"))),
            text(data.get("from_repo")),
            text(data.get("from_commit")),
        )


def routes(
    pages: Pages, repos: Callable[[], Sequence[Repo]], open_from: OpenFrom
) -> APIRouter:
    router = APIRouter(prefix="/additions")

    def documents_page(
        request: Request,
        data: FormData | None = None,
        *,
        error: str = "",
        status_code: int = 200,
    ) -> HTMLResponse:
        def field(name: str, default: str = "") -> str:
            return text(data.get(name)) if data is not None else default

        return pages.render(
            request,
            "additions_documents.html",
            STEPS,
            "documents",
            corpora=CORPORA.values(),
            from_repos=[HARVEST_REPO_ID, *_corpus_repo_ids()],
            harvest=HARVEST_REPO_ID,
            corpus=field("corpus"),
            from_repo=field("from_repo") or HARVEST_REPO_ID,
            revision=field("from_commit") or field("revision") or "main",
            ids=field("ids"),
            all_new=field("all_new") == "yes",
            error=error,
            status_code=status_code,
        )

    def where(data: FormData) -> _From:
        try:
            return _From.from_form(data)
        except FormError as exc:
            raise PageError(400, str(exc)) from exc

    def draft_of(data: FormData) -> Draft:
        try:
            return Draft.from_form(data)
        except FormError as exc:
            raise PageError(400, str(exc)) from exc

    def reasons_page(
        request: Request,
        source: _From,
        draft: Draft,
        *,
        error: str = "",
        status_code: int = 200,
    ) -> HTMLResponse:
        return pages.render(
            request,
            "additions_reasons.html",
            STEPS,
            "reasons",
            source=source,
            draft=draft,
            error=error,
            status_code=status_code,
        )

    async def review(
        request: Request,
        source: _From,
        draft: Draft,
        *,
        checked: Checked | None = None,
        error: str = "",
        status_code: int = 200,
    ) -> HTMLResponse:
        user, problem = pages.who()
        if problem:
            return reasons_page(request, source, draft, status_code=401)
        if checked is None:
            try:
                checked = await run_in_threadpool(
                    _check, open_from, source, draft, repos(), user
                )
            except (AdditionRowsError, RepoReadError) as exc:
                return reasons_page(
                    request, source, draft, error=str(exc), status_code=502
                )
        checks, proposals = checked
        return pages.render(
            request,
            "additions_review.html",
            STEPS,
            "review",
            source=source,
            draft=draft,
            checks=[_check_view(c) for c in checks],
            proposals=[_proposal_view(p) for p in proposals],
            all_pass=bool(checks) and all(c.ok for c in checks),
            passed=sum(c.ok for c in checks),
            error=error,
            status_code=status_code,
        )

    @router.get("")
    def documents(request: Request) -> HTMLResponse:
        return documents_page(request)

    @router.post("/edit")
    async def edit(request: Request) -> HTMLResponse:
        return documents_page(request, await pages.form(request))

    @router.post("/reasons")
    async def reasons(request: Request) -> HTMLResponse:
        data = await pages.form(request)
        draft = draft_of(data)
        if draft.uids:
            return reasons_page(request, where(data), draft)
        revision = text(data.get("revision")).strip() or "main"
        try:
            corpus = _corpus(text(data.get("corpus")))
            from_repo = text(data.get("from_repo"))
            reader, commit = await run_in_threadpool(open_from, from_repo, revision)
            if text(data.get("all_new")) == "yes":
                ids = await run_in_threadpool(
                    new_in, corpus, from_repo, reader, repos()
                )
                if not ids:
                    raise FormError(
                        f"{from_repo} at {revision} holds nothing new for {corpus.name}."
                    )
            else:
                ids = parse_ids(text(data.get("ids")))
            check_count(ids)
        except (FormError, AdditionRowsError, RepoReadError) as exc:
            return documents_page(request, data, error=str(exc), status_code=400)
        uids = tuple(dict.fromkeys(ids))
        return reasons_page(
            request,
            _From(corpus, from_repo, commit),
            Draft(ids="\n".join(uids), uids=uids, own={u: "" for u in uids}),
        )

    @router.post("/check")
    async def check(request: Request) -> HTMLResponse:
        data = await pages.form(request)
        source = where(data)
        draft = draft_of(data).without(text(data.get("leave_out")))
        if not draft.uids:
            return documents_page(
                request,
                data,
                error="Every document has been left out. Choose documents again.",
            )
        return await review(request, source, draft)

    @router.post("/submit")
    async def submit(request: Request) -> HTMLResponse:
        data = await pages.form(request)
        source, draft = where(data), draft_of(data)
        user, problem = pages.who()
        if problem:
            return await review(request, source, draft)
        if not pages.can_submit:
            return await review(
                request,
                source,
                draft,
                error="This is a preview against a local build, so nothing can be "
                "submitted.",
                status_code=403,
            )
        if text(data.get("confirm")) != "yes":
            return await review(
                request,
                source,
                draft,
                error="Tick the box to confirm you have reviewed the additions.",
                status_code=400,
            )
        try:
            checks, proposals = await run_in_threadpool(
                _check, open_from, source, draft, repos(), user
            )
        except (AdditionRowsError, RepoReadError) as exc:
            raise PageError(502, str(exc)) from exc
        if not all(c.ok for c in checks):
            return await review(
                request,
                source,
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
                source,
                draft,
                checked=(checks, proposals),
                error="The files to propose changed after you reviewed them. Review "
                "them again.",
                status_code=409,
            )
        outcomes = [await run_in_threadpool(_submit, p, checks) for p in proposals]
        return pages.render(
            request,
            "additions_done.html",
            STEPS,
            "done",
            source=source,
            draft=draft,
            outcomes=outcomes,
            all_ok=all(o["ok"] for o in outcomes),
        )

    return router


def _corpus(name: str) -> Corpus:
    if name not in CORPORA:
        raise FormError("Choose the corpus the documents come from.")
    return CORPORA[name]


def _corpus_repo_ids() -> list[str]:
    return [
        corpus.repo_id(tier) for corpus in CORPORA.values() for tier in corpus.tiers
    ]


def _check(
    open_from: OpenFrom,
    source: _From,
    draft: Draft,
    repos: Sequence[Repo],
    user: str,
) -> Checked:
    reader, _ = open_from(source.repo_id, source.commit)
    return check_additions(
        source.corpus,
        source.repo_id,
        source.commit,
        reader,
        draft.requests(),
        repos,
        user,
    )


def _submit(
    proposal: AdditionProposal, checks: Sequence[AdditionCheck]
) -> dict[str, Any]:
    """One repo's outcome; a failure here leaves the other repos' pull requests be."""
    repo_id = proposal.repo.repo_id
    try:
        submission = submit_additions(proposal, checks)
    except ProposalError as exc:
        return {"repo_id": repo_id, "ok": False, "error": str(exc)}
    return {
        "repo_id": repo_id,
        "ok": True,
        "pr_num": submission.pr_num,
        "pr_url": safe_url(submission.pr_url),
        "title": submission.title,
    }


def _check_view(check: AdditionCheck) -> dict[str, Any]:
    return {
        "query": check.query,
        "id": check.id or check.query,
        "ok": check.ok,
        "errors": list(check.errors),
        "warnings": list(check.warnings),
        "split": check.split,
        "licence": check.licence,
        "repo_name": check.repo_id.rsplit("/", 1)[-1],
        "reason": check.reason,
    }


def _proposal_view(proposal: AdditionProposal) -> dict[str, Any]:
    added = proposal.after.removeprefix(proposal.before or "")
    return {
        "repo_id": proposal.repo.repo_id,
        "tier": proposal.repo.tier.value,
        "path": layout.ADDITIONS_PATH,
        "count": len(proposal.additions),
        "before": proposal.before,
        "added_lines": added.rstrip("\n").split("\n"),
        "digest": digest(proposal.after),
    }
