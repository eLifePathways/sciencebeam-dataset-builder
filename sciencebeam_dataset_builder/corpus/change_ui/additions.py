"""The add documents pages: documents, then the checks and the build that adds them.

No pull request: a document arrives through `build_cli`, run where the snapshot is.
"""

from collections.abc import Callable, Sequence

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from starlette.concurrency import run_in_threadpool

from sciencebeam_dataset_builder.corpus.additions import (
    AdditionError,
    BuildInputs,
    check_additions,
)
from sciencebeam_dataset_builder.corpus.change_ui.forms import (
    FormError,
    check_count,
    parse_ids,
)
from sciencebeam_dataset_builder.corpus.change_ui.pages import Pages, Steps, text
from sciencebeam_dataset_builder.corpus.lookup import Repo
from sciencebeam_dataset_builder.corpus.registry import CORPORA, Corpus

STEPS: Steps = (
    ("type", "Change type"),
    ("documents", "Documents"),
    ("review", "Checks and next step"),
)

# The other documents a build would add are listed up to this many.
MAX_LISTED = 20


def routes(
    pages: Pages,
    repos: Callable[[], Sequence[Repo]],
    inputs: BuildInputs | None,
) -> APIRouter:
    router = APIRouter(prefix="/additions")

    def documents_page(
        request: Request,
        corpus: str,
        ids: str,
        *,
        error: str = "",
        status_code: int = 200,
    ) -> HTMLResponse:
        return pages.render(
            request,
            "additions_documents.html",
            STEPS,
            "documents",
            corpora=CORPORA.values(),
            corpus=corpus,
            ids=ids,
            inputs=inputs,
            error=error,
            status_code=status_code,
        )

    @router.get("")
    def documents(request: Request) -> HTMLResponse:
        return documents_page(request, "", "")

    @router.post("/check")
    async def check(request: Request) -> HTMLResponse:
        data = await pages.form(request)
        name, ids = text(data.get("corpus")), text(data.get("ids"))
        if inputs is None:
            return documents_page(request, name, ids, status_code=409)
        try:
            corpus = _corpus(name)
            queries = parse_ids(ids)
            check_count(queries)
            checks, plans = await run_in_threadpool(
                check_additions, corpus, queries, repos(), inputs
            )
        except (FormError, AdditionError) as exc:
            return documents_page(request, name, ids, error=str(exc), status_code=400)
        return pages.render(
            request,
            "additions_review.html",
            STEPS,
            "review",
            ids=ids,
            checks=checks,
            plans=plans,
            max_listed=MAX_LISTED,
            passed=sum(c.ok for c in checks),
        )

    return router


def _corpus(name: str) -> Corpus:
    if name not in CORPORA:
        raise FormError("Choose the corpus the documents come from.")
    return CORPORA[name]
