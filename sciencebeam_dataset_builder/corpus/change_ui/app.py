"""The change request pages: one form per step, rendered on the server.

Each page carries the request so far in hidden fields. Every check reads the repos
afresh, and a submission checks again before opening anything, so what is opened is
what the person reviewed or nothing at all.
"""

from collections.abc import Callable, Sequence

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.trustedhost import TrustedHostMiddleware

from sciencebeam_dataset_builder.corpus.change_ui import (
    additions,
    corrections,
    metadata,
    removals,
)
from sciencebeam_dataset_builder.corpus.change_ui.additions import OpenFrom, hub_from
from sciencebeam_dataset_builder.corpus.change_ui.pages import HERE, PageError, Pages
from sciencebeam_dataset_builder.corpus.lookup import Repo

# Only a page served from this machine may post: a site that rebinds its own name to
# 127.0.0.1 arrives with a different Host and is refused.
LOCAL_HOSTS = ("127.0.0.1", "localhost")


def create_app(
    repos: Callable[[], Sequence[Repo]],
    identity: Callable[[], str],
    can_submit: bool,
    open_from: OpenFrom = hub_from,
) -> FastAPI:
    app = FastAPI(
        title="Corpus change requests",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=list(LOCAL_HOSTS))
    app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")
    pages = Pages(identity, can_submit)
    app.state.csrf = pages.csrf

    @app.exception_handler(PageError)
    async def page_error(request: Request, exc: PageError) -> HTMLResponse:
        return pages.render(
            request, "error.html", (), "", error=str(exc), status_code=exc.status_code
        )

    @app.get("/")
    def start(request: Request) -> HTMLResponse:
        return pages.render(request, "type.html", removals.STEPS, "type")

    app.include_router(removals.routes(pages, repos))
    app.include_router(corrections.routes(pages, repos))
    app.include_router(metadata.routes(pages, repos))
    app.include_router(additions.routes(pages, repos, open_from))
    return app
