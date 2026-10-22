"""What every change request page shares: the layout, the user and the form token."""

import hashlib
import secrets
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.datastructures import FormData

from sciencebeam_dataset_builder.corpus.change_ui.forms import MAX_DOCUMENTS, MAX_REASON
from sciencebeam_dataset_builder.corpus.lookup import Location
from sciencebeam_dataset_builder.corpus.proposal import ProposalError
from sciencebeam_dataset_builder.corpus.validate import (
    MAX_XML_BYTES,
    CorrectionProposal,
)

HERE = Path(__file__).parent

# A uid and a reason per document, plus a digest per repo and the request's own fields.
MAX_FIELDS = 2 * MAX_DOCUMENTS + 64
# An uploaded XML is carried to the next page base64 encoded, a third larger.
MAX_PART_BYTES = MAX_XML_BYTES * 4 // 3 + 4096

Steps = tuple[tuple[str, str], ...]


class PageError(Exception):
    """A request that cannot go on; shown as its own page."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass(frozen=True)
class Pages:
    identity: Callable[[], str]
    can_submit: bool
    # Every form carries this, so another site cannot post one on the person's behalf.
    csrf: str = field(default_factory=lambda: secrets.token_urlsafe(32))
    templates: Jinja2Templates = field(
        default_factory=lambda: Jinja2Templates(directory=HERE / "templates")
    )

    def who(self) -> tuple[str, str]:
        """The user, or why there is none."""
        try:
            name = self.identity()
        except ProposalError as exc:
            return "", f"{exc}. Set HF_TOKEN or run 'hf auth login', then reload."
        if not name.strip():
            return "", "There is no user to record the change against."
        return name, ""

    def render(
        self,
        request: Request,
        template: str,
        steps: Steps,
        step: str,
        *,
        status_code: int = 200,
        error: str = "",
        **context: Any,
    ) -> HTMLResponse:
        user, problem = self.who()
        keys = [key for key, _ in steps]
        return self.templates.TemplateResponse(
            request,
            template,
            {
                "steps": steps,
                "step_index": keys.index(step) if step in keys else -1,
                "user": user,
                "can_submit": self.can_submit,
                "csrf": self.csrf,
                "error": error or problem,
                "max_documents": MAX_DOCUMENTS,
                "max_reason": MAX_REASON,
                **context,
            },
            status_code=status_code,
        )

    async def form(self, request: Request, *, max_files: int = 1) -> FormData:
        data = await request.form(
            max_files=max_files, max_fields=MAX_FIELDS, max_part_size=MAX_PART_BYTES
        )
        if not secrets.compare_digest(text(data.get("csrf")), self.csrf):
            raise PageError(
                403, "This page has expired. Start again from the first page."
            )
        return data


def text(value: Any) -> str:
    return value if isinstance(value, str) else ""


def digest(*parts: str) -> str:
    return hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()


def proposal_digest(proposal: CorrectionProposal | None) -> str:
    """Everything a correction's pull request would write, so a change is noticed."""
    if proposal is None:
        return ""
    return digest(
        proposal.seed or "", proposal.corrected_after or "", proposal.metadata_after
    )


def safe_url(url: str) -> str:
    return url if url.startswith("https://") else ""


def location_view(location: Location) -> dict[str, Any]:
    metadata = location.metadata
    return {
        "repo_id": location.repo.repo_id,
        "repo_name": location.repo.repo_id.rsplit("/", 1)[-1],
        "tier": location.repo.tier.value,
        "release": location.release,
        "uid": location.uid,
        "split": location.split,
        "parquet_file_name": location.parquet_file_name,
        "licence": metadata.get("licence") or "",
        "doi": metadata.get("doi") or "",
        "pub_date": metadata.get("pub_date") or "",
    }
