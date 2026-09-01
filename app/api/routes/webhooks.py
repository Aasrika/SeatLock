"""POST /api/webhooks/payment -- thin router, same philosophy as every
other route module in this codebase: no business logic here, just
wiring app/payments/ingest.py's ingest_webhook() to HTTP. Metrics are
incremented inside ingest_webhook itself, not here -- see that
function's own docstring.

`await request.body()` returns the RAW, unparsed bytes -- captured here
and passed straight into ingest_webhook() (which verifies the HMAC
signature over exactly these bytes BEFORE calling json.loads() on
anything) rather than letting FastAPI parse a Pydantic body model first.
A Pydantic-parsed body would already have discarded the original byte
sequence (re-serializing it for signature comparison is not the same
input the provider actually signed -- see app/payments/signature.py's
module docstring), so this route deliberately does NOT declare a
request body model.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Header, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.infra.config import settings
from app.infra.db import get_session
from app.payments.ingest import (
    Accepted,
    Duplicate,
    MalformedPayload,
    SignatureInvalid,
    Unresolved,
    ingest_webhook,
)

router = APIRouter()


@router.post("/webhooks/payment", status_code=status.HTTP_200_OK)
async def receive_payment_webhook(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
    x_signature: Annotated[str | None, Header(alias="X-Signature")] = None,
) -> JSONResponse:
    raw_body = await request.body()
    now = datetime.now(UTC)

    try:
        outcome = await ingest_webhook(
            session, raw_body, x_signature, settings.webhook_hmac_secret, now
        )
    except MalformedPayload as exc:
        # The signature verified (this IS the provider) but the payload
        # itself is unusable -- see MalformedPayload's own docstring for
        # why this is 400, not the 200 every other outcome here gets.
        return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content={"reason": str(exc)})

    if isinstance(outcome, SignatureInvalid):
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED, content={"reason": "invalid_signature"}
        )
    if isinstance(outcome, Duplicate):
        return JSONResponse(status_code=status.HTTP_200_OK, content={"status": "duplicate"})
    if isinstance(outcome, Unresolved):
        return JSONResponse(status_code=status.HTTP_200_OK, content={"status": "unresolved"})

    assert isinstance(outcome, Accepted)  # exhaustive: the only case left
    return JSONResponse(status_code=status.HTTP_200_OK, content={"status": "accepted"})
