# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""HTTP helpers for Google REST APIs — retries, readable errors, downloads.

Both Google sources talk to ``sheets.googleapis.com`` / ``www.googleapis.com``
through an ``AuthorizedSession`` (see :mod:`.auth`). This module wraps its
``request`` with:

* retries with exponential backoff on 429 / 5xx and on Drive's 403
  ``rateLimitExceeded`` / ``userRateLimitExceeded`` (Drive signals quota with
  403, not 429), honouring ``Retry-After``;
* errors that say what to check — a 404 on a file you cannot see and a 403
  from a disabled API look alike from the outside.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from typing import IO, Any

MAX_RETRIES = 5
TIMEOUT_SECONDS = 120.0
_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
_RATE_LIMIT_REASONS = frozenset({"rateLimitExceeded", "userRateLimitExceeded"})

# Indirection so tests can skip the real backoff sleeps.
sleep: Callable[[float], None] = time.sleep


class GoogleApiError(RuntimeError):
    """A Google API call failed for good (after retries, or not retryable)."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


def request_json(
    session: Any,
    url: str,
    params: Mapping[str, Any] | list[tuple[str, Any]] | None = None,
    *,
    what: str,
) -> dict[str, Any]:
    """GET ``url`` and return the decoded JSON body, retrying transient failures."""
    response = _request(session, url, params, what=what, stream=False)
    try:
        body = response.json()
    finally:
        response.close()
    if not isinstance(body, dict):
        raise GoogleApiError(response.status_code, f"{what}: unexpected response body")
    return body


def download(
    session: Any,
    url: str,
    params: Mapping[str, Any] | None,
    sink: IO[bytes],
    *,
    what: str,
) -> None:
    """Stream the body of GET ``url`` into ``sink`` (a binary file object)."""
    response = _request(session, url, params, what=what, stream=True)
    try:
        for chunk in response.iter_content(chunk_size=1 << 20):
            if chunk:
                sink.write(chunk)
    finally:
        response.close()
    sink.seek(0)


def _request(
    session: Any,
    url: str,
    params: Mapping[str, Any] | list[tuple[str, Any]] | None,
    *,
    what: str,
    stream: bool,
) -> Any:
    attempt = 0
    while True:
        response = session.request(
            "GET", url, params=params, timeout=TIMEOUT_SECONDS, stream=stream
        )
        status = int(response.status_code)
        if status < 400:
            return response
        detail, reason = _error_detail(response)
        response.close()
        retryable = status in _RETRY_STATUSES or (status == 403 and reason in _RATE_LIMIT_REASONS)
        if retryable and attempt < MAX_RETRIES:
            sleep(_backoff(attempt, response))
            attempt += 1
            continue
        raise GoogleApiError(status, _explain(status, what, detail))


def _backoff(attempt: int, response: Any) -> float:
    retry_after = (response.headers or {}).get("Retry-After")
    if retry_after:
        try:
            return min(float(retry_after), 60.0)
        except ValueError:
            pass
    return float(min(2**attempt, 32))


def _error_detail(response: Any) -> tuple[str, str]:
    """(message, first reason) from a Google error body; best effort."""
    try:
        body = response.json()
    except Exception:  # noqa: BLE001 — a non-JSON error page
        return (str(getattr(response, "text", "") or "")[:300], "")
    error = body.get("error") if isinstance(body, dict) else None
    if not isinstance(error, dict):
        return ("", "")
    message = str(error.get("message") or "")
    reason = ""
    errors = error.get("errors")
    if isinstance(errors, list) and errors and isinstance(errors[0], dict):
        reason = str(errors[0].get("reason") or "")
    if not reason:
        for item in error.get("details") or []:
            if isinstance(item, dict) and item.get("reason"):
                reason = str(item["reason"])
                break
    return (message, reason)


def _explain(status: int, what: str, detail: str) -> str:
    base = f"{what} failed: HTTP {status}"
    if detail:
        base += f" — {detail}"
    if status == 404:
        return (
            f"{base}. The file does not exist or is not shared with the "
            f"credentials' identity (share it with the service account's email, "
            f"or with the user behind Application Default Credentials)."
        )
    if status in (401, 403):
        return (
            f"{base}. Check that the credentials can read the file, that the "
            f"Google Sheets / Drive API is enabled in the credentials' project, "
            f"and that user ADC was created with the Sheets/Drive read-only "
            f"scopes (see the connector README)."
        )
    return base
