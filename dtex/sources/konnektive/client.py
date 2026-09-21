"""Konnektive CRM HTTP client — thin wrapper over `requests`.

Two surfaces:

* `query(path, params)` — for the paginated ``*/query/`` endpoints
  (``order``, ``transactions``, ``purchase``, ``customer``). Konnektive
  paginates with ``page`` (1-based) + ``resultsPerPage`` (max 200) and
  reports ``totalResults`` in every envelope; the walk ends when
  ``page * resultsPerPage >= totalResults`` or a page comes back short.
* `report(path, params)` — one-shot call for report endpoints
  (``transactions/summary``) whose ``message`` is the row list itself.

Auth is a login id + password sent as REQUEST PARAMETERS on every call —
Konnektive has no header scheme. That has one consequence this module is
built around: over ``GET`` the password is part of the URL, and URLs leak
(urllib3 logs the full request line at DEBUG; `requests` exceptions embed
the URL in their message; proxies log it). The client therefore defaults
to ``POST`` with a form-encoded body, which keeps both credentials out of
every URL. ``http_method="GET"`` remains available for an account or proxy
that only accepts query strings, and the error paths below never include a
URL's query string or an exception's text either way.

Response envelope: ``{"result": "SUCCESS", "message": <payload>}`` on
success and ``{"result": "ERROR", "message": "<text>"}`` on failure — under
HTTP 200 in both cases, so the status code alone says nothing. Three kinds
of ERROR are told apart by their text:

* "No <things> matching those parameters could be found" — an EMPTY
  RESULT, not a failure. Konnektive reports an empty window this way; it is
  returned as zero rows.
* Anything that reads as an auth / permission problem — raised
  IMMEDIATELY, never retried. Re-sending bad credentials cannot succeed,
  and repeated failed logins can lock the API user.
* Everything else — treated as transient (Konnektive returns bare ERRORs
  under load) and retried with backoff, bounded by ``max_retries``; then
  raised with the server's own message.

Retry policy for the transport leg: HTTP 429 honors ``Retry-After`` but
still counts as an attempt; 5xx and network-level errors (timeout,
connection reset) back off exponentially; all bounded by ``max_retries``.
Every request carries a hard (connect, read) timeout — a hung response
must fail the run loudly, not stall it for hours.

Credentials never appear in logs or error messages: both fields are
declared ``repr=False``, the client does no logging, and every raised
message is scrubbed of the credential values before it leaves.
"""

from __future__ import annotations

import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import requests

# Connect-leg timeout in seconds. The read leg is configurable
# (`timeout_seconds`) because wide windows on large accounts are slow.
_CONNECT_TIMEOUT: float = 10.0

# Konnektive's hard cap on resultsPerPage.
MAX_PAGE_SIZE: int = 200

# "No orders matching those parameters could be found", "No records
# matching ...", "No transactions found" — an empty window, not an error.
# Anchored at the start (used with `.match`): only a message that OPENS
# with "No ..." counts, so an error that merely mentions "not found" does not.
_EMPTY_RESULT = re.compile(r"no\b.*\b(found|matching)\b", re.IGNORECASE | re.DOTALL)

# Auth / permission failures: never retried.
_AUTH_FAILURE = re.compile(
    r"login|credential|password|authori[sz]|authenticat|permission|"
    r"not allowed|access denied|whitelist|ip address",
    re.IGNORECASE,
)


class KonnektiveError(RuntimeError):
    """The API answered, and the answer was an error that retrying won't fix."""


class KonnektiveAuthError(KonnektiveError):
    """The API rejected the login id / password (or the caller's IP)."""


@dataclass
class KonnektiveClient:
    login_id: str = field(repr=False)
    password: str = field(repr=False)
    base_url: str = "https://api.konnektive.com"
    http_method: str = "POST"
    max_retries: int = 5
    timeout_seconds: float = 120.0
    # Minimum spacing between requests, in seconds. 0 disables pacing.
    min_request_interval: float = 0.0
    _session: requests.Session = field(
        default_factory=requests.Session, init=False, repr=False
    )
    _last_request_at: float = field(default=0.0, init=False, repr=False)

    def __post_init__(self) -> None:
        self.base_url = self.base_url.rstrip("/")
        self.http_method = self.http_method.upper()
        if self.http_method not in ("GET", "POST"):
            raise ValueError(
                f"konnektive: http_method must be GET or POST, got {self.http_method!r}"
            )
        self._session.headers.update({"Accept": "application/json"})

    # -- public surface ------------------------------------------------------

    def query(
        self, path: str, params: dict[str, Any] | None = None
    ) -> Iterator[dict]:
        """Yield every row from a paginated ``*/query/`` endpoint."""
        query = dict(params or {})
        page_size = min(int(query.get("resultsPerPage") or MAX_PAGE_SIZE), MAX_PAGE_SIZE)
        query["resultsPerPage"] = page_size
        page = 1
        while True:
            query["page"] = page
            message = self._call(path, query)
            if message is None:
                return  # empty window
            if not isinstance(message, dict):
                raise KonnektiveError(
                    f"konnektive: unexpected response shape on {path}: expected an "
                    f"object under 'message', got {type(message).__name__}"
                )
            rows = message.get("data") or []
            if not isinstance(rows, list):
                raise KonnektiveError(
                    f"konnektive: unexpected response shape on {path}: expected a "
                    f"list under 'message.data', got {type(rows).__name__}"
                )
            yield from rows
            if not rows or len(rows) < page_size:
                return
            total = _as_int(message.get("totalResults"))
            if total is not None and page * page_size >= total:
                return
            page += 1

    def report(self, path: str, params: dict[str, Any] | None = None) -> list[dict]:
        """One-shot call to a report endpoint. Returns its rows (may be empty)."""
        message = self._call(path, dict(params or {}))
        if message is None:
            return []
        if isinstance(message, dict):
            # Some report types wrap their rows like the query endpoints do.
            data = message.get("data")
            if isinstance(data, list):
                return data
            return [message]
        if isinstance(message, list):
            return message
        raise KonnektiveError(
            f"konnektive: unexpected response shape on {path}: expected a list "
            f"under 'message', got {type(message).__name__}"
        )

    # -- transport -----------------------------------------------------------

    def _call(self, path: str, params: dict[str, Any]) -> Any:
        """One API call with bounded retries. Returns ``message``, or ``None``
        when Konnektive reports that nothing matched."""
        url = f"{self.base_url}/{path.strip('/')}/"
        payload = {"loginId": self.login_id, "password": self.password, **params}
        attempt = 0
        while True:
            self._pace()
            try:
                if self.http_method == "POST":
                    resp = self._session.post(
                        url, data=payload, timeout=(_CONNECT_TIMEOUT, self.timeout_seconds)
                    )
                else:
                    resp = self._session.get(
                        url, params=payload, timeout=(_CONNECT_TIMEOUT, self.timeout_seconds)
                    )
            except requests.exceptions.RequestException as exc:
                if attempt < self.max_retries:
                    time.sleep(min(2**attempt, 60))
                    attempt += 1
                    continue
                # The exception's own text embeds the request URL — which
                # over GET carries the password. Name the type only.
                raise KonnektiveError(
                    f"konnektive: network failure after {self.max_retries} retries "
                    f"on {url} ({type(exc).__name__})"
                ) from None

            if resp.status_code == 429:
                if attempt >= self.max_retries:
                    raise KonnektiveError(
                        f"konnektive: rate-limited after {self.max_retries} retries "
                        f"on {url}; Retry-After={resp.headers.get('Retry-After')}"
                    )
                time.sleep(_retry_after(resp.headers.get("Retry-After")))
                attempt += 1
                continue
            if resp.status_code >= 500 and attempt < self.max_retries:
                time.sleep(min(2**attempt, 60))
                attempt += 1
                continue
            if resp.status_code in (401, 403):
                raise KonnektiveAuthError(
                    f"konnektive: HTTP {resp.status_code} on {url} — check the login "
                    "id / password and the API user's IP allow-list"
                )
            if resp.status_code >= 400:
                raise KonnektiveError(f"konnektive: HTTP {resp.status_code} on {url}")

            try:
                body: Any = resp.json()
            except ValueError:
                if attempt < self.max_retries:
                    time.sleep(min(2**attempt, 60))
                    attempt += 1
                    continue
                raise KonnektiveError(
                    f"konnektive: non-JSON response after {self.max_retries} retries on {url}"
                ) from None

            if not isinstance(body, dict) or "result" not in body:
                raise KonnektiveError(
                    f"konnektive: unexpected response shape on {url}: no 'result' key"
                )
            if str(body.get("result")).upper() == "SUCCESS":
                return body.get("message")

            text = self._scrub(body.get("message"))
            # Auth FIRST: "Invalid login: no user found" must never be read
            # as an empty window — that would be a silent, green, empty sync.
            if _AUTH_FAILURE.search(text):
                raise KonnektiveAuthError(
                    f"konnektive: API rejected the request on {url}: {text!r}"
                )
            if _EMPTY_RESULT.match(text.strip()):
                return None
            if attempt < self.max_retries:
                time.sleep(min(2**attempt, 60))
                attempt += 1
                continue
            raise KonnektiveError(
                f"konnektive: API error after {self.max_retries} retries on {url}: {text!r}"
            )

    def _pace(self) -> None:
        if self.min_request_interval <= 0:
            return
        wait = self._last_request_at + self.min_request_interval - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        self._last_request_at = time.monotonic()

    def _scrub(self, message: Any) -> str:
        """The server's message as text, with credential values removed —
        defence in depth against an error that echoes the request back."""
        text = message if isinstance(message, str) else repr(message)
        for secret in (self.password, self.login_id):
            if secret:
                text = text.replace(secret, "***")
        return text[:500]


def _as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _retry_after(header: str | None) -> float:
    try:
        return max(0.0, min(float(header or 30), 300.0))
    except ValueError:
        return 30.0
