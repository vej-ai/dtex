# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Intercom HTTP client — Bearer auth, version pin, retries, rate limiting.

A small wrapper around :mod:`requests` that the Intercom ``@stream``
functions in :mod:`source` use to talk to ``api.intercom.io`` (or the EU /
AU hosts). Plain module — no decorators, no dtex contract — exactly the
"API client" role docs/04 assigns to ``client.py``.

Concerns handled here so the extract loops stay about extraction:

* **Auth + version.** ``Authorization: Bearer <token>`` and
  ``Intercom-Version: <version>`` on a long-lived :class:`requests.Session`.
* **Rate limiting.** A token bucket smooths traffic to ``requests_per_second``;
  a ``429`` waits for Intercom's ``X-RateLimit-Reset`` (unix seconds) or
  ``Retry-After``, capped at ``rate_limit_max_wait_seconds``, then retries —
  counted against ``max_retries``.
* **Retry / backoff.** ``5xx`` and connection errors retry with exponential
  backoff; any other ``4xx`` raises :class:`IntercomAPIError` immediately
  with Intercom's own ``[code] message`` text.
* **Pagination.** :meth:`search` walks ``POST /<resource>/search`` pages
  via ``pages.next.starting_after``; :meth:`list_cursor` does the same for
  cursor-based ``GET`` lists; :meth:`list_pages` walks page-number lists
  (articles); :meth:`scroll` walks ``GET /companies/scroll``.
* **Secret redaction.** The token is set once on the Session and never
  appears in a log line or an exception message.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any

import requests

HOSTS: dict[str, str] = {
    "us": "https://api.intercom.io",
    "eu": "https://api.eu.intercom.io",
    "au": "https://api.au.intercom.io",
}

_STATUS_RATE_LIMITED = 429
_STATUS_SERVER_ERROR_FLOOR = 500
_STATUS_CLIENT_ERROR_FLOOR = 400

_MAX_PER_PAGE = 150


class IntercomAPIError(Exception):
    """An Intercom API call failed — a non-retryable 4xx or exhausted retries.

    Carries the HTTP ``status``; the message includes Intercom's
    ``errors[].code`` / ``message`` when the body carried them. The access
    token is never part of the message.
    """

    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"Intercom API error {status}: {message}")
        self.status = status


def _error_text(response: requests.Response) -> str:
    """``[code] message; [code] message`` from an Intercom error body, else the raw text."""
    try:
        body = response.json()
    except ValueError:
        return response.text[:400]
    errors = body.get("errors") if isinstance(body, dict) else None
    if isinstance(errors, list) and errors:
        parts = []
        for err in errors:
            if isinstance(err, dict):
                parts.append(f"[{err.get('code')}] {err.get('message')}")
        if parts:
            return "; ".join(parts)
    return response.text[:400]


@dataclass
class _TokenBucket:
    """A minimal token-bucket rate limiter shared across a client's requests."""

    rate: float
    capacity: float
    sleep: Any = time.sleep
    _tokens: float = field(default=0.0, init=False)
    _last: float = field(default=0.0, init=False)

    def __post_init__(self) -> None:
        self._tokens = self.capacity
        self._last = time.monotonic()

    def acquire(self) -> None:
        if self.rate <= 0:
            return
        while True:
            now = time.monotonic()
            self._tokens = min(self.capacity, self._tokens + (now - self._last) * self.rate)
            self._last = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return
            self.sleep((1.0 - self._tokens) / self.rate)


class IntercomClient:
    """An Intercom REST client — used by every ``@stream`` in this connector.

    One instance lives for one ``@stream`` invocation. Constructor args
    mirror the connector's declared ``params`` (see ``register.yaml``).
    """

    def __init__(
        self,
        *,
        access_token: str,
        region: str = "us",
        base_url: str = "",
        api_version: str = "2.16",
        page_size: int = _MAX_PER_PAGE,
        max_retries: int = 5,
        retry_backoff_seconds: float = 1.0,
        requests_per_second: float = 12.0,
        rate_limit_max_wait_seconds: float = 60.0,
        timeout_seconds: float = 60.0,
        log: logging.Logger | logging.LoggerAdapter[Any] | None = None,
        session: requests.Session | None = None,
        sleep: Any = time.sleep,
        clock: Any = time.time,
    ) -> None:
        region_key = (region or "us").strip().lower()
        if region_key not in HOSTS:
            raise ValueError(f"intercom: region must be one of {sorted(HOSTS)}, got {region!r}")
        self.base_url = (base_url or HOSTS[region_key]).rstrip("/")
        self.api_version = api_version
        self.page_size = max(1, min(int(page_size), _MAX_PER_PAGE))
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.rate_limit_max_wait_seconds = rate_limit_max_wait_seconds
        self.timeout_seconds = timeout_seconds
        self._log: logging.Logger | logging.LoggerAdapter[Any] = (
            log if log is not None else logging.getLogger(__name__)
        )
        self._sleep = sleep
        self._clock = clock
        self._session = session if session is not None else requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {access_token}",
                "Intercom-Version": api_version,
                "Accept": "application/json",
            }
        )
        self._bucket = _TokenBucket(
            rate=requests_per_second, capacity=max(1.0, requests_per_second), sleep=sleep
        )

    # -- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> IntercomClient:
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    # -- transport ---------------------------------------------------------

    def _rate_limit_wait(self, response: requests.Response) -> float:
        reset = response.headers.get("X-RateLimit-Reset")
        if reset:
            try:
                wait = float(reset) - float(self._clock())
                return max(1.0, min(wait, self.rate_limit_max_wait_seconds))
            except ValueError:
                pass
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(1.0, min(float(retry_after), self.rate_limit_max_wait_seconds))
            except ValueError:
                pass
        return min(5.0, self.rate_limit_max_wait_seconds)

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        json_body: Any = None,
    ) -> dict[str, Any]:
        """One ``<method> <base_url>/<path>`` returning the parsed JSON body.

        Retries 429 (waiting for the reset header), 5xx and connection
        errors up to ``max_retries``; other 4xx raise immediately.
        """
        url = f"{self.base_url}/{path.lstrip('/')}"
        last_error = ""
        for attempt in range(self.max_retries + 1):
            self._bucket.acquire()
            try:
                response = self._session.request(
                    method,
                    url,
                    params=dict(params) if params else None,
                    json=json_body,
                    timeout=self.timeout_seconds,
                )
            except requests.RequestException as exc:  # connection / timeout
                last_error = f"{type(exc).__name__}: {exc}"
                if attempt < self.max_retries:
                    delay = self.retry_backoff_seconds * (2**attempt)
                    self._log.warning(
                        "intercom: %s %s failed (%s); retry %d/%d in %.1fs",
                        method, path, type(exc).__name__, attempt + 1, self.max_retries, delay,
                    )
                    self._sleep(delay)
                    continue
                break

            status = response.status_code
            if status == _STATUS_RATE_LIMITED:
                last_error = f"rate limited: {_error_text(response)}"
                if attempt < self.max_retries:
                    delay = self._rate_limit_wait(response)
                    self._log.warning(
                        "intercom: 429 on %s %s; waiting %.0fs (retry %d/%d)",
                        method, path, delay, attempt + 1, self.max_retries,
                    )
                    self._sleep(delay)
                    continue
                break
            if status >= _STATUS_SERVER_ERROR_FLOOR:
                last_error = f"server error {status}: {_error_text(response)}"
                if attempt < self.max_retries:
                    delay = self.retry_backoff_seconds * (2**attempt)
                    self._log.warning(
                        "intercom: %d on %s %s; retry %d/%d in %.1fs",
                        status, method, path, attempt + 1, self.max_retries, delay,
                    )
                    self._sleep(delay)
                    continue
                break
            if status >= _STATUS_CLIENT_ERROR_FLOOR:
                raise IntercomAPIError(status, _error_text(response))
            if not response.content:
                return {}
            try:
                body = response.json()
            except ValueError as exc:
                raise IntercomAPIError(status, f"non-JSON response: {response.text[:200]}") from exc
            return body if isinstance(body, dict) else {"data": body}

        raise IntercomAPIError(
            _STATUS_RATE_LIMITED if "rate limited" in last_error else 599,
            f"gave up after {self.max_retries} retries — {last_error}",
        )

    def get(self, path: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        return self.request("GET", path, params=params)

    def post(self, path: str, json_body: Any) -> dict[str, Any]:
        return self.request("POST", path, json_body=json_body)

    # -- pagination --------------------------------------------------------

    @staticmethod
    def _next_starting_after(body: Mapping[str, Any]) -> str | None:
        pages = body.get("pages")
        if not isinstance(pages, dict):
            return None
        nxt = pages.get("next")
        if isinstance(nxt, dict):
            value = nxt.get("starting_after")
            return str(value) if value else None
        return None

    def search(
        self, path: str, query: Mapping[str, Any], items_key: str
    ) -> Iterator[list[dict[str, Any]]]:
        """Walk ``POST <path>`` (a search endpoint) one page per yield."""
        starting_after: str | None = None
        while True:
            body: dict[str, Any] = {
                "query": dict(query),
                "pagination": {"per_page": self.page_size},
            }
            if starting_after:
                body["pagination"]["starting_after"] = starting_after
            response = self.post(path, body)
            items = response.get(items_key) or []
            if not isinstance(items, list):
                items = []
            if items:
                yield [item for item in items if isinstance(item, dict)]
            starting_after = self._next_starting_after(response)
            if not starting_after or not items:
                return

    def list_cursor(
        self, path: str, items_key: str, params: Mapping[str, Any] | None = None
    ) -> Iterator[list[dict[str, Any]]]:
        """Walk a cursor-paginated ``GET`` list (``starting_after``)."""
        query: dict[str, Any] = dict(params or {})
        query.setdefault("per_page", self.page_size)
        while True:
            response = self.get(path, query)
            items = response.get(items_key) or []
            if isinstance(items, list) and items:
                yield [item for item in items if isinstance(item, dict)]
            starting_after = self._next_starting_after(response)
            if not starting_after or not items:
                return
            query["starting_after"] = starting_after

    def list_pages(
        self, path: str, items_key: str, params: Mapping[str, Any] | None = None
    ) -> Iterator[list[dict[str, Any]]]:
        """Walk a page-number list (``page`` / ``per_page`` + ``pages.next``)."""
        query: dict[str, Any] = dict(params or {})
        query.setdefault("per_page", self.page_size)
        page = 1
        while True:
            query["page"] = page
            response = self.get(path, query)
            items = response.get(items_key) or []
            if isinstance(items, list) and items:
                yield [item for item in items if isinstance(item, dict)]
            pages = response.get("pages") if isinstance(response.get("pages"), dict) else {}
            total_pages = pages.get("total_pages") if isinstance(pages, dict) else None
            has_next = bool(pages.get("next")) if isinstance(pages, dict) else False
            if not items or not has_next:
                return
            if isinstance(total_pages, int) and page >= total_pages:
                return
            page += 1

    def scroll(self, path: str, items_key: str) -> Iterator[list[dict[str, Any]]]:
        """Walk ``GET <path>`` (``/companies/scroll``) via ``scroll_param``."""
        scroll_param: str | None = None
        while True:
            params = {"scroll_param": scroll_param} if scroll_param else None
            response = self.get(path, params)
            items = response.get(items_key) or []
            if isinstance(items, list) and items:
                yield [item for item in items if isinstance(item, dict)]
            scroll_param = response.get("scroll_param")
            if not items or not scroll_param:
                return
