# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Klaviyo HTTP client — key auth, revision pin, cursor paging, retries.

A small wrapper around :mod:`requests` that the Klaviyo ``@stream`` functions
in :mod:`source` use to talk to ``a.klaviyo.com/api``. Plain module — no
decorators, no dtex imports — the "API client" role docs/04 assigns to
``client.py``.

Concerns handled here so the extract loops stay about extraction:

* **Auth + revision.** ``Authorization: Klaviyo-API-Key <pk_...>`` and a
  ``revision: <YYYY-MM-DD>`` header on a long-lived :class:`requests.Session`.
  Klaviyo pins response *shape* to the revision, so pinning it explicitly is
  what stops a vendor-side change from silently reshaping a landed table.

* **Rate limiting.** A token bucket smooths traffic to
  ``requests_per_second``; a ``429`` waits for ``Retry-After`` (capped at
  ``rate_limit_max_wait_seconds``) and retries, counted against
  ``max_retries``. Klaviyo advertises its own budget in ``RateLimit-Limit``
  (e.g. ``"350, 350;w=1, 3500;w=60"``) — burst, then steady.

* **Retry / backoff.** ``5xx`` and connection errors retry, counted against
  ``max_retries``. A ``5xx`` carrying ``Retry-After`` waits for exactly that
  (the Reporting API answers 503 that way during an outage); otherwise the
  wait is exponential backoff with jitter. Any ``4xx`` other than 429 raises
  :class:`KlaviyoAPIError` immediately — retrying a deterministic error
  cannot help — carrying Klaviyo's own ``[code] detail`` text.

  Retrying a POST is safe for every endpoint this connector calls: the
  Reporting API's POSTs are queries (statistics over a timeframe), not
  mutations, so a repeated submission is idempotent.

* **Paging.** Klaviyo is JSON:API: a page carries ``links.next`` as a full
  URL with an opaque ``page[cursor]``. :meth:`pages` follows it verbatim
  rather than reconstructing the query, which is what makes a filtered walk
  (``?filter=...&include=...``) page correctly.

* **The ``included`` block.** :meth:`pages` yields the WHOLE page body, not
  just ``data``, because this connector's entire reason to exist is joining
  ``included`` (the attribution sidecar) back onto ``data``. A client that
  yielded only records would reproduce the Airbyte defect.

* **Secret redaction.** The key is set once on the Session and never appears
  in a log line or an exception message.
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any

import requests

_STATUS_RATE_LIMITED = 429
_STATUS_SERVER_ERROR_FLOOR = 500
_STATUS_CLIENT_ERROR_FLOOR = 400

DEFAULT_BASE_URL = "https://a.klaviyo.com/api"
DEFAULT_REVISION = "2026-07-15"


class KlaviyoAPIError(Exception):
    """A Klaviyo API call failed — a non-retryable 4xx or exhausted retries.

    Carries the HTTP ``status``; the message includes Klaviyo's
    ``errors[].code`` / ``detail`` when the body carried them. The API key is
    never part of the message.
    """

    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"Klaviyo API error {status}: {message}")
        self.status = status


def _error_text(response: requests.Response) -> str:
    """``[code] detail; [code] detail`` from a Klaviyo error body, else raw text."""
    try:
        body = response.json()
    except ValueError:
        return response.text[:400]
    errors = body.get("errors") if isinstance(body, dict) else None
    if isinstance(errors, list) and errors:
        parts = []
        for err in errors:
            if isinstance(err, dict):
                parts.append(f"[{err.get('code')}] {err.get('detail') or err.get('title')}")
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


class KlaviyoClient:
    """A Klaviyo JSON:API client — used by every ``@stream`` in this connector.

    One instance lives for one ``@stream`` invocation. Constructor args mirror
    the connector's declared ``params`` (see ``register.yaml``).
    """

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = DEFAULT_BASE_URL,
        api_revision: str = DEFAULT_REVISION,
        max_retries: int = 5,
        retry_backoff_seconds: float = 1.0,
        requests_per_second: float = 8.0,
        rate_limit_max_wait_seconds: float = 60.0,
        timeout_seconds: float = 60.0,
        log: logging.Logger | logging.LoggerAdapter[Any] | None = None,
        session: requests.Session | None = None,
        sleep: Any = time.sleep,
    ) -> None:
        self.base_url = (base_url or DEFAULT_BASE_URL).rstrip("/")
        self.api_revision = api_revision or DEFAULT_REVISION
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.rate_limit_max_wait_seconds = rate_limit_max_wait_seconds
        self.timeout_seconds = timeout_seconds
        self._log: logging.Logger | logging.LoggerAdapter[Any] = (
            log if log is not None else logging.getLogger(__name__)
        )
        self._sleep = sleep
        self._session = session if session is not None else requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Klaviyo-API-Key {api_key}",
                "revision": self.api_revision,
                "accept": "application/vnd.api+json",
            }
        )
        self._bucket = _TokenBucket(
            rate=requests_per_second, capacity=max(1.0, requests_per_second), sleep=sleep
        )

    # -- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> KlaviyoClient:
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    # -- transport ---------------------------------------------------------

    def _request(
        self,
        url: str,
        params: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Issue one request with rate limiting, retry and backoff. Returns the body.

        ``json_body`` switches the verb to POST — the Reporting API's values
        endpoints are POST-only (a GET returns 405), because the report
        definition (statistics, timeframe, grouping) is too large for a query
        string.
        """
        attempt = 0
        while True:
            self._bucket.acquire()
            try:
                if json_body is not None:
                    response = self._session.post(
                        url,
                        params=dict(params or {}),
                        json=json_body,
                        headers={"content-type": "application/vnd.api+json"},
                        timeout=self.timeout_seconds,
                    )
                else:
                    response = self._session.get(
                        url, params=dict(params or {}), timeout=self.timeout_seconds
                    )
            except requests.exceptions.RequestException as exc:
                if attempt >= self.max_retries:
                    raise KlaviyoAPIError(0, f"connection failure on {url}: {exc}") from exc
                self._backoff(attempt)
                attempt += 1
                continue

            status = response.status_code
            if status == _STATUS_RATE_LIMITED:
                if attempt >= self.max_retries:
                    raise KlaviyoAPIError(status, _error_text(response))
                self._wait_for_rate_limit(response, attempt)
                attempt += 1
                continue
            if status >= _STATUS_SERVER_ERROR_FLOOR:
                if attempt >= self.max_retries:
                    raise KlaviyoAPIError(status, _error_text(response))
                # A 503 from the Reporting API carries Retry-After with the
                # seconds to wait; exponential backoff ignores it and can
                # exhaust the retry budget long before the service is back.
                self._wait_for_rate_limit(response, attempt)
                attempt += 1
                continue
            if status >= _STATUS_CLIENT_ERROR_FLOOR:
                # 4xx other than 429 is deterministic — retrying cannot help.
                raise KlaviyoAPIError(status, _error_text(response))

            body: dict[str, Any] = response.json()
            return body

    def _backoff(self, attempt: int) -> None:
        """Exponential backoff with jitter, so parallel streams do not sync up."""
        delay = min(self.retry_backoff_seconds * (2.0**attempt), 60.0)
        self._sleep(delay * (0.5 + random.random() / 2))

    def _wait_for_rate_limit(self, response: requests.Response, attempt: int) -> None:
        """Honour ``Retry-After`` (429 and 503), bounded; else fall back to backoff."""
        raw = response.headers.get("Retry-After") or response.headers.get("retry-after")
        wait: float | None = None
        if raw:
            try:
                wait = float(raw)
            except ValueError:
                wait = None
        if wait is None:
            self._backoff(attempt)
            return
        wait = max(0.0, min(wait, self.rate_limit_max_wait_seconds))
        self._log.warning("klaviyo: rate limited, waiting %.1fs", wait)
        self._sleep(wait)

    # -- paging ------------------------------------------------------------

    def get(self, path: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """GET one endpoint and return the parsed body (no paging)."""
        return self._request(f"{self.base_url}/{path.lstrip('/')}", params)

    def post(self, path: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        """POST a body to one endpoint and return the parsed response.

        Used by the Reporting API streams. Those endpoints answer a 503 with a
        ``Retry-After`` header during temporary outages, which the shared retry
        path already honours.
        """
        return self._request(f"{self.base_url}/{path.lstrip('/')}", None, payload)

    def pages(
        self, path: str, params: Mapping[str, Any] | None = None
    ) -> Iterator[dict[str, Any]]:
        """Yield each page BODY of a JSON:API list endpoint.

        The whole body is yielded — ``data`` *and* ``included`` — because the
        caller needs the sidecar to fold attribution onto its records.

        ``links.next`` is a fully-formed URL carrying an opaque
        ``page[cursor]``; it is followed verbatim. Re-deriving the query
        string here would drop the filter/include/sort of the original
        request, which is the classic way a filtered JSON:API walk silently
        turns into an unfiltered one on page 2.
        """
        url = f"{self.base_url}/{path.lstrip('/')}"
        query: Mapping[str, Any] | None = params
        while True:
            body = self._request(url, query)
            yield body
            nxt = (body.get("links") or {}).get("next")
            if not nxt:
                return
            url = str(nxt)
            query = None  # the cursor URL already carries every parameter
