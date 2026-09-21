"""RevenueCat v2 HTTP client — throttled, concurrent, bounded retries.

HTTP concerns only (auth header, pagination, retries, rate limiting); it
does not import from ``dtex`` and does not know which streams exist.

RC paginates with ``starting_after`` cursors: a list response carries
``next_page``, the absolute URL of the next page with the cursor and the
original query string baked in, so follow-up requests pass NO params.

Two things make the per-customer fan-out fit an hourly window:

* **A shared token-bucket throttle** (``rate_per_second``). RC enforces
  480 req/min on the Customer Information domain; 8 threads measured at
  ~11 req/s unthrottled, which would trip 429s within a minute.
* **``map_concurrent``** — run one request per item on a small thread
  pool, in input order, so ~4.5k per-customer calls take ~11 min instead
  of ~60 (RC answers in ~0.8 s regardless of concurrency).

``requests.Session`` is not documented thread-safe, so each worker thread
gets its own session (``threading.local``); the token never appears in
logs or error messages.

``get_or_none`` / ``collect_or_none`` map a 404 to ``None`` — an id RC no
longer knows (a deleted or merged customer) must skip, not fail the stream.

Every wait goes through ``time.sleep`` of THIS module, so tests patch one
name. Retries are bounded by ``max_retries`` for network errors, 429 and
5xx alike: an unbounded retry loop once turned a dead connection into a
stream that slept forever.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

import requests

_TIMEOUT: tuple[float, float] = (10.0, 90.0)

T = TypeVar("T")
R = TypeVar("R")


class _TokenBucket:
    """Blocking rate limiter shared by all worker threads."""

    def __init__(self, rate_per_second: float) -> None:
        self._interval = 1.0 / rate_per_second if rate_per_second > 0 else 0.0
        self._lock = threading.Lock()
        self._next = time.monotonic()

    def wait(self) -> None:
        if self._interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            slot = max(self._next, now)
            self._next = slot + self._interval
        delay = slot - now
        if delay > 0:
            time.sleep(delay)


class RevenueCatClient:
    def __init__(
        self,
        api_key: str,
        project_id: str,
        base_url: str = "https://api.revenuecat.com/v2",
        rate_per_second: float = 7.0,
        workers: int = 8,
        max_retries: int = 5,
    ) -> None:
        self._api_key = api_key
        self.project_id = project_id
        self.base_url = base_url.rstrip("/")
        self.workers = max(1, int(workers))
        self.max_retries = int(max_retries)
        self._bucket = _TokenBucket(float(rate_per_second))
        self._local = threading.local()
        self.requests_made = 0
        self._count_lock = threading.Lock()

    # -- sessions -----------------------------------------------------------

    def _session(self) -> requests.Session:
        sess = getattr(self._local, "session", None)
        if sess is None:
            sess = requests.Session()
            sess.headers.update(
                {"Authorization": f"Bearer {self._api_key}", "Accept": "application/json"}
            )
            self._local.session = sess
        return sess

    # -- public surface -----------------------------------------------------

    def project_path(self, suffix: str) -> str:
        return f"/projects/{self.project_id}{suffix}"

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict:
        return self._get(f"{self.base_url}{path}", params=params)

    def get_or_none(self, path: str, params: dict[str, Any] | None = None) -> dict | None:
        """GET that returns ``None`` on 404 instead of raising."""
        try:
            return self.get(path, params)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return None
            raise

    def paginate(self, path: str, params: dict[str, Any] | None = None) -> Iterator[dict]:
        """Yield every item from a list endpoint, following ``next_page``.

        ``params`` apply to the first request only: ``next_page`` is an
        absolute URL with the query string and ``starting_after`` baked in.
        """
        next_url: str | None = f"{self.base_url}{path}"
        first = True
        while next_url:
            data = self._get(next_url, params=params if first else None)
            first = False
            yield from data.get("items", [])
            next_url = data.get("next_page")

    def collect(self, path: str, params: dict[str, Any] | None = None) -> list[dict]:
        return list(self.paginate(path, params))

    def collect_or_none(self, path: str, params: dict[str, Any] | None = None) -> list[dict] | None:
        try:
            return self.collect(path, params)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return None
            raise

    def map_concurrent(self, items: Iterable[T], fn: Callable[[T], R]) -> Iterator[tuple[T, R]]:
        """Apply ``fn`` to every item on the worker pool; yield ``(item,
        result)`` in input order. Exceptions propagate on the item that
        raised them, after the pool drains."""
        items = list(items)
        if not items:
            return
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            yield from zip(items, pool.map(fn, items), strict=True)

    # -- transport ----------------------------------------------------------

    def _get(self, url: str, params: dict | None = None) -> dict:
        attempt = 0
        while True:
            self._bucket.wait()
            with self._count_lock:
                self.requests_made += 1
            try:
                resp = self._session().get(url, params=params, timeout=_TIMEOUT)
            except requests.exceptions.RequestException as exc:
                if attempt >= self.max_retries:
                    raise RuntimeError(
                        f"revenuecat: network failure after {self.max_retries} retries "
                        f"on {url}: {exc}"
                    ) from exc
                time.sleep(min(2**attempt, 60))
                attempt += 1
                continue

            if resp.status_code == 429:
                if attempt >= self.max_retries:
                    raise RuntimeError(
                        f"revenuecat: rate-limited after {self.max_retries} retries on {url}; "
                        f"Retry-After={resp.headers.get('Retry-After')}"
                    )
                time.sleep(int(resp.headers.get("Retry-After", 30)))
                attempt += 1
                continue
            if resp.status_code in (500, 502, 503, 504) and attempt < self.max_retries:
                time.sleep(min(2**attempt, 60))
                attempt += 1
                continue
            resp.raise_for_status()
            return resp.json()
