# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Tests for the baked Intercom connector.

Every test stands up a tiny stdlib ``HTTPServer`` on a random port and
points :class:`IntercomClient` at it — no real network calls.

Areas:

* ``IntercomClient`` — auth + version headers, 429 honouring
  ``X-RateLimit-Reset`` (bounded), 5xx retry, 4xx raising with Intercom's
  ``[code] message``, search / cursor / page / scroll pagination.
* Window tiling and the search query shape.
* The ``@stream`` functions through ``dtex.run`` into DuckDB: a windowed
  search stream (query bodies, cursor per completed window), ticket-state
  flattening, the transcript fan-out (ordering, cap, source row), and a
  ``replace`` dimension stream.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import duckdb
import pytest

import dtex
from dtex.sources.intercom import source as intercom_source
from dtex.sources.intercom.client import IntercomAPIError, IntercomClient

# --------------------------------------------------------------------------
# Stub Intercom server
# --------------------------------------------------------------------------


class _Request:
    def __init__(self, method: str, path: str, headers: dict[str, str], body: Any) -> None:
        self.method = method
        self.path = path
        self.headers = headers
        self.body = body
        parsed = urlparse(path)
        self.route = parsed.path
        self.query = {k: v[0] for k, v in parse_qs(parsed.query).items()}


Responder = Callable[[_Request], tuple[int, Any, dict[str, str]]]


class _Stub:
    """Either a scripted queue of responses or a routing function."""

    def __init__(self) -> None:
        self.queue: list[tuple[int, Any, dict[str, str]]] = []
        self.captured: list[_Request] = []
        self.responder: Responder | None = None

    def add(self, json_body: Any = None, *, status: int = 200,
            headers: dict[str, str] | None = None) -> None:
        self.queue.append((status, json_body, headers or {}))

    def respond(self, request: _Request) -> tuple[int, Any, dict[str, str]]:
        if self.responder is not None:
            return self.responder(request)
        if not self.queue:
            return 500, {"errors": [{"code": "stub", "message": "scenario exhausted"}]}, {}
        return self.queue.pop(0)


def _make_handler(stub: _Stub) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args: Any) -> None:
            return

        def _serve(self, method: str) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b""
            body = json.loads(raw) if raw else None
            request = _Request(method, self.path, dict(self.headers), body)
            stub.captured.append(request)
            status, payload, extra = stub.respond(request)
            data = json.dumps(payload if payload is not None else {}).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            for k, v in extra.items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:  # noqa: N802
            self._serve("GET")

        def do_POST(self) -> None:  # noqa: N802
            self._serve("POST")

    return Handler


@pytest.fixture
def stub() -> Iterator[tuple[_Stub, str]]:
    scenario = _Stub()
    server = HTTPServer(("127.0.0.1", 0), _make_handler(scenario))
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield scenario, f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5.0)


def _client(base_url: str, **kw: Any) -> IntercomClient:
    kw.setdefault("requests_per_second", 0)  # no pacing in tests
    kw.setdefault("sleep", lambda _s: None)
    return IntercomClient(access_token="tok_unit", base_url=base_url, **kw)


# --------------------------------------------------------------------------
# Client
# --------------------------------------------------------------------------


def test_client_sends_bearer_and_version_headers(stub: tuple[_Stub, str]) -> None:
    scenario, base_url = stub
    scenario.add({"admins": []})
    _client(base_url, api_version="2.16").get("/admins")
    headers = scenario.captured[0].headers
    assert headers["Authorization"] == "Bearer tok_unit"
    assert headers["Intercom-Version"] == "2.16"
    assert headers["Accept"] == "application/json"


def test_region_picks_host() -> None:
    assert IntercomClient(access_token="t", region="eu").base_url == "https://api.eu.intercom.io"
    assert IntercomClient(access_token="t", region="au").base_url == "https://api.au.intercom.io"
    assert IntercomClient(access_token="t").base_url == "https://api.intercom.io"
    with pytest.raises(ValueError, match="region"):
        IntercomClient(access_token="t", region="mars")


def test_client_429_waits_for_rate_limit_reset(stub: tuple[_Stub, str]) -> None:
    scenario, base_url = stub
    now = 1_000_000.0
    scenario.add({"errors": [{"code": "rate_limit_exceeded", "message": "slow down"}]},
                 status=429, headers={"X-RateLimit-Reset": str(int(now) + 7)})
    scenario.add({"admins": [{"id": "1"}]})
    sleeps: list[float] = []
    client = _client(base_url, sleep=sleeps.append, clock=lambda: now)
    assert client.get("/admins") == {"admins": [{"id": "1"}]}
    assert sleeps == [7.0]
    assert len(scenario.captured) == 2


def test_client_429_wait_is_capped_and_bounded(stub: tuple[_Stub, str]) -> None:
    scenario, base_url = stub
    for _ in range(10):
        scenario.add({"errors": [{"code": "rate_limit_exceeded", "message": "x"}]},
                     status=429, headers={"Retry-After": "600"})
    sleeps: list[float] = []
    client = _client(base_url, sleep=sleeps.append, max_retries=2, rate_limit_max_wait_seconds=30)
    with pytest.raises(IntercomAPIError, match="gave up after 2 retries") as exc:
        client.get("/admins")
    assert exc.value.status == 429
    assert sleeps == [30.0, 30.0]
    assert len(scenario.captured) == 3


def test_client_5xx_retries_with_backoff_then_succeeds(stub: tuple[_Stub, str]) -> None:
    scenario, base_url = stub
    scenario.add({"errors": [{"code": "server_error", "message": "boom"}]}, status=503)
    scenario.add({"errors": [{"code": "server_error", "message": "boom"}]}, status=502)
    scenario.add({"data": [{"id": "t1"}]})
    sleeps: list[float] = []
    assert _client(base_url, sleep=sleeps.append).get("/tags") == {"data": [{"id": "t1"}]}
    assert sleeps == [1.0, 2.0]


def test_client_4xx_raises_with_intercom_error_text(stub: tuple[_Stub, str]) -> None:
    scenario, base_url = stub
    scenario.add({"type": "error.list",
                  "errors": [{"code": "api_plan_restricted", "message": "needs scope"}]},
                 status=403)
    with pytest.raises(IntercomAPIError, match=r"403: \[api_plan_restricted\] needs scope"):
        _client(base_url).get("/tickets/1")
    assert len(scenario.captured) == 1  # no retry on 403
    assert "tok_unit" not in str(IntercomAPIError(403, "x"))


def test_search_walks_starting_after_pages(stub: tuple[_Stub, str]) -> None:
    scenario, base_url = stub
    scenario.add({"conversations": [{"id": "1"}, {"id": "2"}],
                  "pages": {"next": {"starting_after": "c2"}}})
    scenario.add({"conversations": [{"id": "3"}], "pages": {"next": None}})
    pages = list(_client(base_url, page_size=2).search(
        "/conversations/search", {"field": "state", "operator": "=", "value": "open"},
        "conversations"))
    assert [[r["id"] for r in p] for p in pages] == [["1", "2"], ["3"]]
    first, second = scenario.captured
    assert first.method == "POST"
    assert first.body["pagination"] == {"per_page": 2}
    assert first.body["query"]["field"] == "state"
    assert second.body["pagination"] == {"per_page": 2, "starting_after": "c2"}


def test_list_pages_and_scroll(stub: tuple[_Stub, str]) -> None:
    scenario, base_url = stub

    def responder(req: _Request) -> tuple[int, Any, dict[str, str]]:
        if req.route == "/articles":
            page = int(req.query.get("page", "1"))
            body = {"data": [{"id": f"a{page}"}],
                    "pages": {"page": page, "total_pages": 2,
                              "next": {"page": 2} if page == 1 else None}}
            return 200, body, {}
        if req.route == "/companies/scroll":
            if "scroll_param" not in req.query:
                return 200, {"data": [{"id": "c1"}], "scroll_param": "s1"}, {}
            return 200, {"data": [], "scroll_param": "s1"}, {}
        return 404, {"errors": [{"code": "not_found", "message": req.route}]}, {}

    scenario.responder = responder
    client = _client(base_url)
    assert [r["id"] for p in client.list_pages("/articles", "data") for r in p] == ["a1", "a2"]
    assert [r["id"] for p in client.scroll("/companies/scroll", "data") for r in p] == ["c1"]
    scroll_calls = [r for r in scenario.captured if r.route == "/companies/scroll"]
    assert len(scroll_calls) == 2 and scroll_calls[1].query["scroll_param"] == "s1"


# --------------------------------------------------------------------------
# Windows and queries
# --------------------------------------------------------------------------


def test_iter_windows_tiles_inclusively() -> None:
    day = 86400
    windows = intercom_source._iter_windows(0, 3 * day + 5, 1)
    assert windows == [
        (0, day - 1), (day, 2 * day - 1), (2 * day, 3 * day - 1), (3 * day, 3 * day + 5),
    ]
    assert intercom_source._iter_windows(None, 99, 7) == [(None, 99)]
    assert intercom_source._iter_windows(50, 60, 0) == [(50, 60)]
    assert intercom_source._iter_windows(70, 60, 7) == [(70, 60)]


def test_window_query_uses_strict_operators_around_the_window() -> None:
    q = intercom_source._window_query(100, 200)
    assert q == {"operator": "AND", "value": [
        {"field": "updated_at", "operator": ">", "value": 99},
        {"field": "updated_at", "operator": "<", "value": 201},
    ]}
    assert intercom_source._window_query(None, 200) == {
        "field": "updated_at", "operator": "<", "value": 201}


def test_flatten_ticket_state_object() -> None:
    row = intercom_source._flatten_ticket({
        "id": "t1", "ticket_state": {"id": "9", "category": "resolved",
                                     "internal_label": "Done", "external_label": "Resolved"}})
    assert row["ticket_state"] == "resolved"
    assert row["ticket_state_id"] == "9"
    assert row["ticket_state_internal_label"] == "Done"
    assert row["ticket_state_external_label"] == "Resolved"
    assert intercom_source._flatten_ticket({"ticket_state": "open"})["ticket_state"] == "open"


# --------------------------------------------------------------------------
# End to end through the engine
# --------------------------------------------------------------------------


def _write_project(tmp_path: Path) -> None:
    (tmp_path / "dtex_project.yml").write_text(
        "name: t\nversion: '0.1'\nsource_paths: []\n"
        "destination_paths: []\nconfig_paths:\n  - configs\n"
    )
    (tmp_path / "profiles.yml").write_text(
        "duckdb:\n  default_target: dev\n  targets:\n    dev:\n"
        "      path: '.dtex/warehouse.duckdb'\n"
    )


def _write_config(tmp_path: Path, *, base_url: str, streams: str, params: str = "") -> None:
    (tmp_path / "configs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "configs" / "ic_test.yml").write_text(
        "name: ic_test\nsource: intercom\ndestination: duckdb\ntarget: dev\n"
        f"params:\n  base_url: '{base_url}'\n  requests_per_second: 0\n{params}"
        f"streams:\n{streams}\n"
    )


def _run(tmp_path: Path, **kw: Any) -> tuple[Any, str]:
    db_path = str(tmp_path / "warehouse.duckdb")
    result = dtex.run(config="ic_test", project_dir=str(tmp_path),
                      destination_params_override={"path": db_path}, **kw)
    return result, db_path


def _cursor(db_path: str, stream: str) -> Any:
    conn = duckdb.connect(db_path)
    try:
        row = conn.execute(
            "SELECT cursor_value FROM _dtex_state WHERE stream = ?", [stream]
        ).fetchone()
    finally:
        conn.close()
    return json.loads(row[0]) if row and isinstance(row[0], str) else (row[0] if row else None)


def test_end_to_end_contacts_windowed(
    stub: tuple[_Stub, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two windows, page-per-batch, cursor observed per completed window."""
    scenario, base_url = stub
    monkeypatch.setenv("INTERCOM_ACCESS_TOKEN", "tok_unit")
    day = 86400
    now = 10 * day + 100
    monkeypatch.setattr(intercom_source.time, "time", lambda: now)

    def responder(req: _Request) -> tuple[int, Any, dict[str, str]]:
        assert req.route == "/contacts/search"
        clauses = req.body["query"]["value"]
        lo = clauses[0]["value"] + 1
        after = req.body["pagination"].get("starting_after")
        if lo == 9 * day:  # first window: two pages
            if after is None:
                return 200, {"data": [{"id": "c1", "updated_at": lo + 5, "email": "a@x",
                                       "location": {"country": "LT"}}],
                             "pages": {"next": {"starting_after": "c1"}}}, {}
            return 200, {"data": [{"id": "c2", "updated_at": lo + 9}], "pages": {}}, {}
        return 200, {"data": [{"id": "c3", "updated_at": lo + 1}], "pages": {}}, {}

    scenario.responder = responder
    _write_project(tmp_path)
    # `since:` is the per-stream one-shot cursor floor (docs/12): the walk
    # starts at day 9 instead of the register's 2020 initial_value.
    _write_config(
        tmp_path, base_url=base_url, params="  window_days: 1\n",
        streams=f"  contacts:\n    since: {9 * day}\n",
    )
    result, db_path = _run(tmp_path)
    assert result.status.value == "succeeded", result.error

    searches = [r for r in scenario.captured if r.route == "/contacts/search"]
    windows = sorted({(r.body["query"]["value"][0]["value"] + 1,
                       r.body["query"]["value"][1]["value"] - 1) for r in searches})
    assert windows == [(9 * day, 10 * day - 1), (10 * day, now)]
    assert all(r.body["pagination"]["per_page"] == 150 for r in searches)

    conn = duckdb.connect(db_path)
    rows = conn.execute("SELECT id, updated_at, email FROM contacts ORDER BY id").fetchall()
    loc = conn.execute("SELECT location FROM contacts WHERE id = 'c1'").fetchone()
    conn.close()
    assert rows == [
        ("c1", 9 * day + 5, "a@x"), ("c2", 9 * day + 9, None), ("c3", 10 * day + 1, None),
    ]
    assert json.loads(str(loc[0]))["country"] == "LT"
    assert _cursor(db_path, "contacts") == now


def test_end_to_end_tickets_flatten_state(
    stub: tuple[_Stub, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    scenario, base_url = stub
    monkeypatch.setenv("INTERCOM_ACCESS_TOKEN", "tok_unit")
    monkeypatch.setattr(intercom_source.time, "time", lambda: 1_700_000_000)
    scenario.add({"tickets": [{
        "id": "t1", "ticket_id": "42", "type": "ticket", "updated_at": 1_699_999_000,
        "created_at": 1_699_990_000, "open": True,
        "ticket_state": {"id": "5", "category": "in_progress",
                         "internal_label": "Working", "external_label": "In progress"},
        "ticket_type": {"id": "7", "name": "Bug"},
        "ticket_attributes": {"_default_title_": "Broken"},
        "admin_assignee_id": 12,
    }], "pages": {}})
    _write_project(tmp_path)
    _write_config(tmp_path, base_url=base_url, params="  window_days: 0\n", streams="  tickets:")
    result, db_path = _run(tmp_path)
    assert result.status.value == "succeeded", result.error
    conn = duckdb.connect(db_path)
    row = conn.execute(
        "SELECT ticket_id, ticket_state, ticket_state_id, ticket_state_internal_label, "
        "admin_assignee_id, ticket_type FROM tickets"
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[:4] == ("42", "in_progress", "5", "Working")
    assert row[4] == "12"  # STRING column, Airbyte shape
    assert json.loads(str(row[5]))["name"] == "Bug"
    # The single window ran from the register initial_value (2020-01-01).
    q = scenario.captured[0].body["query"]["value"]
    assert q[0]["value"] == 1577836800 - 1


def test_end_to_end_conversation_parts_orders_caps_and_resumes(
    stub: tuple[_Stub, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    scenario, base_url = stub
    monkeypatch.setenv("INTERCOM_ACCESS_TOKEN", "tok_unit")
    now = 1_700_000_000
    monkeypatch.setattr(intercom_source.time, "time", lambda: now)
    heads = [("9", now - 30), ("7", now - 5000), ("8", now - 60)]  # unsorted from the API
    fetched: list[str] = []

    def responder(req: _Request) -> tuple[int, Any, dict[str, str]]:
        if req.route == "/conversations/search":
            lo = req.body["query"]["value"][0]["value"] + 1
            return 200, {"conversations": [{"id": i, "updated_at": u} for i, u in heads if u >= lo],
                         "pages": {}}, {}
        if req.route.startswith("/conversations/"):
            cid = req.route.rsplit("/", 1)[1]
            fetched.append(cid)
            upd = dict(heads)[cid]
            return 200, {
                "id": cid, "updated_at": upd, "created_at": upd - 1000,
                "source": {"id": f"s{cid}", "body": "<p>hi</p>",
                           "delivered_as": "customer_initiated",
                           "subject": "", "author": {"type": "user", "id": "u1"}},
                "conversation_parts": {"conversation_parts": [
                    {"id": f"p{cid}a", "part_type": "comment", "body": "reply",
                     "created_at": upd - 500, "author": {"type": "admin", "id": "a1"}},
                    {"id": f"p{cid}b", "part_type": "close", "body": None, "created_at": upd},
                ]},
            }, {}
        return 404, {"errors": [{"code": "not_found", "message": req.route}]}, {}

    scenario.responder = responder
    _write_project(tmp_path)
    _write_config(
        tmp_path, base_url=base_url, params="  window_days: 0\n",
        streams="  conversation_parts:\n    params:\n      max_conversations_per_run: 2\n",
    )
    result, db_path = _run(tmp_path)
    assert result.status.value == "succeeded", result.error
    assert fetched == ["7", "8"]  # ascending updated_at, capped at 2
    assert _cursor(db_path, "conversation_parts") == now - 60

    conn = duckdb.connect(db_path)
    rows = conn.execute(
        "SELECT id, conversation_id, part_type, conversation_updated_at FROM conversation_parts "
        "ORDER BY conversation_id, id"
    ).fetchall()
    conn.close()
    assert rows == [
        ("p7a", "7", "comment", now - 5000), ("p7b", "7", "close", now - 5000),
        ("s7", "7", "conversation_source", now - 5000),
        ("p8a", "8", "comment", now - 60), ("p8b", "8", "close", now - 60),
        ("s8", "8", "conversation_source", now - 60),
    ]

    # Second run resumes from the cursor minus the 1h lookback: 7 (older than
    # the lookback) is not re-fetched, 8 is re-pulled and merged idempotently,
    # and 9 — what the cap left behind — lands.
    fetched.clear()
    result, db_path = _run(tmp_path)
    assert result.status.value == "succeeded", result.error
    assert fetched == ["8", "9"]
    conn = duckdb.connect(db_path)
    n = conn.execute("SELECT COUNT(*) FROM conversation_parts").fetchone()
    conn.close()
    assert n == (9,)


def test_end_to_end_replace_dimension(
    stub: tuple[_Stub, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    scenario, base_url = stub
    monkeypatch.setenv("INTERCOM_ACCESS_TOKEN", "tok_unit")
    scenario.add({"teams": [{"id": "1", "type": "team", "name": "Billing", "admin_ids": [3, 4]}]})
    _write_project(tmp_path)
    _write_config(tmp_path, base_url=base_url, streams="  teams:")
    result, db_path = _run(tmp_path)
    assert result.status.value == "succeeded", result.error
    conn = duckdb.connect(db_path)
    row = conn.execute("SELECT id, name, admin_ids FROM teams").fetchone()
    conn.close()
    assert row is not None and row[:2] == ("1", "Billing")
    assert json.loads(str(row[2])) == [3, 4]


def test_client_does_not_sleep_when_unpaced() -> None:
    """requests_per_second=0 disables the bucket entirely (test convenience)."""
    sleeps: list[float] = []
    client = IntercomClient(access_token="t", requests_per_second=0, sleep=sleeps.append)
    client._bucket.acquire()
    client._bucket.acquire()
    assert sleeps == []
    assert time.monotonic() > 0


# --------------------------------------------------------------------------
# Batching and deferred cursor observation (0.12.1)
# --------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, start: int | None) -> None:
        self._start = start
        self.is_full_refresh = False
        self.events: list[tuple[str, Any]] = []

    def start_value(self) -> int | None:
        return self._start

    def observe(self, value: Any) -> None:
        self.events.append(("observe", value))


class _FakeClient:
    """Two windows × two pages of 3 rows each; `updated_at` = window start."""

    def __init__(self) -> None:
        self.queries: list[dict[str, Any]] = []

    def __enter__(self) -> _FakeClient:
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def search(
        self, path: str, query: dict[str, Any], items_key: str
    ) -> Iterator[list[dict[str, Any]]]:
        self.queries.append(query)
        lo = query["value"][0]["value"] + 1
        for page in range(2):
            yield [{"id": f"{lo}-{page}-{i}", "updated_at": lo + i} for i in range(3)]


def _fake_stream_def(name: str) -> Any:
    """Only `.name` and `.schema[].name` are read by the extract helper."""
    from types import SimpleNamespace
    return SimpleNamespace(
        name=name,
        schema=[SimpleNamespace(name="id"), SimpleNamespace(name="updated_at")],
    )


def _fake_config(**params: Any) -> Any:
    class _C:
        secrets = {"access_token": "t"}

        def get(self, key: str, default: Any = None) -> Any:
            return params.get(key, default)
    return _C()


def test_search_batches_span_windows_and_observe_only_landed_windows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """batch_size=8 over 2 windows × 6 rows (pages of 3): the first yield
    (9 rows, after the page that crossed the threshold) lands window 1 plus
    part of window 2 → observe(window-1 end) right after that yield, never
    window 2 until its remaining rows are yielded at the end."""
    client = _FakeClient()
    monkeypatch.setattr(intercom_source, "_build_client", lambda config, log: client)
    day = 86400
    monkeypatch.setattr(intercom_source.time, "time", lambda: 2 * day - 1)
    cursor = _FakeCursor(0)
    events: list[tuple[str, Any]] = cursor.events
    gen = intercom_source._extract_search(
        _fake_stream_def("contacts"), _fake_config(window_days=1, batch_size=8),
        cursor, logging.getLogger("t"),  # type: ignore[arg-type]
    )
    for batch in gen:
        events.append(("yield", len(batch)))
    assert events == [
        ("yield", 9), ("observe", day - 1),      # window 1 landed with the first batch
        ("yield", 3), ("observe", 2 * day - 1),  # window 2 landed by the final flush
    ]
    assert [q["value"][0]["value"] + 1 for q in client.queries] == [0, day]


def test_search_empty_window_observes_immediately_when_nothing_is_buffered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Empty(_FakeClient):
        def search(
            self, path: str, query: dict[str, Any], items_key: str
        ) -> Iterator[list[dict[str, Any]]]:
            self.queries.append(query)
            return iter(())

    client = _Empty()
    monkeypatch.setattr(intercom_source, "_build_client", lambda config, log: client)
    day = 86400
    monkeypatch.setattr(intercom_source.time, "time", lambda: 2 * day - 1)
    cursor = _FakeCursor(0)
    list(intercom_source._extract_search(
        _fake_stream_def("tickets"), _fake_config(window_days=1), cursor, logging.getLogger("t"),  # type: ignore[arg-type]
    ))
    assert cursor.events == [("observe", day - 1), ("observe", 2 * day - 1)]
