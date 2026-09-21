# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Tests for the baked Konnektive CRM connector.

Every test stands up a tiny ``http.server.HTTPServer`` on a random port
and points the connector at it. The stub records every request (method,
path, query string, form body) and answers from a script or a router —
no real network calls.

Test areas:

* Credentials — sent in the POST body by default and never in a URL; the
  GET fallback; nothing leaks into logs or error messages.
* Envelope — SUCCESS unwrap, "No … could be found" as an empty window,
  auth errors raised immediately and never mistaken for empty, transient
  ERRORs retried then raised.
* Transport — bounded 429 / 5xx retries, non-JSON bodies.
* Pagination — page numbers, the totalResults stop, the short-page stop.
* Windows — tiling without gap or overlap, lookback, future cursors.
* End-to-end — ``dtex.run`` lands all five streams in DuckDB, keeps the
  whole object under ``raw``, renames the digit-led key, and advances the
  cursor so the second run starts from (cursor − lookback).
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Iterator
from datetime import date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import duckdb
import pytest

import dtex
from dtex.sources.konnektive import client as client_module
from dtex.sources.konnektive.client import (
    KonnektiveAuthError,
    KonnektiveClient,
    KonnektiveError,
)
from dtex.sources.konnektive.source import _iter_windows, _parse_day, _project

# --------------------------------------------------------------------------
# Stub Konnektive server
# --------------------------------------------------------------------------


class _Request:
    """One captured request."""

    def __init__(self, method: str, raw_path: str, body: str) -> None:
        parsed = urlparse(raw_path)
        self.method = method
        self.raw_path = raw_path
        self.path = parsed.path
        self.query = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        self.form = {k: v[0] for k, v in parse_qs(body).items()}

    @property
    def params(self) -> dict[str, str]:
        """The request parameters, wherever they travelled."""
        return {**self.query, **self.form}


Responder = Callable[[_Request], tuple[int, Any, dict[str, str]]]


class _Stub:
    """Scripted responses first; once the script is empty, the router."""

    def __init__(self) -> None:
        self.captured: list[_Request] = []
        self._queue: list[tuple[int, Any, dict[str, str]]] = []
        self.router: Responder | None = None

    def add(
        self, body: Any, *, status: int = 200, headers: dict[str, str] | None = None
    ) -> None:
        self._queue.append((status, body, headers or {}))

    def respond(self, request: _Request) -> tuple[int, Any, dict[str, str]]:
        self.captured.append(request)
        if self._queue:
            return self._queue.pop(0)
        if self.router is not None:
            return self.router(request)
        return 500, {"error": "scenario exhausted"}, {}


def _make_handler(stub: _Stub) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args: Any) -> None:
            return

        def _serve(self, method: str) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length).decode() if length else ""
            status, payload, headers = stub.respond(_Request(method, self.path, body))
            raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            for key, value in headers.items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self) -> None:  # noqa: N802 — required by stdlib
            self._serve("GET")

        def do_POST(self) -> None:  # noqa: N802 — required by stdlib
            self._serve("POST")

    return Handler


@pytest.fixture
def kon_stub() -> Iterator[tuple[_Stub, str]]:
    stub = _Stub()
    server = HTTPServer(("127.0.0.1", 0), _make_handler(stub))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield stub, f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5.0)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Backoff sleeps are policy, not something a test should wait for."""
    monkeypatch.setattr(client_module.time, "sleep", lambda _s: None)


def _client(base_url: str, **kwargs: Any) -> KonnektiveClient:
    kwargs.setdefault("max_retries", 2)
    return KonnektiveClient(
        login_id="login_secret_value",
        password="password_secret_value",
        base_url=base_url,
        **kwargs,
    )


def _page(rows: list[dict[str, Any]], *, total: int, page: int, per_page: int = 200) -> dict:
    return {
        "result": "SUCCESS",
        "message": {
            "totalResults": total,
            "resultsPerPage": per_page,
            "page": page,
            "data": rows,
        },
    }


def _error(text: str) -> dict[str, str]:
    return {"result": "ERROR", "message": text}


_EMPTY = _error("No orders matching those parameters could be found")


# --------------------------------------------------------------------------
# Credentials
# --------------------------------------------------------------------------


def test_post_keeps_credentials_out_of_the_url(kon_stub: tuple[_Stub, str]) -> None:
    stub, base_url = kon_stub
    stub.add(_page([{"orderId": "A1"}], total=1, page=1))

    rows = list(_client(base_url).query("order/query", {"startDate": "2026-01-01 00:00:00"}))

    assert rows == [{"orderId": "A1"}]
    request = stub.captured[0]
    assert request.method == "POST"
    assert request.path == "/order/query/"
    assert request.query == {}, "nothing — least of all credentials — may ride the URL"
    assert "secret" not in request.raw_path
    assert request.form["loginId"] == "login_secret_value"
    assert request.form["password"] == "password_secret_value"
    assert request.form["startDate"] == "2026-01-01 00:00:00"


def test_get_fallback_sends_query_parameters(kon_stub: tuple[_Stub, str]) -> None:
    stub, base_url = kon_stub
    stub.add(_page([], total=0, page=1))

    list(_client(base_url, http_method="get").query("order/query", {}))

    request = stub.captured[0]
    assert request.method == "GET"
    assert request.query["loginId"] == "login_secret_value"
    assert request.form == {}


def test_unknown_http_method_is_rejected() -> None:
    with pytest.raises(ValueError, match="GET or POST"):
        KonnektiveClient(login_id="l", password="p", http_method="PUT")


def test_network_failure_message_never_carries_the_url_query() -> None:
    """Over GET the password is in the URL, and `requests` exceptions embed
    the URL — the raised message must name the exception type only."""
    client = KonnektiveClient(
        login_id="login_secret_value",
        password="password_secret_value",
        base_url="http://127.0.0.1:9",  # discard port — connection refused
        http_method="GET",
        max_retries=1,
    )
    with pytest.raises(KonnektiveError) as excinfo:
        list(client.query("order/query", {}))

    text = str(excinfo.value)
    assert "network failure" in text
    assert "secret" not in text
    assert excinfo.value.__cause__ is None, "the chained exception would carry the URL"


def test_credentials_echoed_by_the_server_are_scrubbed(kon_stub: tuple[_Stub, str]) -> None:
    stub, base_url = kon_stub
    stub.router = lambda _r: (
        200,
        _error("Unexpected failure for password_secret_value / login_secret_value"),
        {},
    )
    # "password" in the message makes this an auth error; what matters is
    # that the VALUES are gone from whatever is raised.
    with pytest.raises(KonnektiveError) as excinfo:
        list(_client(base_url).query("order/query", {}))

    assert "secret_value" not in str(excinfo.value)
    assert "***" in str(excinfo.value)


def test_repr_hides_credentials() -> None:
    assert "secret" not in repr(_client("http://x"))


# --------------------------------------------------------------------------
# Envelope
# --------------------------------------------------------------------------


def test_no_results_error_is_an_empty_window(kon_stub: tuple[_Stub, str]) -> None:
    stub, base_url = kon_stub
    stub.add(_EMPTY)

    assert list(_client(base_url).query("order/query", {})) == []
    assert len(stub.captured) == 1, "an empty window must not be retried"


def test_auth_error_raises_immediately_and_is_never_retried(
    kon_stub: tuple[_Stub, str],
) -> None:
    stub, base_url = kon_stub
    stub.router = lambda _r: (200, _error("Invalid login credentials"), {})

    with pytest.raises(KonnektiveAuthError, match="Invalid login"):
        list(_client(base_url, max_retries=5).query("order/query", {}))

    assert len(stub.captured) == 1, "retrying bad credentials can lock the API user"


def test_ip_allow_list_rejection_is_an_auth_error_naming_the_ip(
    kon_stub: tuple[_Stub, str],
) -> None:
    """Verbatim from the live API (2026-09-21). The message carries the
    caller's egress IP — the one thing the operator needs — so it must
    survive into the raised error, and must not be retried."""
    stub, base_url = kon_stub
    stub.router = lambda _r: (200, _error("IP must be whitelisted - 203.0.113.7"), {})

    with pytest.raises(KonnektiveAuthError, match=r"IP must be whitelisted - 203\.0\.113\.7"):
        list(_client(base_url, max_retries=5).query("order/query", {}))

    assert len(stub.captured) == 1


def test_auth_error_that_says_not_found_is_not_an_empty_window(
    kon_stub: tuple[_Stub, str],
) -> None:
    """The dangerous misread: a green, silently empty sync on bad creds."""
    stub, base_url = kon_stub
    stub.add(_error("No user found for that login"))

    with pytest.raises(KonnektiveAuthError):
        list(_client(base_url).query("order/query", {}))


def test_error_merely_mentioning_not_found_is_not_empty(kon_stub: tuple[_Stub, str]) -> None:
    stub, base_url = kon_stub
    stub.router = lambda _r: (200, _error("Campaign 12 not found"), {})

    with pytest.raises(KonnektiveError, match="Campaign 12 not found"):
        list(_client(base_url).query("order/query", {}))


def test_transient_error_is_retried_then_succeeds(kon_stub: tuple[_Stub, str]) -> None:
    stub, base_url = kon_stub
    stub.add(_error("Server busy, please retry"))
    stub.add(_page([{"orderId": "A1"}], total=1, page=1))

    assert list(_client(base_url).query("order/query", {})) == [{"orderId": "A1"}]
    assert len(stub.captured) == 2


def test_persistent_error_raises_with_the_servers_message(
    kon_stub: tuple[_Stub, str],
) -> None:
    stub, base_url = kon_stub
    stub.router = lambda _r: (200, _error("Server busy, please retry"), {})

    with pytest.raises(KonnektiveError, match="Server busy") as excinfo:
        list(_client(base_url, max_retries=2).query("order/query", {}))

    assert not isinstance(excinfo.value, KonnektiveAuthError)
    assert len(stub.captured) == 3  # first try + 2 retries


def test_missing_result_key_is_an_error(kon_stub: tuple[_Stub, str]) -> None:
    stub, base_url = kon_stub
    stub.add({"unexpected": True})

    with pytest.raises(KonnektiveError, match="no 'result' key"):
        list(_client(base_url).query("order/query", {}))


# --------------------------------------------------------------------------
# Transport
# --------------------------------------------------------------------------


def test_429_is_retried_then_succeeds(kon_stub: tuple[_Stub, str]) -> None:
    stub, base_url = kon_stub
    stub.add({}, status=429, headers={"Retry-After": "1"})
    stub.add(_page([{"orderId": "A1"}], total=1, page=1))

    assert len(list(_client(base_url).query("order/query", {}))) == 1


def test_429_is_bounded(kon_stub: tuple[_Stub, str]) -> None:
    stub, base_url = kon_stub
    stub.router = lambda _r: (429, {}, {})

    with pytest.raises(KonnektiveError, match="rate-limited"):
        list(_client(base_url, max_retries=2).query("order/query", {}))
    assert len(stub.captured) == 3


def test_5xx_is_retried_then_raises(kon_stub: tuple[_Stub, str]) -> None:
    stub, base_url = kon_stub
    stub.router = lambda _r: (503, {}, {})

    with pytest.raises(KonnektiveError, match="HTTP 503"):
        list(_client(base_url, max_retries=2).query("order/query", {}))
    assert len(stub.captured) == 3


def test_http_403_is_an_auth_error_without_retry(kon_stub: tuple[_Stub, str]) -> None:
    stub, base_url = kon_stub
    stub.router = lambda _r: (403, {}, {})

    with pytest.raises(KonnektiveAuthError):
        list(_client(base_url).query("order/query", {}))
    assert len(stub.captured) == 1


def test_non_json_body_is_retried_then_raises(kon_stub: tuple[_Stub, str]) -> None:
    stub, base_url = kon_stub
    stub.router = lambda _r: (200, b"<html>gateway</html>", {})

    with pytest.raises(KonnektiveError, match="non-JSON"):
        list(_client(base_url, max_retries=1).query("order/query", {}))
    assert len(stub.captured) == 2


# --------------------------------------------------------------------------
# Pagination
# --------------------------------------------------------------------------


def test_paginates_until_total_results_is_reached(kon_stub: tuple[_Stub, str]) -> None:
    stub, base_url = kon_stub
    stub.add(_page([{"id": 1}, {"id": 2}], total=4, page=1, per_page=2))
    stub.add(_page([{"id": 3}, {"id": 4}], total=4, page=2, per_page=2))

    rows = list(_client(base_url).query("order/query", {"resultsPerPage": 2}))

    assert [r["id"] for r in rows] == [1, 2, 3, 4]
    assert [r.params["page"] for r in stub.captured] == ["1", "2"]
    assert len(stub.captured) == 2, "page*perPage >= total must end the walk"


def test_short_page_ends_the_walk_even_if_total_lies(kon_stub: tuple[_Stub, str]) -> None:
    stub, base_url = kon_stub
    stub.add(_page([{"id": 1}], total=999, page=1, per_page=2))

    assert len(list(_client(base_url).query("order/query", {"resultsPerPage": 2}))) == 1
    assert len(stub.captured) == 1


def test_page_size_is_capped_at_the_api_maximum(kon_stub: tuple[_Stub, str]) -> None:
    stub, base_url = kon_stub
    stub.add(_page([], total=0, page=1))

    list(_client(base_url).query("order/query", {"resultsPerPage": 5000}))

    assert stub.captured[0].params["resultsPerPage"] == "200"


def test_report_accepts_list_and_empty(kon_stub: tuple[_Stub, str]) -> None:
    stub, base_url = kon_stub
    stub.add({"result": "SUCCESS", "message": [{"date": "2026-01-05", "newSaleCnt": 3}]})
    stub.add(_error("No records matching those parameters could be found"))

    client = _client(base_url)
    assert client.report("transactions/summary", {}) == [
        {"date": "2026-01-05", "newSaleCnt": 3}
    ]
    assert client.report("transactions/summary", {}) == []


# --------------------------------------------------------------------------
# Windows + projection
# --------------------------------------------------------------------------


def test_windows_are_whole_days_without_gap_or_overlap() -> None:
    """Konnektive's filter ignores times, so a day must appear in exactly
    one window — a shared day would be fetched twice."""
    windows = _iter_windows(date(2026, 1, 1), date(2026, 1, 4), 1)

    assert windows == [(date(2026, 1, d), date(2026, 1, d)) for d in (1, 2, 3, 4)]


def test_windows_respect_window_days_and_floor_at_one() -> None:
    first, last = date(2026, 1, 1), date(2026, 1, 10)

    wide = _iter_windows(first, last, 4)
    assert wide == [
        (date(2026, 1, 1), date(2026, 1, 4)),
        (date(2026, 1, 5), date(2026, 1, 8)),
        (date(2026, 1, 9), date(2026, 1, 10)),
    ]
    covered = [s + timedelta(days=i) for s, e in wide for i in range((e - s).days + 1)]
    assert covered == [first + timedelta(days=i) for i in range(10)], "every day exactly once"
    assert len(_iter_windows(first, last, 0)) == 10


def test_future_cursor_still_yields_one_window() -> None:
    today = date(2026, 1, 1)
    assert _iter_windows(date(2027, 1, 1), today, 1) == [(today, today)]


def test_parse_day_accepts_every_cursor_shape() -> None:
    expected = date(2026, 1, 5)
    assert _parse_day("2026-01-05") == expected
    assert _parse_day("2026-01-05 10:11:12") == expected
    assert _parse_day("2026-01-05T10:11:12Z") == expected
    assert _parse_day(datetime(2026, 1, 5, 23, 59)) == expected
    assert _parse_day(expected) == expected


def test_excluded_keys_reach_neither_a_column_nor_raw() -> None:
    row = {"customerId": 5, "eCommercePassword": "hunter2", "city": "Vilnius"}

    out = _project(
        row, ("customerId", "eCommercePassword", "raw"), frozenset({"eCommercePassword"})
    )

    assert out["eCommercePassword"] is None, "even a DECLARED column stays empty"
    assert out["raw"] == {"customerId": 5, "city": "Vilnius"}
    assert "eCommercePassword" in row, "the caller's row is not mutated"


def test_project_keeps_raw_and_renames_digit_led_keys() -> None:
    row = {"transactionId": 7, "3DTxnResult": "Y", "brandNewField": "x"}

    out = _project(row, ("transactionId", "_3DTxnResult", "missing", "raw"))

    assert out == {
        "transactionId": 7,
        "_3DTxnResult": "Y",
        "missing": None,
        "raw": row,
    }
    assert "brandNewField" not in out, "undeclared keys live in raw only"


# --------------------------------------------------------------------------
# End-to-end
# --------------------------------------------------------------------------


def _write_project(tmp_path: Path, *, base_url: str, streams: str, extra: str = "") -> None:
    (tmp_path / "dtex_project.yml").write_text(
        "name: t\nversion: '0.1'\nsource_paths: []\n"
        "destination_paths: []\nconfig_paths:\n  - configs\n"
    )
    (tmp_path / "profiles.yml").write_text(
        "duckdb:\n  default_target: dev\n  targets:\n    dev:\n"
        "      path: '.dtex/warehouse.duckdb'\n"
    )
    (tmp_path / "configs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "configs" / "kon_test.yml").write_text(
        "name: kon_test\nsource: konnektive\ndestination: duckdb\ntarget: dev\n"
        f"params:\n  base_url: '{base_url}'\n{extra}"
        f"streams:\n{streams}\n"
    )


def _setenv(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KONNEKTIVE_LOGIN_ID", "login_e2e_secret")
    monkeypatch.setenv("KONNEKTIVE_PASSWORD", "password_e2e_secret")


def _recent(days_ago: int, clock: str = "10:00:00") -> str:
    """An account-local timestamp ``days_ago`` days back. Uses the local
    date, which is within a day of any account timezone — windows in these
    tests are matched by DATE, so that is exact enough."""
    return f"{(datetime.now() - timedelta(days=days_ago)).date().isoformat()} {clock}"


def _router_for(rows_by_path: dict[str, list[dict[str, Any]]]) -> Responder:
    """Serve each row from the window whose [startDate, endDate] holds its
    dateUpdated — the way the real API filters: BY DAY. Any time-of-day in
    the params is ignored (verified live 2026-09-21), so only the first ten
    characters of each side take part in the comparison."""

    def route(request: _Request) -> tuple[int, Any, dict[str, str]]:
        params = request.params
        rows = rows_by_path.get(request.path, [])
        if request.path == "/transactions/summary/":
            hits = [r for r in rows if r["date"] == params["startDate"]]
            if not hits:
                return 200, _error("No records matching those parameters could be found"), {}
            return 200, {"result": "SUCCESS", "message": hits}, {}
        hits = [
            r
            for r in rows
            if params["startDate"][:10] <= r["dateUpdated"][:10] <= params["endDate"][:10]
        ]
        if not hits:
            return 200, _EMPTY, {}
        return 200, _page(hits, total=len(hits), page=int(params["page"])), {}

    return route


def test_end_to_end_all_streams(
    kon_stub: tuple[_Stub, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stub, base_url = kon_stub
    _setenv(monkeypatch)
    day = (datetime.now() - timedelta(days=1)).date().isoformat()
    stub.router = _router_for(
        {
            "/order/query/": [
                {
                    "orderId": "ABC123",
                    "dateCreated": _recent(2),
                    "dateUpdated": _recent(1),
                    "customerId": "501",
                    "totalAmount": "29.99",
                    "hasUpsell": True,
                    "items": {"1": {"productId": "9", "price": "29.99"}},
                    "aFieldAddedNextQuarter": "kept",
                }
            ],
            "/transactions/query/": [
                {
                    "transactionId": "9001",
                    "dateUpdated": _recent(1),
                    "orderId": "ABC123",
                    "3DTxnResult": "Y",
                    "isChargedback": 0,
                    "totalAmount": "29.99",
                }
            ],
            "/purchase/query/": [
                {"purchaseId": "P77", "dateUpdated": _recent(1), "affId": "AFF-9", "productQty": 2}
            ],
            "/customer/query/": [
                {"customerId": 501, "dateUpdated": _recent(1), "notes": [{"message": "hi"}]}
            ],
            "/transactions/summary/": [{"date": day, "newSaleCnt": 12, "grossRevenue": "812.40"}],
        }
    )
    start = (datetime.now() - timedelta(days=3)).date().isoformat()
    _write_project(
        tmp_path,
        base_url=base_url,
        extra=f"  start_date: '{start}'\n",
        streams="  orders:\n  transactions:\n  purchases:\n  customers:\n  summary:",
    )

    db_path = str(tmp_path / "warehouse.duckdb")
    result = dtex.run(
        config="kon_test",
        project_dir=str(tmp_path),
        destination_params_override={"path": db_path},
    )
    assert result.status.value == "succeeded", result.error

    conn = duckdb.connect(db_path)
    order = conn.execute(
        "SELECT orderId, customerId, totalAmount, hasUpsell, dateUpdated, raw FROM orders"
    ).fetchall()
    txn = conn.execute(
        'SELECT transactionId, "_3DTxnResult", isChargedback FROM transactions'
    ).fetchall()
    purchase = conn.execute("SELECT purchaseId, affId, productQty FROM purchases").fetchall()
    customer = conn.execute("SELECT customerId, notes FROM customers").fetchall()
    summary_rows = conn.execute(
        "SELECT CAST(date AS VARCHAR), newSaleCnt, grossRevenue FROM summary"
    ).fetchall()
    conn.close()

    assert len(order) == 1
    order_id, customer_id, total, has_upsell, date_updated, raw = order[0]
    assert (order_id, customer_id, total, has_upsell) == ("ABC123", 501, "29.99", True)
    assert date_updated == _recent(1), "account-local timestamps land as sent"
    assert json.loads(raw)["aFieldAddedNextQuarter"] == "kept"
    assert txn == [(9001, "Y", 0)]
    assert purchase == [("P77", "AFF-9", 2)]
    assert customer[0][0] == 501 and json.loads(customer[0][1]) == [{"message": "hi"}]
    assert summary_rows == [(day, 12, "812.40")]

    query_requests = [r for r in stub.captured if r.path == "/order/query/"]
    assert all(r.method == "POST" and r.query == {} for r in stub.captured)
    assert query_requests[0].params["dateRangeType"] == "dateUpdated"
    assert query_requests[0].params["includeCustomFields"] == "1"
    # Bare dates, one whole day per request, consecutive, none repeated.
    spans = [(r.params["startDate"], r.params["endDate"]) for r in query_requests]
    assert spans[0] == (start, start)
    assert all(s == e and len(s) == 10 for s, e in spans)
    days = [date.fromisoformat(s) for s, _ in spans]
    assert days == [days[0] + timedelta(days=i) for i in range(len(days))]


def test_default_exclude_fields_never_land(
    kon_stub: tuple[_Stub, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The API returns credentials and bank-account numbers inside ordinary
    objects. By default they must not reach the warehouse — not even via
    ``raw``."""
    stub, base_url = kon_stub
    _setenv(monkeypatch)
    stub.router = _router_for(
        {
            "/customer/query/": [
                {
                    "customerId": 7,
                    "dateUpdated": _recent(0, "00:00:01"),
                    "eCommercePassword": "s3cr3t-hash",
                    "achAccountNumber": "000123456789",
                    "achRoutingNumber": "021000021",
                    "eCommerceLogin": "kept@example.com",
                }
            ]
        }
    )
    start = (datetime.now() - timedelta(days=1)).date().isoformat()
    _write_project(
        tmp_path, base_url=base_url, extra=f"  start_date: '{start}'\n", streams="  customers:"
    )
    db_path = str(tmp_path / "warehouse.duckdb")

    result = dtex.run(
        config="kon_test",
        project_dir=str(tmp_path),
        destination_params_override={"path": db_path},
    )
    assert result.status.value == "succeeded", result.error

    conn = duckdb.connect(db_path)
    (raw,) = conn.execute("SELECT raw FROM customers").fetchone()
    conn.close()
    landed = json.loads(raw)
    assert landed["eCommerceLogin"] == "kept@example.com"
    for key in ("eCommercePassword", "achAccountNumber", "achRoutingNumber"):
        assert key not in landed
    assert "s3cr3t-hash" not in raw and "000123456789" not in raw


def test_second_run_starts_from_cursor_day_minus_lookback(
    kon_stub: tuple[_Stub, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stub, base_url = kon_stub
    _setenv(monkeypatch)
    newest = _recent(1, "15:30:00")
    stub.router = _router_for(
        {"/order/query/": [{"orderId": "A1", "dateUpdated": newest}]}
    )
    start = (datetime.now() - timedelta(days=3)).date().isoformat()
    _write_project(
        tmp_path,
        base_url=base_url,
        extra=f"  start_date: '{start}'\n  lookback_days: 1\n",
        streams="  orders:",
    )
    db_path = str(tmp_path / "warehouse.duckdb")
    kwargs = {
        "config": "kon_test",
        "project_dir": str(tmp_path),
        "destination_params_override": {"path": db_path},
    }

    first = dtex.run(**kwargs)
    assert first.status.value == "succeeded", first.error
    stub.captured.clear()

    second = dtex.run(**kwargs)
    assert second.status.value == "succeeded", second.error

    # The cursor is a timestamp ("… 15:30:00"); the walk restarts from the
    # START of (its day − lookback_days), because the API filters by day.
    expected = (_parse_day(newest) - timedelta(days=1)).isoformat()
    assert stub.captured[0].params["startDate"] == expected
    assert stub.captured[0].params["endDate"] == expected

    conn = duckdb.connect(db_path)
    assert conn.execute("SELECT COUNT(*) FROM orders").fetchone() == (1,), "merge, not append"
    conn.close()


def test_credentials_never_appear_in_logs(
    kon_stub: tuple[_Stub, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Neither credential is logged across a whole engine run at DEBUG —
    which includes urllib3's request-line logging, the reason the
    connector POSTs."""
    stub, base_url = kon_stub
    monkeypatch.setenv("KONNEKTIVE_LOGIN_ID", "login_should_not_leak")
    monkeypatch.setenv("KONNEKTIVE_PASSWORD", "password_should_not_leak_123")
    stub.router = _router_for({})
    start = (datetime.now() - timedelta(days=1)).date().isoformat()
    _write_project(
        tmp_path, base_url=base_url, extra=f"  start_date: '{start}'\n", streams="  orders:"
    )

    with caplog.at_level("DEBUG"):
        result = dtex.run(
            config="kon_test",
            project_dir=str(tmp_path),
            destination_params_override={"path": str(tmp_path / "warehouse.duckdb")},
        )

    assert result.status.value == "succeeded", result.error
    assert stub.captured, "the stream must actually have called the API"
    full_log = "\n".join(record.getMessage() for record in caplog.records)
    assert "login_should_not_leak" not in full_log
    assert "password_should_not_leak_123" not in full_log
