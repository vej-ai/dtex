# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Tests for the baked Google Ads (gads) connector.

Every test stands up a tiny ``http.server.HTTPServer`` on a random port and
points :class:`GoogleAdsClient` at it for BOTH the OAuth token endpoint and
the GoogleAdsService ``searchStream`` endpoint — routed by request path. The
stub records every request (path, headers, body) and responds from a scripted
scenario, so there are no real network calls and no upstream flakes.

Areas covered:

* `GoogleAdsClient` unit tests — OAuth token exchange (POST body shape +
  caching), the three required headers (Authorization / developer-token /
  login-customer-id with hyphens stripped), 429-then-success, bounded 429,
  5xx retry, non-retryable error raises.
* `search_stream` — parses the JSON-array-of-`{results:[...]}` shape and
  yields every row across chunks.
* `_flatten_row` — nested GoogleAdsRow → flat snake_case columns.
* The `@stream` functions end-to-end through `dtex.run` into DuckDB, with the
  `segments.date` cursor advancing only past complete days.
* The four OAuth secrets never appear in logs.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import duckdb
import pytest

import dtex
from dtex.sources.gads.client import GoogleAdsClient
from dtex.sources.gads.source import _flatten_row

# A valid OAuth token response the stub returns for any POST to the token URL.
_TOKEN_BODY = {"access_token": "ya29.test-access-token", "expires_in": 3600}


# --------------------------------------------------------------------------
# Stub server — routes token vs searchStream by path; on a random port
# --------------------------------------------------------------------------


class _RequestRecord:
    def __init__(self, path: str, headers: dict[str, str], body: Any) -> None:
        self.path = path
        self.headers = headers
        self.body = body


class _Scenario:
    """Scripts searchStream responses + captures requests.

    Token POSTs are always answered with ``_TOKEN_BODY`` (unless a token
    response is explicitly queued). searchStream POSTs pop the next queued
    response. The captured request list is available as ``.captured``.
    """

    def __init__(self) -> None:
        self._stream_queue: list[dict[str, Any]] = []
        self._token_queue: list[dict[str, Any]] = []
        self.captured: list[_RequestRecord] = []

    def add_stream(
        self,
        *,
        status: int = 200,
        json_body: Any = None,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        self._stream_queue.append(
            {"status": status, "body": json_body, "headers": extra_headers or {}}
        )

    def add_token(
        self, *, status: int = 200, json_body: Any = None
    ) -> None:
        self._token_queue.append(
            {"status": status, "body": json_body, "headers": {}}
        )

    def pop_stream(self) -> dict[str, Any]:
        if not self._stream_queue:
            return {"status": 500, "body": {"error": "scenario exhausted"}, "headers": {}}
        return self._stream_queue.pop(0)

    def pop_token(self) -> dict[str, Any]:
        if not self._token_queue:
            return {"status": 200, "body": _TOKEN_BODY, "headers": {}}
        return self._token_queue.pop(0)


def _make_handler(scenario: _Scenario) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args: Any) -> None:
            return

        def do_POST(self) -> None:  # noqa: N802 — required by stdlib
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length) if length else b""
            scenario.captured.append(
                _RequestRecord(self.path, dict(self.headers), raw)
            )
            if "/token" in self.path:
                response = scenario.pop_token()
            else:
                response = scenario.pop_stream()
            body = json.dumps(response["body"] if response["body"] is not None else {})
            encoded = body.encode()
            self.send_response(response["status"])
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            for k, v in response["headers"].items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(encoded)

    return Handler


@pytest.fixture
def gads_stub() -> Iterator[tuple[_Scenario, str]]:
    """Spin up a stub server on a random port; yield ``(scenario, base_url)``."""
    scenario = _Scenario()
    handler_cls = _make_handler(scenario)
    server = HTTPServer(("127.0.0.1", 0), handler_cls)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield scenario, f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5.0)


def _client(base_url: str, *, max_retries: int = 3, login: str | None = None) -> GoogleAdsClient:
    return GoogleAdsClient(
        developer_token="dev_test",
        client_id="client_test",
        client_secret="secret_test",
        refresh_token="refresh_test",
        login_customer_id=login,
        base_url=base_url,
        token_url=f"{base_url}/token",
        max_retries=max_retries,
    )


def _stream_chunk(results: list[dict]) -> list[dict]:
    """searchStream returns a JSON array of chunks, each with a results list."""
    return [{"results": results}]


# --------------------------------------------------------------------------
# OAuth token exchange
# --------------------------------------------------------------------------


def test_token_exchange_post_body(gads_stub: tuple[_Scenario, str]) -> None:
    """The token POST carries grant_type=refresh_token + client creds (form-encoded)."""
    scenario, base_url = gads_stub
    scenario.add_stream(json_body=_stream_chunk([]))

    list(_client(base_url).search_stream("123", "SELECT campaign.id FROM campaign"))

    token_req = next(r for r in scenario.captured if "/token" in r.path)
    body = token_req.body.decode()
    assert "grant_type=refresh_token" in body
    assert "client_id=client_test" in body
    assert "refresh_token=refresh_test" in body


def test_token_is_cached_across_calls(gads_stub: tuple[_Scenario, str]) -> None:
    """A second searchStream within token expiry reuses the token — no re-mint."""
    scenario, base_url = gads_stub
    scenario.add_stream(json_body=_stream_chunk([]))
    scenario.add_stream(json_body=_stream_chunk([]))

    client = _client(base_url)
    list(client.search_stream("123", "Q1"))
    list(client.search_stream("123", "Q2"))

    token_reqs = [r for r in scenario.captured if "/token" in r.path]
    assert len(token_reqs) == 1  # minted once, reused


# --------------------------------------------------------------------------
# Required headers
# --------------------------------------------------------------------------


def test_search_stream_sends_required_headers(gads_stub: tuple[_Scenario, str]) -> None:
    """Authorization Bearer + developer-token + login-customer-id (hyphens stripped)."""
    scenario, base_url = gads_stub
    scenario.add_stream(json_body=_stream_chunk([]))

    list(_client(base_url, login="123-456-7890").search_stream("999", "Q"))

    stream_req = next(r for r in scenario.captured if ":searchStream" in r.path)
    assert stream_req.headers.get("Authorization") == "Bearer ya29.test-access-token"
    assert stream_req.headers.get("developer-token") == "dev_test"
    assert stream_req.headers.get("login-customer-id") == "1234567890"


def test_no_login_customer_id_header_when_unset(gads_stub: tuple[_Scenario, str]) -> None:
    """Without a manager id, the login-customer-id header is omitted entirely."""
    scenario, base_url = gads_stub
    scenario.add_stream(json_body=_stream_chunk([]))

    list(_client(base_url, login=None).search_stream("999", "Q"))

    stream_req = next(r for r in scenario.captured if ":searchStream" in r.path)
    assert "login-customer-id" not in stream_req.headers


def test_customer_id_path_strips_hyphens(gads_stub: tuple[_Scenario, str]) -> None:
    """The customer id in the URL path has hyphens stripped."""
    scenario, base_url = gads_stub
    scenario.add_stream(json_body=_stream_chunk([]))

    list(_client(base_url).search_stream("123-456-7890", "Q"))

    stream_req = next(r for r in scenario.captured if ":searchStream" in r.path)
    assert "/customers/1234567890/googleAds:searchStream" in stream_req.path


# --------------------------------------------------------------------------
# Retry behavior
# --------------------------------------------------------------------------


def test_429_honored_then_succeeds(
    gads_stub: tuple[_Scenario, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    scenario, base_url = gads_stub
    scenario.add_stream(status=429, json_body={"error": "x"}, extra_headers={"Retry-After": "1"})
    scenario.add_stream(json_body=_stream_chunk([{"campaign": {"id": "1"}}]))

    sleeps: list[float] = []
    monkeypatch.setattr("dtex.sources.gads.client.time.sleep", lambda s: sleeps.append(s))

    rows = list(_client(base_url).search_stream("123", "Q"))
    assert rows == [{"campaign": {"id": "1"}}]
    assert sleeps == [1.0]


def test_429_bounded_by_max_retries(
    gads_stub: tuple[_Scenario, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    scenario, base_url = gads_stub
    for _ in range(10):
        scenario.add_stream(
            status=429, json_body={"error": "x"}, extra_headers={"Retry-After": "1"}
        )

    monkeypatch.setattr("dtex.sources.gads.client.time.sleep", lambda s: None)

    with pytest.raises(RuntimeError, match="rate-limited after"):
        list(_client(base_url, max_retries=3).search_stream("123", "Q"))


def test_500_retries_then_succeeds(
    gads_stub: tuple[_Scenario, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    scenario, base_url = gads_stub
    scenario.add_stream(status=500, json_body={"error": "x"})
    scenario.add_stream(json_body=_stream_chunk([{"campaign": {"id": "1"}}]))

    monkeypatch.setattr("dtex.sources.gads.client.time.sleep", lambda s: None)

    rows = list(_client(base_url).search_stream("123", "Q"))
    assert rows == [{"campaign": {"id": "1"}}]


def test_403_raises_immediately(gads_stub: tuple[_Scenario, str]) -> None:
    scenario, base_url = gads_stub
    scenario.add_stream(status=403, json_body={"error": "permission denied"})

    with pytest.raises(RuntimeError, match="HTTP 403"):
        list(_client(base_url).search_stream("123", "Q"))


# --------------------------------------------------------------------------
# search_stream parsing
# --------------------------------------------------------------------------


def test_search_stream_yields_rows_across_chunks(gads_stub: tuple[_Scenario, str]) -> None:
    """searchStream's JSON array of chunks is flattened into one row stream."""
    scenario, base_url = gads_stub
    scenario.add_stream(
        json_body=[
            {"results": [{"campaign": {"id": "1"}}, {"campaign": {"id": "2"}}]},
            {"results": [{"campaign": {"id": "3"}}]},
        ]
    )

    rows = list(_client(base_url).search_stream("123", "Q"))
    assert [r["campaign"]["id"] for r in rows] == ["1", "2", "3"]


def test_search_stream_empty_array(gads_stub: tuple[_Scenario, str]) -> None:
    """An empty result set (empty array) yields no rows, no error."""
    scenario, base_url = gads_stub
    scenario.add_stream(json_body=[])
    assert list(_client(base_url).search_stream("123", "Q")) == []


# --------------------------------------------------------------------------
# list_child_accounts — MCC auto-discovery
# --------------------------------------------------------------------------


def test_list_child_accounts_returns_leaf_ids(gads_stub: tuple[_Scenario, str]) -> None:
    """customer_client rows are reduced to a list of child ids; query is run as the MCC."""
    scenario, base_url = gads_stub
    scenario.add_stream(
        json_body=_stream_chunk(
            [
                {"customerClient": {"id": "111", "level": "1", "manager": False}},
                {"customerClient": {"id": "222", "level": "1", "manager": False}},
            ]
        )
    )

    ids = _client(base_url).list_child_accounts("99-88-77", max_depth=2)

    assert ids == ["111", "222"]
    # The discovery call is issued as the manager: login-customer-id = MCC
    # (hyphens stripped), and against the MCC's own searchStream path.
    req = next(r for r in scenario.captured if ":searchStream" in r.path)
    assert req.headers.get("login-customer-id") == "998877"
    assert "/customers/998877/googleAds:searchStream" in req.path
    body = json.loads(req.body.decode())
    assert "customer_client.level <= 2" in body["query"]
    assert "customer_client.manager = false" in body["query"]
    assert "ENABLED" in body["query"]


def test_list_child_accounts_restores_login_id(gads_stub: tuple[_Scenario, str]) -> None:
    """After discovery, the client's configured login_customer_id is restored."""
    scenario, base_url = gads_stub
    scenario.add_stream(json_body=_stream_chunk([]))

    client = _client(base_url, login="555")
    client.list_child_accounts("999")
    assert client.login_customer_id == "555"


# --------------------------------------------------------------------------
# _flatten_row
# --------------------------------------------------------------------------


def test_flatten_row_nested_to_flat() -> None:
    """Nested GoogleAdsRow paths map to flat snake_case columns + injected customer_id."""
    from dtex.sources.gads.source import _FIELD_MAP

    row = {
        "segments": {"date": "2026-01-01"},
        "campaign": {"id": "111", "name": "Brand"},
        "metrics": {"clicks": "5", "costMicros": "2500000", "conversions": 1.5},
    }
    flat = _flatten_row(row, _FIELD_MAP["campaign_daily_stats"], "999")
    assert flat["customer_id"] == "999"
    assert flat["date"] == "2026-01-01"
    assert flat["campaign_id"] == "111"
    assert flat["campaign_name"] == "Brand"
    assert flat["clicks"] == "5"
    assert flat["cost_micros"] == "2500000"
    assert flat["conversions"] == 1.5
    # Missing leaf (conversions_value not in the row) → None, not KeyError.
    assert flat["conversions_value"] is None


def test_flatten_row_deep_nesting() -> None:
    """Deep paths (adGroupAd.ad.id) resolve correctly."""
    from dtex.sources.gads.source import _FIELD_MAP

    row = {
        "segments": {"date": "2026-01-01"},
        "adGroupAd": {"ad": {"id": "ad_77"}, "status": "ENABLED"},
    }
    flat = _flatten_row(row, _FIELD_MAP["ad_daily_stats"], "999")
    assert flat["ad_id"] == "ad_77"
    assert flat["status"] == "ENABLED"


# --------------------------------------------------------------------------
# _resolve_customer_ids — explicit list OR MCC auto-expand
# --------------------------------------------------------------------------


class _FakeConfig:
    """Minimal Config stand-in exposing .get() for the resolver tests."""

    def __init__(self, params: dict[str, Any]) -> None:
        self._params = params

    def get(self, name: str, default: Any = None) -> Any:
        return self._params.get(name, default)


class _FakeClient:
    """Records list_child_accounts calls; returns a canned id list."""

    def __init__(self, children: list[str]) -> None:
        self.children = children
        self.calls: list[tuple[str, int]] = []

    def list_child_accounts(self, manager_id: str, *, max_depth: int = 1) -> list[str]:
        self.calls.append((manager_id, max_depth))
        return self.children


def _resolve(params: dict[str, Any], children: list[str]) -> tuple[list[str], _FakeClient]:
    import logging

    from dtex.sources.gads.source import _resolve_customer_ids

    client = _FakeClient(children)
    ids = _resolve_customer_ids(_FakeConfig(params), client, logging.getLogger("t"))  # type: ignore[arg-type]
    return ids, client


def test_resolve_explicit_list_wins() -> None:
    """An explicit customer_ids wins — no discovery call even if a manager is set."""
    ids, client = _resolve(
        {"customer_ids": "123-456-7890, 222", "auto_discover_from_manager": "999"},
        children=["should_not_be_used"],
    )
    assert ids == ["1234567890", "222"]
    assert client.calls == []  # discovery never invoked


def test_resolve_auto_discovers_when_no_explicit() -> None:
    """Empty customer_ids + a manager → discovery, passing max_discovery_depth."""
    ids, client = _resolve(
        {
            "customer_ids": "",
            "auto_discover_from_manager": "99-88-77",
            "max_discovery_depth": 3,
        },
        children=["111", "222"],
    )
    assert ids == ["111", "222"]
    assert client.calls == [("998877", 3)]


def test_resolve_raises_when_neither_given() -> None:
    """Neither explicit ids nor a manager → a clear error."""
    with pytest.raises(ValueError, match="customer_ids.*auto_discover_from_manager"):
        _resolve({}, children=[])


def test_resolve_raises_when_discovery_empty() -> None:
    """A manager that expands to nothing raises rather than silently no-op."""
    with pytest.raises(ValueError, match="found no enabled"):
        _resolve({"auto_discover_from_manager": "999"}, children=[])


# --------------------------------------------------------------------------
# End-to-end through dtex.run into DuckDB
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


def _write_config(tmp_path: Path, *, base_url: str, streams: str) -> None:
    (tmp_path / "configs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "configs" / "gads_test.yml").write_text(
        "name: gads_test\n"
        "source: gads\n"
        "destination: duckdb\n"
        "target: dev\n"
        "params:\n"
        "  customer_ids: '123-456-7890'\n"
        f"  base_url: '{base_url}'\n"
        f"  token_url: '{base_url}/token'\n"
        f"streams:\n{streams}\n"
    )


def _set_oauth_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GADS_DEVELOPER_TOKEN", "dev_test")
    monkeypatch.setenv("GADS_CLIENT_ID", "client_test")
    monkeypatch.setenv("GADS_CLIENT_SECRET", "secret_test")
    monkeypatch.setenv("GADS_REFRESH_TOKEN", "refresh_test")


def test_end_to_end_campaigns(
    gads_stub: tuple[_Scenario, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """campaigns (entity, replace) lands rows in DuckDB through the engine."""
    scenario, base_url = gads_stub
    _set_oauth_env(monkeypatch)
    scenario.add_stream(
        json_body=_stream_chunk(
            [
                {"campaign": {"id": "1", "name": "Brand", "status": "ENABLED"}},
                {"campaign": {"id": "2", "name": "Generic", "status": "PAUSED"}},
            ]
        )
    )

    _write_project(tmp_path)
    _write_config(tmp_path, base_url=base_url, streams="  campaigns:")

    db_path = str(tmp_path / "warehouse.duckdb")
    result = dtex.run(
        config="gads_test",
        project_dir=str(tmp_path),
        destination_params_override={"path": db_path},
    )
    assert result.status.value == "succeeded", result.error

    conn = duckdb.connect(db_path)
    rows = conn.execute(
        "SELECT customer_id, campaign_id, campaign_name, status FROM campaigns ORDER BY campaign_id"
    ).fetchall()
    conn.close()
    assert rows == [
        ("1234567890", "1", "Brand", "ENABLED"),
        ("1234567890", "2", "Generic", "PAUSED"),
    ]


def test_end_to_end_campaign_daily_stats_cursor(
    gads_stub: tuple[_Scenario, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """campaign_daily_stats flattens metrics, coerces types, and advances the
    cursor only past complete (before-today) days."""
    scenario, base_url = gads_stub
    _set_oauth_env(monkeypatch)

    # Two days; both are clearly in the past so both are "complete" and the
    # cursor advances to the later one.
    scenario.add_stream(
        json_body=_stream_chunk(
            [
                {
                    "segments": {"date": "2024-01-10"},
                    "campaign": {"id": "1", "name": "Brand"},
                    "metrics": {"clicks": "5", "costMicros": "2500000", "conversions": 1.0},
                },
                {
                    "segments": {"date": "2024-01-11"},
                    "campaign": {"id": "1", "name": "Brand"},
                    "metrics": {"clicks": "9", "costMicros": "4000000", "conversions": 2.0},
                },
            ]
        )
    )

    _write_project(tmp_path)
    _write_config(
        tmp_path,
        base_url=base_url,
        streams=(
            "  campaign_daily_stats:\n    params:\n"
            "      segments_initial_since_date: '2024-01-01'\n"
            "      segments_lookback_days: 0"
        ),
    )

    db_path = str(tmp_path / "warehouse.duckdb")
    result = dtex.run(
        config="gads_test",
        project_dir=str(tmp_path),
        destination_params_override={"path": db_path},
    )
    assert result.status.value == "succeeded", result.error

    conn = duckdb.connect(db_path)
    rows = conn.execute(
        "SELECT date, campaign_id, clicks, cost_micros, conversions "
        "FROM campaign_daily_stats ORDER BY date"
    ).fetchall()
    conn.close()
    # Metrics arrived as strings; NORMALIZE coerced clicks/cost to INTEGER.
    assert rows[0][1] == "1"
    assert rows[0][2] == 5
    assert rows[0][3] == 2500000
    assert len(rows) == 2


def test_oauth_secrets_never_appear_in_logs(
    gads_stub: tuple[_Scenario, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """None of the four resolved OAuth secrets leak into captured logs."""
    scenario, base_url = gads_stub
    secrets = {
        "GADS_DEVELOPER_TOKEN": "dev_super_secret_111",
        "GADS_CLIENT_ID": "client_super_secret_222",
        "GADS_CLIENT_SECRET": "secret_super_secret_333",
        "GADS_REFRESH_TOKEN": "refresh_super_secret_444",
    }
    for k, v in secrets.items():
        monkeypatch.setenv(k, v)
    scenario.add_stream(json_body=_stream_chunk([]))

    _write_project(tmp_path)
    _write_config(tmp_path, base_url=base_url, streams="  campaigns:")

    db_path = str(tmp_path / "warehouse.duckdb")
    with caplog.at_level("DEBUG"):
        result = dtex.run(
            config="gads_test",
            project_dir=str(tmp_path),
            destination_params_override={"path": db_path},
        )
    assert result.status.value == "succeeded", result.error

    full_log = "\n".join(record.getMessage() for record in caplog.records)
    for v in secrets.values():
        assert v not in full_log, f"OAuth secret {v!r} leaked into logs"


# ---------------------------------------------------------------------------
# get_refresh_token — the one-shot OAuth helper.
#
# Regression coverage for three defects found in the field (2026-09-10): a
# real consent was completed in a browser and the token still could not be
# minted, because the loopback server had already closed.
# ---------------------------------------------------------------------------


@pytest.fixture()
def free_tcp_port() -> int:
    """A port no one is listening on.

    Local rather than pytest-asyncio's fixture of the same name: that plugin
    is not a declared test dependency, so relying on it passes wherever it
    happens to be installed and errors in a clean venv (and in CI).
    """
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _capture_in_thread(port: int, state: str, timeout: int) -> dict[str, Any]:
    """Run _capture_code off-thread and report what it returned or raised."""
    from dtex.sources.gads.scripts import get_refresh_token as grt

    out: dict[str, Any] = {}

    def run() -> None:
        try:
            out["code"] = grt._capture_code(
                f"http://localhost:{port}/",
                "http://example.invalid/consent",
                port,
                state,
                timeout,
            )
        except SystemExit as exc:
            out["err"] = str(exc)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    out["_thread"] = thread
    return out


def _get(url: str) -> int:
    """GET a URL, returning the status code (including for 4xx)."""
    import urllib.error
    import urllib.request

    try:
        with urllib.request.urlopen(url, timeout=3) as resp:
            return int(resp.status)
    except urllib.error.HTTPError as exc:
        return int(exc.code)


@pytest.fixture()
def _no_browser(monkeypatch: pytest.MonkeyPatch) -> None:
    """Never launch a real browser from a test."""
    from dtex.sources.gads.scripts import get_refresh_token as grt

    monkeypatch.setattr(grt.webbrowser, "open", lambda *a, **k: True)


def test_refresh_token_favicon_does_not_consume_the_callback(
    _no_browser: None, free_tcp_port: int
) -> None:
    """A /favicon.ico probe must not end the wait for the real redirect.

    The helper used to call handle_request() exactly once, so the FIRST HTTP
    request of any kind closed the server. Browsers routinely ask for
    /favicon.ico, which meant the genuine callback then hit a dead port and
    the user saw ERR_CONNECTION_REFUSED with a valid code in the URL bar.
    """
    result = _capture_in_thread(free_tcp_port, "STATE123", timeout=20)
    time.sleep(0.5)  # let the server bind

    assert _get(f"http://127.0.0.1:{free_tcp_port}/favicon.ico") == 404
    assert _get(f"http://127.0.0.1:{free_tcp_port}/") == 404

    assert (
        _get(f"http://127.0.0.1:{free_tcp_port}/?code=REALCODE&state=STATE123")
        == 200
    )
    result["_thread"].join(10)

    assert result.get("code") == "REALCODE", result


def test_refresh_token_rejects_state_mismatch(
    _no_browser: None, free_tcp_port: int
) -> None:
    """A redirect whose state does not round-trip is refused.

    `state` was previously generated and sent but never compared, leaving the
    loopback server willing to accept a stray or forged redirect.
    """
    result = _capture_in_thread(free_tcp_port, "GOODSTATE", timeout=20)
    time.sleep(0.5)

    _get(f"http://127.0.0.1:{free_tcp_port}/?code=X&state=FORGED")
    result["_thread"].join(10)

    assert "code" not in result
    assert "state mismatch" in result.get("err", "")


def test_refresh_token_times_out_with_rescue_hint(
    _no_browser: None, free_tcp_port: int
) -> None:
    """An abandoned consent times out instead of blocking forever.

    The message must point at --code, which rescues a consent whose redirect
    never reached the loopback server.
    """
    result = _capture_in_thread(free_tcp_port, "S", timeout=2)
    result["_thread"].join(20)

    assert "code" not in result
    assert "--code" in result.get("err", "")


def test_refresh_token_port_in_use_is_a_clear_error(
    _no_browser: None, free_tcp_port: int
) -> None:
    """Binding a taken port explains itself instead of raising a bare OSError."""
    from dtex.sources.gads.scripts import get_refresh_token as grt

    blocker = HTTPServer(("127.0.0.1", free_tcp_port), BaseHTTPRequestHandler)
    try:
        with pytest.raises(SystemExit) as excinfo:
            grt._capture_code(
                f"http://localhost:{free_tcp_port}/",
                "http://example.invalid/consent",
                free_tcp_port,
                "S",
                5,
            )
        assert "--port" in str(excinfo.value)
    finally:
        blocker.server_close()


# ---------------------------------------------------------------------------
# _date_window — which days an incremental run actually asks Google for.
#
# Regression coverage for a backfill that silently fetched nothing
# (2026-09-10). The window was `max(cursor or initial, today - lookback)`,
# which is wrong at BOTH ends: on a first run it picks today-minus-lookback
# over the configured start date (skipping the whole history), and on a
# resumed run it picks the cursor over cursor-minus-lookback (so the lookback
# never re-pulls anything).
# ---------------------------------------------------------------------------


class _FakeCursor:
    """Minimal Cursor stand-in exposing just start_value()."""

    def __init__(self, value: Any) -> None:
        self._value = value

    def start_value(self) -> Any:
        return self._value


def test_date_window_first_run_backfills_the_whole_history() -> None:
    """No cursor → the configured start date, not today-minus-lookback.

    The old `max()` form backfilled only the last `lookback` days. With a
    2024-01-01 start and a 14-day lookback that skipped 969 days; worse, if
    the account had no activity in that window (paused campaigns) the run
    "succeeded" with zero rows and looked like a healthy no-op.
    """
    from dtex.sources.gads.source import _date_window

    since, until = _date_window(
        _FakeConfig(
            {"segments_initial_since_date": "2024-01-01", "segments_lookback_days": 14}
        ),
        _FakeCursor(None),
    )

    assert since == date(2024, 1, 1)
    assert until == datetime.now(tz=UTC).date()


def test_date_window_resumed_run_steps_back_by_the_lookback() -> None:
    """A cursor → cursor minus lookback, so recent days are actually re-pulled.

    Google Ads restates conversions for weeks (view-through and modelled
    conversions land late). The old form returned the cursor itself, so the
    declared lookback re-pulled nothing and late conversions were never seen.
    """
    from dtex.sources.gads.source import _date_window

    since, _ = _date_window(
        _FakeConfig({"segments_lookback_days": 14}),
        _FakeCursor(date(2026, 9, 5)),
    )

    assert since == date(2026, 8, 22)


def test_date_window_accepts_a_string_cursor() -> None:
    """State round-trips through JSON, so the cursor can come back as a str."""
    from dtex.sources.gads.source import _date_window

    since, _ = _date_window(
        _FakeConfig({"segments_lookback_days": 7}),
        _FakeCursor("2026-09-05"),
    )

    assert since == date(2026, 8, 29)


def test_date_window_never_starts_after_today() -> None:
    """A cursor in the future (clock skew, a bad manual state edit) is clamped."""
    from dtex.sources.gads.source import _date_window

    today = datetime.now(tz=UTC).date()
    since, until = _date_window(
        _FakeConfig({"segments_lookback_days": 0}),
        _FakeCursor(today + timedelta(days=30)),
    )

    assert since <= until == today
