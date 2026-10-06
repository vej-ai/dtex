"""Synthetic, offline delivery tests. No provider accounts or recorded payloads."""

from __future__ import annotations

import json
import traceback
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import duckdb
import pytest
import requests

import dtex
from dtex import Config, Field, FieldType, RunStatus, Schema, StreamMeta, WriteDisposition
from dtex.destinations.klaviyo import client as transport
from dtex.destinations.klaviyo import destination as dest
from dtex.engine import discovery


def event(n: int = 1, **overrides: Any) -> dict[str, Any]:
    return {
        "unique_id": f"fixture-{n}", "metric_name": "fixture_metric",
        "time": datetime(2026, 1, 1, tzinfo=UTC),
        "profile": {"id": "fixture-profile"}, "properties": {"count": n},
        **overrides,
    }


class Response:
    def __init__(self, status: int, retry_after: str | None = None) -> None:
        self.status_code = status
        self.headers = {"Retry-After": retry_after} if retry_after else {}
        self.closed = False

    def close(self) -> None:
        self.closed = True

    @property
    def text(self) -> str:
        raise AssertionError("response body must never be read or logged")


class Session:
    def __init__(self, replies: list[Any]) -> None:
        self.headers: dict[str, str] = {}
        self.replies = replies
        self.calls: list[dict[str, Any]] = []
        self.closed = False

    def post(self, url: str, **kwargs: Any) -> Response:
        self.calls.append({"url": url, **kwargs})
        reply = self.replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply  # type: ignore[no-any-return]

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def session(monkeypatch: pytest.MonkeyPatch) -> Session:
    result = Session([])
    monkeypatch.setattr(requests, "Session", lambda: result)
    monkeypatch.setattr(transport.time, "sleep", lambda _: None)
    return result


def entries(payload: bytes) -> list[dict[str, Any]]:
    return json.loads(payload)["data"]["attributes"]["events-bulk-create"]["data"]


def test_envelope_preserves_id_time_and_explicit_backfill() -> None:
    payload = transport.prepare_requests([event(_dtex_synced_at="ignored")], backfill=True)[0]
    bulk = entries(payload)[0]["attributes"]
    sent = bulk["events"]["data"][0]["attributes"]
    assert bulk["backfill"] is True
    assert bulk["profile"]["data"] == {"type": "profile", "id": "fixture-profile"}
    assert sent["unique_id"] == "fixture-1"
    assert sent["time"] == "2026-01-01T00:00:00+00:00"
    assert sent["properties"] == {"count": 1}
    assert "_dtex_synced_at" not in payload.decode()


@pytest.mark.parametrize("change", [
    {"unique_id": None}, {"metric_name": ""}, {"time": "2026-01-01"},
    {"time": "not-a-time"}, {"profile": {}}, {"properties": []},
    {"properties": {"x": float("nan")}}, {"value": True},
    {"value_currency": "usd"}, {"profile": {"id": ""}},
    {"properties": {"x": "x" * 100_001}},
])
def test_rejects_invalid_records(change: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        transport.prepare_requests([event(**change)], backfill=False)


def test_bulk_limits_and_duplicate_ids() -> None:
    chunks = transport.prepare_requests([event(n) for n in range(1001)], backfill=False)
    assert [len(entries(c)) for c in chunks] == [1000, 1]
    assert all(len(c) <= transport.MAX_PAYLOAD_BYTES for c in chunks)
    repeated = transport.prepare_requests([event(), event()], backfill=False)
    assert len(repeated) == 2
    assert repeated[0] == repeated[1]


def test_size_split_and_single_record_rejection(monkeypatch: pytest.MonkeyPatch) -> None:
    one = transport.prepare_requests([event()], backfill=False)[0]
    monkeypatch.setattr(transport, "MAX_PAYLOAD_BYTES", len(one) + 1)
    assert len(transport.prepare_requests([event(), event(2)], backfill=False)) == 2
    with pytest.raises(ValueError, match="size limit"):
        transport.prepare_requests([event(properties={"text": "x" * 100})], backfill=False)


def test_transport_retries_identical_bytes_without_redirects(session: Session) -> None:
    throttled, failed, accepted = Response(429, "1"), Response(503), Response(202)
    session.replies[:] = [throttled, failed, accepted]
    client = transport.KlaviyoDeliveryClient("fixture-key")
    payload = transport.prepare_requests([event()], backfill=False)[0]
    client.send(payload)
    assert [c["data"] for c in session.calls] == [payload] * 3
    assert all(c["url"] == transport.ENDPOINT and c["allow_redirects"] is False
               for c in session.calls)
    assert all(c["timeout"] == (5.0, 60.0) for c in session.calls)
    assert all(r.closed for r in [throttled, failed, accepted])


@pytest.mark.parametrize("status", [301, 302, 307, 400, 401, 403, 413])
def test_no_redirect_or_deterministic_error_retry(session: Session, status: int) -> None:
    session.replies[:] = [Response(status)]
    with pytest.raises(transport.DeliveryError, match=f"HTTP {status}"):
        transport.KlaviyoDeliveryClient("fixture-key").send(b"{}")
    assert len(session.calls) == 1


def test_network_error_cannot_leak_in_traceback(session: Session) -> None:
    sensitive = "fixture-private-payload-and-credential"
    session.replies[:] = [requests.ConnectionError(sensitive)]
    try:
        transport.KlaviyoDeliveryClient("fixture-key", max_attempts=1).send(b"{}")
    except transport.DeliveryError:
        assert sensitive not in traceback.format_exc()
    else:
        pytest.fail("expected delivery failure")


@pytest.mark.parametrize("retry_after", ["600", "nan", "bad-delay"])
def test_retry_delay_is_bounded_and_sanitized(session: Session, retry_after: str) -> None:
    session.replies[:] = [Response(429, retry_after)]
    with pytest.raises(transport.DeliveryError):
        transport.KlaviyoDeliveryClient("fixture-key").send(b"{}")
    assert len(session.calls) == 1


def test_discovery_contains_only_delivery_hooks(tmp_path: Path) -> None:
    loaded = discovery.resolve_destination("klaviyo", tmp_path, [])
    assert loaded.registry.missing_mandatory_hooks() == ()
    assert "transaction" not in loaded.registry.hook_names
    assert "state_backend" not in loaded.registry.hook_names


def test_invalid_batch_validated_before_network(session: Session, tmp_path: Path) -> None:
    conn = dest.open(Config(params={"backfill": True, "state_path": str(tmp_path / "state.db")},
                            secrets={"api_key": "fixture-key"}))
    meta = StreamMeta(table="events", write_disposition=WriteDisposition.APPEND,
                      schema=Schema(fields=(
                          Field(name="unique_id", type=FieldType.STRING),
                          Field(name="metric_name", type=FieldType.STRING),
                          Field(name="time", type=FieldType.TIMESTAMP),
                          Field(name="profile", type=FieldType.JSON),
                      )))
    try:
        with pytest.raises(ValueError):
            dest.write_batch(conn, [event(), event(2, unique_id=None)], meta)
        assert not session.calls
    finally:
        dest.close(conn)


def test_explicit_mode_and_persistent_state_required(session: Session) -> None:
    with pytest.raises(ValueError, match="explicit boolean"):
        dest.open(Config())
    with pytest.raises(ValueError, match="persistent"):
        dest.open(Config(params={"backfill": True, "state_path": ":memory:"}))
    assert not session.calls


def test_bigquery_backend_receives_only_state_configuration(
    monkeypatch: pytest.MonkeyPatch, session: Session,
) -> None:
    captured: list[Config] = []
    backend = SimpleNamespace(open=lambda c: captured.append(c) or object(), close=lambda c: None)
    monkeypatch.setattr(dest.importlib, "import_module", lambda name: backend)
    conn = dest.open(Config(params={"backfill": False, "state_backend": "bigquery",
                                   "state_project": "fixture-project",
                                   "state_dataset": "delivery_state",
                                   "state_staging_bucket": "fixture-bucket"},
                            secrets={"api_key": "fixture-key"}))
    try:
        assert captured[0].params["project"] == "fixture-project"
        assert captured[0].params["dataset"] == "delivery_state"
        assert not captured[0].secrets
    finally:
        dest.close(conn)


def make_project(root: Path) -> Path:
    (root / "sources" / "fixture").mkdir(parents=True)
    (root / "configs").mkdir()
    state_path = root / "delivery.duckdb"
    (root / "dtex_project.yml").write_text("name: fixture\nsource_paths: [sources]\n")
    (root / "profiles.yml").write_text(
        "klaviyo:\n  default_target: test\n  targets:\n    test:\n"
        f"      backfill: true\n      state_path: {state_path}\n"
    )
    (root / "configs" / "delivery.yml").write_text(
        "name: delivery\nsource: fixture\ndestination: klaviyo\nstreams: all\n"
    )
    (root / "sources" / "fixture" / "register.yaml").write_text('''\
name: fixture
kind: source
streams:
  - name: events
    table: events
    write_disposition: append
    incremental: {cursor_field: offset, cursor_type: int, initial_value: 0, ordered: false}
    schema:
      - {name: offset, type: integer}
      - {name: unique_id, type: string}
      - {name: metric_name, type: string}
      - {name: time, type: timestamp}
      - {name: profile, type: json}
      - {name: properties, type: json}
''')
    (root / "sources" / "fixture" / "source.py").write_text('''\
from dtex import stream
@stream(name="events")
def events(cursor):
    for n in range(int(cursor.start_value() or 0) + 1, 3):
        cursor.observe(n)
        yield [{"offset": n, "unique_id": f"fixture-{n}", "metric_name": "fixture_metric",
                "time": "2026-01-01T00:00:00Z", "profile": {"id": "fixture-profile"},
                "properties": {"count": n}}]
''')
    return state_path


def test_partial_failure_replays_stable_ids_and_only_success_advances_checkpoint(
    session: Session, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setenv("KLAVIYO_API_KEY", "fixture-key")
    state_path = make_project(tmp_path)
    session.replies[:] = [Response(202), Response(400)]
    failed = dtex.run("delivery", project_dir=tmp_path)
    assert failed.status is RunStatus.FAILED
    with duckdb.connect(str(state_path)) as db:
        assert json.loads(db.execute("SELECT cursor_value FROM _dtex_state").fetchone()[0]) == 0
    first_request = session.calls[0]["data"]
    session.replies[:] = [Response(202), Response(202)]
    succeeded = dtex.run("delivery", project_dir=tmp_path)
    assert succeeded.status is RunStatus.SUCCEEDED
    assert session.calls[2]["data"] == first_request
    with duckdb.connect(str(state_path)) as db:
        assert json.loads(db.execute("SELECT cursor_value FROM _dtex_state").fetchone()[0]) == 2
        assert db.execute("SELECT COUNT(*) FROM _dtex_runs").fetchone()[0] == 2
    assert dtex.run("delivery", project_dir=tmp_path).status is RunStatus.SUCCEEDED
    assert len(session.calls) == 4
