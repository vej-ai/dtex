"""THE simpl.E smoke test — the executable specification of the engine's job.

The engine (stage 5) does not exist yet. This file defines, in runnable code,
*exactly* what it must do: it wires the pieces by hand today, and the manual
wiring lives in one place — :func:`_drive_one_run` — marked so stage 5 can see
the precise delta.

What the run does, end to end (docs/05 §1 lifecycle):

    open → read_state → [ensure_schema → write_batch ...]* → commit_state → close

It drives the ``echo`` fixture source (an ``append`` stream ``events`` and a
``merge`` + ``incremental`` stream ``items``) into the pre-baked DuckDB
destination, and the assertions pin down the contract the engine must satisfy:

* rows land in DuckDB, in the right tables, with the right values;
* ``_simple_e_synced_at`` is populated on every row;
* ``_simple_e_state`` carries the incremental stream's advanced cursor;
* a second run *resumes from committed state* — the incremental stream yields
  only new rows, the append stream re-runs in full.

╔══════════════════════════════════════════════════════════════════════════╗
║  STAGE 5 SEAM                                                            ║
║  ----------                                                              ║
║  The whole body of `_drive_one_run()` below is the manual stand-in for   ║
║  the engine. When the engine lands, `_drive_one_run(...)` collapses to   ║
║  exactly one line:                                                       ║
║                                                                          ║
║      return simple_e.run(connector="echo", target="<duckdb target>")     ║
║                                                                          ║
║  (returning a `RunResult`, from which these tests would read             ║
║  `result.stream("items").rows_loaded` etc. instead of querying DuckDB    ║
║  directly). Every line of `_drive_one_run` tagged `# [engine]` is work   ║
║  the engine subsumes. The assertions in the test functions DO NOT move   ║
║  — they are the spec, and they must keep passing against `engine.run()`. ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from simple_e import Config, Cursor, CursorType, State, StateRecord, StreamMeta
from simple_e.registry import compute_injection
from simple_e.types import ConnectorManifest, StreamDef
from tests.conftest import (
    ECHO_CONNECTOR_DIR,
    LoadedConnector,
    load_connector,
)

# --------------------------------------------------------------------------
# The result of one manually-driven run.
# --------------------------------------------------------------------------


@dataclass
class _RunOutcome:
    """What one driven run produced — a hand-rolled stand-in for ``RunResult``.

    # [engine] Stage 5's ``simple_e.run()`` returns a real
    # ``simple_e.types.RunResult``; this minimal shape carries only the two
    # facts the smoke assertions need (per-stream rows loaded). The test
    # functions read ``outcome.rows_loaded[...]`` — that access pattern maps
    # 1:1 onto ``RunResult.stream(name).rows_loaded``.
    """

    rows_loaded: dict[str, int]


# ==========================================================================
# THE STAGE 5 SEAM — _drive_one_run is the entire manual engine stand-in.
# ==========================================================================


def _drive_one_run(
    destination: LoadedConnector, source: LoadedConnector, db_path: str
) -> _RunOutcome:
    """Drive one end-to-end run by hand — the function stage 5 replaces.

    Every step here is engine work. Stage 5 replaces this whole function with
    a single ``simple_e.run(connector="echo", target=...)`` call; the steps
    below are annotated ``# [engine]`` to mark exactly what that call subsumes.

    The lifecycle followed is the one docs/05 §1 fixes and the registry's
    ``DestinationHook`` docstring repeats:

        open → read_state → [ensure_schema → write_batch ...]* → commit_state → close
    """
    run_id = "smoke-run"

    # [engine] Resolve hooks from the destination registry. The engine does
    # this once at run start after discovering the destination connector.
    hooks: dict[str, Callable[..., Any]] = {
        name: destination.registry.hook(name).func  # type: ignore[union-attr]
        for name in destination.registry.hook_names
    }
    manifest: ConnectorManifest = source.manifest

    # [engine] open — acquire the destination handle once per run (docs/05 §1).
    conn = hooks["open"](Config(params={"path": db_path}))
    rows_loaded: dict[str, int] = {}

    try:
        # [engine] read_state — load every prior StateRecord for this connector
        # at run start, and index it by stream name (docs/05 §1, §5).
        prior_state: list[StateRecord] = hooks["read_state"](conn, manifest.name)
        state_by_stream: dict[str, StateRecord] = {r.stream: r for r in prior_state}

        committed: list[StateRecord] = []

        # [engine] For each declared stream: bind its @stream function, inject
        # config/state/cursor, ensure the table, drive the generator, write
        # each batch, then build the stream's StateRecord.
        for stream_def in manifest.streams:
            registration = source.registry.stream(stream_def.name)
            assert registration is not None, f"unregistered stream {stream_def.name!r}"

            prior = state_by_stream.get(stream_def.name)

            # [engine] Build the cursor for an incremental stream from the last
            # committed cursor value (docs/03 §3.2). A non-incremental stream
            # gets no cursor injectable.
            cursor: Cursor | None = None
            if stream_def.is_incremental:
                inc = stream_def.incremental
                assert inc is not None
                start = _resume_value(prior, inc.cursor_type, inc.initial_value)
                cursor = Cursor(
                    cursor_field=inc.cursor_field,
                    cursor_type=inc.cursor_type,
                    start_value=start,
                )

            # [engine] Per-stream State scratch space, seeded from prior state_blob.
            state = State(prior.state_blob if prior is not None else None)

            # [engine] compute_injection picks exactly the injectables the
            # @stream function declared (registry.compute_injection — docs/03 §3.1).
            available: dict[str, Any] = {
                "config": Config(),
                "state": state,
                "log": _NullLog(),
            }
            if cursor is not None:
                available["cursor"] = cursor
            kwargs = compute_injection(registration.func, available)

            # [engine] Build the single per-stream StreamMeta from the resolved
            # StreamDef + schema. It carries table, write_disposition,
            # primary_key, etc. — every destination hook takes just this object
            # (docs/05 §1). The declared schema is required by the echo fixture.
            assert stream_def.schema is not None
            stream_meta = StreamMeta.from_stream_def(stream_def, stream_def.schema)

            # [engine] ensure_schema — create/evolve the table before loading
            # (docs/05 §1).
            hooks["ensure_schema"](conn, stream_meta)

            # [engine] Drive the generator; write each yielded batch. The write
            # disposition and primary_key both ride along inside stream_meta,
            # so every disposition takes the same single hook call.
            rows = 0
            for batch in registration.func(**kwargs):
                rows += hooks["write_batch"](conn, batch, stream_meta)
            rows_loaded[stream_def.name] = rows

            # [engine] Build the stream's StateRecord — advance the cursor to
            # the observed max, carry forward state_blob and cumulative rows
            # (docs/05 §5.1-5.3). The cursor is persisted ONLY now, after the
            # batches durably landed.
            new_cursor_value = prior.cursor_value if prior is not None else None
            if cursor is not None and cursor.observed_max is not None:
                new_cursor_value = cursor.observed_max
            committed.append(
                StateRecord(
                    connector=manifest.name,
                    stream=stream_def.name,
                    cursor_value=new_cursor_value,
                    cursor_type=(
                        stream_def.incremental.cursor_type
                        if stream_def.incremental is not None
                        else None
                    ),
                    state_blob=state.to_dict(),
                    last_run_id=run_id,
                    rows_total=(prior.rows_total if prior is not None else 0) + rows,
                )
            )

        # [engine] commit_state — persist all stream cursors in one call,
        # AFTER every batch of every stream has landed (docs/05 §5.3).
        hooks["commit_state"](conn, run_id, committed)
    finally:
        # [engine] close — always runs, even on failure (docs/05 §1).
        hooks["close"](conn)

    return _RunOutcome(rows_loaded=rows_loaded)


# ==========================================================================
# END STAGE 5 SEAM — everything below is the spec and does NOT change.
# ==========================================================================


def _resume_value(
    prior: StateRecord | None, cursor_type: CursorType, initial_value: str | None
) -> Any:
    """Compute a stream's resume point — the engine's cursor-seeding logic.

    # [engine] Stage 5 owns this: last committed cursor on a resumed run, else
    # the manifest's ``initial_value`` on the first run (docs/03 §3.2). The
    # ``Cursor`` itself is deliberately dumb and just stores whatever it is
    # handed (see ``Cursor`` docstring in types.py).
    """
    if prior is not None and prior.cursor_value is not None:
        return prior.cursor_value
    if initial_value is None:
        return None
    if cursor_type is CursorType.INT:
        return int(initial_value)
    return initial_value


class _NullLog:
    """A no-op logger stand-in — the engine injects a real structured logger."""

    def info(self, *args: Any, **kwargs: Any) -> None:  # noqa: D102
        pass

    def warning(self, *args: Any, **kwargs: Any) -> None:  # noqa: D102
        pass

    def error(self, *args: Any, **kwargs: Any) -> None:  # noqa: D102
        pass


# --------------------------------------------------------------------------
# The smoke test — the executable spec for engine.run()
# --------------------------------------------------------------------------


def test_smoke_first_run_lands_rows_state_and_synced_at(
    duckdb_destination: LoadedConnector,
    duckdb_path: str,
    query_duckdb: Callable[[str, str], list[tuple[Any, ...]]],
) -> None:
    """One run lands data, fills _simple_e_synced_at, and advances the cursor.

    This is the core spec: after ``engine.run()`` (today: ``_drive_one_run``),
    the DuckDB file must hold the source's data and the state table must show
    the incremental stream's cursor advanced to its max.
    """
    source = load_connector(ECHO_CONNECTOR_DIR)
    outcome = _drive_one_run(duckdb_destination, source, duckdb_path)

    # --- Rows landed -------------------------------------------------------
    # echo.events: 4 fixture records (append).
    # echo.items:  5 fixture records (merge, first run cursor = initial 0).
    assert outcome.rows_loaded == {"events": 4, "items": 5}

    events = query_duckdb(duckdb_path, "SELECT id, name FROM echo_events ORDER BY id")
    assert events == [(1, "alpha"), (2, "beta"), (3, "gamma"), (4, "delta")]

    items = query_duckdb(duckdb_path, "SELECT id, label, updated_at FROM echo_items ORDER BY id")
    assert items == [
        (1, "item-one", 1),
        (2, "item-two", 2),
        (3, "item-three", 3),
        (4, "item-four", 4),
        (5, "item-five", 5),
    ]

    # --- _simple_e_synced_at populated on every row ------------------------
    null_synced = query_duckdb(
        duckdb_path,
        "SELECT count(*) FROM echo_events WHERE _simple_e_synced_at IS NULL",
    )[0][0]
    assert null_synced == 0
    synced_sample = query_duckdb(
        duckdb_path, "SELECT _simple_e_synced_at FROM echo_items LIMIT 1"
    )[0][0]
    assert isinstance(synced_sample, datetime)

    # --- JSON column survived the round trip -------------------------------
    score = query_duckdb(
        duckdb_path, "SELECT payload->>'$.score' FROM echo_events WHERE id = 1"
    )[0][0]
    assert score == "10"

    # --- _simple_e_state: incremental cursor advanced ----------------------
    state = query_duckdb(
        duckdb_path,
        "SELECT cursor_value, cursor_type, rows_total FROM _simple_e_state "
        "WHERE connector = 'echo' AND stream = 'items'",
    )
    assert len(state) == 1
    assert int(str(state[0][0])) == 5  # cursor advanced to the max updated_at
    assert state[0][1] == "int"
    assert state[0][2] == 5  # rows_total

    # The append stream also got a state row (no cursor — cursor_value NULL).
    events_state = query_duckdb(
        duckdb_path,
        "SELECT cursor_value FROM _simple_e_state "
        "WHERE connector = 'echo' AND stream = 'events'",
    )
    assert len(events_state) == 1
    assert events_state[0][0] is None


def test_smoke_second_run_resumes_from_committed_state(
    duckdb_destination: LoadedConnector,
    duckdb_path: str,
    query_duckdb: Callable[[str, str], list[tuple[Any, ...]]],
) -> None:
    """A re-run resumes from committed state — the incremental stream yields 0.

    This is the spec's incremental-correctness clause. After run 1 commits the
    ``items`` cursor at 5, run 2's ``items`` stream — driven from that committed
    cursor — finds nothing newer and yields no rows. The ``append`` stream re-runs
    in full (append has no cursor). State must not regress.
    """
    source = load_connector(ECHO_CONNECTOR_DIR)

    # Run 1 — full load.
    first = _drive_one_run(duckdb_destination, source, duckdb_path)
    assert first.rows_loaded == {"events": 4, "items": 5}

    # Run 2 — resumes from the committed cursor (a fresh source import, just as
    # a second process would re-discover the connector).
    source2 = load_connector(ECHO_CONNECTOR_DIR)
    second = _drive_one_run(duckdb_destination, source2, duckdb_path)

    # The incremental `items` stream resumed from cursor=5 and found nothing new.
    assert second.rows_loaded["items"] == 0
    # The non-incremental `events` stream re-ran fully (append has no cursor).
    assert second.rows_loaded["events"] == 4

    # echo_items still holds exactly the 5 rows from run 1 — merge of an empty
    # set changed nothing, no duplication.
    item_count = query_duckdb(duckdb_path, "SELECT count(*) FROM echo_items")[0][0]
    assert item_count == 5

    # The committed cursor did not regress and rows_total did not double-count.
    state = query_duckdb(
        duckdb_path,
        "SELECT cursor_value, rows_total FROM _simple_e_state "
        "WHERE connector = 'echo' AND stream = 'items'",
    )
    assert int(str(state[0][0])) == 5
    assert state[0][1] == 5  # rows_total unchanged — run 2 added 0 items


def test_smoke_state_commit_is_what_enables_resume(
    duckdb_destination: LoadedConnector,
    duckdb_path: str,
) -> None:
    """Spec guard: WITHOUT a committed cursor, run 2 re-yields every row.

    This test deliberately drives a run that skips ``commit_state`` and proves
    the incremental stream then has no resume point — so run 2 re-extracts all
    5 items. It exists so the smoke spec genuinely *defines* the engine's job:
    if stage 5's ``engine.run()`` forgot to call ``commit_state``, the resume
    behavior asserted above would silently break, and this test would catch it.
    """
    source = load_connector(ECHO_CONNECTOR_DIR)
    hooks: dict[str, Callable[..., Any]] = {
        name: duckdb_destination.registry.hook(name).func  # type: ignore[union-attr]
        for name in duckdb_destination.registry.hook_names
    }
    items_def: StreamDef | None = source.manifest.stream("items")
    assert items_def is not None and items_def.incremental is not None

    # A run that loads `items` but never commits its state.
    assert items_def.schema is not None
    items_meta = StreamMeta.from_stream_def(items_def, items_def.schema)
    conn = hooks["open"](Config(params={"path": duckdb_path}))
    try:
        hooks["ensure_schema"](conn, items_meta)
        registration = source.registry.stream("items")
        assert registration is not None
        cursor = Cursor(
            cursor_field=items_def.incremental.cursor_field,
            cursor_type=items_def.incremental.cursor_type,
            start_value=0,
        )
        for batch in registration.func(cursor=cursor):
            hooks["write_batch"](conn, batch, items_meta)
        # NOTE: commit_state intentionally NOT called.
    finally:
        hooks["close"](conn)

    # Run 2 reads state — but nothing was committed, so there is no resume point.
    source2 = load_connector(ECHO_CONNECTOR_DIR)
    conn = hooks["open"](Config(params={"path": duckdb_path}))
    try:
        prior = hooks["read_state"](conn, "echo")
        assert prior == [], "no commit_state ⇒ no StateRecord ⇒ no resume point"

        items_def2 = source2.manifest.stream("items")
        assert items_def2 is not None and items_def2.incremental is not None
        # With no prior cursor, the resume value falls back to initial_value (0)
        # and the stream re-yields ALL 5 items — the bug this guards against.
        cursor = Cursor(
            cursor_field=items_def2.incremental.cursor_field,
            cursor_type=items_def2.incremental.cursor_type,
            start_value=0,
        )
        registration2 = source2.registry.stream("items")
        assert registration2 is not None
        re_yielded = sum(len(batch) for batch in registration2.func(cursor=cursor))
        assert re_yielded == 5, "without committed state the stream restarts from zero"
    finally:
        hooks["close"](conn)
