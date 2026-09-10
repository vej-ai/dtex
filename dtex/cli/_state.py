# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Incremental-state inspection + reset for ``dtex state``.

dtex keeps incremental state in the destination's ``_dtex_state`` table
(docs/05 §5). The CLI never owns state — it borrows the destination's own
hooks:

* **list** — opens the destination via its ``@destination.open`` hook and calls
  the ``@destination.read_state`` hook. Fully abstract: any Tier-A destination
  works, the CLI never touches SQL.
* **reset** — opens the destination the same way, then issues a targeted
  ``DELETE FROM _dtex_state``. This is the one place the CLI reaches past
  the hook contract.

  # NOTE: there is no ``delete_state`` / ``reset_state`` hook in the
  # destination contract (docs/03 §3.4 / docs/05 §1) — the engine never needed
  # one. Adding one is an engine change, and the task says the CLI is a thin
  # shell with NO new engine logic. So ``reset`` does the cleanest thing
  # available: it executes a parameterized DELETE on the connection the
  # destination's ``open`` hook returns. That assumes a SQL-ish destination
  # exposing a ``conn.conn.execute(sql, params)`` cursor and a
  # ``_dtex_state`` table — true for DuckDB, the only Tier-A destination
  # shipped in v1 (docs/05 §2). A non-SQL destination would need the engine
  # hook; reset fails cleanly (caught by the CLI) rather than silently. This
  # is an accepted v1 limitation, flagged here rather than hidden.

Stage 8.B made *configs* the runtime unit; state operations now take a config
NAME instead of a source name. The config resolves the (source, destination,
target) triple — but state rows themselves are still keyed by *source* name
in ``_dtex_state`` (a property of where data lives, not how it was extracted).
A re-run under a different config that names the same source resumes off
the same rows.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any

from dtex.engine import config as cfg
from dtex.engine import configs as cfgs
from dtex.engine import discovery as disc
from dtex.types import Config, CursorType, StateRecord

# The engine-owned state table name (docs/05 §5.1).
_STATE_TABLE = "_dtex_state"


class StateError(Exception):
    """A state operation could not complete — surfaced cleanly by the CLI."""


@dataclass
class _ResolvedDestination:
    """The destination hooks + an open connection for a state operation."""

    source_name: str
    name: str
    hooks: Mapping[str, Any]
    conn: Any
    streams: tuple[str, ...] = ()
    stream_defs: Mapping[str, Any] = field(default_factory=dict)


def _resolve_destination(
    config_name: str,
    *,
    project_dir: str | Path | None,
    target: str | None,
    destination_params: Mapping[str, Any] | None,
    extra_hooks: tuple[str, ...] = (),
) -> tuple[Path, _ResolvedDestination]:
    """Resolve + open the destination a config binds to.

    Runs the same discovery + config path the engine's ``run`` does (stages
    1-3, DISCOVER → RESOLVE → INIT DEST), but stops once the destination is
    open: a state op needs a live connection, not a run. Returns the project
    root and the open destination handle. The caller must ``close`` it.
    """
    project_root = disc.find_project_root(project_dir)
    project = cfg.ProjectConfig.load(project_root)
    profiles = cfg.Profiles.load(project_root)
    pipeline = cfgs.load_config(
        config_name, project_root, list(project.config_paths)
    )
    target_name = cfg.resolve_target_name(
        target if target is not None else pipeline.target,
        pipeline.destination,
        profiles,
    )

    source = disc.resolve_source(
        pipeline.source, project_root, list(project.source_paths)
    )
    dest = disc.resolve_destination(
        pipeline.destination, project_root, list(project.destination_paths)
    )

    dest_config = cfg.build_destination_config(
        dest.manifest,
        project,
        pipeline,
        target_name=target_name,
        profiles=profiles,
        overrides=dict(destination_params or {}),
    )

    hooks: dict[str, Any] = {}
    for hook_name in ("open", "close", "read_state", *extra_hooks):
        hook = dest.registry.hook(hook_name)
        if hook is None:
            raise StateError(
                f"destination {pipeline.destination!r} is missing the "
                f"@destination.{hook_name} hook required for state operations"
            )
        hooks[hook_name] = hook.func

    conn = hooks["open"](Config(params=dict(dest_config.params)))
    return project_root, _ResolvedDestination(
        source_name=source.manifest.name,
        name=pipeline.destination,
        hooks=hooks,
        conn=conn,
        streams=tuple(sd.name for sd in source.manifest.streams),
        stream_defs={sd.name: sd for sd in source.manifest.streams},
    )


def list_state(
    config_name: str,
    *,
    project_dir: str | Path | None = None,
    target: str | None = None,
    destination_params: Mapping[str, Any] | None = None,
) -> list[StateRecord]:
    """Return the ``_dtex_state`` rows for one config's source.

    Opens the bound destination and calls its ``read_state`` hook — the same
    call the engine makes at run start (docs/05 §1). The connection is always
    closed. An empty list means the source has never committed state (or its
    state was reset).
    """
    _, dest = _resolve_destination(
        config_name,
        project_dir=project_dir,
        target=target,
        destination_params=destination_params,
    )
    try:
        records = dest.hooks["read_state"](dest.conn, dest.source_name)
        return list(records)
    finally:
        dest.hooks["close"](dest.conn)


def reset_state(
    config_name: str,
    *,
    stream: str | None = None,
    project_dir: str | Path | None = None,
    target: str | None = None,
    destination_params: Mapping[str, Any] | None = None,
) -> int:
    """Clear ``_dtex_state`` rows so the next run of this config is a full re-extract.

    Deletes the ``(source)`` rows — or the single ``(source, stream)`` row
    when ``stream`` is given — from the destination's ``_dtex_state`` table.
    The next run then finds no prior cursor and seeds from each stream's
    ``initial_value`` (docs/03 §3.2), exactly as a first run does. Loaded data
    is untouched — this is the surgical alternative to ``--full-refresh``.

    Returns the number of rows deleted. See the module ``# NOTE:`` for why
    this issues a DELETE directly rather than going through a destination
    hook.
    """
    _, dest = _resolve_destination(
        config_name,
        project_dir=project_dir,
        target=target,
        destination_params=destination_params,
    )
    try:
        raw = getattr(dest.conn, "conn", None)
        if raw is None or not hasattr(raw, "execute"):
            raise StateError(
                f"destination {dest.name!r} does not expose a SQL connection; "
                f"`state reset` supports SQL-backed Tier-A destinations only "
                f"(DuckDB in v1). Use `dtex run --full-refresh` instead."
            )
        prior = dest.hooks["read_state"](dest.conn, dest.source_name)
        if stream is not None:
            count_before = sum(1 for r in prior if r.stream == stream)
        else:
            count_before = len(list(prior))
        params: list[Any] = [dest.source_name]
        sql = f"DELETE FROM {_STATE_TABLE} WHERE connector = ?"
        if stream is not None:
            sql += " AND stream = ?"
            params.append(stream)
        raw.execute(sql, params)
        return count_before
    finally:
        dest.hooks["close"](dest.conn)


def _parse_cursor(raw: str, cursor_type: CursorType) -> Any:
    """Parse a CLI cursor string into the value the declared type expects.

    The cursor round-trips through the state table's JSON column, so the type
    matters: a ``date`` cursor stored as ``"2026-08-18T00:00:00"`` compares
    wrong against ``date.fromisoformat`` on the next run, and an ``int``
    cursor stored as a string breaks ``>`` comparisons silently. Parsing here
    means a bad value is rejected at the CLI rather than corrupting a resume.
    """
    text = raw.strip()
    if not text:
        raise StateError("cursor value must not be empty")

    if cursor_type is CursorType.DATE:
        try:
            return date.fromisoformat(text).isoformat()
        except ValueError as exc:
            raise StateError(
                f"cursor {text!r} is not a valid ISO date for a `date` cursor "
                f"— expected YYYY-MM-DD"
            ) from exc

    if cursor_type is CursorType.TIMESTAMP:
        # Accept a bare date as midnight, and normalise 'Z' to +00:00 which
        # fromisoformat rejects before 3.11's relaxation.
        candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
        try:
            return datetime.fromisoformat(candidate).isoformat()
        except ValueError:
            try:
                return datetime.combine(
                    date.fromisoformat(text), time.min, tzinfo=UTC
                ).isoformat()
            except ValueError as exc:
                raise StateError(
                    f"cursor {text!r} is not a valid ISO timestamp for a "
                    f"`timestamp` cursor"
                ) from exc

    if cursor_type is CursorType.INT:
        try:
            return int(text)
        except ValueError as exc:
            raise StateError(
                f"cursor {text!r} is not an integer for an `int` cursor"
            ) from exc

    return text


def set_state(
    config_name: str,
    *,
    stream: str,
    cursor: str,
    project_dir: str | Path | None = None,
    target: str | None = None,
    destination_params: Mapping[str, Any] | None = None,
) -> StateRecord:
    """Set one stream's incremental cursor, without re-extracting anything.

    The counterpart to ``reset``. Reset throws state away so the next run
    re-pulls from ``initial_value``; ``set`` moves the cursor to a value you
    already know is correct — the usual case being data that is *already*
    loaded while the cursor was lost or never advanced (a state table
    restored from backup, a destination migration, a connector bug that
    failed to observe the cursor). Re-extracting years of history just to
    rediscover a date you can read off the loaded rows is pure waste.

    Unlike ``reset``, this goes through the destination's own
    ``commit_state`` hook, so it works on every Tier-A destination rather
    than SQL-backed ones only.

    The value is validated against the stream's DECLARED ``cursor_type``
    (docs/03 §3.2) before it is written, and the stream must exist in the
    source manifest — a typo writes nothing rather than creating an orphan
    row the engine will never read. Every other field on the row
    (``state_blob``, ``rows_total``, ``last_run_id``) is preserved, so a
    resume pointer set by an ``ordered`` stream survives.

    Returns the record as written.
    """
    _, dest = _resolve_destination(
        config_name,
        project_dir=project_dir,
        target=target,
        destination_params=destination_params,
        extra_hooks=("commit_state",),
    )
    try:
        stream_def = dest.stream_defs.get(stream)
        if stream_def is None:
            known = ", ".join(sorted(dest.streams)) or "(none declared)"
            raise StateError(
                f"stream {stream!r} is not declared by source "
                f"{dest.source_name!r}. Declared streams: {known}"
            )

        incremental = getattr(stream_def, "incremental", None)
        if incremental is None:
            raise StateError(
                f"stream {stream!r} is not incremental (no `incremental:` "
                f"block in register.yaml), so it has no cursor to set"
            )

        cursor_type = incremental.cursor_type
        if cursor_type is None:
            raise StateError(
                f"stream {stream!r} declares no `incremental.cursor_type`, so "
                f"a cursor value cannot be validated or stored for it"
            )
        value = _parse_cursor(cursor, cursor_type)

        prior = {
            record.stream: record
            for record in dest.hooks["read_state"](dest.conn, dest.source_name)
        }
        previous = prior.get(stream)

        record = StateRecord(
            connector=dest.source_name,
            stream=stream,
            cursor_value=value,
            cursor_type=cursor_type,
            # Preserve everything the cursor is not. state_blob in particular
            # carries the mid-stream resume pointer for `ordered` streams;
            # dropping it would strand a partially-walked stream.
            state_blob=(previous.state_blob if previous else {}) or {},
            last_run_id=previous.last_run_id if previous else None,
            rows_total=previous.rows_total if previous else 0,
            updated_at=datetime.now(UTC),
        )
        dest.hooks["commit_state"](dest.conn, "dtex-state-set", [record])
        return record
    finally:
        dest.hooks["close"](dest.conn)
