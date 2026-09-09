# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Intercom source — the ``@stream`` functions the engine discovers and runs.

Three shapes, all built on :class:`IntercomClient`:

* :func:`_extract_search` — ``contacts`` / ``conversations`` / ``tickets``.
  Tiles the span from the cursor to now into ``window_days``-wide
  ``updated_at`` windows and paginates ``POST /<resource>/search`` for each.
  Pages are yielded as they arrive; ``cursor.observe`` fires once per
  *completed* window with the window's end, so ``ordered: true`` holds
  exactly: a mid-run state flush persists the end of the last complete
  window, never a value inside a window still being walked. With no cursor
  (first run without ``initial_value``, or ``--full-refresh``) the walk is
  one unbounded search and the cursor is observed at the end.

* :func:`conversation_parts` — for every conversation whose ``updated_at``
  falls in the window, ``GET /conversations/{id}`` and yield the opening
  message plus every part. Conversations are fetched in ascending
  ``updated_at`` order and the cursor is observed after each one lands, so
  ``max_conversations_per_run`` turns a backfill into resumable chunks.

* Dimension streams — one request (or one paginated walk) each, projected
  onto the declared schema.

docs/03 §3.1 — a ``@stream`` yields batches (``list[dict]``), not records.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from dtex import Batch, Config, Cursor, StreamDef, stream
from dtex.sources.intercom.client import IntercomClient

# --------------------------------------------------------------------------
# Client construction
# --------------------------------------------------------------------------


def _build_client(
    config: Config, log: logging.Logger | logging.LoggerAdapter[Any]
) -> IntercomClient:
    """Construct an :class:`IntercomClient` from ``config`` — single construction site."""
    return IntercomClient(
        access_token=config.secrets["access_token"],
        region=str(config.get("region") or "us"),
        base_url=str(config.get("base_url") or ""),
        api_version=str(config.get("api_version") or "2.16"),
        page_size=int(config.get("page_size") or 150),
        max_retries=int(config.get("max_retries") if config.get("max_retries") is not None else 5),
        retry_backoff_seconds=float(config.get("retry_backoff_seconds") or 1.0),
        requests_per_second=float(config.get("requests_per_second") or 12.0),
        rate_limit_max_wait_seconds=float(config.get("rate_limit_max_wait_seconds") or 60.0),
        timeout_seconds=float(config.get("timeout_seconds") or 60.0),
        log=log,
    )


# --------------------------------------------------------------------------
# Projection helpers
# --------------------------------------------------------------------------


def _declared_columns(stream_def: StreamDef) -> list[str]:
    return [f.name for f in (stream_def.schema or [])]


def _project(record: Mapping[str, Any], columns: Sequence[str]) -> dict[str, Any]:
    """Keep the declared columns only; nested values stay as-is (JSON columns)."""
    return {col: record.get(col) for col in columns}


def _flatten_ticket(record: Mapping[str, Any]) -> dict[str, Any]:
    """Flatten the 2.12+ ``ticket_state`` object into the Airbyte-era columns."""
    out = dict(record)
    state = record.get("ticket_state")
    if isinstance(state, dict):
        out["ticket_state"] = state.get("category")
        out["ticket_state_id"] = state.get("id")
        out["ticket_state_internal_label"] = state.get("internal_label")
        out["ticket_state_external_label"] = state.get("external_label")
    return out


def _as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------
# updated_at windows
# --------------------------------------------------------------------------


def _iter_windows(start: int | None, end: int, window_days: int) -> list[tuple[int | None, int]]:
    """Inclusive ``[a, b]`` windows tiling ``start → end``.

    ``[(None, end)]`` when there is no start (a single unbounded search).
    ``window_days <= 0`` means one window for the whole span.
    """
    if start is None:
        return [(None, end)]
    if start > end:
        return [(start, end)]
    width = int(window_days) * 86400
    if width <= 0:
        return [(start, end)]
    windows: list[tuple[int | None, int]] = []
    a = start
    while a <= end:
        b = min(a + width - 1, end)
        windows.append((a, b))
        a = b + 1
    return windows


def _window_query(a: int | None, b: int) -> dict[str, Any]:
    """Intercom search query for ``a <= updated_at <= b`` (``>`` / ``<`` are strict)."""
    clauses: list[dict[str, Any]] = []
    if a is not None:
        clauses.append({"field": "updated_at", "operator": ">", "value": a - 1})
    clauses.append({"field": "updated_at", "operator": "<", "value": b + 1})
    if len(clauses) == 1:
        return clauses[0]
    return {"operator": "AND", "value": clauses}



# Contact objects embed at most 10 entries of their `tags` / `companies` /
# `notes` lists and flag the truncation (`has_more: true`, `total_count`,
# `url`). Airbyte's connector shipped that truncated list; the full list is
# one GET away, and only the flagged contacts (a few hundred per workspace)
# pay for it.
_CONTACT_LISTS = ("tags", "companies", "notes")


def _complete_contact_lists(client: IntercomClient, record: dict[str, Any]) -> dict[str, Any]:
    """Replace every truncated embedded list on a contact with the full one."""
    out = record
    for key in _CONTACT_LISTS:
        envelope = record.get(key)
        if not isinstance(envelope, dict) or not envelope.get("has_more"):
            continue
        url = envelope.get("url") or f"/contacts/{record.get('id')}/{key}"
        items: list[dict[str, Any]] = []
        for page in client.list_cursor(str(url), "data"):
            items.extend(page)
        if out is record:
            out = dict(record)
        out[key] = {
            "type": envelope.get("type", "list"),
            "url": url,
            "data": items,
            "total_count": len(items),
            "has_more": False,
        }
    return out


_SEARCH_PATHS: dict[str, tuple[str, str]] = {
    "contacts": ("/contacts/search", "data"),
    "conversations": ("/conversations/search", "conversations"),
    "tickets": ("/tickets/search", "tickets"),
}


def _extract_search(
    stream_def: StreamDef,
    config: Config,
    cursor: Cursor,
    log: logging.Logger | logging.LoggerAdapter[Any],
) -> Iterator[Batch]:
    """Shared extract for the three search streams — see the module docstring.

    Pages are buffered into ``batch_size``-row batches (one destination load
    per batch — BigQuery caps load jobs per table per day, so a 150-row
    page per load would exhaust it on a backfill). The cursor is observed
    for a completed window only once every row of that window has been
    *yielded*: a window whose rows are still in the buffer is not a safe
    resume point, because the engine may persist the observed maximum
    after any yielded batch lands.
    """
    path, items_key = _SEARCH_PATHS[stream_def.name]
    columns = _declared_columns(stream_def)
    window_days = int(config.get("window_days") if config.get("window_days") is not None else 7)
    batch_size = max(1, int(config.get("batch_size") or 5000))
    now = int(time.time())
    start = _as_int(cursor.start_value())
    windows = _iter_windows(start, now, window_days)
    log.info(
        "intercom.%s: %d window(s) of %dd from %s to %d%s",
        stream_def.name, len(windows), window_days, start, now,
        " (full_refresh)" if cursor.is_full_refresh else "",
    )
    records = 0
    batch: list[dict[str, Any]] = []
    landed_window_end: int | None = None    # end of the last window fully yielded
    buffered_window_end: int | None = None  # end of the last window fully buffered
    with _build_client(config, log) as client:
        for index, (a, b) in enumerate(windows, start=1):
            pages = 0
            for page in client.search(path, _window_query(a, b), items_key):
                pages += 1
                records += len(page)
                if stream_def.name == "tickets":
                    batch.extend(_project(_flatten_ticket(r), columns) for r in page)
                elif stream_def.name == "contacts":
                    batch.extend(
                        _project(_complete_contact_lists(client, r), columns) for r in page
                    )
                else:
                    batch.extend(_project(r, columns) for r in page)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
                    # Everything buffered before this yield is now with the
                    # engine; earlier complete windows are safe resume points.
                    if buffered_window_end is not None:
                        landed_window_end = buffered_window_end
                        cursor.observe(landed_window_end)
            # The window is complete: every row Intercom had for it is in
            # the buffer (or already yielded).
            buffered_window_end = b
            if not batch:
                landed_window_end = b
                cursor.observe(b)
            if pages or index % 25 == 0:
                log.info(
                    "intercom.%s: window %d/%d [%s, %d] pages=%d records_total=%d",
                    stream_def.name, index, len(windows), a, b, pages, records,
                )
    if batch:
        yield batch
    if buffered_window_end is not None and buffered_window_end != landed_window_end:
        cursor.observe(buffered_window_end)
    log.info("intercom.%s: extract complete records=%d", stream_def.name, records)


@stream(name="contacts")
def contacts(
    stream_def: StreamDef, config: Config, cursor: Cursor, log: logging.Logger
) -> Iterator[Batch]:
    """Contacts (users and leads) updated in the window — POST /contacts/search."""
    yield from _extract_search(stream_def, config, cursor, log)


@stream(name="conversations")
def conversations(
    stream_def: StreamDef, config: Config, cursor: Cursor, log: logging.Logger
) -> Iterator[Batch]:
    """Conversations updated in the window — POST /conversations/search."""
    yield from _extract_search(stream_def, config, cursor, log)


@stream(name="tickets")
def tickets(
    stream_def: StreamDef, config: Config, cursor: Cursor, log: logging.Logger
) -> Iterator[Batch]:
    """Tickets updated in the window — POST /tickets/search."""
    yield from _extract_search(stream_def, config, cursor, log)


# --------------------------------------------------------------------------
# Transcript fan-out
# --------------------------------------------------------------------------


def _part_rows(conversation: Mapping[str, Any], columns: Sequence[str]) -> list[dict[str, Any]]:
    """One row per conversation part, plus one for the opening message."""
    conv_id = str(conversation.get("id"))
    conv_updated = _as_int(conversation.get("updated_at"))
    rows: list[dict[str, Any]] = []
    source = conversation.get("source")
    if isinstance(source, dict) and source.get("id") is not None:
        rows.append(
            _project(
                {
                    "id": str(source.get("id")),
                    "conversation_id": conv_id,
                    "conversation_updated_at": conv_updated,
                    "part_type": "conversation_source",
                    "body": source.get("body"),
                    "created_at": conversation.get("created_at"),
                    "updated_at": None,
                    "notified_at": None,
                    "author": source.get("author"),
                    "assigned_to": None,
                    "attachments": source.get("attachments"),
                    "external_id": None,
                    "redacted": source.get("redacted"),
                    "delivered_as": source.get("delivered_as"),
                    "subject": source.get("subject"),
                },
                columns,
            )
        )
    parts_obj = conversation.get("conversation_parts")
    parts = parts_obj.get("conversation_parts") if isinstance(parts_obj, dict) else None
    for part in parts or []:
        if not isinstance(part, dict) or part.get("id") is None:
            continue
        row = dict(part)
        row["id"] = str(part["id"])
        row["conversation_id"] = conv_id
        row["conversation_updated_at"] = conv_updated
        rows.append(_project(row, columns))
    return rows


@stream(name="conversation_parts")
def conversation_parts(
    stream_def: StreamDef, config: Config, cursor: Cursor, log: logging.Logger
) -> Iterator[Batch]:
    """Every part of every conversation updated in the window — GET /conversations/{id}."""
    columns = _declared_columns(stream_def)
    window_days = int(config.get("window_days") if config.get("window_days") is not None else 7)
    cap = int(config.get("max_conversations_per_run") or 0)
    batch_size = max(1, int(config.get("batch_size") or 2000))
    now = int(time.time())
    start = _as_int(cursor.start_value())
    windows = _iter_windows(start, now, window_days)
    log.info(
        "intercom.conversation_parts: %d window(s) from %s to %d, cap=%s",
        len(windows), start, now, cap or "unlimited",
    )
    fetched = 0
    rows_total = 0
    batch: list[dict[str, Any]] = []
    with _build_client(config, log) as client:
        for a, b in windows:
            # Ids first (cheap: one search page per 150 conversations), then
            # ascending by updated_at so the cursor is monotonic per landed
            # conversation — what `ordered: true` promises the engine.
            heads: list[tuple[int, str]] = []
            for page in client.search(
                "/conversations/search", _window_query(a, b), "conversations"
            ):
                for conv in page:
                    updated = _as_int(conv.get("updated_at"))
                    if conv.get("id") is not None and updated is not None:
                        heads.append((updated, str(conv["id"])))
            heads.sort()
            for updated, conv_id in heads:
                if cap and fetched >= cap:
                    if batch:
                        yield batch
                        batch = []
                    log.info(
                        "intercom.conversation_parts: cap %d reached; cursor=%s rows=%d — "
                        "the next run resumes from here",
                        cap, cursor.observed_max, rows_total,
                    )
                    return
                full = client.get(f"/conversations/{conv_id}")
                fetched += 1
                rows = _part_rows(full, columns)
                rows_total += len(rows)
                batch.extend(rows)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
                # A batch that is still buffered has not landed, but the
                # engine only persists the cursor after the batch it belongs
                # to is committed, and a yielded batch always carries every
                # row of the conversations observed so far.
                cursor.observe(updated)
            if not heads:
                cursor.observe(b)
    if batch:
        yield batch
    log.info(
        "intercom.conversation_parts: extract complete conversations=%d rows=%d",
        fetched, rows_total,
    )


# --------------------------------------------------------------------------
# Dimensions and catalogs
# --------------------------------------------------------------------------


def _batched(
    pages: Iterator[list[dict[str, Any]]], columns: Sequence[str], batch_size: int
) -> Iterator[Batch]:
    batch: list[dict[str, Any]] = []
    for page in pages:
        for record in page:
            batch.append(_project(record, columns))
            if len(batch) >= batch_size:
                yield batch
                batch = []
    if batch:
        yield batch


def _single(
    stream_def: StreamDef, config: Config, log: logging.Logger, path: str, items_key: str,
    params: Mapping[str, Any] | None = None,
) -> Iterator[Batch]:
    columns = _declared_columns(stream_def)
    with _build_client(config, log) as client:
        body = client.get(path, params)
        items = body.get(items_key) or []
        rows = [_project(r, columns) for r in items if isinstance(r, dict)]
    log.info("intercom.%s: %d rows", stream_def.name, len(rows))
    if rows:
        yield rows


@stream(name="companies")
def companies(stream_def: StreamDef, config: Config, log: logging.Logger) -> Iterator[Batch]:
    """Every company — GET /companies/scroll (no updated_at filter exists)."""
    columns = _declared_columns(stream_def)
    batch_size = max(1, int(config.get("batch_size") or 2000))
    with _build_client(config, log) as client:
        yield from _batched(client.scroll("/companies/scroll", "data"), columns, batch_size)


@stream(name="admins")
def admins(stream_def: StreamDef, config: Config, log: logging.Logger) -> Iterator[Batch]:
    yield from _single(stream_def, config, log, "/admins", "admins")


@stream(name="teams")
def teams(stream_def: StreamDef, config: Config, log: logging.Logger) -> Iterator[Batch]:
    yield from _single(stream_def, config, log, "/teams", "teams")


@stream(name="tags")
def tags(stream_def: StreamDef, config: Config, log: logging.Logger) -> Iterator[Batch]:
    yield from _single(stream_def, config, log, "/tags", "data")


@stream(name="segments")
def segments(stream_def: StreamDef, config: Config, log: logging.Logger) -> Iterator[Batch]:
    yield from _single(stream_def, config, log, "/segments", "segments", {"include_count": "true"})


@stream(name="ticket_types")
def ticket_types(stream_def: StreamDef, config: Config, log: logging.Logger) -> Iterator[Batch]:
    yield from _single(stream_def, config, log, "/ticket_types", "data")


@stream(name="data_attributes")
def data_attributes(stream_def: StreamDef, config: Config, log: logging.Logger) -> Iterator[Batch]:
    """Contact, company and conversation attributes, archived ones included."""
    yield from _single(
        stream_def, config, log, "/data_attributes", "data", {"include_archived": "true"}
    )


@stream(name="articles")
def articles(stream_def: StreamDef, config: Config, log: logging.Logger) -> Iterator[Batch]:
    """Every help-center article — GET /articles (page-number pagination)."""
    columns = _declared_columns(stream_def)
    batch_size = max(1, int(config.get("batch_size") or 2000))
    with _build_client(config, log) as client:
        yield from _batched(client.list_pages("/articles", "data"), columns, batch_size)
