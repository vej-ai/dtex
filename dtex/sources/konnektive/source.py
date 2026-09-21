"""Konnektive CRM source — five @stream functions over one shared walk.

Four of the streams (``orders``, ``transactions``, ``purchases``,
``customers``) are the same shape: a ``*/query/`` endpoint filtered by
``dateRangeType=dateUpdated`` + ``startDate`` / ``endDate``, paginated 200
rows at a time, merged on the object's id. ``summary`` is a per-day report.

THE DATE FILTER IS DAY-GRANULAR. Konnektive accepts a time in ``startDate``
/ ``endDate`` and ignores it: verified live 2026-09-21, a one-hour window
(``12:00:00``–``12:59:59``), bare dates, and explicit ``00:00:00``–
``23:59:59`` bounds all returned the identical 4,366 orders spanning the
whole day. Everything below follows from that:

* Windows are whole CALENDAR DAYS and the request carries bare dates. A
  window that started mid-day would silently fetch both of the days it
  touches, and its neighbour would fetch one of them again.
* Lookback is in days. There is no such thing as re-pulling "the last six
  hours"; the day the cursor sits in is always re-pulled in full.

How a run walks:

1. Resolve the first day: the persisted cursor's date minus
   ``lookback_days``, or ``start_date`` on a virgin run / ``--full-refresh``.
2. Tile first day → today into ``window_days``-wide windows, ascending.
   Small windows keep each request cheap and individually retryable, and
   bound how deep a page walk goes: Konnektive paginates by page NUMBER, so
   a row updated mid-walk leaves its day and shifts the pages behind it.
   The row that moved is safe — its new ``dateUpdated`` puts it in a later
   window. A bystander skipped by the shift is only re-fetched if its day
   is walked again, which is what ``lookback_days`` is for: the default
   re-walks yesterday, the one past day still being edited heavily.
3. For each window, paginate, drop ``exclude_fields``, project each row onto
   the declared schema, and keep the whole object beside it under ``raw``.
4. ``cursor.observe(...)`` once per WINDOW, after that window's last batch
   has been yielded. Windows ascend, so the observed value never decreases
   — which is what lets the streams declare ``ordered: true``: the engine
   may persist the cursor at a mid-stream flush, and a multi-hour history
   backfill that dies resumes from its last completed window instead of
   from ``start_date``. Rows WITHIN a window arrive in no promised order,
   which is exactly why the observe is per window and never per row.

All Konnektive date-times are wall-clock values in the ACCOUNT's timezone
with no offset (``2026-09-18 13:31:23``). They are landed as STRING,
untouched — typing them TIMESTAMP would stamp them UTC and be wrong by the
account's offset. The ``account_timezone`` param is used for one thing
only: working out which day "today" is in the account's terms.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from dtex import Batch, Config, Cursor, StreamDef, stream

from .client import MAX_PAGE_SIZE, KonnektiveClient

_RAW_COLUMN = "raw"


def _client(config: Config) -> KonnektiveClient:
    return KonnektiveClient(
        login_id=config.secrets["login_id"],
        password=config.secrets["password"],
        base_url=str(config.base_url),
        http_method=str(config.http_method),
        max_retries=int(config.max_retries),
        timeout_seconds=float(config.timeout_seconds),
        min_request_interval=float(config.min_request_interval_seconds),
    )


def _today_in_account_tz(config: Config) -> date:
    """Today's date on the account's wall clock."""
    return datetime.now(tz=ZoneInfo(str(config.account_timezone))).date()


def _parse_day(value: Any) -> date:
    """The calendar day of a cursor / param value.

    Accepts a bare date, the API's ``YYYY-MM-DD HH:MM:SS``, an ISO ``T``
    form, and ``date`` / ``datetime`` objects (the engine hands a typed
    cursor back as one). Only the first ten characters matter.
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value).strip()[:10])


def _iter_windows(first: date, last: date, window_days: int) -> list[tuple[date, date]]:
    """Inclusive whole-day ``(start, end)`` windows tiling first → last.

    Adjacent windows share no day and skip none. A first day past the last
    — clock skew, a wrong ``account_timezone``, a cursor from the future —
    still yields ONE window covering ``last``: an empty list would sync
    nothing and look healthy.
    """
    if first > last:
        first = last
    span = max(1, int(window_days))
    windows: list[tuple[date, date]] = []
    window_start = first
    while window_start <= last:
        window_end = min(window_start + timedelta(days=span - 1), last)
        windows.append((window_start, window_end))
        window_start = window_end + timedelta(days=1)
    return windows


def _first_day(cursor: Cursor, config: Config, lookback_days: int) -> date:
    start_value = cursor.start_value()
    if start_value is None:
        return _parse_day(config.start_date)
    return _parse_day(start_value) - timedelta(days=max(0, lookback_days))


def _excluded(config: Config) -> frozenset[str]:
    return frozenset(
        name.strip() for name in str(config.exclude_fields or "").split(",") if name.strip()
    )


def _column_name(key: str) -> str:
    """The destination column for an API key. Warehouses reject a leading
    digit (``3DTxnResult``), so those gain a ``_`` prefix; ``raw`` keeps
    the API's own spelling."""
    return f"_{key}" if key[:1].isdigit() else key


def _project(
    row: dict[str, Any], columns: tuple[str, ...], excluded: frozenset[str] = frozenset()
) -> dict[str, Any]:
    """Declared columns + the whole object under ``raw``. ``excluded`` keys
    are removed FIRST, so they reach neither a column nor ``raw``."""
    if excluded:
        row = {key: value for key, value in row.items() if key not in excluded}
    renamed = {_column_name(key): value for key, value in row.items()}
    out = {name: renamed.get(name) for name in columns if name != _RAW_COLUMN}
    out[_RAW_COLUMN] = row
    return out


def _columns(stream_def: StreamDef) -> tuple[str, ...]:
    if stream_def.schema is None:  # pragma: no cover — every stream declares one
        raise RuntimeError(f"konnektive: stream {stream_def.name!r} declares no schema")
    return stream_def.schema.names


def _extract_query(
    path: str,
    stream_def: StreamDef,
    config: Config,
    cursor: Cursor,
    log: logging.Logger,
) -> Iterator[Batch]:
    """The shared walk for the four ``*/query/`` streams."""
    client = _client(config)
    columns = _columns(stream_def)
    excluded = _excluded(config)
    cursor_field = cursor.cursor_field
    batch_size = max(1, int(config.batch_size))
    name = stream_def.name

    windows = _iter_windows(
        _first_day(cursor, config, int(config.lookback_days)),
        _today_in_account_tz(config),
        int(config.window_days),
    )
    log.info(
        "konnektive.%s: %d window(s), %s → %s",
        name,
        len(windows),
        windows[0][0].isoformat(),
        windows[-1][1].isoformat(),
    )

    base_params: dict[str, Any] = {
        "dateRangeType": "dateUpdated",
        "resultsPerPage": min(int(config.page_size), MAX_PAGE_SIZE),
    }
    if bool(config.include_custom_fields):
        base_params["includeCustomFields"] = 1

    rows_seen = 0
    batch: list[dict] = []
    for index, (window_start, window_end) in enumerate(windows, start=1):
        params = dict(base_params)
        params["startDate"] = window_start.isoformat()
        params["endDate"] = window_end.isoformat()

        window_rows = 0
        window_max: str | None = None
        for row in client.query(path, params):
            window_rows += 1
            value = row.get(cursor_field)
            if value is not None:
                text = str(value)
                if window_max is None or text > window_max:
                    window_max = text
            batch.append(_project(row, columns, excluded))
            if len(batch) >= batch_size:
                yield batch
                batch = []

        # Flush the window's tail BEFORE observing, so every row at or
        # below the observed value has already been handed to the engine.
        if batch:
            yield batch
            batch = []
        if window_max is not None:
            cursor.observe(window_max)

        rows_seen += window_rows
        if window_rows or index == len(windows) or index % 50 == 0:
            log.info(
                "konnektive.%s: window %d/%d (%s) → %d rows, %d total",
                name,
                index,
                len(windows),
                window_start.isoformat(),
                window_rows,
                rows_seen,
            )


@stream(name="orders")
def orders(
    stream_def: StreamDef, config: Config, cursor: Cursor, log: logging.Logger
) -> Iterator[Batch]:
    yield from _extract_query("order/query", stream_def, config, cursor, log)


@stream(name="transactions")
def transactions(
    stream_def: StreamDef, config: Config, cursor: Cursor, log: logging.Logger
) -> Iterator[Batch]:
    yield from _extract_query("transactions/query", stream_def, config, cursor, log)


@stream(name="purchases")
def purchases(
    stream_def: StreamDef, config: Config, cursor: Cursor, log: logging.Logger
) -> Iterator[Batch]:
    yield from _extract_query("purchase/query", stream_def, config, cursor, log)


@stream(name="customers")
def customers(
    stream_def: StreamDef, config: Config, cursor: Cursor, log: logging.Logger
) -> Iterator[Batch]:
    yield from _extract_query("customer/query", stream_def, config, cursor, log)


@stream(name="summary")
def summary(
    stream_def: StreamDef, config: Config, cursor: Cursor, log: logging.Logger
) -> Iterator[Batch]:
    """Per-day transaction summary, one request per day, merged on ``date``.

    A day's figures keep moving after the day ends — refunds and
    chargebacks are booked against the original transaction date — so every
    run re-pulls the trailing ``summary_lookback_days`` and merges.
    """
    client = _client(config)
    columns = _columns(stream_def)
    excluded = _excluded(config)

    today = _today_in_account_tz(config)
    day = min(_first_day(cursor, config, int(config.summary_lookback_days)), today)
    log.info("konnektive.summary: %s → %s", day.isoformat(), today.isoformat())

    batch: list[dict] = []
    rows_seen = 0
    while day <= today:
        rows = client.report(
            "transactions/summary",
            {
                "reportType": "date",
                "dateRangeType": "txnDate",
                "startDate": day.isoformat(),
                "endDate": day.isoformat(),
            },
        )
        for row in rows:
            record = _project(row, columns, excluded)
            # The window IS the day; never trust a row to restate it.
            record["date"] = _summary_date(row.get("date"), day)
            batch.append(record)
            rows_seen += 1
        if len(batch) >= 100:
            yield batch
            batch = []
        day += timedelta(days=1)

    if batch:
        yield batch
    # The cursor records how far the walk got, not the newest day that had
    # transactions — a quiet "today" must not pin the start a day back.
    if rows_seen:
        cursor.observe(today.isoformat())
    log.info("konnektive.summary: yielded %d day-rows", rows_seen)


def _summary_date(value: Any, day: date) -> str:
    """The row's own ``date`` when it parses, else the requested day — so
    the merge key is always ``YYYY-MM-DD``."""
    if value:
        try:
            return _parse_day(value).isoformat()
        except ValueError:
            pass
    return day.isoformat()
