"""Konnektive CRM source — five @stream functions over one shared walk.

Four of the streams (``orders``, ``transactions``, ``purchases``,
``customers``) are the same shape: a ``*/query/`` endpoint filtered by
``dateRangeType=dateUpdated`` + ``startDate`` / ``endDate``, paginated 200
rows at a time, merged on the object's id. ``summary`` is a per-day report.

How a run walks:

1. Resolve the start: the persisted cursor minus ``lookback_hours``, or the
   ``start_date`` param on a virgin run (or under ``--full-refresh``).
2. Tile start → now into ``window_days``-wide windows, ascending. Small
   windows keep each request cheap and individually retryable, and bound
   how deep any one page walk goes: Konnektive paginates by page NUMBER, so
   a row updated mid-walk shifts later pages — the shallower the walk, the
   smaller that exposure. (A row that does slip gets a newer
   ``dateUpdated``, so a later window or the next run's lookback picks it
   up; ``merge`` makes the re-pull idempotent.)
3. For each window, paginate, project each row onto the declared schema,
   and keep the whole object beside it under ``raw``.
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
only: working out what "now" is in the account's terms, for the end of the
last window.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from dtex import Batch, Config, Cursor, StreamDef, stream

from .client import MAX_PAGE_SIZE, KonnektiveClient

# The wire format for startDate / endDate AND for the dateUpdated values the
# API returns — the cursor is stored in the same form, so it compares
# correctly as a string.
_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
_DATE_FORMAT = "%Y-%m-%d"

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


def _now_in_account_tz(config: Config) -> datetime:
    """The current wall-clock time in the account's timezone, naive."""
    return datetime.now(tz=ZoneInfo(str(config.account_timezone))).replace(
        tzinfo=None, microsecond=0
    )


def _parse_datetime(value: Any) -> datetime:
    """A cursor / param value as a naive datetime. Accepts a bare date."""
    text = str(value).strip().replace("T", " ")
    if len(text) <= 10:
        return datetime.strptime(text, _DATE_FORMAT)
    return datetime.strptime(text[:19], _DATETIME_FORMAT)


def _iter_windows(
    start: datetime, end: datetime, window_days: int
) -> list[tuple[datetime, datetime]]:
    """Inclusive ``(start, end)`` windows tiling start → end, ascending.

    Adjacent windows are one second apart (Konnektive's resolution), so no
    second is requested twice and none is skipped. A start past the end —
    clock skew, a wrong ``account_timezone``, a cursor from the future —
    still yields ONE window ending at ``end``: an empty list would sync
    nothing and look healthy.
    """
    if start > end:
        start = end
    span = timedelta(days=max(1, int(window_days)))
    windows: list[tuple[datetime, datetime]] = []
    window_start = start
    while window_start <= end:
        window_end = min(window_start + span - timedelta(seconds=1), end)
        windows.append((window_start, window_end))
        window_start = window_end + timedelta(seconds=1)
    return windows


def _column_name(key: str) -> str:
    """The destination column for an API key. Warehouses reject a leading
    digit (``3DTxnResult``), so those gain a ``_`` prefix; ``raw`` keeps
    the API's own spelling."""
    return f"_{key}" if key[:1].isdigit() else key


def _project(row: dict[str, Any], columns: tuple[str, ...]) -> dict[str, Any]:
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
    cursor_field = cursor.cursor_field
    batch_size = max(1, int(config.batch_size))
    name = stream_def.name

    start_value = cursor.start_value()
    if start_value is None:
        start = _parse_datetime(config.start_date)
    else:
        start = _parse_datetime(start_value) - timedelta(hours=int(config.lookback_hours))
    windows = _iter_windows(start, _now_in_account_tz(config), int(config.window_days))
    log.info(
        "konnektive.%s: %d window(s) from %s to %s",
        name,
        len(windows),
        windows[0][0].strftime(_DATETIME_FORMAT),
        windows[-1][1].strftime(_DATETIME_FORMAT),
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
        params["startDate"] = window_start.strftime(_DATETIME_FORMAT)
        params["endDate"] = window_end.strftime(_DATETIME_FORMAT)

        window_rows = 0
        window_max: str | None = None
        for row in client.query(path, params):
            window_rows += 1
            value = row.get(cursor_field)
            if value is not None:
                text = str(value)
                if window_max is None or text > window_max:
                    window_max = text
            batch.append(_project(row, columns))
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
                window_start.strftime(_DATETIME_FORMAT),
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

    today = _now_in_account_tz(config).date()
    start_value = cursor.start_value()
    if start_value is None:
        day = _parse_datetime(config.start_date).date()
    else:
        day = _parse_datetime(start_value).date() - timedelta(
            days=int(config.summary_lookback_days)
        )
    day = min(day, today)
    log.info("konnektive.summary: %s → %s", day.isoformat(), today.isoformat())

    batch: list[dict] = []
    rows_seen = 0
    while day <= today:
        rows = client.report(
            "transactions/summary",
            {
                "reportType": "date",
                "dateRangeType": "txnDate",
                "startDate": day.strftime(_DATE_FORMAT),
                "endDate": day.strftime(_DATE_FORMAT),
            },
        )
        for row in rows:
            record = _project(row, columns)
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
    """The row's own ``date`` when it parses to the requested day's form,
    else the requested day — so the merge key is always ``YYYY-MM-DD``."""
    if value:
        try:
            return _parse_datetime(value).date().isoformat()
        except ValueError:
            pass
    return day.isoformat()
