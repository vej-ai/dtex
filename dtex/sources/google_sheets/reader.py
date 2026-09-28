# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Read one tab of a Google Sheet into records — values, date typing, grid rules.

Used by the ``google_sheets`` source for every discovered tab and by the
``google_drive`` source for native Google Sheets files found in a folder.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterator, Sequence
from datetime import datetime, timedelta, tzinfo
from typing import Any

from dtex.sources.google_sheets.a1 import (
    GridRange,
    parse_a1,
    parse_a1_top_left,
    quote_sheet_title,
)
from dtex.sources.google_sheets.client import SheetsClient, TabInfo
from dtex.sources.google_sheets.grid import GridOptions, records_from_rows

# Day 0 of Google Sheets (and Excel 1900-system) date serials. Sheets counts
# days from 1899-12-30, which also absorbs Lotus' phantom 1900-02-29, so the
# conversion is a plain offset for every date Sheets can hold.
SERIAL_EPOCH = datetime(1899, 12, 30)
_DATE_TYPES = frozenset({"DATE", "DATE_TIME", "TIME"})


def serial_to_value(serial: float, number_format: str, tz: tzinfo | None) -> Any:
    """Convert a Sheets date/time serial per the cell's number-format type.

    * ``DATE`` → :class:`datetime.date` (the integer day; a time part the
      format hides is dropped, matching what the sheet shows);
    * ``DATE_TIME`` → :class:`datetime.datetime` rounded to the millisecond,
      in the spreadsheet's time zone (``tz``) — Sheets date-times are wall
      clock in that zone, so the aware value is the correct instant;
    * ``TIME`` → ``"HH:MM:SS"`` text. Sheets uses the TIME type for both a
      time of day and a duration (``[h]:mm:ss``); a value of a day or more
      renders as total hours (``"36:00:00"``).
    """
    if number_format == "DATE":
        return (SERIAL_EPOCH + timedelta(days=math.floor(serial))).date()
    if number_format == "DATE_TIME":
        value = SERIAL_EPOCH + timedelta(milliseconds=round(serial * 86_400_000))
        return value.replace(tzinfo=tz) if tz is not None else value
    total = round(abs(serial) * 86_400)
    hours, rest = divmod(total, 3600)
    minutes, seconds = divmod(rest, 60)
    sign = "-" if serial < 0 else ""
    return f"{sign}{hours:02d}:{minutes:02d}:{seconds:02d}"


def convert_cell(value: Any, number_format: str | None, tz: tzinfo | None) -> Any:
    """One UNFORMATTED_VALUE cell → its Python value (``None`` for empty)."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if number_format in _DATE_TYPES:
            return serial_to_value(float(value), number_format, tz)
        return value
    return value


def load_time_zone(name: str | None) -> tzinfo | None:
    """The spreadsheet's IANA time zone, or ``None`` if unknown to this system."""
    if not name:
        return None
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(name)
    except Exception:  # noqa: BLE001 — ZoneInfoNotFoundError, missing tzdata
        return None


def read_tab(
    client: SheetsClient,
    spreadsheet_id: str,
    tab: TabInfo,
    *,
    range_text: str | None,
    header_row: int | None,
    columns: Sequence[str],
    parse_dates: bool,
    time_zone: tzinfo | None,
) -> Iterator[dict[str, Any]]:
    """Yield the records of one tab, cut per the grid rules (``grid`` module).

    ``range_text`` is A1 without a sheet name (``A2:N`` / ``A2:N10``) or
    ``None`` for the tab's whole used area. ``header_row`` is an absolute sheet
    row (``None`` = auto, ``0`` = no header). With ``parse_dates`` a second,
    format-only request types date / date-time / time cells.
    """
    bounds = parse_a1(range_text) if range_text else None
    fetch = bounds or GridRange()
    if header_row:
        fetch = fetch.with_first_row(header_row)
    if fetch == GridRange():
        a1 = quote_sheet_title(tab.title)  # the whole tab — the API trims to the used area
    else:
        cells = fetch.to_a1(tab.row_count, tab.column_count)
        if cells is None:
            return  # the range starts past the tab's grid: nothing to read
        a1 = f"{quote_sheet_title(tab.title)}!{cells}"

    [(returned_range, rows)] = client.values(spreadsheet_id, [a1])
    if not rows:
        return
    origin_row, origin_col = parse_a1_top_left(returned_range) if "!" in returned_range else (
        fetch.first_row,
        fetch.first_col,
    )
    fmt_at: Callable[[int, int], str | None]
    if parse_dates and _has_numbers(rows):
        fmt_at = client.number_formats(spreadsheet_id, a1)
    else:
        def fmt_at(_row: int, _col: int) -> str | None:
            return None

    def converted() -> Iterator[tuple[int, list[Any]]]:
        for i, row in enumerate(rows):
            r = origin_row + i
            yield r, [
                convert_cell(v, fmt_at(r, origin_col + j), time_zone) for j, v in enumerate(row)
            ]

    options = GridOptions(
        bounds=bounds, header_row=header_row, columns=tuple(columns), unify_types=True
    )
    yield from records_from_rows(converted(), origin_col=origin_col, options=options)


def _has_numbers(rows: list[list[Any]]) -> bool:
    """Whether any cell is numeric — only then can a date serial hide in the tab."""
    for row in rows:
        for v in row:
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                return True
    return False

