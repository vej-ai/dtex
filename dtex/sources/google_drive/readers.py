# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Per-format readers for Drive files — CSV, XLSX and native Google Sheets.

Every format goes through the same grid rules as the ``google_sheets`` source
(:mod:`dtex.sources.google_sheets.grid`): the same ``range`` / ``header_row``
/ ``columns`` semantics and the same ``snake_case`` column names, so one
stream can union ``orders.csv`` and ``orders.xlsx`` into one table.

* **CSV** streams row by row (a file of any size) and keeps every value as
  text — exactly like the ``filesystem`` source's CSV reader; the engine
  infers or coerces types from there. A UTF-8 byte-order mark is dropped;
  ``.tsv`` files default to a tab delimiter.
* **XLSX** is read with ``openpyxl`` in read-only mode with cached formula
  values (``data_only``); cells keep their Excel types — numbers, booleans,
  dates (a midnight value in a date-only format becomes a ``date``), naive
  date-times — with per-column type unification.
* **Google Sheets** files in the folder are read through the Sheets API, as
  the ``google_sheets`` source reads one tab.
"""

from __future__ import annotations

import csv
import io
import re
from collections.abc import Iterator, Sequence
from dataclasses import replace
from datetime import datetime, time, timedelta
from typing import IO, Any

from dtex.sources.google_drive.client import GOOGLE_SHEET_MIME, DriveFile
from dtex.sources.google_sheets.a1 import GridRange
from dtex.sources.google_sheets.client import SheetsClient
from dtex.sources.google_sheets.grid import GridOptions, records_from_rows
from dtex.sources.google_sheets.reader import load_time_zone, read_tab

FORMATS = ("csv", "xlsx", "google_sheets")

_EXTENSIONS = {
    ".csv": "csv",
    ".tsv": "csv",
    ".xlsx": "xlsx",
    ".xlsm": "xlsx",
}
_MIME_FORMATS = {
    "text/csv": "csv",
    "text/tab-separated-values": "csv",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
    "application/vnd.ms-excel.sheet.macroEnabled.12": "xlsx",
    GOOGLE_SHEET_MIME: "google_sheets",
}


def detect_format(file: DriveFile, forced: str) -> str | None:
    """The format to read ``file`` as, or ``None`` when ``auto`` cannot tell.

    ``forced`` is the ``format`` param: ``auto`` (extension first, then the
    Drive mimeType) or one of :data:`FORMATS`. A native Google Sheet is always
    read as ``google_sheets`` — it has no bytes to download.
    """
    if file.mime_type == GOOGLE_SHEET_MIME:
        return "google_sheets"
    if forced != "auto":
        return forced
    lower = file.name.lower()
    for ext, fmt in _EXTENSIONS.items():
        if lower.endswith(ext):
            return fmt
    return _MIME_FORMATS.get(file.mime_type)


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------


def read_csv(
    handle: IO[bytes], file: DriveFile, *, delimiter: str, options: GridOptions
) -> Iterator[dict[str, Any]]:
    """Stream a CSV's records — values stay text (see the module docstring)."""
    if delimiter == "," and file.name.lower().endswith(".tsv"):
        delimiter = "\t"
    text = io.TextIOWrapper(handle, encoding="utf-8-sig", errors="replace", newline="")
    reader = csv.reader(text, delimiter=delimiter, strict=True)

    def rows() -> Iterator[tuple[int, list[str]]]:
        try:
            yield from enumerate(reader, start=1)
        except csv.Error as exc:
            raise ValueError(
                f"failed to parse CSV file {file.path!r} at line {reader.line_num}: {exc}"
            ) from exc

    try:
        yield from records_from_rows(
            rows(), origin_col=1, options=replace(options, unify_types=False)
        )
    finally:
        text.detach()


# ---------------------------------------------------------------------------
# XLSX
# ---------------------------------------------------------------------------

_QUOTED = re.compile(r'"[^"]*"|\[[^\]]*\]|\\.')


def _lazy_openpyxl() -> Any:
    try:
        import openpyxl
    except ImportError as exc:  # pragma: no cover — a base dependency
        raise ImportError(
            "the google_drive connector needs `openpyxl` to read .xlsx files; it "
            "ships with `pip install dtex` — reinstall dtex if it is missing"
        ) from exc
    return openpyxl


def read_xlsx(
    handle: IO[bytes], file: DriveFile, *, sheet: str, options: GridOptions
) -> Iterator[dict[str, Any]]:
    """Read one worksheet of an XLSX file (``sheet``: name or 1-based position)."""
    openpyxl = _lazy_openpyxl()
    try:
        workbook = openpyxl.load_workbook(handle, read_only=True, data_only=True)
    except Exception as exc:  # noqa: BLE001 — zipfile / openpyxl parse errors
        raise ValueError(f"failed to open XLSX file {file.path!r}: {exc}") from exc
    try:
        worksheet = _pick_worksheet(workbook, sheet, file)
        worksheet.reset_dimensions()  # never trust a stale <dimension> tag
        bounds = options.bounds or GridRange()
        first_row = bounds.first_row
        if options.header_row:
            first_row = min(first_row, options.header_row)
        first_col = bounds.first_col

        def rows() -> Iterator[tuple[int, list[Any]]]:
            for offset, cells in enumerate(
                worksheet.iter_rows(
                    min_row=first_row,
                    max_row=bounds.end_row,
                    min_col=first_col,
                    max_col=bounds.end_col,
                )
            ):
                yield first_row + offset, [_xlsx_value(c) for c in cells]

        records = list(records_from_rows(rows(), origin_col=first_col, options=options))
    finally:
        workbook.close()
    yield from records


def _pick_worksheet(workbook: Any, sheet: str, file: DriveFile) -> Any:
    sheets = list(workbook.worksheets)
    if not sheets:
        raise ValueError(f"XLSX file {file.path!r} has no worksheets")
    wanted = sheet.strip()
    if not wanted:
        return sheets[0]
    for ws in sheets:
        if ws.title == wanted:
            return ws
    if wanted.isdigit() and 1 <= int(wanted) <= len(sheets):
        return sheets[int(wanted) - 1]
    names = ", ".join(repr(ws.title) for ws in sheets)
    raise ValueError(
        f"XLSX file {file.path!r} has no sheet {wanted!r} (by name or 1-based position); "
        f"sheets: {names}"
    )


def _xlsx_value(cell: Any) -> Any:
    value = getattr(cell, "value", None)
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        if value.time() == time(0) and _is_date_only_format(getattr(cell, "number_format", "")):
            return value.date()
        return value
    if isinstance(value, time):
        return value.replace(microsecond=0).isoformat()
    if isinstance(value, timedelta):
        total = int(round(value.total_seconds()))
        sign = "-" if total < 0 else ""
        hours, rest = divmod(abs(total), 3600)
        minutes, seconds = divmod(rest, 60)
        return f"{sign}{hours:02d}:{minutes:02d}:{seconds:02d}"
    return value


def _is_date_only_format(number_format: str) -> bool:
    """Whether an Excel number format shows no time part (no hours / seconds)."""
    stripped = _QUOTED.sub("", number_format or "").lower()
    return "h" not in stripped and "s" not in stripped


# ---------------------------------------------------------------------------
# Native Google Sheets
# ---------------------------------------------------------------------------


def read_google_sheet(
    client: SheetsClient,
    file: DriveFile,
    *,
    sheet: str,
    range_text: str | None,
    header_row: int | None,
    columns: Sequence[str],
    parse_dates: bool,
) -> Iterator[dict[str, Any]]:
    """Read one tab (``sheet``: title or 1-based position; default the first)."""
    info = client.spreadsheet(file.id)
    tabs = list(info.grid_tabs)
    if not tabs:
        return
    wanted = sheet.strip()
    tab = tabs[0] if not wanted else None
    if tab is None:
        tab = next((t for t in tabs if t.title == wanted), None)
    if tab is None and wanted.isdigit() and 1 <= int(wanted) <= len(tabs):
        tab = tabs[int(wanted) - 1]
    if tab is None:
        names = ", ".join(repr(t.title) for t in tabs)
        raise ValueError(
            f"Google Sheet {file.path!r} has no tab {wanted!r} (by title or 1-based "
            f"position); tabs: {names}"
        )
    yield from read_tab(
        client,
        file.id,
        tab,
        range_text=range_text,
        header_row=header_row,
        columns=columns,
        parse_dates=parse_dates,
        time_zone=load_time_zone(info.time_zone),
    )
