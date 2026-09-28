# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""A thin Google Sheets API v4 client — the three read calls the connector needs.

* :meth:`SheetsClient.spreadsheet` — ``spreadsheets.get`` restricted to the
  spreadsheet's title / time zone and each tab's properties (id, title, type,
  grid size). No cell data.
* :meth:`SheetsClient.values` — ``spreadsheets.values.batchGet`` with
  ``valueRenderOption=UNFORMATTED_VALUE`` and
  ``dateTimeRenderOption=SERIAL_NUMBER``: numbers arrive as numbers (not
  locale-formatted text), booleans as booleans, and dates / times as their
  serial number, which is exact and locale-free.
* :meth:`SheetsClient.number_formats` — ``spreadsheets.get`` for the same
  range, restricted to each cell's ``effectiveFormat.numberFormat.type``. It
  is what tells a date serial (``DATE`` / ``DATE_TIME`` / ``TIME``) apart
  from an ordinary number, per cell, so a date column becomes dates without
  guessing from a locale-formatted string.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

from dtex.sources.google_sheets import http

SHEETS_API = "https://sheets.googleapis.com/v4/spreadsheets"

_META_FIELDS = (
    "spreadsheetId,properties(title,timeZone),"
    "sheets(properties(sheetId,title,index,sheetType,hidden,"
    "gridProperties(rowCount,columnCount)))"
)
_FORMAT_FIELDS = (
    "sheets(data(startRow,startColumn,rowData(values(effectiveFormat(numberFormat(type))))))"
)


@dataclass(frozen=True)
class TabInfo:
    """One tab (sheet) of a spreadsheet, from its properties."""

    sheet_id: int
    title: str
    index: int
    sheet_type: str = "GRID"
    hidden: bool = False
    row_count: int | None = None
    column_count: int | None = None


@dataclass(frozen=True)
class SpreadsheetInfo:
    """A spreadsheet's title, time zone and tabs (in tab order)."""

    spreadsheet_id: str
    title: str
    time_zone: str | None
    tabs: tuple[TabInfo, ...]

    @property
    def grid_tabs(self) -> tuple[TabInfo, ...]:
        """Tabs that hold cells — chart sheets and data-source sheets have none."""
        return tuple(t for t in self.tabs if t.sheet_type == "GRID")


class SheetsClient:
    """Read-only access to one Google account's view of Google Sheets."""

    def __init__(self, session: Any) -> None:
        self._session = session

    def spreadsheet(self, spreadsheet_id: str) -> SpreadsheetInfo:
        """Title, time zone and tab list of a spreadsheet (no cell data)."""
        body = http.request_json(
            self._session,
            f"{SHEETS_API}/{quote(spreadsheet_id)}",
            {"fields": _META_FIELDS},
            what=f"reading spreadsheet {spreadsheet_id}",
        )
        props = body.get("properties") or {}
        tabs: list[TabInfo] = []
        for sheet in body.get("sheets") or []:
            p = sheet.get("properties") or {}
            grid = p.get("gridProperties") or {}
            tabs.append(
                TabInfo(
                    sheet_id=int(p.get("sheetId", 0)),
                    title=str(p.get("title", "")),
                    index=int(p.get("index", len(tabs))),
                    sheet_type=str(p.get("sheetType", "GRID")),
                    hidden=bool(p.get("hidden", False)),
                    row_count=_int_or_none(grid.get("rowCount")),
                    column_count=_int_or_none(grid.get("columnCount")),
                )
            )
        tabs.sort(key=lambda t: t.index)
        return SpreadsheetInfo(
            spreadsheet_id=spreadsheet_id,
            title=str(props.get("title", "")),
            time_zone=props.get("timeZone") or None,
            tabs=tuple(tabs),
        )

    def values(self, spreadsheet_id: str, ranges: list[str]) -> list[tuple[str, list[list[Any]]]]:
        """``values.batchGet`` → ``[(returned_range, rows), …]`` in request order."""
        params: list[tuple[str, Any]] = [("ranges", r) for r in ranges]
        params += [
            ("majorDimension", "ROWS"),
            ("valueRenderOption", "UNFORMATTED_VALUE"),
            ("dateTimeRenderOption", "SERIAL_NUMBER"),
        ]
        body = http.request_json(
            self._session,
            f"{SHEETS_API}/{quote(spreadsheet_id)}/values:batchGet",
            params,
            what=f"reading values of spreadsheet {spreadsheet_id}",
        )
        out: list[tuple[str, list[list[Any]]]] = []
        for value_range in body.get("valueRanges") or []:
            out.append((str(value_range.get("range", "")), list(value_range.get("values") or [])))
        return out

    def number_formats(
        self, spreadsheet_id: str, a1_range: str
    ) -> Callable[[int, int], str | None]:
        """Each cell's effective number-format type for ``a1_range``.

        Returns a lookup ``(sheet_row, sheet_col) → type`` (1-based; ``None``
        for a cell without a number format).
        """
        body = http.request_json(
            self._session,
            f"{SHEETS_API}/{quote(spreadsheet_id)}",
            [("ranges", a1_range), ("fields", _FORMAT_FIELDS)],
            what=f"reading number formats of spreadsheet {spreadsheet_id}",
        )
        types: dict[tuple[int, int], str] = {}
        for sheet in body.get("sheets") or []:
            for data in sheet.get("data") or []:
                row0 = int(data.get("startRow", 0)) + 1
                col0 = int(data.get("startColumn", 0)) + 1
                for i, row in enumerate(data.get("rowData") or []):
                    for j, cell in enumerate(row.get("values") or []):
                        fmt = ((cell or {}).get("effectiveFormat") or {}).get("numberFormat")
                        if fmt and fmt.get("type"):
                            types[(row0 + i, col0 + j)] = str(fmt["type"])

        def lookup(row: int, col: int) -> str | None:
            return types.get((row, col))

        return lookup


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
