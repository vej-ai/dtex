"""In-memory fakes of the Google Sheets v4 and Drive v3 REST endpoints.

The google_sheets / google_drive connectors only call
``session.request("GET", url, params=..., timeout=..., stream=...)`` on the
session :func:`dtex.sources.google_sheets.auth.authorized_session` returns;
tests monkeypatch that function to hand back a :class:`FakeGoogle`, which
answers from Python data and records every request. No network.

The fake mimics the API behaviour the connectors depend on:

* ``values:batchGet`` trims trailing empty cells of each row and trailing
  empty rows, keeps leading/inner empty cells as ``""``, and reports the
  resolved range (a whole-tab request reports the full grid, from ``A1``);
* a range that reaches past the tab's grid is rejected like the real API
  ("exceeds grid limits");
* ``spreadsheets.get`` with ``ranges`` returns grid data for number formats,
  with 0-based ``startRow`` / ``startColumn`` omitted when zero;
* ``files.list`` paginates (``page_size`` per page, ``nextPageToken``).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import unquote

from dtex.sources.google_sheets.a1 import column_letters, parse_a1

SHEETS = "https://sheets.googleapis.com/v4/spreadsheets"
DRIVE = "https://www.googleapis.com/drive/v3/files"
FOLDER_MIME = "application/vnd.google-apps.folder"
SHEET_MIME = "application/vnd.google-apps.spreadsheet"


class FakeResponse:
    def __init__(
        self,
        status: int = 200,
        body: Any = None,
        content: bytes = b"",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status
        self._body = body
        self._content = content
        self.headers = headers or {}
        self.closed = False

    def json(self) -> Any:
        if self._body is None:
            raise ValueError("no JSON body")
        return self._body

    @property
    def text(self) -> str:
        return json.dumps(self._body) if self._body is not None else self._content.decode()

    def iter_content(self, chunk_size: int = 1024) -> Any:
        for i in range(0, len(self._content), 7):  # tiny chunks: exercise reassembly
            yield self._content[i : i + 7]

    def close(self) -> None:
        self.closed = True


def api_error(status: int, message: str = "boom", reason: str = "") -> FakeResponse:
    err: dict[str, Any] = {"code": status, "message": message}
    if reason:
        err["errors"] = [{"reason": reason, "message": message}]
    return FakeResponse(status, {"error": err})


@dataclass
class FakeTab:
    sheet_id: int
    title: str
    cells: list[list[Any]] = field(default_factory=list)
    formats: dict[tuple[int, int], str] = field(default_factory=dict)  # 1-based (row, col)
    sheet_type: str = "GRID"
    row_count: int = 1000
    column_count: int = 26
    hidden: bool = False


@dataclass
class FakeSpreadsheet:
    spreadsheet_id: str
    title: str
    tabs: list[FakeTab]
    time_zone: str = "Europe/Vilnius"


@dataclass
class FakeDriveItem:
    id: str
    name: str
    mime_type: str
    parents: list[str]
    modified_time: str = "2026-09-01T10:00:00.000Z"
    content: bytes = b""
    trashed: bool = False
    drive_id: str | None = None


class FakeGoogle:
    """One fake Google account: spreadsheets + drive items; records requests."""

    def __init__(self, *, page_size: int = 2) -> None:
        self.spreadsheets: dict[str, FakeSpreadsheet] = {}
        self.items: dict[str, FakeDriveItem] = {}
        self.requests: list[tuple[str, list[tuple[str, Any]]]] = []
        self.injected: list[FakeResponse] = []  # served (FIFO) before real answers
        self.page_size = page_size

    # -- setup helpers -----------------------------------------------------

    def add_spreadsheet(self, sheet: FakeSpreadsheet) -> FakeSpreadsheet:
        self.spreadsheets[sheet.spreadsheet_id] = sheet
        return sheet

    def add_item(self, item: FakeDriveItem) -> FakeDriveItem:
        self.items[item.id] = item
        return item

    # -- the session surface ----------------------------------------------

    def request(
        self,
        method: str,
        url: str,
        params: Any = None,
        timeout: float | None = None,
        stream: bool = False,
    ) -> FakeResponse:
        assert method == "GET"
        pairs = list(params.items()) if isinstance(params, dict) else list(params or [])
        self.requests.append((url, pairs))
        if self.injected:
            return self.injected.pop(0)
        if url.startswith(SHEETS):
            return self._sheets(url[len(SHEETS) :], pairs)
        if url.startswith(DRIVE):
            return self._drive(url[len(DRIVE) :], pairs)
        return api_error(404, f"unknown URL {url}")

    def requests_to(self, fragment: str) -> list[dict[str, Any]]:
        """Params of every request whose URL contains ``fragment`` (multi → list)."""
        out: list[dict[str, Any]] = []
        for url, pairs in self.requests:
            if fragment in url:
                merged: dict[str, Any] = {}
                for k, v in pairs:
                    if k in merged:
                        merged[k] = [
                            *(merged[k] if isinstance(merged[k], list) else [merged[k]]),
                            v,
                        ]
                    else:
                        merged[k] = v
                out.append(merged)
        return out

    # -- Sheets ------------------------------------------------------------

    def _sheets(self, path: str, pairs: list[tuple[str, Any]]) -> FakeResponse:
        m = re.match(r"^/([^/]+)(/values:batchGet)?$", path)
        if m is None:
            return api_error(404, "bad sheets path")
        sid = unquote(m.group(1))
        book = self.spreadsheets.get(sid)
        if book is None:
            return api_error(404, "Requested entity was not found.")
        ranges = [v for k, v in pairs if k == "ranges"]
        if m.group(2):
            out = []
            for rng in ranges:
                resolved = self._resolve(book, rng)
                if isinstance(resolved, FakeResponse):
                    return resolved
                tab, r0, c0, r1, c1, label = resolved
                out.append(
                    {"range": label, "majorDimension": "ROWS", **_values(tab, r0, c0, r1, c1)}
                )
            return FakeResponse(200, {"spreadsheetId": sid, "valueRanges": out})
        if ranges:
            sheets = []
            for rng in ranges:
                resolved = self._resolve(book, rng)
                if isinstance(resolved, FakeResponse):
                    return resolved
                tab, r0, c0, r1, c1, _label = resolved
                sheets.append({"data": [_format_data(tab, r0, c0, r1, c1)]})
            return FakeResponse(200, {"sheets": sheets})
        return FakeResponse(
            200,
            {
                "spreadsheetId": sid,
                "properties": {"title": book.title, "timeZone": book.time_zone},
                "sheets": [
                    {
                        "properties": {
                            "sheetId": t.sheet_id,
                            "title": t.title,
                            "index": i,
                            "sheetType": t.sheet_type,
                            "hidden": t.hidden,
                            "gridProperties": {
                                "rowCount": t.row_count,
                                "columnCount": t.column_count,
                            },
                        }
                    }
                    for i, t in enumerate(book.tabs)
                ],
            },
        )

    def _resolve(self, book: FakeSpreadsheet, rng: str) -> Any:
        if "!" in rng:
            title_part, cells = rng.rsplit("!", 1)
        else:
            title_part, cells = rng, ""
        if title_part.startswith("'") and title_part.endswith("'"):
            title = title_part[1:-1].replace("''", "'")
        else:
            title = title_part
        tab = next((t for t in book.tabs if t.title == title), None)
        if tab is None:
            return api_error(400, f"Unable to parse range: {rng}")
        if not cells:
            label = f"{title_part}!A1:{column_letters(tab.column_count)}{tab.row_count}"
            return tab, 1, 1, tab.row_count, tab.column_count, label
        g = parse_a1(cells)
        r0 = g.first_row
        c0 = g.first_col
        r1 = g.end_row or tab.row_count
        c1 = g.end_col or tab.column_count
        if r1 > tab.row_count or c1 > tab.column_count:
            return api_error(400, f"Range ({rng}) exceeds grid limits.")
        label = f"{title_part}!{column_letters(c0)}{r0}:{column_letters(c1)}{r1}"
        return tab, r0, c0, r1, c1, label

    # -- Drive -------------------------------------------------------------

    def _drive(self, path: str, pairs: list[tuple[str, Any]]) -> FakeResponse:
        params = dict(pairs)
        if path == "":
            return self._list(params)
        fid = unquote(path.lstrip("/"))
        item = self.items.get(fid)
        if item is None:
            return api_error(404, f"File not found: {fid}.")
        if params.get("alt") == "media":
            if item.mime_type.startswith("application/vnd.google-apps"):
                return api_error(403, "Only files with binary content can be downloaded.")
            return FakeResponse(200, None, content=item.content)
        meta: dict[str, Any] = {"id": item.id, "name": item.name, "mimeType": item.mime_type}
        if item.drive_id:
            meta["driveId"] = item.drive_id
        return FakeResponse(200, meta)

    def _list(self, params: dict[str, Any]) -> FakeResponse:
        m = re.match(r"^'([^']+)' in parents and trashed=false$", params["q"])
        assert m is not None, params["q"]
        parent = m.group(1)
        children = [i for i in self.items.values() if parent in i.parents and not i.trashed]
        children.sort(key=lambda i: i.id)
        start = int(params.get("pageToken") or 0)
        page = children[start : start + self.page_size]
        body: dict[str, Any] = {
            "files": [
                {
                    "id": i.id,
                    "name": i.name,
                    "mimeType": i.mime_type,
                    "modifiedTime": i.modified_time,
                    **({"size": str(len(i.content))} if i.content else {}),
                }
                for i in page
            ]
        }
        if start + self.page_size < len(children):
            body["nextPageToken"] = str(start + self.page_size)
        return FakeResponse(200, body)


def _cell(tab: FakeTab, r: int, c: int) -> Any:
    if r - 1 < len(tab.cells) and c - 1 < len(tab.cells[r - 1]):
        return tab.cells[r - 1][c - 1]
    return ""


def _values(tab: FakeTab, r0: int, c0: int, r1: int, c1: int) -> dict[str, Any]:
    rows: list[list[Any]] = []
    for r in range(r0, r1 + 1):
        row = [_cell(tab, r, c) for c in range(c0, c1 + 1)]
        row = [None if v is None else v for v in row]
        while row and (row[-1] == "" or row[-1] is None):
            row.pop()
        rows.append(["" if v is None else v for v in row])
    while rows and not rows[-1]:
        rows.pop()
    return {"values": rows} if rows else {}


def _format_data(tab: FakeTab, r0: int, c0: int, r1: int, c1: int) -> dict[str, Any]:
    data: dict[str, Any] = {}
    if r0 > 1:
        data["startRow"] = r0 - 1
    if c0 > 1:
        data["startColumn"] = c0 - 1
    row_data = []
    last = min(r1, max(len(tab.cells), 1))
    for r in range(r0, last + 1):
        values = []
        for c in range(c0, c1 + 1):
            fmt = tab.formats.get((r, c))
            values.append({"effectiveFormat": {"numberFormat": {"type": fmt}}} if fmt else {})
        while values and values[-1] == {}:
            values.pop()
        row_data.append({"values": values})
    data["rowData"] = row_data
    return data
