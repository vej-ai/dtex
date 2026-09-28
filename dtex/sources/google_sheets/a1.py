# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""A1 notation, tab selectors and Google link parsing — pure functions, no I/O.

Everything here is 1-based, like the spreadsheet UI: row 1 is the first row,
column 1 is column ``A``. A :class:`GridRange` bound of ``None`` means "open":
``A2:N`` has no end row (it runs to the last row holding data), ``A:C`` has
neither a start nor an end row.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse

_COL_RE = re.compile(r"^[A-Za-z]{1,3}$")
_CELL_RE = re.compile(r"^([A-Za-z]{0,3})(\d*)$")
_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_SPREADSHEET_PATH_RE = re.compile(r"/spreadsheets/(?:u/\d+/)?d/([A-Za-z0-9_-]+)")
_FOLDER_PATH_RE = re.compile(r"/folders/([A-Za-z0-9_-]+)")
_GID_RE = re.compile(r"^#?gid\s*[=:]\s*(\d+)$", re.IGNORECASE)

# Excel / Sheets hard limits: 18278 columns (ZZZ) — anything beyond is a typo.
_MAX_COLUMN = 18278


def column_index(letters: str) -> int:
    """``"A"`` → 1, ``"Z"`` → 26, ``"AA"`` → 27. Raises on anything but 1–3 letters."""
    if not _COL_RE.match(letters):
        raise ValueError(f"invalid column letters {letters!r}")
    value = 0
    for ch in letters.upper():
        value = value * 26 + (ord(ch) - ord("A") + 1)
    return value


def column_letters(index: int) -> str:
    """1 → ``"A"``, 27 → ``"AA"`` — the inverse of :func:`column_index`."""
    if index < 1 or index > _MAX_COLUMN:
        raise ValueError(f"column index {index} is out of range 1..{_MAX_COLUMN}")
    out = ""
    while index:
        index, rem = divmod(index - 1, 26)
        out = chr(ord("A") + rem) + out
    return out


@dataclass(frozen=True)
class GridRange:
    """A rectangular block of cells; ``None`` bounds are open (see module docstring)."""

    start_row: int | None = None
    start_col: int | None = None
    end_row: int | None = None
    end_col: int | None = None

    @property
    def first_row(self) -> int:
        """The first row the range covers (1 when the start row is open)."""
        return self.start_row or 1

    @property
    def first_col(self) -> int:
        """The first column the range covers (1 when the start column is open)."""
        return self.start_col or 1

    def contains_row(self, row: int) -> bool:
        """Whether 1-based ``row`` lies inside the range's row bounds."""
        return row >= self.first_row and (self.end_row is None or row <= self.end_row)

    def with_first_row(self, row: int) -> GridRange:
        """The same range, grown upward so it starts at ``row`` (never shrunk)."""
        if row >= self.first_row:
            return self
        return GridRange(row, self.start_col, self.end_row, self.end_col)

    def to_a1(self, max_rows: int | None = None, max_cols: int | None = None) -> str | None:
        """Render to A1 for an API request, clamped to the tab's grid size.

        The Sheets API rejects a range that reaches past the grid ("exceeds
        grid limits"), so a known grid size clamps the end bounds. Open
        bounds render in the forms Sheets accepts: ``A2:N`` (to the last
        row), ``A:C`` (whole columns), ``2:10`` (whole rows). Returns ``None``
        when the range starts past the grid — there is nothing to read.
        """
        if max_rows is not None and self.first_row > max_rows:
            return None
        if max_cols is not None and self.first_col > max_cols:
            return None
        end_row = self.end_row
        if end_row is not None and max_rows is not None:
            end_row = min(end_row, max_rows)
        if self.start_col is None:  # whole rows
            if end_row is None:
                end_row = max_rows if max_rows is not None else _LAST_ROW
            return f"{self.first_row}:{end_row}"
        end_col = self.end_col or self.start_col
        if max_cols is not None:
            end_col = min(end_col, max_cols)
        start_letters = column_letters(self.start_col)
        end_letters = column_letters(end_col)
        if self.start_row is None and end_row is None:
            return f"{start_letters}:{end_letters}"
        return f"{start_letters}{self.first_row}:{end_letters}{end_row or ''}"


# A row-only open range ("2:") needs a concrete end row when the grid size is
# unknown; this is past the Sheets 10M-cell ceiling.
_LAST_ROW = 10_000_000


def parse_a1(text: str) -> GridRange:
    """Parse an A1 range WITHOUT a sheet name: ``A2:N``, ``A2:N10``, ``B3``, ``A:C``, ``2:10``.

    ``A2:N`` is open-ended downward — row 2 to the last row holding data,
    columns A..N. ``A2:N10`` is bounded. A single cell ``B3`` is a 1×1 range.
    Raises ``ValueError`` naming the input on anything else (a sheet prefix
    such as ``Orders!A1:C`` belongs in the ``tabs`` param, not here).
    """
    raw = text.strip()
    if not raw:
        raise ValueError("empty A1 range")
    if "!" in raw:
        raise ValueError(
            f"A1 range {text!r} includes a sheet name; give the tab in `tabs` "
            f"(e.g. tabs: \"Orders!A2:N\") or give only the cell range here"
        )
    parts = raw.split(":")
    if len(parts) > 2:
        raise ValueError(f"invalid A1 range {text!r}")
    start = _parse_cell(parts[0], text)
    end = _parse_cell(parts[1], text) if len(parts) == 2 else start
    start_col, start_row = start
    end_col, end_row = end
    if len(parts) == 1 and (start_col is None or start_row is None):
        raise ValueError(f"invalid A1 range {text!r}: a single cell needs a column and a row")
    if start_col is not None and end_col is not None and end_col < start_col:
        raise ValueError(f"invalid A1 range {text!r}: end column is before start column")
    if start_row is not None and end_row is not None and end_row < start_row:
        raise ValueError(f"invalid A1 range {text!r}: end row is before start row")
    if (start_col is None) != (end_col is None):
        raise ValueError(f"invalid A1 range {text!r}: give a column on both sides or neither")
    return GridRange(start_row=start_row, start_col=start_col, end_row=end_row, end_col=end_col)


def _parse_cell(part: str, whole: str) -> tuple[int | None, int | None]:
    match = _CELL_RE.match(part.strip())
    if match is None or (not match.group(1) and not match.group(2)):
        raise ValueError(f"invalid A1 range {whole!r}")
    col = column_index(match.group(1)) if match.group(1) else None
    row = int(match.group(2)) if match.group(2) else None
    if row is not None and row < 1:
        raise ValueError(f"invalid A1 range {whole!r}: rows start at 1")
    return col, row


def parse_a1_top_left(text: str) -> tuple[int, int]:
    """The (row, col) of the top-left cell of an API-returned range like ``'Tab'!C3:F10``."""
    cells = text.rsplit("!", 1)[-1]
    first = cells.split(":", 1)[0]
    col, row = _parse_cell(first, text)
    return (row or 1, col or 1)


def quote_sheet_title(title: str) -> str:
    """Quote a tab title for an A1 reference: ``Q1 'final'`` → ``'Q1 ''final'''``."""
    return "'" + title.replace("'", "''") + "'"


@dataclass(frozen=True)
class TabSelector:
    """One entry of the ``tabs`` param: a tab (by title or gid) plus an optional range."""

    title: str | None = None
    gid: int | None = None
    range: str | None = None

    def describe(self) -> str:
        """Human form for error messages."""
        who = f"gid={self.gid}" if self.gid is not None else repr(self.title)
        return f"{who}!{self.range}" if self.range else who


def parse_tab_selectors(text: str) -> list[TabSelector]:
    """Parse the ``tabs`` param: comma-separated ``<tab>[!<range>]`` entries.

    ``<tab>`` is a tab title (``Orders``), a single-quoted title for one that
    holds a comma or ``!`` (``'Q1, final'`` — double a quote inside it, as in
    Sheets formulas), or a gid (``gid=123``, as in a link's ``#gid=123``).
    ``<range>`` is A1 notation without a sheet name (``A2:N``, ``A2:N10``).

    >>> parse_tab_selectors("Orders!A2:N, 'Q1, final', gid=42")  # doctest: +SKIP
    """
    entries: list[TabSelector] = []
    for token in _split_outside_quotes(text, ","):
        token = token.strip()
        if not token:
            continue
        tab_part, range_part = _split_last_bang(token)
        rng = range_part.strip() or None
        if rng is not None:
            parse_a1(rng)  # validate now: a bad range fails before any API call
        tab_part = tab_part.strip()
        gid_match = _GID_RE.match(tab_part)
        if gid_match is not None:
            entries.append(TabSelector(gid=int(gid_match.group(1)), range=rng))
            continue
        if len(tab_part) >= 2 and tab_part.startswith("'") and tab_part.endswith("'"):
            tab_part = tab_part[1:-1].replace("''", "'")
        if not tab_part:
            raise ValueError(f"tabs entry {token!r} has an empty tab title")
        entries.append(TabSelector(title=tab_part, range=rng))
    return entries


def _split_outside_quotes(text: str, sep: str) -> list[str]:
    parts: list[str] = []
    buf: list[str] = []
    in_quote = False
    for ch in text:
        if ch == "'":
            in_quote = not in_quote
        if ch == sep and not in_quote:
            parts.append("".join(buf))
            buf = []
            continue
        buf.append(ch)
    if in_quote:
        raise ValueError(f"unbalanced single quote in {text!r}")
    parts.append("".join(buf))
    return parts


def _split_last_bang(token: str) -> tuple[str, str]:
    """Split ``<tab>!<range>`` at the last ``!`` outside single quotes."""
    in_quote = False
    cut = -1
    for i, ch in enumerate(token):
        if ch == "'":
            in_quote = not in_quote
        elif ch == "!" and not in_quote:
            cut = i
    if cut < 0:
        return token, ""
    return token[:cut], token[cut + 1 :]


def parse_list(text: str) -> list[str]:
    """Comma-separated list param → stripped, non-empty entries."""
    return [p.strip() for p in (text or "").split(",") if p.strip()]


def parse_spreadsheet_id(value: str) -> str:
    """A spreadsheet link or a bare spreadsheet ID → the ID.

    Accepts ``https://docs.google.com/spreadsheets/d/<id>/edit#gid=0`` (any
    suffix; the ``#gid`` is ignored — every tab is read unless ``tabs`` says
    otherwise), ``https://drive.google.com/open?id=<id>``, and the bare ID.
    """
    text = (value or "").strip()
    if not text:
        raise ValueError("google_sheets: `spreadsheet` is required (a link or an ID)")
    if "://" in text or text.startswith("docs.google.com") or text.startswith("drive.google.com"):
        parsed = urlparse(text if "://" in text else f"https://{text}")
        match = _SPREADSHEET_PATH_RE.search(parsed.path) or re.search(
            r"/file/d/([A-Za-z0-9_-]+)", parsed.path
        )
        if match is not None:
            return match.group(1)
        ids = parse_qs(parsed.query).get("id")
        if ids and _ID_RE.match(ids[0]):
            return ids[0]
        raise ValueError(f"could not find a spreadsheet ID in link {value!r}")
    if not _ID_RE.match(text):
        raise ValueError(f"{value!r} is neither a Google Sheets link nor a spreadsheet ID")
    return text


def parse_folder_id(value: str) -> str:
    """A Drive folder link or a bare folder ID → the ID.

    Accepts ``https://drive.google.com/drive/folders/<id>?usp=sharing``,
    ``https://drive.google.com/drive/u/1/folders/<id>``,
    ``https://drive.google.com/open?id=<id>`` and the bare ID.
    """
    text = (value or "").strip()
    if not text:
        raise ValueError("google_drive: `folder` is required (a link or a folder ID)")
    if "://" in text or text.startswith("drive.google.com"):
        parsed = urlparse(text if "://" in text else f"https://{text}")
        match = _FOLDER_PATH_RE.search(parsed.path)
        if match is not None:
            return match.group(1)
        ids = parse_qs(parsed.query).get("id")
        if ids and _ID_RE.match(ids[0]):
            return ids[0]
        raise ValueError(f"could not find a folder ID in link {value!r}")
    if not _ID_RE.match(text):
        raise ValueError(f"{value!r} is neither a Google Drive folder link nor a folder ID")
    return text
