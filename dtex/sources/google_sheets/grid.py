# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Cell grid → records: header detection, column naming, type unification.

Shared by every spreadsheet-shaped input dtex reads from Google — a Sheets
tab, an XLSX sheet, a CSV file in Drive — so the three produce the same column
names and the same row semantics. The caller hands in rows of already-decoded
cell values (``None`` for an empty cell) tagged with their 1-based sheet row
number, plus the 1-based column of each row's first cell.

The rules (documented for users in the connector READMEs):

* **Bounds.** An optional :class:`~.a1.GridRange` limits rows and columns.
  Without an end column, the columns run to the widest row.
* **Header row.** ``header_row=None`` (auto) takes the first non-empty row
  inside the bounds. ``header_row=N`` takes sheet row N — it may sit above the
  bounds (``range: A5:N`` with ``header_row: 1``); rows between the header and
  the data are skipped. ``header_row=0`` means there is no header row.
* **Column names.** ``columns`` (explicit names, left to right from the first
  column) win; otherwise the header cell's text. Every name is slugified to
  ``snake_case`` ASCII (``"Order ID"`` → ``order_id``, a leading digit gets a
  ``_`` prefix); a blank or unsluggable name becomes ``column_<n>`` with ``n``
  the sheet column number (C → ``column_3``); duplicates get ``_2``, ``_3``…
* **Rows.** A row with no value in any column is skipped. A named column is
  always present in the record (``None`` when empty); a column with a blank
  header appears only in rows where it holds a value, so a spacer column
  never becomes an all-NULL warehouse column.
* **Lineage.** Every record carries ``_dtex_row_number`` — the sheet row.
* **Types** (``unify_types=True``). A column's non-empty values settle on one
  type: all integers stay integers; integers mixed with decimals become
  floats; dates mixed with date-times become date-times; any other mix
  becomes text. This keeps a column's type stable across batches, which the
  engine's first-batch schema inference relies on.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time
from typing import Any

from dtex.sources.google_sheets.a1 import GridRange

ROW_NUMBER_FIELD = "_dtex_row_number"

_NON_WORD = re.compile(r"[^A-Za-z0-9]+")


def slugify(text: str) -> str:
    """``"Order ID (€)"`` → ``"order_id"``; ``"2025 Revenue"`` → ``"_2025_revenue"``.

    Accents fold to ASCII (``"Užsakymas"`` → ``"uzsakymas"``); a name with no
    ASCII letters or digits left slugifies to ``""`` and the caller falls back
    to a positional name.
    """
    folded = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    slug = _NON_WORD.sub("_", folded).strip("_").lower()
    if slug and slug[0].isdigit():
        slug = f"_{slug}"
    return slug


def unique_name(base: str, used: set[str]) -> str:
    """``base`` if unused, else ``base_2``, ``base_3``… — and record it as used."""
    name = base
    n = 2
    while name in used:
        name = f"{base}_{n}"
        n += 1
    used.add(name)
    return name


def to_text(value: Any) -> str:
    """Render a cell value as text — for header cells and mixed-type columns."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    return str(value)


@dataclass(frozen=True)
class GridOptions:
    """How to cut records out of a grid — see the module docstring."""

    bounds: GridRange | None = None
    header_row: int | None = None
    columns: Sequence[str] = field(default_factory=tuple)
    unify_types: bool = True


def records_from_rows(
    rows: Iterable[tuple[int, Sequence[Any]]],
    *,
    origin_col: int,
    options: GridOptions,
) -> Iterator[dict[str, Any]]:
    """Yield one record per data row. See the module docstring for the rules.

    ``rows`` yields ``(sheet_row_number, cells)`` in ascending row order;
    ``cells[0]`` sits in sheet column ``origin_col``. Rows may be ragged.
    With ``unify_types=False`` the rows are consumed lazily (a CSV of any
    size streams); with type unification the records are collected first —
    a column's type depends on all of its values.
    """
    records = _cut_records(rows, origin_col=origin_col, options=options)
    if not options.unify_types:
        yield from records
        return
    materialized = list(records)
    yield from unify_column_types(materialized)


def _cut_records(
    rows: Iterable[tuple[int, Sequence[Any]]],
    *,
    origin_col: int,
    options: GridOptions,
) -> Iterator[dict[str, Any]]:
    bounds = options.bounds or GridRange()
    first_col = bounds.first_col
    last_col = bounds.end_col
    header_row = options.header_row
    explicit = [slugify(c) or f"column_{first_col + i}" for i, c in enumerate(options.columns)]

    names: dict[int, str] = {}  # sheet column → column name (named columns only)
    used: set[str] = {ROW_NUMBER_FIELD}
    blank_names: dict[int, str] = {}  # sheet column → positional name, assigned lazily
    header_done = header_row == 0

    def assign_names(header_cells: dict[int, Any]) -> None:
        width = max([*header_cells, first_col + len(explicit) - 1, first_col - 1])
        if last_col is not None:
            width = min(width, last_col)
        for col in range(first_col, width + 1):
            i = col - first_col
            if i < len(explicit):
                names[col] = unique_name(explicit[i], used)
                continue
            text = to_text(header_cells.get(col)).strip()
            if text:
                names[col] = unique_name(slugify(text) or f"column_{col}", used)

    if header_done:
        assign_names({})

    for row_number, cells in rows:
        if bounds.end_row is not None and row_number > bounds.end_row:
            break
        values = _slice(cells, origin_col, first_col, last_col)
        if not header_done:
            is_header = (
                row_number == header_row
                if header_row is not None
                else bounds.contains_row(row_number) and bool(values)
            )
            if is_header:
                assign_names(values)
                header_done = True
                continue
            if header_row is not None and row_number < header_row:
                continue
            if header_row is None:
                continue  # auto: nothing before the first non-empty row is data
            # header_row is set but this row is past it without the header row
            # ever appearing (the API omits empty rows' trailing cells, never
            # the rows themselves — so the header row is simply empty).
            assign_names({})
            header_done = True
        if not bounds.contains_row(row_number) or not values:
            continue
        record: dict[str, Any] = {name: values.get(col) for col, name in names.items()}
        for col, value in values.items():
            if col in names:
                continue
            name = blank_names.get(col)
            if name is None:
                name = unique_name(f"column_{col}", used)
                blank_names[col] = name
            record[name] = value
        record[ROW_NUMBER_FIELD] = row_number
        yield record


def _slice(
    cells: Sequence[Any], origin_col: int, first_col: int, last_col: int | None
) -> dict[int, Any]:
    """Non-empty cells of one row inside the column bounds, keyed by sheet column."""
    out: dict[int, Any] = {}
    for i, value in enumerate(cells):
        col = origin_col + i
        if col < first_col:
            continue
        if last_col is not None and col > last_col:
            break
        if value is None or value == "":
            continue
        out[col] = value
    return out


# ---------------------------------------------------------------------------
# Column type unification
# ---------------------------------------------------------------------------


def _kind(value: Any) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, datetime):
        return "datetime"
    if isinstance(value, date):
        return "date"
    return "text"


def unify_column_types(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Give every column one type across all ``records`` (in place; returns them)."""
    kinds: dict[str, set[str]] = {}
    tz_by_col: dict[str, Any] = {}
    for record in records:
        for key, value in record.items():
            if value is None or key == ROW_NUMBER_FIELD:
                continue
            kind = _kind(value)
            kinds.setdefault(key, set()).add(kind)
            if kind == "datetime" and key not in tz_by_col:
                tz_by_col[key] = value.tzinfo
    for key, seen in kinds.items():
        if len(seen) <= 1:
            continue
        if seen <= {"int", "float"}:
            convert: Any = float
        elif seen <= {"date", "datetime"}:
            tz = tz_by_col.get(key)

            def convert(v: Any, tz: Any = tz) -> Any:
                if isinstance(v, datetime):
                    return v
                return datetime(v.year, v.month, v.day, tzinfo=tz)
        else:
            convert = to_text
        for record in records:
            value = record.get(key)
            if value is not None:
                record[key] = convert(value)
    return records


def batched(records: Iterable[dict[str, Any]], size: int) -> Iterator[list[dict[str, Any]]]:
    """Group records into lists of at most ``size`` (the connector's ``batch_size``)."""
    batch: list[dict[str, Any]] = []
    for record in records:
        batch.append(record)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch
