# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""The google_sheets source — one stream per tab, discovered at run time.

``register.yaml`` declares a single stream *template*, ``tabs``, with
``discover: true``. :func:`discover_tabs` expands it each run: it reads the
spreadsheet's tab list (one ``spreadsheets.get``, properties only), applies the
optional ``tabs`` filter, and returns one :class:`~dtex.DiscoveredStream` per
tab. :func:`tabs` then reads one tab per call.

Stream names are the slugified tab titles (``"Q3 Orders"`` → ``q3_orders``),
computed over **every** tab of the spreadsheet, so a name does not change when
the ``tabs`` filter does. When two titles slugify to the same name
(``Orders`` and ``orders``), each of them gets its gid appended
(``orders_0``, ``orders_184927``) — a gid never changes for the life of a tab,
so neither stream can silently start reading the other tab.

Sheets have no change cursor, so every stream is a full refresh
(``write_disposition: replace``): each run lands the tab exactly as it is.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterator, Sequence
from typing import Any

from dtex import Batch, Config, DiscoveredStream, StreamDef, discover, stream
from dtex.sources.google_sheets import auth
from dtex.sources.google_sheets.a1 import (
    TabSelector,
    parse_a1,
    parse_list,
    parse_spreadsheet_id,
    parse_tab_selectors,
)
from dtex.sources.google_sheets.client import SheetsClient, SpreadsheetInfo, TabInfo
from dtex.sources.google_sheets.grid import batched, slugify, unique_name
from dtex.sources.google_sheets.reader import load_time_zone, read_tab

TEMPLATE = "tabs"


@discover(stream=TEMPLATE)
def discover_tabs(config: Config, log: Any) -> list[DiscoveredStream]:
    """One stream per (selected) tab of ``config.spreadsheet``."""
    spreadsheet_id = parse_spreadsheet_id(str(config.get("spreadsheet") or ""))
    selectors = parse_tab_selectors(str(config.get("tabs") or ""))
    range_param = str(config.get("range") or "").strip()
    if range_param:
        parse_a1(range_param)  # a typo fails here, before any tab is read
    prefix = str(config.get("table_prefix") or "")

    client = SheetsClient(auth.authorized_session(config, [auth.SHEETS_READONLY_SCOPE]))
    info = client.spreadsheet(spreadsheet_id)
    names = tab_stream_names(info.grid_tabs, log)
    found: list[DiscoveredStream] = []
    for tab, tab_range in select_tabs(info, selectors):
        name = names[tab.sheet_id]
        found.append(
            DiscoveredStream(
                name=name,
                table=f"{prefix}{name}",
                context={
                    "spreadsheet_id": spreadsheet_id,
                    "sheet_id": tab.sheet_id,
                    "title": tab.title,
                    "range": tab_range,
                    "row_count": tab.row_count,
                    "column_count": tab.column_count,
                    "time_zone": info.time_zone,
                },
            )
        )
    log.info(
        "google_sheets: spreadsheet %r has %d tab(s); reading %d",
        info.title,
        len(info.grid_tabs),
        len(found),
    )
    return found


@stream(name=TEMPLATE)
def tabs(config: Config, stream_def: StreamDef, log: Any) -> Iterator[Batch]:
    """Read one discovered tab — full refresh, header from its first row by default."""
    ctx = stream_def.context
    if not ctx:
        raise ValueError(
            "google_sheets: the `tabs` stream is a template; it runs once per tab "
            "found by discovery, never on its own"
        )
    tab = TabInfo(
        sheet_id=int(ctx["sheet_id"]),
        title=str(ctx["title"]),
        index=0,
        row_count=ctx.get("row_count"),
        column_count=ctx.get("column_count"),
    )
    # A range given with the tab in `tabs` ("Orders!A2:N") is tab-specific and
    # wins over the `range` param (connector-wide or per stream).
    range_text = ctx.get("range") or str(config.get("range") or "").strip() or None
    header_row = _header_row(config)
    columns = parse_list(str(config.get("columns") or ""))
    batch_size = max(1, int(config.get("batch_size", 1000)))

    client = SheetsClient(auth.authorized_session(config, [auth.SHEETS_READONLY_SCOPE]))
    records = read_tab(
        client,
        str(ctx["spreadsheet_id"]),
        tab,
        range_text=range_text,
        header_row=header_row,
        columns=columns,
        parse_dates=bool(config.get("parse_dates", True)),
        time_zone=load_time_zone(ctx.get("time_zone")),
    )
    rows = 0
    for batch in batched(records, batch_size):
        rows += len(batch)
        yield batch
    if rows == 0:
        # An empty snapshot is still a snapshot: the empty batch makes the
        # `replace` destination truncate, so a cleared tab clears its table
        # instead of keeping the last run's rows.
        log.info("google_sheets: tab %r has no data rows", tab.title)
        yield []


def _header_row(config: Config) -> int | None:
    value = config.get("header_row")
    if value is None or value == "":
        return None
    row = int(value)
    if row < 0:
        raise ValueError(f"header_row must be 0 (no header) or a sheet row number, got {row}")
    return row


def tab_stream_names(grid_tabs: Sequence[TabInfo], log: Any = None) -> dict[int, str]:
    """``sheet_id → stream name`` for every grid tab (see the module docstring)."""
    slugs = {t.sheet_id: slugify(t.title) or f"sheet_{t.sheet_id}" for t in grid_tabs}
    counts = Counter(slugs.values())
    used: set[str] = set()
    names: dict[int, str] = {}
    # Plain slugs first, so a collision suffix can never steal a plain name.
    for tab in grid_tabs:
        slug = slugs[tab.sheet_id]
        if counts[slug] == 1:
            names[tab.sheet_id] = unique_name(slug, used)
    for tab in grid_tabs:
        slug = slugs[tab.sheet_id]
        if counts[slug] > 1:
            names[tab.sheet_id] = unique_name(f"{slug}_{tab.sheet_id}", used)
            if log is not None:
                log.warning(
                    "google_sheets: tab %r shares the stream name %r with another tab; "
                    "streaming it as %r (its gid appended)",
                    tab.title,
                    slug,
                    names[tab.sheet_id],
                )
    return names


def select_tabs(
    info: SpreadsheetInfo, selectors: Sequence[TabSelector]
) -> list[tuple[TabInfo, str | None]]:
    """Apply the ``tabs`` filter: ``[(tab, range_or_None), …]``.

    No selectors → every grid tab, in tab order. A selector that matches no tab
    (a typo, a renamed tab) is a hard error listing the tabs that exist — a
    silently skipped tab is a table that silently stops updating.
    """
    grid = info.grid_tabs
    if not selectors:
        return [(t, None) for t in grid]
    by_gid = {t.sheet_id: t for t in info.tabs}
    chosen: list[tuple[TabInfo, str | None]] = []
    seen: set[int] = set()
    for sel in selectors:
        tab = _match(sel, info.tabs, by_gid)
        if tab is None:
            available = ", ".join(f"{t.title!r} (gid={t.sheet_id})" for t in grid) or "(none)"
            raise ValueError(
                f"google_sheets: tabs entry {sel.describe()} matches no tab in "
                f"spreadsheet {info.title!r}; tabs: {available}"
            )
        if tab.sheet_type != "GRID":
            raise ValueError(
                f"google_sheets: tab {tab.title!r} is a {tab.sheet_type} sheet "
                f"(no cells to read)"
            )
        if tab.sheet_id in seen:
            raise ValueError(f"google_sheets: tab {tab.title!r} is listed twice in `tabs`")
        seen.add(tab.sheet_id)
        chosen.append((tab, sel.range))
    return chosen


def _match(
    sel: TabSelector, tabs: Sequence[TabInfo], by_gid: dict[int, TabInfo]
) -> TabInfo | None:
    if sel.gid is not None:
        return by_gid.get(sel.gid)
    for tab in tabs:
        if tab.title == sel.title:
            return tab
    # Forgiving second pass: case / surrounding whitespace, if unambiguous.
    wanted = (sel.title or "").strip().casefold()
    loose = [t for t in tabs if t.title.strip().casefold() == wanted]
    return loose[0] if len(loose) == 1 else None
