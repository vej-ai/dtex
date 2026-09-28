"""Tests for the baked ``google_sheets`` source — no network.

The Sheets API is served by :mod:`tests.connectors.google_fakes`; the
connector's session factory is monkeypatched to return it. Covered:

* link / ID parsing, A1 ranges (open ``A2:N`` and bounded ``A2:N10``), tab
  selectors (titles, quoted titles, gids);
* discovery: all tabs by default, the ``tabs`` filter, stable slugified names
  with gid-suffixed collisions, unknown-tab errors, ``table_prefix``;
* reading: header detection and naming (slugify, dedupe, blank → column_<n>),
  header_row / columns overrides, empty tabs, date / date-time / time typing
  from number formats, per-column type unification;
* HTTP: retry on 429 / Drive-style 403 rate limits, readable 404s;
* credentials: reference-only ``credentials_json``;
* end to end through ``dtex.run`` into DuckDB with ``streams: all`` and with
  per-tab stream params.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import duckdb
import pytest

import dtex
from dtex import Config, StreamDef
from dtex.sources.google_sheets import auth, http
from dtex.sources.google_sheets import source as gs
from dtex.sources.google_sheets.a1 import (
    GridRange,
    parse_a1,
    parse_spreadsheet_id,
    parse_tab_selectors,
)
from dtex.sources.google_sheets.grid import GridOptions, records_from_rows, slugify
from dtex.sources.google_sheets.reader import serial_to_value
from tests.connectors.google_fakes import (
    FakeGoogle,
    FakeResponse,
    FakeSpreadsheet,
    FakeTab,
    api_error,
)

SID = "1SpreadSheetId_abc-XYZ"


class _Log:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def _add(self, msg: str, *args: Any) -> None:
        self.messages.append(msg % args if args else msg)

    info = warning = debug = error = _add


@pytest.fixture
def google(monkeypatch: pytest.MonkeyPatch) -> FakeGoogle:
    fake = FakeGoogle()
    monkeypatch.setattr(auth, "authorized_session", lambda config, scopes: fake)
    monkeypatch.setattr(http, "sleep", lambda seconds: None)
    return fake


def _book(google: FakeGoogle, tabs: list[FakeTab]) -> FakeSpreadsheet:
    return google.add_spreadsheet(FakeSpreadsheet(SID, "Budget 2026", tabs))


def _discover(**params: Any) -> list[dtex.DiscoveredStream]:
    return gs.discover_tabs(Config(params={"spreadsheet": SID, **params}), _Log())


def _read(found: dtex.DiscoveredStream, **params: Any) -> list[dict[str, Any]]:
    stream_def = StreamDef(name=found.name, table=found.table or found.name, context=found.context)
    config = Config(params={"spreadsheet": SID, "batch_size": 2, **params})
    batches = list(gs.tabs(config, stream_def, _Log()))
    assert batches == [[]] or all(1 <= len(b) <= 2 for b in batches)
    return [r for b in batches for r in b]


def _strip(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{k: v for k, v in r.items() if k != "_dtex_row_number"} for r in records]


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        f"https://docs.google.com/spreadsheets/d/{SID}/edit#gid=0",
        f"https://docs.google.com/spreadsheets/d/{SID}/edit?usp=sharing",
        f"https://docs.google.com/spreadsheets/u/1/d/{SID}/view",
        f"docs.google.com/spreadsheets/d/{SID}",
        f"https://drive.google.com/open?id={SID}",
        SID,
        f"  {SID}  ",
    ],
)
def test_spreadsheet_link_or_id_parses_to_the_id(value: str) -> None:
    assert parse_spreadsheet_id(value) == SID


@pytest.mark.parametrize(
    "value",
    [
        "",
        "https://example.com/x",
        "not an id!",
        "https://docs.google.com/document/",
        f"https://evil.example/docs.google.com/spreadsheets/d/{SID}",
        f"https://evil.example/?next=drive.google.com&id={SID}",
        f"https://docs.google.com.evil.example/spreadsheets/d/{SID}",
        f"ftp://docs.google.com/spreadsheets/d/{SID}",
    ],
)
def test_spreadsheet_parse_rejects_garbage(value: str) -> None:
    with pytest.raises(ValueError):
        parse_spreadsheet_id(value)


def test_a1_open_and_bounded_ranges() -> None:
    assert parse_a1("A2:N") == GridRange(start_row=2, start_col=1, end_row=None, end_col=14)
    assert parse_a1("a2:n10") == GridRange(start_row=2, start_col=1, end_row=10, end_col=14)
    assert parse_a1("B3") == GridRange(3, 2, 3, 2)
    assert parse_a1("A:C") == GridRange(None, 1, None, 3)
    assert parse_a1("2:10") == GridRange(2, None, 10, None)
    # Rendered back for the API, clamped to the tab's grid.
    assert parse_a1("A2:N").to_a1() == "A2:N"
    assert parse_a1("A2:N").to_a1(max_rows=100, max_cols=5) == "A2:E"
    assert parse_a1("A2:N10").to_a1(max_rows=100, max_cols=26) == "A2:N10"
    assert parse_a1("A2:N500").to_a1(max_rows=100, max_cols=26) == "A2:N100"
    assert parse_a1("C200:D").to_a1(max_rows=100, max_cols=26) is None
    for bad in ["", "A2:", "N2:A3", "A10:A2", "Orders!A1:B2", "A0:B2", "A2:5"]:
        with pytest.raises(ValueError):
            parse_a1(bad)


def test_tab_selectors_titles_quotes_gids_and_ranges() -> None:
    sels = parse_tab_selectors("Orders!A2:N, 'Q1, final'!B2:C3, gid=42, #gid=7!A1:B, 'It''s'")
    assert [(s.title, s.gid, s.range) for s in sels] == [
        ("Orders", None, "A2:N"),
        ("Q1, final", None, "B2:C3"),
        (None, 42, None),
        (None, 7, "A1:B"),
        ("It's", None, None),
    ]
    assert parse_tab_selectors("") == []
    with pytest.raises(ValueError):
        parse_tab_selectors("Orders!ZZ")  # invalid range fails before any API call
    with pytest.raises(ValueError):
        parse_tab_selectors("'unterminated")


def test_slugify() -> None:
    assert slugify("Order ID") == "order_id"
    assert slugify("  Amount ($) ") == "amount"
    assert slugify("Užsakymo Nr.") == "uzsakymo_nr"
    assert slugify("2025 Revenue") == "_2025_revenue"
    assert slugify("€€€") == ""


# ---------------------------------------------------------------------------
# Grid rules (pure)
# ---------------------------------------------------------------------------


def _grid(rows: list[list[Any]], **opts: Any) -> list[dict[str, Any]]:
    return list(
        records_from_rows(
            ((i + 1, r) for i, r in enumerate(rows)), origin_col=1, options=GridOptions(**opts)
        )
    )


def test_header_names_are_slugified_deduped_and_blank_named_by_column() -> None:
    rows = [
        ["Order ID", "Amount", "", "amount", "Amount", "column_3"],
        ["A-1", 10, "note", 1, 2, "x"],
        ["A-2", 20, None, 3, 4, "y"],
    ]
    out = _grid(rows)
    assert list(out[0]) == [
        "order_id",
        "amount",
        "amount_2",
        "amount_3",
        "column_3",
        "column_3_2",
        "_dtex_row_number",
    ]
    assert out[0]["column_3_2"] == "note"
    # The blank-header column is present only where it holds a value.
    assert "column_3_2" not in out[1]
    assert out[1]["_dtex_row_number"] == 3


def test_auto_header_skips_leading_blank_rows_and_blank_rows_are_dropped() -> None:
    rows = [[], ["", ""], ["", "Name", "Qty"], ["", "a", 1], [], ["", "b", 2]]
    out = _grid(rows)
    assert _strip(out) == [{"name": "a", "qty": 1}, {"name": "b", "qty": 2}]
    assert [r["_dtex_row_number"] for r in out] == [4, 6]


def test_header_row_above_range_and_no_header_with_columns() -> None:
    rows = [["Title row"], ["id", "val"], ["x", "skip-me"], ["1", "a"], ["2", "b"]]
    out = _grid(rows, bounds=parse_a1("A4:B"), header_row=2)
    assert _strip(out) == [{"id": "1", "val": "a"}, {"id": "2", "val": "b"}]
    out = _grid(rows[3:], header_row=0, columns=("Key", "Value"))
    assert _strip(out) == [{"key": "1", "value": "a"}, {"key": "2", "value": "b"}]
    out = _grid([["1", "a", "extra"]], header_row=0)
    assert _strip(out) == [{"column_1": "1", "column_2": "a", "column_3": "extra"}]


def test_column_types_unify_across_the_whole_column() -> None:
    rows = [
        ["n", "mixed", "when", "flag"],
        [1, 1, date(2026, 1, 2), True],
        [2.5, "x", datetime(2026, 1, 3, 12, 0), False],
    ]
    out = _grid(rows)
    assert [r["n"] for r in out] == [1.0, 2.5]
    assert [r["mixed"] for r in out] == ["1", "x"]
    assert out[0]["when"] == datetime(2026, 1, 2, 0, 0)
    assert [r["flag"] for r in out] == [True, False]


def test_serial_conversion() -> None:
    tz = ZoneInfo("Europe/Vilnius")
    assert serial_to_value(46023, "DATE", tz) == date(2026, 1, 1)
    assert serial_to_value(46023.75, "DATE", tz) == date(2026, 1, 1)  # hidden time dropped
    assert serial_to_value(46023.5, "DATE_TIME", tz) == datetime(2026, 1, 1, 12, 0, tzinfo=tz)
    assert serial_to_value(0.5, "TIME", None) == "12:00:00"
    assert serial_to_value(1.5, "TIME", None) == "36:00:00"  # a duration
    assert serial_to_value(1, "DATE", None) == date(1899, 12, 31)


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def _three_tabs() -> list[FakeTab]:
    return [
        FakeTab(0, "Orders", [["Order ID", "Amount"], ["A-1", 10]]),
        FakeTab(11, "Q3 Refunds", [["id"], [1]]),
        FakeTab(22, "Chart 1", sheet_type="OBJECT"),
        FakeTab(33, "Empty"),
    ]


def test_discovery_defaults_to_every_grid_tab(google: FakeGoogle) -> None:
    _book(google, _three_tabs())
    found = _discover()
    assert [(f.name, f.table) for f in found] == [
        ("orders", "orders"),
        ("q3_refunds", "q3_refunds"),
        ("empty", "empty"),
    ]
    assert found[1].context["title"] == "Q3 Refunds"
    assert found[1].context["sheet_id"] == 11
    assert found[1].context["time_zone"] == "Europe/Vilnius"
    # Only tab properties were requested — no cell data.
    (meta,) = google.requests_to(f"/spreadsheets/{SID}")
    assert "gridProperties" in meta["fields"] and "rowData" not in meta["fields"]


def test_discovery_tab_filter_by_title_gid_and_range(google: FakeGoogle) -> None:
    _book(google, _three_tabs())
    found = _discover(tabs="gid=11!A1:A5, orders!A2:N", table_prefix="gs_")
    assert [(f.name, f.table, f.context["range"]) for f in found] == [
        ("q3_refunds", "gs_q3_refunds", "A1:A5"),
        ("orders", "gs_orders", "A2:N"),  # case-insensitive fallback match
    ]


def test_discovery_unknown_tab_is_a_hard_error(google: FakeGoogle) -> None:
    _book(google, _three_tabs())
    with pytest.raises(ValueError, match="matches no tab.*'Orders' \\(gid=0\\)"):
        _discover(tabs="Ordres")
    with pytest.raises(ValueError, match="OBJECT sheet"):
        _discover(tabs="Chart 1")
    with pytest.raises(ValueError, match="listed twice"):
        _discover(tabs="Orders, gid=0")


def test_colliding_tab_names_get_their_gid_and_stay_stable(google: FakeGoogle) -> None:
    _book(
        google,
        [FakeTab(0, "Orders"), FakeTab(5, "orders"), FakeTab(9, "Orders 5"), FakeTab(7, "Misc")],
    )
    names = [f.name for f in _discover()]
    assert names == ["orders_0", "orders_5_2", "orders_5", "misc"]
    # A filter does not change a tab's stream name.
    assert [f.name for f in _discover(tabs="orders")] == ["orders_5_2"]


# ---------------------------------------------------------------------------
# Reading tabs
# ---------------------------------------------------------------------------


def _orders_tab() -> FakeTab:
    cells: list[list[Any]] = [
        ["Order ID", "Order Date", "Amount", "Amount", "", "Paid"],
        ["A-1", 46023, 10, 1.5, "", True],
        ["A-2", 46024, 20, 2, "note", False],
        [],
        ["A-3", 46025.25, 30, 3, "", True],
    ]
    formats = {(2, 2): "DATE", (3, 2): "DATE", (5, 2): "DATE"}
    return FakeTab(0, "Orders", cells, formats, row_count=100, column_count=10)


def test_whole_tab_read_types_dates_and_names_columns(google: FakeGoogle) -> None:
    _book(google, [_orders_tab()])
    (found,) = _discover()
    out = _read(found)
    assert _strip(out) == [
        {
            "order_id": "A-1",
            "order_date": date(2026, 1, 1),
            "amount": 10,
            "amount_2": 1.5,
            "paid": True,
        },
        {
            "order_id": "A-2",
            "order_date": date(2026, 1, 2),
            "amount": 20,
            "amount_2": 2.0,
            "paid": False,
            "column_5": "note",
        },
        {
            "order_id": "A-3",
            "order_date": date(2026, 1, 3),
            "amount": 30,
            "amount_2": 3.0,
            "paid": True,
        },
    ]
    assert [r["_dtex_row_number"] for r in out] == [2, 3, 5]
    (values,) = google.requests_to("values:batchGet")
    assert values["ranges"] == "'Orders'"
    assert values["valueRenderOption"] == "UNFORMATTED_VALUE"
    assert values["dateTimeRenderOption"] == "SERIAL_NUMBER"


def test_parse_dates_off_keeps_serials_and_skips_the_format_read(google: FakeGoogle) -> None:
    _book(google, [_orders_tab()])
    (found,) = _discover()
    out = _read(found, parse_dates=False)
    assert out[0]["order_date"] == 46023
    assert not [
        p for p in google.requests_to(f"/spreadsheets/{SID}") if "fields" in p and "ranges" in p
    ]


def test_date_time_cells_are_aware_in_the_spreadsheet_zone(google: FakeGoogle) -> None:
    tab = FakeTab(
        0, "Log", [["At", "Took"], [46023.5, 0.25]], {(2, 1): "DATE_TIME", (2, 2): "TIME"}
    )
    _book(google, [tab])
    (found,) = _discover()
    (row,) = _read(found)
    assert row["at"] == datetime(2026, 1, 1, 12, 0, tzinfo=ZoneInfo("Europe/Vilnius"))
    assert row["took"] == "06:00:00"


def test_open_range_reads_from_row_to_the_last_row(google: FakeGoogle) -> None:
    rows = [["ignored title"], ["id", "name", "x"]] + [[i, f"n{i}", "cut"] for i in range(1, 6)]
    _book(google, [FakeTab(0, "Data", rows, column_count=3)])
    (found,) = _discover(tabs="Data!A2:B")
    out = _read(found)
    assert _strip(out) == [{"id": i, "name": f"n{i}"} for i in range(1, 6)]
    (values,) = google.requests_to("values:batchGet")
    assert values["ranges"] == "'Data'!A2:B"


def test_bounded_range_and_the_range_param(google: FakeGoogle) -> None:
    rows = [["id", "name"]] + [[i, f"n{i}"] for i in range(1, 20)]
    _book(google, [FakeTab(0, "Data", rows)])
    (found,) = _discover()
    out = _read(found, range="A1:B4")
    assert [r["id"] for r in out] == [1, 2, 3]
    (values,) = google.requests_to("values:batchGet")
    assert values["ranges"] == "'Data'!A1:B4"


def test_range_below_the_header_with_explicit_header_row(google: FakeGoogle) -> None:
    rows = [["id", "name"], ["skip", "me"], [1, "a"], [2, "b"]]
    _book(google, [FakeTab(0, "Data", rows)])
    (found,) = _discover(tabs="Data!A3:B10")
    out = _read(found, header_row=1)
    assert _strip(out) == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    (values,) = google.requests_to("values:batchGet")
    assert values["ranges"] == "'Data'!A1:B10"  # grown upward to include the header


def test_no_header_with_explicit_columns(google: FakeGoogle) -> None:
    _book(google, [FakeTab(0, "Raw", [[1, "a"], [2, "b"]])])
    (found,) = _discover()
    out = _read(found, header_row=0, columns="Key, Value")
    assert _strip(out) == [{"key": 1, "value": "a"}, {"key": 2, "value": "b"}]


def test_empty_tab_and_header_only_tab_yield_nothing(google: FakeGoogle) -> None:
    _book(google, [FakeTab(0, "Empty"), FakeTab(1, "Header", [["a", "b"]])])
    empty, header_only = _discover()
    assert _read(empty) == []
    assert _read(header_only) == []


def test_range_past_the_grid_reads_nothing(google: FakeGoogle) -> None:
    _book(google, [FakeTab(0, "Small", [["a"], [1]], row_count=5, column_count=2)])
    (found,) = _discover(tabs="Small!D1:E")
    assert _read(found) == []
    assert google.requests_to("values:batchGet") == []


def test_template_stream_refuses_to_run_without_discovery() -> None:
    with pytest.raises(ValueError, match="template"):
        list(gs.tabs(Config(params={}), StreamDef(name="tabs", table="tabs"), _Log()))


# ---------------------------------------------------------------------------
# HTTP + credentials
# ---------------------------------------------------------------------------


def test_transient_errors_are_retried(google: FakeGoogle) -> None:
    _book(google, _three_tabs())
    google.injected = [
        api_error(429),
        api_error(503),
        api_error(403, "Rate Limit Exceeded", reason="userRateLimitExceeded"),
    ]
    assert len(_discover()) == 3
    assert len(google.requests) == 4


def test_not_found_explains_sharing(google: FakeGoogle) -> None:
    with pytest.raises(http.GoogleApiError, match="not shared with the credentials"):
        _discover()


def test_permission_errors_are_not_retried(google: FakeGoogle) -> None:
    google.injected = [api_error(403, "The caller does not have permission")]
    with pytest.raises(http.GoogleApiError, match="HTTP 403.*caller does not have permission"):
        _discover()
    assert len(google.requests) == 1


def test_retry_after_is_honoured(monkeypatch: pytest.MonkeyPatch) -> None:
    waits: list[float] = []
    monkeypatch.setattr(http, "sleep", waits.append)

    class _Session:
        def __init__(self) -> None:
            self.answers = [
                FakeResponse(429, {}, headers={"Retry-After": "3"}),
                FakeResponse(200, {"ok": 1}),
            ]

        def request(self, *a: Any, **k: Any) -> FakeResponse:
            return self.answers.pop(0)

    assert http.request_json(_Session(), "https://x", None, what="t") == {"ok": 1}
    assert waits == [3.0]


def test_credentials_json_must_be_a_reference(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValueError, match="must be a reference"):
        auth.resolve_credentials_reference('{"type": "service_account"}')
    monkeypatch.delenv("GS_KEY", raising=False)
    with pytest.raises(ValueError, match="GS_KEY"):
        auth.resolve_credentials_reference("${env.GS_KEY}")
    monkeypatch.setenv("GS_KEY", "not json -----BEGIN PRIVATE KEY-----")
    with pytest.raises(ValueError, match="valid JSON") as info:
        auth.load_credentials(Config(params={"credentials_json": "${env.GS_KEY}"}), ["s"])
    assert "PRIVATE KEY" not in str(info.value)


def test_auth_type_conflicts_are_explained() -> None:
    with pytest.raises(ValueError, match="auth_type is 'oauth'"):
        auth.load_credentials(
            Config(params={"auth_type": "oauth", "credentials_path": "k.json"}), []
        )
    with pytest.raises(ValueError, match="needs credentials_path"):
        auth.load_credentials(Config(params={"auth_type": "service_account"}), [])
    with pytest.raises(ValueError, match="auth_type must be"):
        auth.load_credentials(Config(params={"auth_type": "magic"}), [])


# ---------------------------------------------------------------------------
# End to end — dtex.run into DuckDB
# ---------------------------------------------------------------------------


def _project(tmp_path: Path, streams: str, params: str = "") -> None:
    (tmp_path / "dtex_project.yml").write_text(
        "name: t\nversion: '0.1'\nsource_paths: []\n"
        "destination_paths: []\nconfig_paths:\n  - configs\n"
    )
    (tmp_path / "profiles.yml").write_text(
        "duckdb:\n  default_target: dev\n  targets:\n    dev:\n"
        "      path: '.dtex/warehouse.duckdb'\n"
    )
    (tmp_path / "configs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "configs" / "gs.yml").write_text(
        "name: gs\nsource: google_sheets\ndestination: duckdb\ntarget: dev\n"
        f"params:\n  spreadsheet: 'https://docs.google.com/spreadsheets/d/{SID}/edit#gid=0'\n"
        f"{params}"
        f"streams:{streams}\n"
    )


def test_end_to_end_every_tab_lands_as_its_own_table(google: FakeGoogle, tmp_path: Path) -> None:
    _book(
        google,
        [
            _orders_tab(),
            FakeTab(11, "Q3 Refunds", [["Refund ID"], ["R-1"], ["R-2"]]),
            FakeTab(33, "Empty"),
        ],
    )
    _project(tmp_path, " all", params="  table_prefix: 'budget_'\n")
    db = str(tmp_path / "w.duckdb")
    result = dtex.run(
        config="gs", project_dir=str(tmp_path), destination_params_override={"path": db}
    )
    assert result.status.value == "succeeded", result.error
    assert [s.name for s in result.streams] == ["orders", "q3_refunds", "empty"]
    conn = duckdb.connect(db)
    orders = conn.execute(
        "SELECT order_id, order_date, amount, amount_2, paid, _dtex_row_number "
        "FROM budget_orders ORDER BY _dtex_row_number"
    ).fetchall()
    refunds = conn.execute("SELECT refund_id FROM budget_q3_refunds ORDER BY 1").fetchall()
    tables = {
        r[0] for r in conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
    }
    conn.close()
    assert orders[0] == ("A-1", date(2026, 1, 1), 10, 1.5, True, 2)
    assert len(orders) == 3
    assert refunds == [("R-1",), ("R-2",)]
    assert "budget_q3_refunds" in tables

    # A second run replaces (full refresh), it does not append; a cleared tab
    # clears its table.
    google.spreadsheets[SID].tabs[1].cells.append(["R-3"])
    google.spreadsheets[SID].tabs[0].cells = [["Order ID"]]
    result = dtex.run(
        config="gs", project_dir=str(tmp_path), destination_params_override={"path": db}
    )
    assert result.status.value == "succeeded", result.error
    conn = duckdb.connect(db)
    assert conn.execute("SELECT count(*) FROM budget_q3_refunds").fetchone() == (3,)
    assert conn.execute("SELECT count(*) FROM budget_orders").fetchone() == (0,)
    conn.close()


def test_end_to_end_selected_tabs_with_per_tab_params(google: FakeGoogle, tmp_path: Path) -> None:
    rows = [["id", "name"]] + [[i, f"n{i}"] for i in range(1, 20)]
    _book(google, [FakeTab(0, "Data", rows), FakeTab(1, "Other", [["x"], [1]])])
    _project(tmp_path, "\n  data:\n    params:\n      range: 'A1:B6'\n")
    db = str(tmp_path / "w.duckdb")
    result = dtex.run(
        config="gs", project_dir=str(tmp_path), destination_params_override={"path": db}
    )
    assert result.status.value == "succeeded", result.error
    ran = {s.name: s.status.value for s in result.streams}
    assert ran == {"data": "succeeded", "other": "skipped"}
    conn = duckdb.connect(db)
    assert conn.execute("SELECT count(*), max(id) FROM data").fetchone() == (5, 5)
    conn.close()


def test_end_to_end_unknown_tab_stream_lists_the_discovered_ones(
    google: FakeGoogle, tmp_path: Path
) -> None:
    _book(google, [FakeTab(0, "Data", [["a"], [1]])])
    _project(tmp_path, "\n  dta:\n")
    result = dtex.run(
        config="gs",
        project_dir=str(tmp_path),
        destination_params_override={"path": str(tmp_path / "w.duckdb")},
    )
    assert result.status.value == "failed"
    assert "'dta'" in str(result.error) and "'data'" in str(result.error)
