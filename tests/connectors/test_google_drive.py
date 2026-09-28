"""Tests for the baked ``google_drive`` source — no network.

Drive (and Sheets, for native Google Sheets files) is served by
:mod:`tests.connectors.google_fakes`. XLSX fixtures are built in-test with
openpyxl. Covered: folder link parsing, listing (pagination, sub-folders,
shared drives, shortcuts, trash), glob + format detection, CSV / XLSX /
Google Sheets reading with sheet + range + header selection, lineage columns,
and the ``<modifiedTime>|<fileId>`` cursor across runs — unit-level and end
to end through ``dtex.run`` into DuckDB.
"""

from __future__ import annotations

import io
from datetime import date, datetime, time
from pathlib import Path
from typing import Any

import duckdb
import openpyxl
import pytest

import dtex
from dtex import Config, Cursor, CursorType
from dtex.sources.google_drive import source as gd
from dtex.sources.google_drive.client import normalize_modified_time
from dtex.sources.google_sheets import auth, http
from dtex.sources.google_sheets.a1 import parse_folder_id
from tests.connectors.google_fakes import (
    FOLDER_MIME,
    SHEET_MIME,
    FakeDriveItem,
    FakeGoogle,
    FakeSpreadsheet,
    FakeTab,
)

FOLDER = "0FolderRoot"
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


class _Log:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def _add(self, msg: str, *args: Any) -> None:
        self.messages.append(msg % args if args else msg)

    info = warning = debug = error = _add


@pytest.fixture
def google(monkeypatch: pytest.MonkeyPatch) -> FakeGoogle:
    fake = FakeGoogle(page_size=2)
    monkeypatch.setattr(auth, "authorized_session", lambda config, scopes: fake)
    monkeypatch.setattr(http, "sleep", lambda seconds: None)
    fake.add_item(FakeDriveItem(FOLDER, "Exports", FOLDER_MIME, parents=[]))
    return fake


def _csv(
    google: FakeGoogle, fid: str, name: str, text: str, mtime: str, parent: str = FOLDER
) -> None:
    google.add_item(
        FakeDriveItem(fid, name, "text/csv", [parent], modified_time=mtime, content=text.encode())
    )


def _xlsx_bytes(
    sheets: dict[str, list[list[Any]]], formats: dict[tuple[str, str], str] | None = None
) -> bytes:
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    for title, rows in sheets.items():
        ws = wb.create_sheet(title)
        for row in rows:
            ws.append(row)
    for (title, cell), fmt in (formats or {}).items():
        wb[title][cell].number_format = fmt
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _run(cursor_start: str | None = "", **params: Any) -> tuple[list[dict[str, Any]], Cursor, _Log]:
    config = Config(params={"folder": FOLDER, "batch_size": 2, **params})
    cursor = Cursor("_dtex_file_cursor", CursorType.STRING, start_value=cursor_start)
    log = _Log()
    batches = list(gd.files(config, cursor, log))
    assert all(1 <= len(b) <= 2 for b in batches)
    return [r for b in batches for r in b], cursor, log


def _data(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{k: v for k, v in r.items() if not k.startswith("_dtex_")} for r in records]


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        f"https://drive.google.com/drive/folders/{FOLDER}?usp=sharing",
        f"https://drive.google.com/drive/u/1/folders/{FOLDER}",
        f"drive.google.com/drive/folders/{FOLDER}",
        f"https://drive.google.com/open?id={FOLDER}",
        FOLDER,
    ],
)
def test_folder_link_or_id_parses_to_the_id(value: str) -> None:
    assert parse_folder_id(value) == FOLDER


def test_modified_time_normalizes_to_fixed_width_utc() -> None:
    assert normalize_modified_time("2026-09-29T10:00:00Z") == "2026-09-29T10:00:00.000Z"
    assert normalize_modified_time("2026-09-29T10:00:00.5Z") == "2026-09-29T10:00:00.500Z"
    assert normalize_modified_time("2026-09-29T13:00:00.123+03:00") == "2026-09-29T10:00:00.123Z"


# ---------------------------------------------------------------------------
# Listing
# ---------------------------------------------------------------------------


def test_listing_paginates_filters_and_skips_what_it_cannot_read(google: FakeGoogle) -> None:
    for i in range(5):
        _csv(google, f"f{i}", f"orders_{i}.csv", "id\n1\n", f"2026-09-0{i + 1}T00:00:00.000Z")
    google.add_item(
        FakeDriveItem("pdf", "readme.pdf", "application/pdf", [FOLDER], content=b"%PDF")
    )
    google.add_item(FakeDriveItem("sc", "link", "application/vnd.google-apps.shortcut", [FOLDER]))
    google.add_item(
        FakeDriveItem("tr", "trashed.csv", "text/csv", [FOLDER], trashed=True, content=b"id\n9\n")
    )
    google.add_item(FakeDriveItem("sub", "2026", FOLDER_MIME, [FOLDER]))
    _csv(google, "deep", "orders_deep.csv", "id\n7\n", "2026-09-09T00:00:00.000Z", parent="sub")

    records, _, log = _run()
    assert sorted(r["_dtex_file_path"] for r in records) == [f"orders_{i}.csv" for i in range(5)]
    assert any("readme.pdf" in m for m in log.messages)
    lists = google.requests_to("drive/v3/files?") or [
        p for u, p in [(u, dict(p)) for u, p in google.requests] if u.endswith("/files")
    ]
    listing = [p for p in lists if "q" in p]
    assert len(listing) >= 4  # 8 children / 2 per page → paginated
    assert all(
        p["supportsAllDrives"] == "true" and p["includeItemsFromAllDrives"] == "true"
        for p in listing
    )
    assert all("corpora" not in p for p in listing)

    records, _, _ = _run(recursive=True, glob="orders_*.csv")
    paths = sorted(r["_dtex_file_path"] for r in records)
    assert "2026/orders_deep.csv" in paths and len(paths) == 6


def test_shared_drive_folders_list_with_the_drive_corpus(google: FakeGoogle) -> None:
    google.items[FOLDER].drive_id = "0SharedDrive"
    _csv(google, "a", "a.csv", "id\n1\n", "2026-09-01T00:00:00.000Z")
    records, _, _ = _run()
    assert len(records) == 1
    listing = [dict(p) for u, p in google.requests if u.endswith("/files")]
    assert listing and all(
        p["corpora"] == "drive" and p["driveId"] == "0SharedDrive" for p in listing
    )


def test_folder_must_be_a_folder(google: FakeGoogle) -> None:
    _csv(google, "a", "a.csv", "id\n1\n", "2026-09-01T00:00:00.000Z")
    with pytest.raises(ValueError, match="not a folder"):
        _run(folder="a")


# ---------------------------------------------------------------------------
# Cursor
# ---------------------------------------------------------------------------


def test_cursor_loads_only_new_or_changed_files(google: FakeGoogle) -> None:
    _csv(google, "b", "b.csv", "id\n2\n", "2026-09-02T00:00:00.000Z")
    _csv(google, "a", "a.csv", "id\n1\n", "2026-09-01T00:00:00.000Z")
    records, cursor, _ = _run()
    assert [r["_dtex_file_path"] for r in records] == ["a.csv", "b.csv"]  # change order
    first = cursor.observed_max
    assert first == "2026-09-02T00:00:00.000Z|b"
    assert records[0]["_dtex_file_cursor"] == "2026-09-01T00:00:00.000Z|a"
    assert records[0]["_dtex_file_id"] == "a"

    # Nothing new: no rows, and the cursor stays where it was.
    records, cursor, _ = _run(cursor_start=first)
    assert records == [] and cursor.observed_max == first

    # A new file at the SAME modifiedTime with a later id, plus an edit to a.csv.
    _csv(google, "c", "c.csv", "id\n3\n", "2026-09-02T00:00:00.000Z")
    google.items["a"].modified_time = "2026-09-03T00:00:00.000Z"
    google.items["a"].content = b"id\n1\n11\n"
    records, cursor, _ = _run(cursor_start=first)
    assert [(r["_dtex_file_path"], r["id"]) for r in records] == [
        ("c.csv", "3"),
        ("a.csv", "1"),
        ("a.csv", "11"),
    ]
    assert cursor.observed_max == "2026-09-03T00:00:00.000Z|a"


def test_full_refresh_reads_everything() -> None:
    cursor = Cursor("_dtex_file_cursor", CursorType.STRING, start_value="x", is_full_refresh=True)
    assert cursor.start_value() is None  # extract_files then applies no filter


# ---------------------------------------------------------------------------
# Formats
# ---------------------------------------------------------------------------


def test_csv_values_stay_text_with_bom_tsv_and_header_rules(google: FakeGoogle) -> None:
    _csv(
        google,
        "a",
        "a.csv",
        "﻿Order ID,Amount,Amount\nA-1,10,x\n,,\nA-2,20,y\n",
        "2026-09-01T00:00:00.000Z",
    )
    google.add_item(
        FakeDriveItem(
            "t",
            "t.tsv",
            "text/tab-separated-values",
            [FOLDER],
            modified_time="2026-09-02T00:00:00.000Z",
            content=b"k\tv\n1\t2\n",
        )
    )
    records, _, _ = _run()
    assert _data(records) == [
        {"order_id": "A-1", "amount": "10", "amount_2": "x"},
        {"order_id": "A-2", "amount": "20", "amount_2": "y"},
        {"k": "1", "v": "2"},
    ]
    assert [r["_dtex_row_number"] for r in records] == [2, 4, 2]


def test_csv_range_and_header_row(google: FakeGoogle) -> None:
    _csv(
        google,
        "a",
        "a.csv",
        "report,,\nid,name,junk\n1,a,z\n2,b,z\n3,c,z\n",
        "2026-09-01T00:00:00.000Z",
    )
    records, _, _ = _run(range="A3:B4", header_row=2)
    assert _data(records) == [{"id": "1", "name": "a"}, {"id": "2", "name": "b"}]


def test_malformed_csv_names_the_file(google: FakeGoogle) -> None:
    _csv(google, "a", "bad.csv", 'id,name\n1,"unterminated\n2,x"y\n', "2026-09-01T00:00:00.000Z")
    with pytest.raises(ValueError, match="bad.csv"):
        _run()


def test_xlsx_sheet_by_name_or_position_with_range(google: FakeGoogle) -> None:
    content = _xlsx_bytes(
        {
            "Cover": [["Quarterly export"]],
            "Data": [
                ["Generated 2026-09-01"],
                [],
                ["Order ID", "Order Date", "Amount", "At", "Took"],
                ["A-1", datetime(2026, 1, 2), 10, datetime(2026, 1, 2, 13, 30), time(1, 30)],
                ["A-2", datetime(2026, 1, 3), 20.5, datetime(2026, 1, 3, 0, 0), time(2, 0)],
                ["A-3", datetime(2026, 1, 4), 30, datetime(2026, 1, 4, 9, 0), time(3, 0)],
            ],
        },
        formats={
            ("Data", "B4"): "yyyy-mm-dd",
            ("Data", "B5"): "dd/mm/yyyy",
            ("Data", "B6"): "yyyy-mm-dd",
            ("Data", "D4"): "yyyy-mm-dd hh:mm",
            ("Data", "D5"): "yyyy-mm-dd hh:mm",
            ("Data", "D6"): "yyyy-mm-dd hh:mm",
        },
    )
    google.add_item(
        FakeDriveItem("x", "orders.xlsx", XLSX_MIME, [FOLDER], "2026-09-01T00:00:00.000Z", content)
    )

    # The auto header would be the "Generated …" banner; header_row names row 3.
    records, _, _ = _run(sheet="Data", header_row=3)
    assert _data(records) == [
        {
            "order_id": "A-1",
            "order_date": date(2026, 1, 2),
            "amount": 10.0,
            "at": datetime(2026, 1, 2, 13, 30),
            "took": "01:30:00",
        },
        {
            "order_id": "A-2",
            "order_date": date(2026, 1, 3),
            "amount": 20.5,
            "at": datetime(2026, 1, 3, 0, 0),
            "took": "02:00:00",
        },
        {
            "order_id": "A-3",
            "order_date": date(2026, 1, 4),
            "amount": 30.0,
            "at": datetime(2026, 1, 4, 9, 0),
            "took": "03:00:00",
        },
    ]
    assert [r["_dtex_row_number"] for r in records] == [4, 5, 6]

    # Position 2, a bounded range (two data rows, three columns), header above it.
    records, _, _ = _run(sheet="2", range="A4:C5", header_row=3)
    assert _data(records) == [
        {"order_id": "A-1", "order_date": date(2026, 1, 2), "amount": 10.0},
        {"order_id": "A-2", "order_date": date(2026, 1, 3), "amount": 20.5},
    ]

    # Default = the first sheet.
    records, _, _ = _run()
    assert records == []  # "Quarterly export" is the header; no data rows

    with pytest.raises(ValueError, match="no sheet 'Nope'.*'Cover', 'Data'"):
        _run(sheet="Nope")


def test_xlsx_open_range_and_no_header(google: FakeGoogle) -> None:
    rows: list[list[Any]] = [[i, f"n{i}", "cut"] for i in range(1, 6)]
    content = _xlsx_bytes({"S": rows})
    google.add_item(
        FakeDriveItem("x", "r.xlsx", XLSX_MIME, [FOLDER], "2026-09-01T00:00:00.000Z", content)
    )
    records, _, _ = _run(range="A2:B", header_row=0, columns="id,name")
    assert _data(records) == [{"id": i, "name": f"n{i}"} for i in range(2, 6)]


def test_native_google_sheet_in_the_folder(google: FakeGoogle) -> None:
    google.add_item(
        FakeDriveItem("gs1", "Budget", SHEET_MIME, [FOLDER], "2026-09-01T00:00:00.000Z")
    )
    google.add_spreadsheet(
        FakeSpreadsheet(
            "gs1",
            "Budget",
            [
                FakeTab(0, "Intro", [["hello"]]),
                FakeTab(1, "Lines", [["Line", "When"], ["rent", 46023]], {(2, 2): "DATE"}),
            ],
        )
    )
    records, _, _ = _run(sheet="Lines")
    assert _data(records) == [{"line": "rent", "when": date(2026, 1, 1)}]
    assert records[0]["_dtex_file_id"] == "gs1"
    assert not [u for u, p in google.requests if dict(p).get("alt") == "media"]


def test_forced_format_and_mixed_union(google: FakeGoogle) -> None:
    google.add_item(
        FakeDriveItem(
            "a", "a.txt", "text/plain", [FOLDER], "2026-09-01T00:00:00.000Z", b"id,name\n1,a\n"
        )
    )
    records, _, log = _run()
    assert records == [] and any("a.txt" in m for m in log.messages)
    records, _, _ = _run(format="csv")
    assert _data(records) == [{"id": "1", "name": "a"}]
    with pytest.raises(ValueError, match="format must be"):
        _run(format="parquet")


# ---------------------------------------------------------------------------
# End to end — dtex.run into DuckDB, incremental across runs
# ---------------------------------------------------------------------------


def _project(tmp_path: Path, params: str = "") -> None:
    (tmp_path / "dtex_project.yml").write_text(
        "name: t\nversion: '0.1'\nsource_paths: []\n"
        "destination_paths: []\nconfig_paths:\n  - configs\n"
    )
    (tmp_path / "profiles.yml").write_text(
        "duckdb:\n  default_target: dev\n  targets:\n    dev:\n"
        "      path: '.dtex/warehouse.duckdb'\n"
    )
    (tmp_path / "configs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "configs" / "gd.yml").write_text(
        "name: gd\nsource: google_drive\ndestination: duckdb\ntarget: dev\n"
        f"params:\n  folder: 'https://drive.google.com/drive/folders/{FOLDER}'\n{params}"
        "streams:\n  files:\n"
    )


def test_end_to_end_incremental_csv_and_xlsx_union(google: FakeGoogle, tmp_path: Path) -> None:
    _csv(google, "a", "orders_1.csv", "Order ID,Amount\nA-1,10\n", "2026-09-01T00:00:00.000Z")
    content = _xlsx_bytes({"Orders": [["Order ID", "Amount"], ["A-2", 20]]})
    google.add_item(
        FakeDriveItem(
            "x", "orders_2.xlsx", XLSX_MIME, [FOLDER], "2026-09-02T00:00:00.000Z", content
        )
    )
    _project(tmp_path, params="  glob: 'orders_*'\n")
    db = str(tmp_path / "w.duckdb")

    def run() -> Any:
        result = dtex.run(
            config="gd", project_dir=str(tmp_path), destination_params_override={"path": db}
        )
        assert result.status.value == "succeeded", result.error
        return result

    run()
    conn = duckdb.connect(db)
    rows = conn.execute(
        "SELECT order_id, CAST(amount AS VARCHAR), _dtex_file_path, _dtex_file_cursor "
        "FROM files ORDER BY order_id"
    ).fetchall()
    conn.close()
    assert [r[:3] for r in rows] == [("A-1", "10", "orders_1.csv"), ("A-2", "20", "orders_2.xlsx")]
    assert rows[1][3] == "2026-09-02T00:00:00.000Z|x"

    # Second run: nothing new → nothing appended.
    result = run()
    assert result.streams[0].rows_loaded == 0

    # A new file arrives → only it is loaded.
    _csv(google, "c", "orders_3.csv", "Order ID,Amount\nA-3,30\n", "2026-09-03T00:00:00.000Z")
    result = run()
    assert result.streams[0].rows_loaded == 1
    conn = duckdb.connect(db)
    assert conn.execute("SELECT count(*) FROM files").fetchone() == (3,)
    state = conn.execute("SELECT cursor_value FROM _dtex_state WHERE stream = 'files'").fetchone()
    conn.close()
    assert state is not None and "orders" not in str(state[0]) and "|c" in str(state[0])


_LOCAL_REGISTER = """\
name: finance_drive
kind: source
version: "1.0.0"
params:
  folder: {type: string, required: true}
streams:
  - name: orders
    table: orders
    write_disposition: append
    incremental: {cursor_field: _dtex_file_cursor, cursor_type: string, ordered: true,
                  initial_value: ""}
  - name: refunds
    table: refunds
    write_disposition: append
    incremental: {cursor_field: _dtex_file_cursor, cursor_type: string, ordered: true,
                  initial_value: ""}
"""

_LOCAL_SOURCE = """\
from dtex import stream
from dtex.sources.google_drive.extract import extract_files


@stream(name="orders")
def orders(config, cursor, log):
    yield from extract_files(config, cursor, log, glob="orders_*.csv")


@stream(name="refunds")
def refunds(config, cursor, log):
    yield from extract_files(config, cursor, log, glob="refunds*.xlsx", sheet="Refunds",
                             range="A2:C", header_row=1)
"""


def test_end_to_end_project_local_multi_stream_copy(google: FakeGoogle, tmp_path: Path) -> None:
    """The README's pattern: one stream per glob, each with its own cursor + table."""
    _csv(google, "a", "orders_1.csv", "Order ID,Amount\nA-1,10\n", "2026-09-01T00:00:00.000Z")
    content = _xlsx_bytes(
        {"Cover": [["x"]], "Refunds": [["Refund ID", "Amount", "Note"], ["R-1", 5, "dup"]]}
    )
    google.add_item(
        FakeDriveItem("r", "refunds.xlsx", XLSX_MIME, [FOLDER], "2026-09-02T00:00:00.000Z", content)
    )
    _project(tmp_path)
    (tmp_path / "dtex_project.yml").write_text(
        "name: t\nversion: '0.1'\nsource_paths: [sources]\n"
        "destination_paths: []\nconfig_paths:\n  - configs\n"
    )
    local = tmp_path / "sources" / "finance_drive"
    local.mkdir(parents=True)
    (local / "register.yaml").write_text(_LOCAL_REGISTER)
    (local / "source.py").write_text(_LOCAL_SOURCE)
    (tmp_path / "configs" / "gd.yml").write_text(
        "name: gd\nsource: finance_drive\ndestination: duckdb\ntarget: dev\n"
        f"params:\n  folder: {FOLDER}\nstreams: all\n"
    )
    db = str(tmp_path / "w.duckdb")
    result = dtex.run(
        config="gd", project_dir=str(tmp_path), destination_params_override={"path": db}
    )
    assert result.status.value == "succeeded", result.error
    conn = duckdb.connect(db)
    orders = conn.execute("SELECT order_id, _dtex_file_path FROM orders").fetchall()
    refunds = conn.execute("SELECT refund_id, amount, note FROM refunds").fetchall()
    conn.close()
    assert orders == [("A-1", "orders_1.csv")]
    assert refunds == [("R-1", 5, "dup")]
