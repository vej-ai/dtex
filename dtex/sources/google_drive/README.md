# google_drive — Google Drive folder source connector

A baked dtex source that loads **CSV, XLSX and native Google Sheets files from
a Google Drive folder** — in My Drive or a shared drive — into one table,
**incrementally by file**. It is the Drive counterpart of the `filesystem`
source: `folder` + `glob` choose the files, every matching file is unioned
into the stream's table, and a run loads only files added or changed since the
last one.

## Quick start

```yaml
# configs/exports.yml — every orders_*.csv in the folder, one table
name: exports
source: google_drive
destination: bigquery
params:
  folder: https://drive.google.com/drive/folders/1FoLdEr…Id
  glob: "orders_*.csv"
streams:
  files:
```

```yaml
# XLSX: a named sheet, a range, the header above it
params:
  folder: 1FoLdEr…Id
  glob: "*.xlsx"
  sheet: Data            # or a 1-based position: "2"
  range: "A5:N"          # rows 5.. to the last row, columns A..N
  header_row: 4
```

## Several tables from one folder — CSV + XLSX streams

A config runs the connector's single `files` stream, and state is kept per
source + stream, so two configs over `google_drive` would share one cursor.
For several tables, make a project-local copy with one stream per pattern
(exactly as with the `filesystem` source) — the extraction is one import:

```yaml
# sources/finance_drive/register.yaml
name: finance_drive
kind: source
version: "1.0.0"
params:
  folder: {type: string, required: true}
streams:
  - name: orders
    table: orders
    write_disposition: append
    incremental: {cursor_field: _dtex_file_cursor, cursor_type: string,
                  ordered: true, initial_value: ""}
  - name: refunds
    table: refunds
    write_disposition: append
    incremental: {cursor_field: _dtex_file_cursor, cursor_type: string,
                  ordered: true, initial_value: ""}
```

```python
# sources/finance_drive/source.py
from dtex import stream
from dtex.sources.google_drive.extract import extract_files


@stream(name="orders")
def orders(config, cursor, log):
    yield from extract_files(config, cursor, log, glob="orders_*.csv")


@stream(name="refunds")
def refunds(config, cursor, log):
    yield from extract_files(
        config, cursor, log, glob="refunds*.xlsx", sheet="Refunds", range="A2:N", header_row=1
    )
```

```yaml
# configs/finance.yml
name: finance
source: finance_drive
destination: bigquery
params:
  folder: https://drive.google.com/drive/folders/1FoLdEr…Id
streams: all
```

Keyword arguments to `extract_files` override the config's params for that
stream; every param below can be set either way. Import from `.extract`, not
`.source` (importing `source.py` would register its `files` stream into your
connector).

## Which files

* **Listing.** Direct children of `folder` (`recursive: true` walks
  sub-folders at any depth). Trashed files and shortcuts are skipped. Shared
  drives work: the folder's drive is detected and listed with
  `corpora=drive`; `supportsAllDrives` / `includeItemsFromAllDrives` are
  always on.
* **`glob`** is matched against the file **name** (not the path),
  case-sensitively: `orders_*.csv`, `*.xlsx`, `report ????-??.csv`.
* **`format: auto`** reads `.csv` / `.tsv` as CSV, `.xlsx` / `.xlsm` as XLSX
  (falling back to the Drive mimeType), and native Google Sheets through the
  Sheets API. Anything else (PDFs, `.xls`, Docs) is skipped with an info log.
  `format: csv|xlsx|google_sheets` reads every matched file that way.

## Reading a file

The rules are the `google_sheets` source's, so a CSV, an XLSX and a Google
Sheet with the same layout land with the same columns:

* **`range`** (A1, e.g. `A2:N` or `A2:N10`) limits rows and columns; empty =
  the whole used area. For CSV, rows are records and columns are fields.
* **`header_row`**: unset = the first non-empty row of the range; `N` = row N
  (it may sit above the range); `0` = no header (names from `columns`, else
  `column_<n>`).
* **Column names** are `snake_case` ASCII (`Order ID` → `order_id`); blank
  headers become `column_<n>` (sheet column number), duplicates `_2`, `_3`.
  Empty rows are skipped.
* **CSV values stay text**, like the `filesystem` source — the warehouse types
  them (or declare `streams.<name>.schema`). A UTF-8 BOM is dropped; `.tsv`
  defaults to a tab delimiter; `csv_delimiter` sets another. CSV streams row
  by row, so file size is not limited by memory.
* **XLSX** keeps Excel types: numbers, booleans, dates (a midnight value in a
  date-only format becomes a date), date-times (naive — Excel has no time
  zone), times as `"HH:MM:SS"`. Formula cells use the value Excel last
  calculated (a file written by a tool that never calculates has none).
  `sheet` picks the worksheet by name or 1-based position (default: the
  first).
* **Google Sheets** files: `sheet` picks the tab; dates are typed from number
  formats (`parse_dates`).

Within one file a column settles on one type (integers + decimals → float,
mixed → text). Across files the first batch fixes the table's inferred types,
so if one file's `amount` is `10` and another's `10.5`, declare the column's
type with `streams.files.schema`.

## Incremental loading and lineage

Every record carries:

| Column | Meaning |
|---|---|
| `_dtex_file_cursor` | `<modifiedTime>\|<fileId>` — the stream's cursor, e.g. `2026-09-29T10:00:00.123Z\|1AbC…` |
| `_dtex_file_id` | The Drive file ID |
| `_dtex_file_path` | Path relative to `folder` (`2026/orders.csv`) |
| `_dtex_row_number` | Row within the file / sheet |

Files are read in cursor order (by last modification, ties by id) and the
cursor advances per whole file, so a run resumes after the last file it fully
loaded. A file is loaded when its key is above the committed cursor: new
files, and files **modified** since — which are loaded again in full. The
stream appends, so keep each file's latest load downstream:

```sql
SELECT * FROM files
QUALIFY _dtex_file_cursor = MAX(_dtex_file_cursor) OVER (PARTITION BY _dtex_file_id)
```

Things the cursor cannot see:

* A file uploaded with a `modifiedTime` **older** than the cursor (some sync
  clients preserve the local file's time) sorts below it and is not loaded.
  Run with `--full-refresh` (then deduplicate as above) or
  `dtex state reset` to backfill.
* A **deleted** file's rows stay in the table.
* A native Google Sheet whose values change through `IMPORTRANGE` /
  `NOW()`-style formulas keeps its `modifiedTime`. Read such sheets with the
  `google_sheets` source (full refresh) instead.

## Authentication

The same params as `google_sheets`: `auth_type` (`auto` / `oauth` /
`service_account`), `credentials_path` (key file) or `credentials_json` (a
`${env.X}` / `secret://` reference to the key JSON — never the key itself).
**Share the folder** with the identity (a service account's e-mail, or add it
to the shared drive) and enable the **Google Drive API** (and the **Google
Sheets API** for native Sheets files). For user ADC, log in with the
`drive.readonly` and `spreadsheets.readonly` scopes — see the `google_sheets`
README. Scopes requested: `drive.readonly`, `spreadsheets.readonly`.

## Params

| Param | Default | Meaning |
|---|---|---|
| `folder` | — (required) | Folder link or ID (My Drive or shared drive). |
| `glob` | `*` | fnmatch pattern on file names. |
| `recursive` | `false` | Walk sub-folders. |
| `format` | `auto` | `auto` / `csv` / `xlsx` / `google_sheets`. |
| `sheet` | `""` | XLSX / Sheets: sheet name or 1-based position; empty = first. |
| `range` | `""` | A1 range, e.g. `A2:N`, `A2:N10`. |
| `header_row` | unset | Header row number; `0` = none. |
| `columns` | `""` | Comma-separated column names, left to right. |
| `csv_delimiter` | `,` | CSV separator (`.tsv` → tab). |
| `parse_dates` | `true` | Google Sheets files: type date/time cells. |
| `batch_size` | `1000` | Records per batch. |
| `auth_type` | `auto` | `auto` / `oauth` / `service_account`. |
| `credentials_path` | `""` | Service-account key file. |
| `credentials_json` | `""` | `${env.X}` / `secret://…` reference to the key JSON. |

Downloads stream through a temporary file (in memory up to 64 MB, then on
disk). 429 / 5xx and Drive's 403 rate-limit responses are retried with
backoff.
