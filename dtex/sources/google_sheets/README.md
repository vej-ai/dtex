# google_sheets — Google Sheets source connector

A baked dtex source that reads a Google Sheets spreadsheet. **Every tab is its
own stream and its own table**, found at run time: add a tab to the
spreadsheet and the next `streams: all` run lands it too. Each run is a full
refresh of each tab (`write_disposition: replace`) — Sheets exposes no change
cursor, so the table always mirrors the tab as it is now.

## Quick start

```yaml
# configs/budget.yml — every tab of the spreadsheet
name: budget
source: google_sheets
destination: bigquery
params:
  spreadsheet: https://docs.google.com/spreadsheets/d/1AbC…xyz/edit#gid=0
  table_prefix: budget_          # optional: budget_orders, budget_refunds, …
streams: all
```

```yaml
# configs/budget_selected.yml — chosen tabs and ranges
name: budget_selected
source: google_sheets
destination: bigquery
params:
  spreadsheet: 1AbC…xyz                       # the bare ID works too
  tabs: "Orders!A2:N, Refunds!A2:N10, gid=184927"
streams: all
```

`Orders!A2:N` reads rows 2 to the last row with data, columns A..N;
`Refunds!A2:N10` reads exactly rows 2..10. In both, the first non-empty row of
the range is the header (so row 2 here) unless `header_row` says otherwise.

The same selection can live in the `streams:` block instead — stream names are
the slugified tab titles, and each stream takes its own `params`:

```yaml
streams:
  orders:
    params: {range: "A2:N"}
  refunds:
    params: {range: "A5:N", header_row: 1}   # header on row 1, data from row 5
```

## Streams — one per tab

| Tab title | Stream (and table) name |
|---|---|
| `Orders` | `orders` |
| `Q3 Refunds (EUR)` | `q3_refunds_eur` |
| `2026` | `_2026` |
| `Užsakymai` | `uzsakymai` |
| `Orders` **and** `orders` in one spreadsheet | `orders_0` and `orders_184927` — each gets its gid |

Names are computed over **all** tabs, so narrowing `tabs` never renames a
stream. A collision (two titles that slugify alike) suffixes every colliding
tab with its gid, which never changes for the life of a tab — neither stream
can start reading the other tab because a tab was added or reordered. Rename a
tab and its stream is renamed with it (a new table, fresh state). `table_prefix`
prefixes every table name, not the stream names.

Chart sheets and data-source sheets hold no cells and are skipped. Hidden tabs
are read like any other; leave them out with `tabs` or an explicit `streams:`
mapping.

## Reading a tab

* **Range.** No range = the tab's used area. `range` (connector-wide or per
  stream) or a range in a `tabs` entry (which wins) narrows it: `A2:N`
  (open-ended down), `A2:N10` (bounded), `A:C` (whole columns). A range past
  the tab's grid reads nothing.
* **Header row.** Unset = the first non-empty row of the range. `header_row: N`
  names sheet row N as the header — it may sit above the range. `header_row: 0`
  = no header; names come from `columns` (`columns: "id, name, amount"`) or are
  positional.
* **Column names** are `snake_case` ASCII: `Order ID` → `order_id`,
  `Amount ($)` → `amount`, `2025` → `_2025`. A blank or unnameable header
  becomes `column_<n>` (`n` = sheet column, so C → `column_3`); duplicates get
  `_2`, `_3`. A column with a blank header appears only in rows where it holds
  a value, so spacer columns never become all-NULL warehouse columns.
* **Rows.** Rows with no values are skipped. Every record carries
  `_dtex_row_number`, the sheet row it came from.
* **Values** are read unformatted (`valueRenderOption=UNFORMATTED_VALUE`):
  numbers are numbers, checkboxes are booleans, text is text — no locale
  formatting, no currency symbols. Error cells (`#N/A`) arrive as text.
* **Dates.** Values are fetched with `dateTimeRenderOption=SERIAL_NUMBER`,
  which is exact and locale-free, and a second format-only read types each
  cell by its number format: `DATE` → date, `DATE_TIME` → timestamp in the
  spreadsheet's time zone (Sheets date-times are wall-clock times in that
  zone), `TIME` → `"HH:MM:SS"` text (durations: total hours, `"36:00:00"`).
  `parse_dates: false` skips the extra read and lands the raw serials
  (days since 1899-12-30).
* **Types per column.** A column settles on one type: integers stay integers,
  integers mixed with decimals become floats, dates mixed with date-times
  become timestamps, anything else mixed becomes text. Declare
  `streams.<tab>.schema` in the config to pin a type.

An empty tab, or one with only a header row, lands as an empty table — and
clearing a tab empties its table on the next run rather than leaving the
previous rows behind.

## Authentication

| `auth_type` | Credentials |
|---|---|
| `auto` (default) | A service account if `credentials_path` / `credentials_json` is set, else ADC |
| `oauth` | Application Default Credentials |
| `service_account` | `credentials_path` (a key file) or `credentials_json` |

**Share the spreadsheet** with the identity — the service account's
`…@….iam.gserviceaccount.com` address, or your user for ADC — as a Viewer.
Enable the **Google Sheets API** in the credentials' project.

* **ADC, local development:** user credentials need the Sheets scope, which
  `gcloud` does not request by default:
  ```sh
  gcloud auth application-default login \
    --scopes=https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/spreadsheets.readonly,https://www.googleapis.com/auth/drive.readonly
  ```
  On GCE / Cloud Run / GKE, ADC is the attached service account; share the
  sheet with it.
* **Cloud Build, GCE and other places where the ambient token is
  `cloud-platform` only** (the Sheets API refuses it): set
  `impersonate_service_account` to the identity that should read the sheet,
  which may be the build's own service account. dtex mints a short-lived token
  with exactly the Sheets/Drive scopes through the IAM Credentials API; the
  source identity needs `roles/iam.serviceAccountTokenCreator` on the target
  (grant the account the role on itself for self-impersonation). No key.
* **Key file:** `credentials_path: /secrets/sheets-reader.json`.
* **Key in a secret store:** `credentials_json` takes a *reference*, resolved
  at run time and never logged — `${env.SHEETS_SA_JSON}` or
  `secret://gcp-secret-manager/projects/p/secrets/sheets-reader/versions/latest`
  (docs/08 §3). A literal key is refused, so it can never sit in a config file.

Scope requested: `spreadsheets.readonly`.

## Params

| Param | Default | Meaning |
|---|---|---|
| `spreadsheet` | — (required) | Link or ID. A `#gid` in the link is ignored. |
| `tabs` | `""` (all) | Comma-separated `<tab>[!<range>]`: a title, a quoted title (`'Q1, final'`), or `gid=<n>`. An entry that matches no tab fails the run, listing the tabs. |
| `range` | `""` | A1 range (no sheet name) for the selected tabs. |
| `header_row` | unset | Sheet row of the header; `0` = none. |
| `columns` | `""` | Comma-separated column names, left to right. |
| `parse_dates` | `true` | Type date/time cells via their number format. |
| `table_prefix` | `""` | Prefix for table names. |
| `batch_size` | `1000` | Records per batch. |
| `auth_type` | `auto` | `auto` / `oauth` / `service_account`. |
| `credentials_path` | `""` | Service-account key file. |
| `credentials_json` | `""` | `${env.X}` / `secret://…` reference to the key JSON. |

## API use

Per run: one `spreadsheets.get` (tab properties only) for discovery, then per
tab one `spreadsheets.values.batchGet` and — with `parse_dates` and any numeric
cell — one `spreadsheets.get` restricted to number-format types. 429 / 5xx
responses are retried with backoff (honouring `Retry-After`).

## Why no incremental mode

Drive's `modifiedTime` could let a run skip an unchanged spreadsheet, but it
only moves when someone edits the file. Values computed by `IMPORTRANGE`,
`GOOGLEFINANCE`, `NOW()` or a connected data source change without it, so a
skip would silently serve stale numbers — and it would need the Drive scope
and API as well. Reading a sheet is cheap, so every run re-reads every
selected tab; schedule the config as often as you need.
