# Konnektive CRM — baked source connector

Extracts data from the [Konnektive CRM](https://www.checkoutchamp.com)
(Checkout Champ) API at `https://api.konnektive.com`. Five streams:

| Stream | Endpoint | Key | What it is |
| --- | --- | --- | --- |
| `orders` | `order/query/` | `orderId` | One row per order; `items` holds the line items |
| `transactions` | `transactions/query/` | `transactionId` | Sales, rebills, refunds, declines; chargeback fields restate in place |
| `purchases` | `purchase/query/` | `purchaseId` | Subscription / recurring-purchase state |
| `customers` | `customer/query/` | `customerId` | Customer records with `notes` and `paySources` |
| `summary` | `transactions/summary/` | `date` | Konnektive's own per-day transaction report |

All five are `merge`. The tables are **current state**, not an event log: a
refund, a chargeback flag or a rebill re-lands the affected row.

## How it syncs

The four query streams are incremental on `dateUpdated`. Every run asks for
rows *updated* since the cursor minus `lookback_hours` (default 24), in
`window_days`-wide windows (default 1), 200 rows per page — Konnektive's
maximum — and merges on the object's id.

Windows are walked oldest-first and the cursor is committed **per completed
window**, so a long history backfill that dies partway resumes from its last
finished window rather than from `start_date`.

Small windows are deliberate. Konnektive paginates by page *number*, so a
row updated while a walk is in progress shifts the pages behind it. Shallow
walks keep that exposure small, and a row that does slip comes back anyway:
being updated gives it a newer `dateUpdated`, which a later window or the
next run's lookback picks up.

`summary` issues one request per day and re-pulls the trailing
`summary_lookback_days` (default 35) on every run, because refunds and
chargebacks are booked against the original transaction date — a day's
figures keep moving for weeks.

An empty window is not an error. Konnektive answers one with
`{"result": "ERROR", "message": "No orders matching those parameters could be
found"}`; the connector reads that as zero rows.

## Date-times are account-local

Konnektive returns wall-clock values in the **account's** timezone, with no
offset: `2026-09-18 13:31:23`. The connector lands them as `STRING`,
untouched. Typing them `TIMESTAMP` would stamp them UTC and be silently
wrong by the account's offset. Convert downstream, with the account's zone:

```sql
TIMESTAMP(dateCreated, 'America/New_York')   -- BigQuery
```

The `account_timezone` param (default `America/New_York`, Konnektive's own
default) is used for one thing only: working out what "now" is in the
account's terms, for the end of the last window.

## Columns

Konnektive's response shape is the same for every account, so its documented
fields are declared as columns under the API's own camelCase names.

* **Amounts stay `STRING`**, exactly as sent (`"29.99"`). Cast downstream,
  where the precision choice belongs.
* **Nested values land as `JSON`** — `items`, `fulfillments`, `transactions`,
  `notes`, `paySources`, and the per-account `customFields` bag.
* **Every row carries `raw`** — the entire object as returned. A field
  Konnektive adds next quarter is already landed, before any connector
  release, and reachable with `JSON_VALUE(raw, '$.newField')`.
* **One rename.** A key that starts with a digit (`3DTxnResult`) becomes
  `_3DTxnResult` as a column, because warehouses reject a leading digit.
  `raw` keeps the API's spelling.

## Authentication

Konnektive has no header scheme: the API user's login id and password travel
as request parameters on every call. The connector sends them in a **POST
form body** by default, so neither ever appears in a URL — URLs end up in
proxy logs, in `urllib3`'s DEBUG output and in exception messages. Set
`http_method: GET` only for an account or proxy that rejects POST.

Create a dedicated API user in the CRM (Admin → Users, type *API*), and if
the account enforces an IP allow-list, add the runner's egress IP.

An auth failure is raised immediately and **never retried** — repeated bad
logins can lock the API user.

```sh
export KONNEKTIVE_LOGIN_ID="..."
export KONNEKTIVE_PASSWORD="..."
```

For production, override the secret refs per profile with any
resolver-backed `secret://` URL:

```yaml
# profiles.yml
konnektive:
  default_target: prod
  targets:
    prod:
      login_id: secret://gcp-secret-manager/projects/<proj>/secrets/konnektive-login-id/versions/latest
      password: secret://gcp-secret-manager/projects/<proj>/secrets/konnektive-password/versions/latest
```

(Requires the matching extra: `pip install 'dtex[gcp-secrets]'` /
`[aws-secrets]` / `[vault]`.)

## Config

```yaml
name: konnektive_bq
source: konnektive
destination: bigquery
target: prod

params:
  start_date: "2024-01-01"            # required — when this account opened
  account_timezone: "America/New_York"

destination_params:
  dataset: dtex_konnektive

streams:
  orders:
  transactions:
  purchases:
  customers:
  summary:
```

| Param | Default | |
| --- | --- | --- |
| `start_date` | *(required)* | First `dateUpdated` pulled when there is no cursor. One request per empty day per stream, so don't set it years early. |
| `account_timezone` | `America/New_York` | IANA zone of the account. |
| `window_days` | `1` | Width of one request window. Widen for a low-volume account to shorten a long backfill. |
| `lookback_hours` | `24` | Overlap re-pulled each run (query streams). |
| `summary_lookback_days` | `35` | Trailing days of `summary` re-pulled each run. |
| `page_size` | `200` | `resultsPerPage`; Konnektive caps it at 200. |
| `batch_size` | `1000` | Rows per batch handed to the destination. |
| `include_custom_fields` | `true` | Sends `includeCustomFields=1`. |
| `http_method` | `POST` | `GET` puts the credentials in the URL. |
| `timeout_seconds` | `120` | Hard read timeout per request. |
| `max_retries` | `5` | For 429 / 5xx / network errors / transient `ERROR` results. |
| `min_request_interval_seconds` | `0` | Pacing between requests. |

## Sizing a first run

A virgin run walks `start_date` → now, one window at a time. Budget roughly
one request per window per stream plus one per 200 rows — an account with
1.5M transactions is ~7,500 pages for that stream alone. It is resumable, so
a build timeout costs only the window in flight; re-run until it catches up.
Steady-state runs cost a handful of requests per stream.

## Failure modes

| Symptom | Cause |
| --- | --- |
| `KonnektiveAuthError: API rejected the request` | Wrong login id / password, the user is not an API user, or the runner's IP is not allow-listed. Not retried. |
| `API error after N retries: '...'` | Konnektive kept answering `result: ERROR` with a message that is neither "no results" nor auth. The message is the server's own. |
| `network failure after N retries (ReadTimeout)` | A request exceeded `timeout_seconds`. Narrow `window_days`, or raise the timeout. |
| Newest rows arrive one run late | `account_timezone` is west of the account's real zone, so "now" falls short. |
