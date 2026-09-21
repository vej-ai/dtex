# RevenueCat — baked source connector

Extracts [RevenueCat](https://www.revenuecat.com)'s v2 API
(`https://api.revenuecat.com/v2`) at **period grain**: one row per store
transaction (every billing period of every subscription), plus the
subscriptions, customers, product catalogue and daily chart metrics around
them, and a built-in check that what landed adds up to what RevenueCat
itself counts.

v2 keys are **separate** from v1 keys and the two APIs are not
interchangeable.

## Authentication

The connector reads a **v2 RevenueCat secret key** (`sk_...`) from the
`REVENUECAT_API_KEY` environment variable by default:

```sh
export REVENUECAT_API_KEY="sk_..."
```

Required scopes (set on the key in the RC dashboard):

| Streams | Scope |
|---|---|
| `customers`, `customer_details` | `customer_information:customers:read` |
| `subscriptions`, `subscription_transactions` | `customer_information:subscriptions:read` |
| `products`, `entitlement_products` | `project_configuration:products:read`, `project_configuration:entitlements:read` |
| `metrics_daily`, `reconciliation_daily` | `charts_metrics:overview:read` |

For production, override the secret ref per profile (`profiles.yml`)
with any resolver-backed `secret://` URL — GCP Secret Manager, AWS
Secrets Manager, or HashiCorp Vault:

```yaml
# profiles.yml
revenuecat:
  default_target: prod
  targets:
    prod:
      api_key: secret://gcp-secret-manager/projects/<proj>/secrets/<name>/versions/latest
```

(Requires the matching extra: `pip install 'dtex[gcp-secrets]'` /
`[aws-secrets]` / `[vault]`.)

The key never appears in log output — it's set on the `requests.Session`
header once and the connector's logging emits no header contents.

## Why this connector looks the way it does

The v2 API has no change feed, and everything else follows from that.

* `/customers` has no date filter and no sort, and a customer row carries no
  purchase signal, only `first_seen_at` / `last_seen_at`. Subscriptions exist
  only per customer, transactions only per subscription. **Finding out who
  bought something is the connector's main job.**
* `last_seen_at` does **not** reliably move when a customer buys. On a
  production project, 20 of 2,104 first purchases happened more than an hour
  after the customer's final `last_seen_at` (the longest gap was 20 hours).
  A rule like "fetch whoever was seen since the last run" loses those, and a
  memory like "this customer has no subscriptions, skip them" loses them for
  ever. Connector 1.x derivatives that kept such a skip list dropped about 13%
  of new subscriptions. 2.0 never trusts a negative result; see
  [`targets.py`](./targets.py).
* State is current state. A refund rewrites a transaction to its post-refund
  amount with no timestamp and no original price; a cancellation is just
  `auto_renewal_status` as of now. Those facts are the *difference between two
  pulls*, so the diff streams read their previous snapshot back from the
  destination ([`landed.py`](./landed.py)), keep first-seen values and stamp
  `*_detected_at` on an observed transition (resolution = your run cadence).

## Streams

They hand off through the landed tables, so run them from **one config, in
this order** (`streams: all` does).

| Stream | What it lands | Needs `landed_reader` |
|---|---|---|
| `products` | Product catalogue. | no |
| `entitlement_products` | Entitlement → product mapping (a subscription lists its entitlements only while active). | no |
| `customers` | The full `/customers` walk: `first_seen_at`, `last_seen_at`. | no |
| `subscriptions` | One row per RC subscription, with `changed_at` and `auto_renew_off_detected_at` / `billing_issue_detected_at` / `refund_detected_at`. | **yes** |
| `customer_details` | Aliases (the original `$RCAnonymousID`), `email`, and every attribute as JSON. Once per customer with a subscription. | **yes** |
| `subscription_transactions` | One row per store transaction: amounts in USD and local currency (gross, tax, commission, proceeds), `first_gross_usd` (the price before any refund), `refund_detected_at`. | **yes** |
| `reconciliation_daily` | Landed transactions per UTC day vs RevenueCat's own count. | **yes** |
| `metrics_daily` | RC charts in long format (date × chart × measure). | no |

### Who gets fetched each run

`subscriptions` builds its fetch list in tiers (details and rationale in
`targets.py`):

1. **seeds** — optional operator queries naming customers (or store
   subscription ids) that probably changed. A fast path, never required.
2. **bootstrap** — optional operator query of ids *known* to have transacted
   (e.g. RevenueCat's scheduled export). One with no landed subscription is a
   proven gap, so it goes ahead of everything merely plausible: seeds history
   on a first run, safety net afterwards.
3. **sweep** — every customer without a hot subscription is re-checked on a
   fixed, decaying schedule after RC last saw them: 0, 1, 2, 3, 4, 6, 8, 12 …
   hours, thinning out to 30 days (22 fetches per sighting). The schedule is a
   pure function of the customer's timestamps; the only memory is a watermark
   that advances over checkpoints that were actually processed. A failed run,
   an outage or a capped backlog widens the next window instead of dropping
   anyone.
4. **hot** — customers with a subscription that is active or ended within
   `hot_window_days`: every run, stalest first.
5. **heal** — when `reconciliation_daily` shows a recent day short, every
   plausible customer around that day is re-checked.
6. **cold** — a slow rotation over long-expired subscribers (a resubscription
   made outside the app moves no timestamp).

### `reconciliation_daily`: is anything missing?

Every run compares, for the trailing `reconcile_days`, the number of landed
production transactions purchased that UTC day with the **Transactions**
measure of RevenueCat's revenue chart. `missing = max(rc - landed, 0)`.

The definitions differ on purpose (RC counts revenue-generating purchases; the
landed count includes zero-price periods): on 77 clean production days RC's
count was never above the landed one, so the check has no false alarms, while
a run of missed purchases shows at once. It is a lower bound, not an exact
figure. Recent short days heal themselves (tier 4); put a warehouse test on
the table for the rest:

```sql
SELECT * FROM reconciliation_daily
WHERE missing > 0 AND NOT incomplete AND day < CURRENT_DATE - 2
```

### `customers`: a walk that cannot leave a gap

One cursor chain over ~250k customers takes over an hour; 33 concurrent chains
take a few minutes. `starting_after` accepts any string, so chains can start
anywhere, but how RC *orders* the list is undocumented. The walk therefore
never compares ids: each chain's first row becomes a boundary, a chain runs
until it meets another chain's boundary or the end of the list, and one chain
starts at the very beginning. Whatever the collation, the segments tile the
list; a wrong guess costs duplicate rows (harmless under merge), never a gap.

### `metrics_daily`

First run: from `metrics_initial_since_date`, in 90-day requests. Every later
run re-pulls the trailing `metrics_lookback_days`, because RC keeps revising a
day after it stops being `incomplete`. (1.x re-pulled only from the last
complete day, so landed values froze at their first, often low, reading.)

## Config

```yaml
# configs/revenuecat_bq.yml
name: revenuecat_bq
source: revenuecat
destination: bigquery
target: prod

params:
  project_id: "proj1ab2c3d4"
  landed_reader: bigquery                 # bigquery | duckdb
  landed_dataset: "my-project.revenuecat" # = the destination below
  # Optional fast path: first column = RC customer ids that probably changed.
  # seed_customers_sql: |
  #   SELECT DISTINCT user_id FROM `my-project.events.purchases`
  #   WHERE ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)

destination_params:
  dataset: revenuecat

streams: all
```

For DuckDB: `landed_reader: duckdb` and `landed_dataset:` the absolute path of
the database file (`/path/warehouse.duckdb`, or `/path/warehouse.duckdb#schema`).

The read-back uses the default table names, so do not rename this source's
tables. All params are documented in [`register.yaml`](./register.yaml).

## Budget and scale

Measured on a project with ~255k customers and ~5.5k recently active
subscribers, hourly runs, default params: the customers walk is ~2,550
requests (7 min), the hot set ~5.5k, the sweep ~1k, transactions a few dozen.
About 20 minutes per run at 7 requests/second.

The hot set is one request per customer per run, so an hourly cadence fits
roughly 15-20k recently active subscribers at the default rate. Beyond that,
run less often, shrink `hot_window_days`, or raise `rate_per_second` if
RevenueCat raised your limit. `max_customers_per_run` spreads a first run or a
post-outage backlog over several runs; nothing that does not fit is dropped.

A backlog is worked off oldest checkpoint first, at whatever the cap leaves
after the hot set, so it clears slowly (the first run's 7-day window was ~21k
customers on that project, a day of hourly runs). The `subscriptions` log line
prints `sweep backlog N` on every run. To clear one at once, run the stream
by hand with a larger cap:

```sh
dtex run -p revenuecat_bq --select subscriptions,subscription_transactions \
  --param max_customers_per_run=40000
```

## Rate limits and timeouts

RC enforces 480 requests/minute on the Customer Information domain. The client
throttles to `rate_per_second` (default 7) across `workers` threads, honours
`Retry-After` on 429, backs off exponentially on 5xx and network errors, and
gives up after `max_retries`. Timeouts are 10 s connect / 90 s read. A 404 on
a customer (deleted or merged away) skips that customer.

## Upgrading from 1.x

* `subscriptions` gains columns and no longer fans out over every customer on
  every run. `customer_last_seen_at` is gone (join `customers`).
* New streams: `products`, `entitlement_products`, `customer_details`,
  `subscription_transactions`, `reconciliation_daily`.
* Set `landed_reader` / `landed_dataset`, or select only the four streams
  that do not need them.
* `metrics_daily` now re-pulls its trailing window on every run.
