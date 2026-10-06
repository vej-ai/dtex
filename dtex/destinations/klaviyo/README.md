# Klaviyo destination

Submits events through Klaviyo's Bulk Create Events API. The destination is
append-only. It accepts canonical event records from any dtex source; filtering,
identity resolution, metric mapping and selection of eligible records belong in
the source or the warehouse.

## Record contract

| Field | Requirement |
|---|---|
| `unique_id` | Required non-empty string. Stable across retries and backfills. |
| `metric_name` | Required non-empty string. |
| `time` | Required timestamp with timezone; the original event time. |
| `profile` | Required JSON object with `id`, `email`, `phone_number` or `external_id`. Other fields are Klaviyo profile attributes. |
| `properties` | JSON object; defaults to an empty object. |
| `value` | Optional finite numeric value. |
| `value_currency` | Optional uppercase three-letter currency code. |

Declare `profile` and `properties` as JSON fields in the source schema. Warehouse
metadata and any additional columns are not sent. The connector never invents
event identifiers or replaces the event timestamp with the submission time.

## Configuration and authentication

Bind a pipeline to `destination: klaviyo` and configure its target under the
`klaviyo` block of the private `profiles.yml`. `KLAVIYO_API_KEY` supplies an
API key with `events:write` permission. Keep the key outside version control.

The boolean `backfill` parameter is mandatory: `true` suppresses metric-triggered
flows for historical imports; `false` permits normal flow triggering. Choose it
explicitly for each target. API revision defaults to `2026-07-15`.

## Durable delivery state

Klaviyo does not host dtex checkpoints. This destination manages `_dtex_state`,
`_dtex_leases` and `_dtex_runs` in a separately configured warehouse, using the
existing DuckDB or BigQuery implementation:

- `state_backend: duckdb` is the default. `state_path` must be a persistent local
  file and defaults to `.dtex/klaviyo-state.duckdb`. In-memory state is rejected.
- `state_backend: bigquery` requires `state_project`, `state_dataset` and
  `state_staging_bucket`, with optional `state_location` (default `US`). It uses
  Application Default Credentials. The shared backend verifies the existing
  staging bucket at initialization; event payloads are sent directly to Klaviyo.

Use remote state for ephemeral build workers. Give delivery its own state file
or dataset: state and leases are keyed by source and stream, so sharing an
extraction pipeline's state store would conflate two independent checkpoints.
State storage needs the same warehouse permissions as the corresponding baked
destination. Only checkpoints, leases and run metadata are stored there.

## Delivery guarantees

The whole engine batch is validated before any requests are sent. Requests are
split at 1,000 events and below 5 MB; repeated identifiers are placed in separate
requests to satisfy the bulk API's uniqueness rule. Requests are limited to two
per second per running connection. Account-wide concurrency still needs an
external limit if several independent workers share a key.

Retries for network errors, HTTP 429 and HTTP 5xx reuse identical payloads.
Klaviyo deduplicates on profile, metric and `unique_id`. Retries are bounded;
long server-requested delays fail the run so an orchestrator can retry later.
Redirects are rejected. HTTP errors contain status codes without response
bodies, event payloads or credentials. Ambient proxy and netrc configuration is
not used.

An HTTP 202 means **accepted for asynchronous processing**, not confirmed
delivery, flow execution or message delivery. Reported loaded rows and successful
run records have that meaning. A rejected request fails the stream and prevents
the unsent remainder from being reported as accepted. A partially accepted batch
can be replayed with the same identifiers. The destination claims neither
transactional HTTP writes nor an exactly-once transaction with warehouse state.

The source must provide safe checkpoint semantics. In particular, do not advance
past unsent events that share a cursor value, and do not use event occurrence time
alone to exclude late arrivals. Prefer a durable outbox of published, eligible
events and a publication cursor. Verify asynchronous processing separately when
the application requires a delivery receipt.

## Pipeline dependencies

dtex does not currently implement `ref` or `depends_on`. Use an orchestrator to
start delivery only after extraction and publication succeed. A tag sweep's
ordering is not a dependency: it continues after a pipeline failure and can run
pipelines concurrently. A successful extraction that skipped a leased stream
also does not prove that a new snapshot was published; check its publication
manifest before creating delivery work.

References: [Bulk Create Events](https://developers.klaviyo.com/en/reference/bulk_create_events)
and [Events API semantics](https://developers.klaviyo.com/en/reference/events_api_overview).
