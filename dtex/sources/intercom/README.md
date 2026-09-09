# intercom

Baked source connector for the [Intercom REST API](https://developers.intercom.com/docs/references/rest-api/api.intercom.io/) (v2.16).

Lands every reporting object of a workspace in the column shapes Airbyte's
`source-intercom` produces — top-level scalars as columns, nested objects as
JSON, unix-second timestamps as INTEGER — so a dbt project that reads the
Airbyte tables can point its `source()` at the dtex dataset without rewriting
models.

## Streams

| Stream | Endpoint | Disposition | Incremental |
|---|---|---|---|
| `contacts` | `POST /contacts/search` | merge on `id` | `updated_at`, windowed |
| `conversations` | `POST /conversations/search` | merge on `id` | `updated_at`, windowed |
| `tickets` | `POST /tickets/search` | merge on `id` | `updated_at`, windowed |
| `conversation_parts` | `GET /conversations/{id}` per touched conversation | merge on `id` | conversation `updated_at`, capped per run |
| `companies` | `GET /companies/scroll` | merge on `id` | full sweep (no filter exists) |
| `articles` | `GET /articles` | merge on `id` | full sweep |
| `admins`, `teams`, `tags`, `segments`, `ticket_types`, `data_attributes` | one `GET` each | replace | — |

### Search streams

The span from the cursor to now is tiled into `window_days`-wide
`updated_at` windows (default 7). Each window is one search
(`updated_at > a-1 AND updated_at < b+1`, paginated with `starting_after`,
150 per page); pages are buffered into `batch_size`-row batches (default
5000 — one destination load each, which matters on BigQuery where load jobs
per table per day are capped). `cursor.observe` fires for a **completed**
window only after every row of that window has been yielded, so `ordered:
true` is exact: a mid-run state flush persists the end of the last complete,
landed window and a crashed run resumes by re-pulling the interrupted one.
Steady state (cursor ≈ now, `lookback: 6h`) is one window per run.

With no cursor (`--full-refresh`) the walk is a single unbounded search.

`tickets` flattens the 2.12+ `ticket_state` object into `ticket_state`
(the category: submitted / in_progress / waiting_on_customer / resolved),
`ticket_state_id`, `ticket_state_internal_label`,
`ticket_state_external_label` — the columns Airbyte produced from API 2.11.

### `conversation_parts` — the transcript

One HTTP call per conversation updated in the window; one row per part plus
a `part_type: conversation_source` row for the opening message (id = the
source's id, `delivered_as` / `subject` filled). Conversations are fetched
in ascending `updated_at` order and the cursor is observed after each one
lands, so `max_conversations_per_run` turns a backfill into resumable
chunks:

```bash
dtex run -p intercom_bq --select conversation_parts --param max_conversations_per_run=3000
```

Select the stream explicitly in a config; it is the expensive one.

## Auth and setup

1. Intercom **Developer Hub → your app → Authentication → Access token**.
   Read scopes for contacts, companies, conversations, tickets, articles,
   admins/teams, tags/segments, data attributes.
2. Set `INTERCOM_ACCESS_TOKEN` (or override the `access_token` secret ref
   with a `secret://` URI in your config).
3. `region`: `us` (default), `eu` or `au` — the token only answers on its
   workspace's host.

```yaml
# configs/intercom_bq.yml
name: intercom_bq
source: intercom
destination: bigquery
target: prod
params:
  region: us
  window_days: 7
destination_params:
  dataset: dtex_intercom
streams:
  contacts:
  conversations:
  tickets:
  companies:
  admins:
  teams:
  tags:
  segments:
  ticket_types:
  data_attributes:
  articles:
  conversation_parts:
    params:
      max_conversations_per_run: 3000
```

## Rate limits and errors

A token bucket paces requests at `requests_per_second` (default 12). A
`429` waits for Intercom's `X-RateLimit-Reset` (or `Retry-After`), capped
at `rate_limit_max_wait_seconds`, then retries; `5xx` and connection
errors retry with exponential backoff; both count against `max_retries`.
Any other `4xx` raises `IntercomAPIError` carrying Intercom's own
`[code] message` — a `403` names the missing scope. The token never
appears in logs or error text.

## Known limits

- Companies and articles have no server-side `updated_at` filter; both are
  full sweeps merged on id (small on most workspaces).
- `conversation_parts` is O(conversations touched) HTTP calls. Backfill a
  workspace with a per-run cap, or set `initial_value` to the date you need.
- Search `updated_at` filters are second-precision on `>` / `<`; the
  6-hour lookback covers indexing lag on freshly changed records.
- Contact `tags` carry ids only (`{data: [{id}]}`); join the `tags` stream
  for names. Conversation and company tags carry names inline.
- Intercom embeds at most 10 `tags` / `companies` / `notes` on a contact
  and flags the rest with `has_more`; the connector fetches the full list
  for those contacts, so the landed arrays are complete (Airbyte's were not).
