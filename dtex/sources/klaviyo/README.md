# klaviyo

Klaviyo's JSON:API — events carrying **resolved attribution**, profiles carrying
**consent**, and the Reporting API that reproduces Klaviyo's own UI numbers.

## Why this exists

Three things are hard to get out of Klaviyo, and every one of them is invisible
rather than loud — the table looks complete and the columns are simply empty.

### 1. Attribution is a sidecar

Klaviyo attributes revenue to email itself. Ask its API for an event and that
verdict is missing: Klaviyo removed `$attribution` from `event_properties` in
revision `2024-02-15` and now serves it only via `?include=attributions`.
Airbyte's `source-klaviyo` sends the include but never joins the `included`
block back onto the event
([#54174](https://github.com/airbytehq/airbyte/issues/54174)), so the warehouse
receives an opaque attribution id and nothing else.

This connector parses `included` and folds the ids onto the event:

| column | meaning |
|---|---|
| `attribution_id` | the attribution object |
| `attributed_channel` | derived — `flow`, `campaign`, or NULL |
| `attributed_flow_id` / `attributed_flow_message_id` | flow-attributed conversions |
| `attributed_campaign_id` / `attributed_campaign_message_id` | campaign-attributed conversions |
| `attributed_event_id` | the triggering open/click |

Deeper includes (`attributions.flow`) are rejected with a 400, and
`fields[attribution]` accepts only `id` — the attribution object's `attributes`
is permanently `{}`. So ids are as far as one request reaches; the catalog
streams supply the names.

### 2. Consent is opt-in

`subscriptions` is **not** in a default `/profiles` response. Without
`additional-fields[profile]=subscriptions` every consent column lands NULL, and
the profile table cannot answer whether anyone is contactable. This connector
requests it and flattens the answer:
`email_marketing_consent`, `email_marketing_can_receive`,
`email_marketing_suppressions` (reasons + timestamps), and the SMS equivalents.

Trust `email_marketing_can_receive` over `consent`: a profile can read
`SUBSCRIBED` and still be unreachable because a hard bounce or spam complaint
put it under suppression.

Klaviyo's predictive analytics (`predicted_clv`, `churn_probability`, ...) ride
along on the same opt-in field.

### 3. The UI numbers come from a different API

The Reporting API is the only surface that matches what a marketing team sees
on screen. The app buckets by **send date**; the raw event stream buckets by
**when the event happened**. An open rate derived from events therefore never
quite matches the dashboard being quoted at you. `campaign_reports`,
`flow_reports` and `segment_reports` POST to that API, with conversion
statistics computed against your own `conversion_metric_id`.

## The late-attribution trap

Klaviyo events are **immutable** and carry no `updated_at`. `datetime` is when
the event happened; attribution is written up to ~3 hours later
([#61001](https://github.com/airbytehq/airbyte/issues/61001)). A plain cursor on
`datetime` extracts most conversions *before* their attribution exists and, the
event being immutable, never revisits them — the attribution is lost
permanently while the table looks fine.

Three declarations fix it, and they only work together:

1. **`lookback: 3d`** — every run re-walks the trailing window.
2. **`write_disposition: merge`** on the event id — the re-fetch *updates* the
   row, overwriting a NULL attribution in place.
3. **`ordered: false`** — the walk is newest-first and revisits older values, so
   a mid-run state flush must not advance the cursor to the observed maximum.
   With `ordered: true` a crash partway would commit the newest `datetime` seen
   and permanently skip every older event in the window.

Widen `lookback` if the delay grows; merge makes re-walking idempotent, so the
only cost is API calls.

The events upper bound is fixed one minute before the run starts. This
leaves room for clock differences that otherwise cause Klaviyo to reject the
query as future-dated. The newest minute is deferred to the next sync and
recovered by the normal lookback; the bound remains identical across metrics.

## Streams

| stream | disposition | notes |
|---|---|---|
| `events` | merge on `id` | The fact table. Attribution + inline profile identity. Scope with `metric_ids`. |
| `profiles` | merge on `id` | Incremental on `updated`, ascending (`ordered: true`). Consent + predictive analytics. |
| `metrics` | replace | `integration_name` separates native-integration metrics (Stripe, Shopify) from ones you send. |
| `flows` | replace | |
| `campaigns` | replace | Two passes per channel — Klaviyo *requires* a channel filter, and hides archived campaigns unless asked. |
| `campaign_messages` | replace | From `campaigns?include=campaign-messages`; there is no `GET /campaign-messages` list endpoint (404). |
| `flow_messages` | replace | **Opt-in.** One GET per flow action. `max_flows_per_run` is a smoke-test valve, not a resume: this stream has no cursor, so a cap narrows the whole replaced snapshot to the first N flows. |
| `segments` | replace | Carries `definition` — the audience logic itself. |
| `lists` | replace | |
| `list_memberships` | replace | **Opt-in.** Who is on each list, with `external_id` for direct customer joins. Point-in-time snapshot. |
| `segment_memberships` | replace | **Opt-in.** The computed-audience counterpart. |
| `templates` | replace | **Opt-in** — lands full rendered HTML. |
| `tags` | replace | What each tag is applied to (campaigns / flows / lists / segments). |
| `forms` | replace | Signup capture — the top of the funnel. |
| `account` | replace | One row. Carries the account **timezone**, which the UI reports in. |
| `campaign_reports` | replace | 1:1 with the Klaviyo UI. Needs `conversion_metric_id`. |
| `flow_reports` | replace | Same, per flow message. |
| `segment_reports` | replace | Membership statistics over the timeframe. |

Catalog streams carry no cursor and `replace` every run, so they always hold the
**full history** — including campaigns archived years ago, which is what lets an
old attributed event resolve to a name.

## Configuration

```yaml
name: klaviyo_bq
source: klaviyo
destination: bigquery
target: prod
destination_params:
  dataset: klaviyo
params:
  metric_ids: "T7Ywek"          # scope the firehose
  conversion_metric_id: "T7Ywek" # required by the reporting streams
streams:
  events:
    since: "2026-06-01T00:00:00Z"
  metrics:
  flows:
  campaigns:
  campaign_messages:
  segments:
  tags:
  forms:
  account:
  lists:
  profiles:
  campaign_reports:
  flow_reports:
  segment_reports:
  # flow_messages:        opt in — one GET per flow action
  # templates:            opt in — full HTML
  # list_memberships:     opt in — one walk per list
  # segment_memberships:  opt in — one walk per segment
```

Auth is a private API key (`pk_...`) with read scope on events, metrics, flows,
campaigns, lists, segments, templates, forms and profiles, read from
`${env.KLAVIYO_API_KEY}` by default; override with a `secret://` ref per
environment. The reporting endpoints additionally need `campaigns:read`,
`flows:read`, `segments:read` and `forms:read`.

`api_revision` pins the `revision` header. Klaviyo ties response *shape* to that
date, so bump it deliberately — never leave it floating.

## Rate limits

Klaviyo publishes burst and steady budgets per endpoint and returns them in
`RateLimit-Limit` (e.g. `350, 350;w=1, 3500;w=60`). The client honours
`Retry-After` on a 429 and a token bucket holds steady traffic at
`requests_per_second` (default 8 — deliberately polite). The reporting
endpoints answer 503 with `Retry-After` during outages; the same retry path
handles it.

## Joining to payments

For Stripe-sourced metrics, `event_id` is the **charge id** — `ch_...`, or
`py_...` for older charges. Both forms appear; a join filtering on `ch_` alone
silently drops the legacy ones. `invoice_id` (`in_...`) and `payment_intent`
(`pi_...`) are also promoted, and `value` is the invoice total **after
discount**, gross of refunds (refunds arrive under a separate metric).

For identity, `profile_external_id` on the event row is your own user id,
carried inline via `include=profile` — so an event-to-customer join does not
depend on the `profiles` stream having caught up.

## A caveat worth knowing

`tracking_options.add_utm` on a campaign, and `add_tracking_params` on a flow
message, can be **false**. Those messages' links carry no UTMs at all, so
revenue Klaviyo attributes to them can never appear in a UTM-based channel
model, no matter how the warehouse is queried. Dunning/transactional flows are
the common case. Both columns are landed so the gap is measurable rather than
mysterious.


Profile records preserve their complete `attributes`, `relationships`, and
top-level `links` JSON alongside typed identity, consent, and prediction fields.
Existing incremental destinations receive these payloads when each profile is
next fetched; replay a bounded history window if older rows need enrichment.
