# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Klaviyo source — the ``@stream`` functions the engine discovers and runs.

Three shapes, all built on :class:`KlaviyoClient`:

* :func:`events` — the fact table. Walks ``GET /events`` newest-first per
  metric with ``?include=attributions`` and folds each page's ``included``
  block onto its records, so the flow / campaign / message Klaviyo credits a
  conversion to lands as columns on the event row. This join is the whole
  point of the connector: Klaviyo removed ``$attribution`` from
  ``event_properties`` in revision 2024-02-15 and serves it only as a
  sidecar, which Airbyte's connector drops on the floor.

* Catalog streams — :func:`metrics`, :func:`flows`, :func:`campaigns`,
  :func:`lists`, :func:`campaign_messages`: one walk each, ``replace``.

* :func:`flow_messages` — a per-flow-action fan-out, opt-in via a config's
  ``streams:`` block because it costs one GET per action.

* :func:`profiles` — incremental on ``updated``, ascending, so
  ``ordered: true`` holds.

docs/03 §3.1 — a ``@stream`` yields batches (``list[dict]``), not records.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from dtex import Batch, Config, Cursor, StreamDef, stream
from dtex.sources.klaviyo.client import KlaviyoClient

# --------------------------------------------------------------------------
# Client construction
# --------------------------------------------------------------------------


def _build_client(
    config: Config, log: logging.Logger | logging.LoggerAdapter[Any]
) -> KlaviyoClient:
    """Construct a :class:`KlaviyoClient` from ``config`` — single construction site."""
    return KlaviyoClient(
        api_key=config.secrets["api_key"],
        base_url=str(config.get("base_url") or "https://a.klaviyo.com/api"),
        api_revision=str(config.get("api_revision") or "2026-07-15"),
        max_retries=int(config.get("max_retries") if config.get("max_retries") is not None else 5),
        retry_backoff_seconds=float(config.get("retry_backoff_seconds") or 1.0),
        requests_per_second=float(config.get("requests_per_second") or 8.0),
        rate_limit_max_wait_seconds=float(config.get("rate_limit_max_wait_seconds") or 60.0),
        timeout_seconds=float(config.get("timeout_seconds") or 60.0),
        log=log,
    )


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _declared_columns(stream_def: StreamDef) -> list[str]:
    return [f.name for f in (stream_def.schema or [])]


def _project(record: Mapping[str, Any], columns: Sequence[str]) -> dict[str, Any]:
    """Keep the declared columns only; nested values stay as-is (JSON columns)."""
    return {col: record.get(col) for col in columns}


def _csv(raw: Any) -> list[str]:
    return [part.strip() for part in str(raw or "").split(",") if part.strip()]


# Klaviyo's `page[size]` ceiling differs PER ENDPOINT, and exceeding it is a
# hard 400 ("Page size must be an integer between 1 and 10"), not a clamp.
# Measured against the live API 2026-09-11. `metrics` rejects the parameter
# outright, so it is absent here and must never be sent one.
_PAGE_SIZE_MAX: dict[str, int] = {
    "lists/": 10,
    "segments/": 10,
    "templates/": 10,
    "flows/": 50,
    "tags/": 50,
    "campaigns/": 100,
    "forms/": 100,
    "profiles/": 100,
    "events/": 1000,
}


def _page_size(path: str, requested: Any, default: int = 50) -> int:
    """Clamp a configured page size to the endpoint's real ceiling.

    A config asking for 50 lists is not an error to surface at the user — the
    ceiling is Klaviyo's, not theirs — so it is silently reduced to what the
    endpoint accepts. Without this, one wrong number fails a whole run
    mid-flight (and did).
    """
    try:
        value = int(requested)
    except (TypeError, ValueError):
        value = default
    ceiling = _PAGE_SIZE_MAX.get(path, default)
    return max(1, min(value, ceiling))


def _rel_id(relationships: Mapping[str, Any], key: str) -> str | None:
    """``relationships[key].data.id``, or None when the relationship is absent."""
    rel = relationships.get(key)
    if not isinstance(rel, dict):
        return None
    data = rel.get("data")
    if not isinstance(data, dict):
        return None
    value = data.get("id")
    return str(value) if value is not None else None


def _nested(payload: Mapping[str, Any], *path: str) -> Any:
    """Walk a nested mapping, returning None the moment a level is missing."""
    node: Any = payload
    for key in path:
        if not isinstance(node, Mapping):
            return None
        node = node.get(key)
    return node


def _parse_promotions(raw: Any) -> list[tuple[str, tuple[str, ...]]]:
    """Parse ``promote_properties`` into ``(column, path)`` pairs.

    Format is ``column=json.path`` entries, comma separated::

        charge_id=$event_id,invoice_id=Invoice.ID,pi=$extra.PaymentIntent

    Dots separate levels; a leading ``$`` is part of Klaviyo's own key name
    (``$event_id``), not syntax. An entry without ``=`` promotes the path's
    last segment under its own name.
    """
    promotions: list[tuple[str, tuple[str, ...]]] = []
    for entry in _csv(raw):
        column, _, path = entry.partition("=")
        if not path:
            path, column = column, column.split(".")[-1]
        segments = tuple(part for part in path.split(".") if part)
        if column and segments:
            promotions.append((column, segments))
    return promotions


def _rel_ids(relationships: Mapping[str, Any], key: str) -> list[str]:
    """Every ``relationships[key].data[].id`` — the to-many counterpart of _rel_id."""
    rel = relationships.get(key)
    if not isinstance(rel, dict):
        return []
    data = rel.get("data")
    if not isinstance(data, list):
        return []
    return [str(e["id"]) for e in data if isinstance(e, dict) and e.get("id") is not None]


def _utc_now() -> str:
    """Now, as the RFC 3339 string the declared TIMESTAMP columns coerce from."""
    return datetime.now(tz=UTC).isoformat()


_NO_CONSENT: dict[str, Any] = {
    "email_marketing_consent": None,
    "email_marketing_can_receive": None,
    "email_marketing_consent_timestamp": None,
    "email_marketing_method": None,
    "email_marketing_suppressions": None,
    "sms_marketing_consent": None,
    "sms_marketing_can_receive": None,
    "sms_marketing_consent_timestamp": None,
    "subscriptions": None,
}


def _consent_columns(subscriptions: Any) -> dict[str, Any]:
    """Flatten the `subscriptions` object into the consent columns.

    Only present when the request asked for
    ``additional-fields[profile]=subscriptions``; a default /profiles response
    omits it entirely, which is why every column here would otherwise be NULL.

    ``can_receive_email_marketing`` is Klaviyo's own verdict and the one to
    trust: a profile can be ``SUBSCRIBED`` and still unreachable because a
    hard bounce or spam complaint put it under suppression.
    """
    if not isinstance(subscriptions, Mapping):
        return dict(_NO_CONSENT)
    email_marketing = _nested(subscriptions, "email", "marketing") or {}
    sms_marketing = _nested(subscriptions, "sms", "marketing") or {}
    return {
        "email_marketing_consent": email_marketing.get("consent"),
        "email_marketing_can_receive": email_marketing.get("can_receive_email_marketing"),
        "email_marketing_consent_timestamp": email_marketing.get("consent_timestamp"),
        "email_marketing_method": email_marketing.get("method"),
        "email_marketing_suppressions": email_marketing.get("suppression"),
        "sms_marketing_consent": sms_marketing.get("consent"),
        "sms_marketing_can_receive": sms_marketing.get("can_receive_sms_marketing"),
        "sms_marketing_consent_timestamp": sms_marketing.get("consent_timestamp"),
        "subscriptions": subscriptions,
    }


_NO_PREDICTIVE: dict[str, Any] = {
    "predicted_clv": None,
    "historic_clv": None,
    "total_clv": None,
    "churn_probability": None,
    "expected_date_of_next_order": None,
    "predictive_analytics": None,
}


def _predictive_columns(predictive: Any) -> dict[str, Any]:
    """Promote Klaviyo's predictive analytics; NULL until the account has history."""
    if not isinstance(predictive, Mapping):
        return dict(_NO_PREDICTIVE)
    return {
        "predicted_clv": predictive.get("predicted_clv"),
        "historic_clv": predictive.get("historic_clv"),
        "total_clv": predictive.get("total_clv"),
        "churn_probability": predictive.get("churn_probability"),
        "expected_date_of_next_order": predictive.get("expected_date_of_next_order"),
        "predictive_analytics": predictive,
    }


# --------------------------------------------------------------------------
# events — the attribution fold
# --------------------------------------------------------------------------


def _attribution_index(body: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Index a page's ``included`` attributions by their own id.

    An attribution object's ``attributes`` is always ``{}`` — Klaviyo exposes
    only ``id`` under ``fields[attribution]``. Everything useful is in its
    ``relationships``, in one of two shapes:

        flow-attributed:     event, attributed-event, flow, flow-message
        campaign-attributed: event, attributed-event, campaign, campaign-message

    Deeper includes (``attributions.flow``) are rejected by the API with a
    400, so the ids are as far as one request reaches; names come from the
    catalog streams.
    """
    index: dict[str, dict[str, Any]] = {}
    for item in body.get("included") or []:
        if not isinstance(item, dict) or item.get("type") != "attribution":
            continue
        rels = item.get("relationships") or {}
        if not isinstance(rels, Mapping):
            continue
        # A missing id must NOT become the string "None": every malformed
        # sidecar would then share one key, and an event whose relationship
        # is equally malformed would match it and be handed another event's
        # flow. Skip it instead.
        raw_id = item.get("id")
        if raw_id is None:
            continue
        flow_id = _rel_id(rels, "flow")
        campaign_id = _rel_id(rels, "campaign")
        channel = "flow" if flow_id else ("campaign" if campaign_id else None)
        attribution_id = str(raw_id)
        index[attribution_id] = {
            "attribution_id": attribution_id,
            "attributed_channel": channel,
            "attributed_flow_id": flow_id,
            "attributed_flow_message_id": _rel_id(rels, "flow-message"),
            "attributed_campaign_id": campaign_id,
            "attributed_campaign_message_id": _rel_id(rels, "campaign-message"),
            "attributed_event_id": _rel_id(rels, "attributed-event"),
        }
    return index


_NO_ATTRIBUTION: dict[str, Any] = {
    "attribution_id": None,
    "attributed_channel": None,
    "attributed_flow_id": None,
    "attributed_flow_message_id": None,
    "attributed_campaign_id": None,
    "attributed_campaign_message_id": None,
    "attributed_event_id": None,
}


def _profile_index(body: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Index a page's ``included`` profiles by id — the inline identity fold.

    With ``include=profile`` each event's profile comes down on the same page,
    so ``email`` / ``external_id`` land on the event row. That makes an
    event-to-customer join independent of the profiles stream having caught
    up, which matters because profiles is a separate incremental walk.
    """
    index: dict[str, dict[str, Any]] = {}
    for item in body.get("included") or []:
        if not isinstance(item, dict) or item.get("type") != "profile":
            continue
        raw_id = item.get("id")
        if raw_id is None:  # never key on the string "None" — see _attribution_index
            continue
        attrs = item.get("attributes") or {}
        index[str(raw_id)] = {
            "profile_email": attrs.get("email"),
            "profile_external_id": attrs.get("external_id"),
        }
    return index


def _event_record(
    event: Mapping[str, Any],
    attributions: Mapping[str, dict[str, Any]],
    profiles_by_id: Mapping[str, dict[str, Any]] | None = None,
    promotions: Sequence[tuple[str, tuple[str, ...]]] | None = None,
) -> dict[str, Any]:
    """One event row: universal scalars, the attribution fold, raw payloads.

    Only fields Klaviyo guarantees for every account are typed. Anything an
    integration happens to emit stays in ``event_properties`` (and in the
    full ``attributes`` copy) unless ``promotions`` — from the config's
    ``promote_properties`` — lifts it into a column.
    """
    attrs = event.get("attributes") or {}
    props = attrs.get("event_properties") or {}
    rels = event.get("relationships") or {}

    # An event links its attribution by relationship; the object itself came
    # down in `included` and was indexed by _attribution_index.
    attribution: dict[str, Any] = dict(_NO_ATTRIBUTION)
    attribution_rel = rels.get("attributions") if isinstance(rels, Mapping) else None
    if isinstance(attribution_rel, dict):
        data = attribution_rel.get("data")
        entries = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
        for entry in entries:
            if not isinstance(entry, dict) or entry.get("id") is None:
                continue
            resolved = attributions.get(str(entry["id"]))
            if resolved:
                # Klaviyo sends at most one attribution per event in practice;
                # the first resolved one wins if that ever changes.
                attribution = dict(resolved)
                break
        else:
            # The relationship named an attribution the page did not include
            # (it can be paginated out). Keep the id so the row is not silently
            # "unattributed" — a NULL channel with a non-NULL id is the tell.
            for entry in entries:
                if isinstance(entry, dict) and entry.get("id") is not None:
                    attribution["attribution_id"] = str(entry["id"])
                    break

    record: dict[str, Any] = {
        "id": str(event.get("id")),
        "datetime": attrs.get("datetime"),
        "timestamp": attrs.get("timestamp"),
        "uuid": attrs.get("uuid"),
        "metric_id": _rel_id(rels, "metric") if isinstance(rels, Mapping) else None,
        "profile_id": _rel_id(rels, "profile") if isinstance(rels, Mapping) else None,
        # Klaviyo's OWN reserved properties — universal across accounts.
        "event_id": props.get("$event_id"),
        "value": props.get("$value"),
        "value_currency": props.get("$value_currency") or props.get("Currency"),
        # Full fidelity. `attributes` carries event_properties too; both are
        # kept so an Airbyte-shaped JSON_VALUE(attributes, ...) extraction
        # survives a repoint unchanged.
        "event_properties": props,
        "attributes": attrs,
        "relationships": rels,
        "profile_email": None,
        "profile_external_id": None,
    }
    # Account-specific fields the CONFIG asked for. Nothing is promoted by
    # default: event_properties is arbitrary per integration, so a built-in
    # list would be right for one account and wrong for the rest.
    for column, path in promotions or ():
        record[column] = _nested(props, *path)
    profile_id = record["profile_id"]
    if profiles_by_id and profile_id:
        record.update(profiles_by_id.get(str(profile_id)) or {})
    record.update(attribution)
    return record


@stream(name="events")
def events(
    stream_def: StreamDef, config: Config, cursor: Cursor, log: logging.Logger
) -> Iterator[Batch]:
    """Events with their attribution folded on — ``GET /events?include=attributions``.

    One pass per configured metric. Klaviyo's ``filter`` on this endpoint
    supports ``equals(metric_id, ...)`` and ``greater-or-equal(datetime, ...)``
    but not ``in(...)`` over metrics, so several metrics means several passes.

    The walk is newest-first. Combined with the declared ``lookback`` and
    ``merge`` on the event id, a run re-visits the trailing window and
    UPDATES rows whose attribution has since been written — the whole point,
    since Klaviyo events are immutable and attribution lands hours late.
    That same re-visiting is why the stream declares ``ordered: false``:
    observed values fall as the walk goes, so only a completed stream may
    advance the cursor.
    """
    batch_size = max(1, int(config.get("batch_size") or 2000))
    page_size = max(1, min(int(config.get("page_size") or 200), 1000))
    metric_ids = _csv(config.get("metric_ids"))
    start = cursor.start_value()

    # Promoted columns are not in register.yaml — they are whatever THIS
    # account's config asked for — so they must be added to the projection
    # explicitly. Without this _project drops every one of them and the
    # feature silently does nothing.
    promotions = _parse_promotions(config.get("promote_properties"))
    declared = _declared_columns(stream_def)
    columns = declared + [c for c, _ in promotions if c not in declared]

    # ONE upper bound, fixed before the first request, shared by every metric
    # pass. Without it a long multi-metric run commits the newest datetime any
    # pass happened to see: metric A scanned at 09:00 and metric B at 13:00
    # commits 13:00, so A's events that arrived after its own scan — with
    # timestamps between the two — are never revisited and are lost. Bounding
    # every pass at the same instant makes the committed cursor mean "every
    # metric has been read up to here".
    # Klaviyo rejects an upper bound it considers future-dated. Leave a
    # minute of headroom for clock differences between the runner and API.
    # The next incremental lookback fetches this deferred tail; every metric
    # still shares one fixed ceiling for this run.
    run_ceiling = (datetime.fromisoformat(_utc_now()) - timedelta(minutes=1)).isoformat()
    filters: list[str] = [f"less-than(datetime,{_as_iso(run_ceiling)})"]
    if start:
        filters.append(f"greater-or-equal(datetime,{_as_iso(start)})")

    with_profile = bool(config.get("event_include_profile", True))
    includes = ["attributions"] + (["profile"] if with_profile else [])

    log.info(
        "klaviyo.events: %s from %s (page_size=%d, include=%s)",
        f"{len(metric_ids)} metric(s)" if metric_ids else "ALL metrics",
        start or "the beginning",
        page_size,
        ",".join(includes),
    )

    total = attributed = 0
    batch: list[dict[str, Any]] = []
    # An empty `metric_ids` means "every metric": one unfiltered pass.
    scopes: list[str | None] = list(metric_ids) if metric_ids else [None]
    if promotions:
        log.info(
            "klaviyo.events: promoting %d event property path(s): %s",
            len(promotions), ", ".join(c for c, _ in promotions),
        )
    with _build_client(config, log) as client:
        for metric_id in scopes:
            scoped = list(filters)
            if metric_id:
                scoped.append(f'equals(metric_id,"{metric_id}")')
            params: dict[str, Any] = {
                "include": ",".join(includes),
                "page[size]": page_size,
                "sort": "-datetime",
            }
            if with_profile:
                # Only the identity fields — the full profile would bloat
                # every page for no gain (the profiles stream carries the rest).
                params["fields[profile]"] = "email,external_id"
            if scoped:
                params["filter"] = ",".join(scoped)

            pages = 0
            for body in client.pages("events/", params):
                pages += 1
                attributions = _attribution_index(body)
                profiles_by_id = _profile_index(body) if with_profile else None
                for event in body.get("data") or []:
                    if not isinstance(event, dict):
                        continue
                    record = _event_record(
                        event, attributions, profiles_by_id, promotions
                    )
                    cursor.observe(record["datetime"])
                    if record.get("attribution_id"):
                        attributed += 1
                    batch.append(_project(record, columns))
                    total += 1
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []
                if pages % 25 == 0:
                    log.info(
                        "klaviyo.events: metric=%s pages=%d records=%d attributed=%d",
                        metric_id or "*", pages, total, attributed,
                    )
            log.info(
                "klaviyo.events: metric=%s complete pages=%d records=%d attributed=%d",
                metric_id or "*", pages, total, attributed,
            )

    if batch:
        yield batch
    log.info(
        "klaviyo.events: extract complete records=%d attributed=%d (%.1f%%)",
        total, attributed, (100.0 * attributed / total) if total else 0.0,
    )


def _as_iso(value: Any) -> str:
    """Render a cursor value as the ISO-8601 string Klaviyo's filter expects."""
    text = str(value)
    # The engine hands back a datetime for a timestamp cursor; Klaviyo wants
    # RFC 3339 and rejects a space separator.
    return text.replace(" ", "T")


# --------------------------------------------------------------------------
# Catalogs
# --------------------------------------------------------------------------


def _emit_stream(
    records: Iterator[dict[str, Any]], columns: Sequence[str], batch_size: int
) -> Iterator[Batch]:
    """Batch an ITERATOR of records — for streams too large to buffer whole.

    Same empty-snapshot contract as :func:`_emit`: a legitimately empty
    snapshot still yields one empty batch so a `replace` truncates.
    """
    batch: list[dict[str, Any]] = []
    emitted = False
    for record in records:
        batch.append(_project(record, columns))
        if len(batch) >= batch_size:
            yield batch
            emitted = True
            batch = []
    if batch:
        yield batch
    elif not emitted:
        yield []


def _emit(
    records: list[dict[str, Any]], columns: Sequence[str], batch_size: int
) -> Iterator[Batch]:
    """Yield ``records`` in batches — ALWAYS at least one, even when empty.

    A `replace` stream that yields nothing never reaches ``write_batch``, so
    the destination keeps the previous snapshot forever: a list deleted in
    Klaviyo, or an audience that emptied, would linger in the warehouse
    looking current. One empty batch is what tells the engine "the snapshot
    is legitimately empty — truncate".

    This is distinct from a stream that `return`s before calling `_emit`
    (e.g. the reporting streams with no conversion_metric_id configured):
    that is "not fetched", not "fetched and empty", and must stay a no-op.
    """
    if not records:
        yield []
        return
    batch: list[dict[str, Any]] = []
    for record in records:
        batch.append(_project(record, columns))
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


@stream(name="metrics")
def metrics(stream_def: StreamDef, config: Config, log: logging.Logger) -> Iterator[Batch]:
    """Every metric (event type) with the integration that emits it — ``GET /metrics``."""
    columns = _declared_columns(stream_def)
    out: list[dict[str, Any]] = []
    with _build_client(config, log) as client:
        # /metrics rejects page[size] — it is a small, unpaginated catalog.
        for body in client.pages("metrics/"):
            for item in body.get("data") or []:
                attrs = item.get("attributes") or {}
                integration = attrs.get("integration") or {}
                out.append(
                    {
                        "id": str(item.get("id")),
                        "name": attrs.get("name"),
                        "created": attrs.get("created"),
                        "updated": attrs.get("updated"),
                        "integration_id": integration.get("id"),
                        "integration_name": integration.get("name"),
                        "integration_category": integration.get("category"),
                        "integration_object": integration.get("object"),
                        "attributes": attrs,
                        "relationships": item.get("relationships") or {},
                    }
                )
    log.info("klaviyo.metrics: %d metric(s)", len(out))
    yield from _emit(out, columns, max(1, int(config.get("batch_size") or 2000)))


@stream(name="flows")
def flows(stream_def: StreamDef, config: Config, log: logging.Logger) -> Iterator[Batch]:
    """Every flow — ``GET /flows``."""
    columns = _declared_columns(stream_def)
    page_size = _page_size("flows/", config.get("catalog_page_size"))
    out: list[dict[str, Any]] = []
    with _build_client(config, log) as client:
        for body in client.pages("flows/", {"page[size]": page_size}):
            for item in body.get("data") or []:
                attrs = item.get("attributes") or {}
                out.append(
                    {
                        "id": str(item.get("id")),
                        "name": attrs.get("name"),
                        "status": attrs.get("status"),
                        "archived": attrs.get("archived"),
                        "trigger_type": attrs.get("trigger_type"),
                        "created": attrs.get("created"),
                        "updated": attrs.get("updated"),
                        "attributes": attrs,
                        "relationships": item.get("relationships") or {},
                    }
                )
    log.info("klaviyo.flows: %d flow(s)", len(out))
    yield from _emit(out, columns, max(1, int(config.get("batch_size") or 2000)))


def _campaign_records(
    client: KlaviyoClient, channels: Sequence[str], page_size: int
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Walk /campaigns per channel; return (campaigns, campaign_messages).

    Klaviyo REQUIRES a channel filter on /campaigns, so each channel is its
    own pass. ``include=campaign-messages`` brings the message definitions
    down with the campaign — there is no GET /campaign-messages list
    endpoint (it 404s), so this is the only way to catalog them in bulk.
    """
    campaigns: list[dict[str, Any]] = []
    messages: list[dict[str, Any]] = []
    # Klaviyo EXCLUDES archived campaigns unless the filter names them, so an
    # unqualified walk silently loses the oldest sends — and an attributed
    # event can reference a campaign archived years ago. Two passes per
    # channel is the only way to see the whole history.
    for channel in channels:
        for archived in ("false", "true"):
            for body in client.pages(
                "campaigns/",
                {
                    "filter": (
                        f'and(equals(messages.channel,"{channel}"),'
                        f"equals(archived,{archived}))"
                    ),
                    "include": "campaign-messages",
                    "page[size]": page_size,
                },
            ):
                for item in body.get("data") or []:
                    attrs = item.get("attributes") or {}
                    campaigns.append(
                        {
                            "id": str(item.get("id")),
                            "name": attrs.get("name"),
                            "channel": channel,
                            "status": attrs.get("status"),
                            "archived": attrs.get("archived"),
                            "created_at": attrs.get("created_at"),
                            "updated_at": attrs.get("updated_at"),
                            "scheduled_at": attrs.get("scheduled_at"),
                            "send_time": attrs.get("send_time"),
                            "audiences": attrs.get("audiences"),
                            "send_options": attrs.get("send_options"),
                            "send_strategy": attrs.get("send_strategy"),
                            "tracking_options": attrs.get("tracking_options"),
                        }
                    )
                # Map each included message back to its owning campaign.
                owner: dict[str, str] = {}
                for item in body.get("data") or []:
                    for entry in (
                        _nested(item, "relationships", "campaign-messages", "data") or []
                    ):
                        if isinstance(entry, dict) and entry.get("id") is not None:
                            owner[str(entry["id"])] = str(item.get("id"))
                for item in body.get("included") or []:
                    if not isinstance(item, dict) or item.get("type") != "campaign-message":
                        continue
                    attrs = item.get("attributes") or {}
                    definition = attrs.get("definition") or {}
                    content = definition.get("content") or {}
                    message_id = str(item.get("id"))
                    messages.append(
                        {
                            "id": message_id,
                            "campaign_id": owner.get(message_id),
                            "channel": definition.get("channel") or channel,
                            "label": definition.get("label"),
                            "subject": content.get("subject"),
                            "preview_text": content.get("preview_text"),
                            "from_email": content.get("from_email"),
                            "from_label": content.get("from_label"),
                            "send_times": attrs.get("send_times"),
                            "definition": definition,
                        }
                    )
    return campaigns, messages


@stream(name="campaigns")
def campaigns(stream_def: StreamDef, config: Config, log: logging.Logger) -> Iterator[Batch]:
    """Every campaign, per channel — ``GET /campaigns?filter=equals(messages.channel,...)``."""
    columns = _declared_columns(stream_def)
    channels = _csv(config.get("campaign_channels")) or ["email"]
    page_size = _page_size("campaigns/", config.get("catalog_page_size"))
    with _build_client(config, log) as client:
        records, _ = _campaign_records(client, channels, page_size)
    log.info("klaviyo.campaigns: %d campaign(s) across %s", len(records), ",".join(channels))
    yield from _emit(records, columns, max(1, int(config.get("batch_size") or 2000)))


@stream(name="campaign_messages")
def campaign_messages(
    stream_def: StreamDef, config: Config, log: logging.Logger
) -> Iterator[Batch]:
    """Message definitions behind each campaign — the names for an attributed message id."""
    columns = _declared_columns(stream_def)
    channels = _csv(config.get("campaign_channels")) or ["email"]
    page_size = _page_size("campaigns/", config.get("catalog_page_size"))
    with _build_client(config, log) as client:
        _, records = _campaign_records(client, channels, page_size)
    log.info("klaviyo.campaign_messages: %d message(s)", len(records))
    yield from _emit(records, columns, max(1, int(config.get("batch_size") or 2000)))


@stream(name="flow_messages")
def flow_messages(stream_def: StreamDef, config: Config, log: logging.Logger) -> Iterator[Batch]:
    """Flow message definitions — flow -> flow-actions -> messages.

    Opt-in: one GET per flow action, so a config selects it deliberately.

    ``max_flows_per_run`` is a SAFETY VALVE, not a resume mechanism. This is a
    `replace` stream with no cursor, so every run rewrites the whole table
    from the first flow: capping it does not extend coverage across runs, it
    narrows the snapshot to the first N flows and drops everything else.
    Leave it at 0 for real use; set it only to smoke-test the walk.
    """
    columns = _declared_columns(stream_def)
    page_size = _page_size("flows/", config.get("catalog_page_size"))
    cap = int(config.get("max_flows_per_run") or 0)
    out: list[dict[str, Any]] = []
    walked = 0
    if cap:
        log.warning(
            "klaviyo.flow_messages: max_flows_per_run=%d — this REPLACES the table "
            "with only the first %d flow(s); it does not resume across runs",
            cap, cap,
        )
    with _build_client(config, log) as client:
        flow_ids: list[str] = []
        for body in client.pages("flows/", {"page[size]": page_size}):
            for item in body.get("data") or []:
                if item.get("id") is not None:
                    flow_ids.append(str(item["id"]))
        for flow_id in flow_ids:
            if cap and walked >= cap:
                log.info("klaviyo.flow_messages: cap %d flows reached", cap)
                break
            walked += 1
            # BOTH nested levels paginate (flow-actions returns links.next at
            # page size 2 on a real flow). A single get() here silently
            # truncated every flow to one page of actions.
            actions_data: list[dict[str, Any]] = []
            for actions_page in client.pages(
                f"flows/{flow_id}/flow-actions/", {"page[size]": page_size}
            ):
                actions_data.extend(
                    a for a in actions_page.get("data") or [] if isinstance(a, dict)
                )
            for action in actions_data:
                if action.get("id") is None:
                    continue
                action_id = str(action["id"])
                messages_data: list[dict[str, Any]] = []
                for messages_page in client.pages(
                    f"flow-actions/{action_id}/flow-messages/"
                ):
                    messages_data.extend(
                        m for m in messages_page.get("data") or [] if isinstance(m, dict)
                    )
                for message in messages_data:
                    attrs = message.get("attributes") or {}
                    # A flow message carries no `name`; everything descriptive
                    # lives under `definition` (verified against the live API —
                    # subject_line, not subject, and channel is "Email"/"SMS").
                    definition = attrs.get("definition") or {}
                    out.append(
                        {
                            "id": str(message.get("id")),
                            "flow_id": flow_id,
                            "flow_action_id": action_id,
                            "channel": attrs.get("channel"),
                            "subject_line": definition.get("subject_line"),
                            "preview_text": definition.get("preview_text"),
                            "from_email": definition.get("from_email"),
                            "from_label": definition.get("from_label"),
                            "template_id": definition.get("template_id"),
                            "transactional": definition.get("transactional"),
                            "smart_sending_enabled": definition.get("smart_sending_enabled"),
                            "add_tracking_params": definition.get("add_tracking_params"),
                            "created": attrs.get("created"),
                            "updated": attrs.get("updated"),
                            "definition": definition,
                        }
                    )
    log.info("klaviyo.flow_messages: %d message(s) from %d flow(s)", len(out), walked)
    yield from _emit(out, columns, max(1, int(config.get("batch_size") or 2000)))


@stream(name="lists")
def lists(stream_def: StreamDef, config: Config, log: logging.Logger) -> Iterator[Batch]:
    """Every list — ``GET /lists``."""
    columns = _declared_columns(stream_def)
    page_size = _page_size("lists/", config.get("catalog_page_size"))
    out: list[dict[str, Any]] = []
    with _build_client(config, log) as client:
        for body in client.pages("lists/", {"page[size]": page_size}):
            for item in body.get("data") or []:
                attrs = item.get("attributes") or {}
                out.append(
                    {
                        "id": str(item.get("id")),
                        "name": attrs.get("name"),
                        "opt_in_process": attrs.get("opt_in_process"),
                        "created": attrs.get("created"),
                        "updated": attrs.get("updated"),
                        "attributes": attrs,
                        "relationships": item.get("relationships") or {},
                    }
                )
    log.info("klaviyo.lists: %d list(s)", len(out))
    yield from _emit(out, columns, max(1, int(config.get("batch_size") or 2000)))


# --------------------------------------------------------------------------
# Audiences, creative, organisation
# --------------------------------------------------------------------------


@stream(name="segments")
def segments(stream_def: StreamDef, config: Config, log: logging.Logger) -> Iterator[Batch]:
    """Every segment, with the audience logic that defines it — ``GET /segments``."""
    columns = _declared_columns(stream_def)
    page_size = _page_size("segments/", config.get("catalog_page_size"))
    out: list[dict[str, Any]] = []
    with _build_client(config, log) as client:
        for body in client.pages("segments/", {"page[size]": page_size}):
            for item in body.get("data") or []:
                attrs = item.get("attributes") or {}
                out.append(
                    {
                        "id": str(item.get("id")),
                        "name": attrs.get("name"),
                        "is_active": attrs.get("is_active"),
                        "is_processing": attrs.get("is_processing"),
                        "is_starred": attrs.get("is_starred"),
                        "created": attrs.get("created"),
                        "updated": attrs.get("updated"),
                        "definition": attrs.get("definition"),
                        "attributes": attrs,
                        "relationships": item.get("relationships") or {},
                    }
                )
    log.info("klaviyo.segments: %d segment(s)", len(out))
    yield from _emit(out, columns, max(1, int(config.get("batch_size") or 2000)))


def _membership_rows(
    client: KlaviyoClient,
    parent_path: str,
    parent_key: str,
    parent_ids: Sequence[str],
    page_size: int,
    now: str,
    log: logging.Logger,
    cap: int = 0,
) -> Iterator[dict[str, Any]]:
    """Yield one row per (parent, profile) — the membership snapshot.

    Membership has no cursor: Klaviyo exposes who is in an audience *now*,
    not a change log, so the stream is a `replace` snapshot stamped with
    ``extracted_at``.

    A GENERATOR on purpose. Accumulating every membership across every
    audience before yielding is unbounded — a large account's lists and
    segments run to millions of rows and `batch_size` does not bound a list
    built before the first yield. Streaming keeps peak memory at one batch.
    """
    walked = 0
    for parent_id in parent_ids:
        if cap and walked >= cap:
            log.info("klaviyo: membership cap %d reached on %s", cap, parent_path)
            break
        walked += 1
        for body in client.pages(
            f"{parent_path}/{parent_id}/profiles/",
            {"page[size]": page_size, "fields[profile]": "email,external_id"},
        ):
            for item in body.get("data") or []:
                if not isinstance(item, dict) or item.get("id") is None:
                    continue
                attrs = item.get("attributes") or {}
                yield {
                    parent_key: parent_id,
                    "profile_id": str(item["id"]),
                    "email": attrs.get("email"),
                    "external_id": attrs.get("external_id"),
                    "extracted_at": now,
                }


def _catalog_ids(client: KlaviyoClient, path: str, page_size: int) -> list[str]:
    ids: list[str] = []
    for body in client.pages(path, {"page[size]": page_size}):
        for item in body.get("data") or []:
            if item.get("id") is not None:
                ids.append(str(item["id"]))
    return ids


@stream(name="list_memberships")
def list_memberships(
    stream_def: StreamDef, config: Config, log: logging.Logger
) -> Iterator[Batch]:
    """Who is on each list — one GET per list page. Point-in-time snapshot."""
    columns = _declared_columns(stream_def)
    page_size = _page_size("profiles/", config.get("profiles_page_size"), default=100)
    catalog_size = _page_size("lists/", config.get("catalog_page_size"))
    now = _utc_now()
    with _build_client(config, log) as client:
        ids = _catalog_ids(client, "lists/", catalog_size)
        log.info("klaviyo.list_memberships: walking %d list(s)", len(ids))
        rows = _membership_rows(
            client, "lists", "list_id", ids, page_size, now, log,
            int(config.get("membership_max_lists") or 0),
        )
        yield from _emit_stream(rows, columns, max(1, int(config.get("batch_size") or 2000)))


@stream(name="segment_memberships")
def segment_memberships(
    stream_def: StreamDef, config: Config, log: logging.Logger
) -> Iterator[Batch]:
    """Who is in each segment — the computed-audience counterpart to lists."""
    columns = _declared_columns(stream_def)
    page_size = _page_size("profiles/", config.get("profiles_page_size"), default=100)
    catalog_size = _page_size("segments/", config.get("catalog_page_size"))
    now = _utc_now()
    with _build_client(config, log) as client:
        ids = _catalog_ids(client, "segments/", catalog_size)
        log.info("klaviyo.segment_memberships: walking %d segment(s)", len(ids))
        rows = _membership_rows(
            client, "segments", "segment_id", ids, page_size, now, log,
            int(config.get("membership_max_segments") or 0),
        )
        yield from _emit_stream(rows, columns, max(1, int(config.get("batch_size") or 2000)))


@stream(name="templates")
def templates(stream_def: StreamDef, config: Config, log: logging.Logger) -> Iterator[Batch]:
    """Email templates, including rendered HTML — ``GET /templates``."""
    columns = _declared_columns(stream_def)
    page_size = _page_size("templates/", config.get("catalog_page_size"))
    out: list[dict[str, Any]] = []
    with _build_client(config, log) as client:
        for body in client.pages("templates/", {"page[size]": page_size}):
            for item in body.get("data") or []:
                attrs = item.get("attributes") or {}
                out.append(
                    {
                        "id": str(item.get("id")),
                        "name": attrs.get("name"),
                        "editor_type": attrs.get("editor_type"),
                        "html": attrs.get("html"),
                        "text": attrs.get("text"),
                        "created": attrs.get("created"),
                        "updated": attrs.get("updated"),
                        "attributes": attrs,
                        "relationships": item.get("relationships") or {},
                    }
                )
    log.info("klaviyo.templates: %d template(s)", len(out))
    yield from _emit(out, columns, max(1, int(config.get("batch_size") or 2000)))


@stream(name="tags")
def tags(stream_def: StreamDef, config: Config, log: logging.Logger) -> Iterator[Batch]:
    """Tags with what they are applied to — ``GET /tags`` + relationship endpoints.

    ``include=campaigns,flows,lists,segments`` is REJECTED on this resource
    ("include is not currently supported for the requested operation"); Get
    Tags accepts only ``tag-group``. The associations therefore come from the
    per-tag relationship endpoints, which return id-only linkage documents —
    four small GETs per tag, and tags are few.
    """
    columns = _declared_columns(stream_def)
    page_size = _page_size("tags/", config.get("catalog_page_size"))
    out: list[dict[str, Any]] = []
    associations = ("campaigns", "flows", "lists", "segments")
    with _build_client(config, log) as client:
        tag_rows: list[tuple[str, str | None]] = []
        for body in client.pages("tags/", {"page[size]": page_size}):
            for item in body.get("data") or []:
                if item.get("id") is None:
                    continue
                tag_rows.append(
                    (str(item["id"]), (item.get("attributes") or {}).get("name"))
                )
        for tag_id, name in tag_rows:
            linked: dict[str, list[str]] = {}
            for kind in associations:
                ids: list[str] = []
                for body in client.pages(f"tags/{tag_id}/relationships/{kind}/"):
                    for entry in body.get("data") or []:
                        if isinstance(entry, dict) and entry.get("id") is not None:
                            ids.append(str(entry["id"]))
                linked[kind] = ids
            out.append(
                {
                    "id": tag_id,
                    "name": name,
                    "campaign_ids": linked["campaigns"],
                    "flow_ids": linked["flows"],
                    "list_ids": linked["lists"],
                    "segment_ids": linked["segments"],
                }
            )
    log.info("klaviyo.tags: %d tag(s)", len(out))
    yield from _emit(out, columns, max(1, int(config.get("batch_size") or 2000)))


@stream(name="forms")
def forms(stream_def: StreamDef, config: Config, log: logging.Logger) -> Iterator[Batch]:
    """Signup forms — the top of the email funnel. ``GET /forms``."""
    columns = _declared_columns(stream_def)
    page_size = _page_size("forms/", config.get("catalog_page_size"))
    out: list[dict[str, Any]] = []
    with _build_client(config, log) as client:
        for body in client.pages("forms/", {"page[size]": page_size}):
            for item in body.get("data") or []:
                attrs = item.get("attributes") or {}
                out.append(
                    {
                        "id": str(item.get("id")),
                        "name": attrs.get("name"),
                        "status": attrs.get("status"),
                        "ab_test": attrs.get("ab_test"),
                        "created_at": attrs.get("created_at"),
                        "updated_at": attrs.get("updated_at"),
                        "attributes": attrs,
                        "relationships": item.get("relationships") or {},
                    }
                )
    log.info("klaviyo.forms: %d form(s)", len(out))
    yield from _emit(out, columns, max(1, int(config.get("batch_size") or 2000)))


@stream(name="account")
def account(stream_def: StreamDef, config: Config, log: logging.Logger) -> Iterator[Batch]:
    """The account itself — ``GET /accounts``.

    One row. Carries the account TIMEZONE, which Klaviyo's own UI reports in:
    compare a warehouse total bucketed in UTC against the app and they
    disagree at day boundaries until this is applied.
    """
    columns = _declared_columns(stream_def)
    out: list[dict[str, Any]] = []
    with _build_client(config, log) as client:
        body = client.get("accounts/")
        for item in body.get("data") or []:
            attrs = item.get("attributes") or {}
            contact = attrs.get("contact_information") or {}
            out.append(
                {
                    "id": str(item.get("id")),
                    "organization_name": contact.get("organization_name"),
                    "timezone": attrs.get("timezone"),
                    "preferred_currency": attrs.get("preferred_currency"),
                    "industry": attrs.get("industry"),
                    "locale": attrs.get("locale"),
                    "public_api_key": attrs.get("public_api_key"),
                    "test_account": attrs.get("test_account"),
                    "contact_information": contact,
                }
            )
    yield from _emit(out, columns, max(1, int(config.get("batch_size") or 2000)))


# --------------------------------------------------------------------------
# Reporting API — 1:1 with the Klaviyo UI
# --------------------------------------------------------------------------


def _report_rows(
    client: KlaviyoClient,
    report_type: str,
    path: str,
    group_by: Sequence[str],
    key_fields: Sequence[str],
    config: Config,
    log: logging.Logger,
    conversion_metric_id: str | None,
) -> list[dict[str, Any]]:
    """POST one values report and flatten `results` into rows.

    The Reporting API is the only surface that reproduces Klaviyo's own
    numbers: the app buckets by SEND date while the raw event stream buckets
    by when the event happened, so an event-derived open rate never quite
    matches the screen the marketing team is looking at.
    """
    timeframe = str(config.get("report_timeframe_key") or "last_12_months")
    statistics = _csv(config.get("report_statistics"))
    attributes: dict[str, Any] = {
        "timeframe": {"key": timeframe},
        "statistics": statistics,
        "group_by": list(group_by),
    }
    if conversion_metric_id:
        attributes["conversion_metric_id"] = conversion_metric_id
    body = client.post(path, {"data": {"type": report_type, "attributes": attributes}})
    now = _utc_now()
    rows: list[dict[str, Any]] = []
    for result in _nested(body, "data", "attributes", "results") or []:
        if not isinstance(result, dict):
            continue
        groupings = result.get("groupings") or {}
        row: dict[str, Any] = {
            "timeframe": timeframe,
            "extracted_at": now,
            "statistics": result.get("statistics"),
        }
        if conversion_metric_id:
            row["conversion_metric_id"] = conversion_metric_id
        for field_name in key_fields:
            row[field_name] = groupings.get(field_name)
        rows.append(row)
    log.info("klaviyo.%s: %d row(s) over %s", report_type, len(rows), timeframe)
    return rows


def _require_conversion_metric(config: Config, log: logging.Logger, stream_name: str) -> str | None:
    metric = str(config.get("conversion_metric_id") or "").strip()
    if not metric:
        log.warning(
            "klaviyo.%s: no conversion_metric_id configured — the endpoint requires one, "
            "so this stream yields nothing. Set it to the metric the business counts as a "
            "conversion (e.g. a Placed Order / Successfully Paid metric id).",
            stream_name,
        )
        return None
    return metric


@stream(name="campaign_reports")
def campaign_reports(
    stream_def: StreamDef, config: Config, log: logging.Logger
) -> Iterator[Batch]:
    """Campaign performance exactly as the Klaviyo UI reports it."""
    columns = _declared_columns(stream_def)
    metric = _require_conversion_metric(config, log, "campaign_reports")
    if not metric:
        return
    with _build_client(config, log) as client:
        rows = _report_rows(
            client, "campaign-values-report", "campaign-values-reports/",
            ["campaign_id", "campaign_message_id", "send_channel"],
            ["campaign_id", "campaign_message_id", "send_channel"],
            config, log, metric,
        )
    yield from _emit(rows, columns, max(1, int(config.get("batch_size") or 2000)))


@stream(name="flow_reports")
def flow_reports(stream_def: StreamDef, config: Config, log: logging.Logger) -> Iterator[Batch]:
    """Flow performance exactly as the Klaviyo UI reports it."""
    columns = _declared_columns(stream_def)
    metric = _require_conversion_metric(config, log, "flow_reports")
    if not metric:
        return
    with _build_client(config, log) as client:
        rows = _report_rows(
            client, "flow-values-report", "flow-values-reports/",
            ["flow_id", "flow_message_id", "send_channel"],
            ["flow_id", "flow_message_id", "send_channel"],
            config, log, metric,
        )
    yield from _emit(rows, columns, max(1, int(config.get("batch_size") or 2000)))


@stream(name="segment_reports")
def segment_reports(
    stream_def: StreamDef, config: Config, log: logging.Logger
) -> Iterator[Batch]:
    """Segment membership statistics — no conversion metric required."""
    columns = _declared_columns(stream_def)
    timeframe = str(config.get("report_timeframe_key") or "last_12_months")
    with _build_client(config, log) as client:
        body = client.post(
            "segment-values-reports/",
            {
                "data": {
                    "type": "segment-values-report",
                    "attributes": {
                        "timeframe": {"key": timeframe},
                        "statistics": [
                            "total_members", "members_added",
                            "members_removed", "net_members_changed",
                        ],
                    },
                }
            },
        )
    now = _utc_now()
    rows = [
        {
            "segment_id": (result.get("groupings") or {}).get("segment_id"),
            "timeframe": timeframe,
            "extracted_at": now,
            "statistics": result.get("statistics"),
        }
        for result in _nested(body, "data", "attributes", "results") or []
        if isinstance(result, dict)
    ]
    log.info("klaviyo.segment_reports: %d row(s) over %s", len(rows), timeframe)
    yield from _emit(rows, columns, max(1, int(config.get("batch_size") or 2000)))


# --------------------------------------------------------------------------
# profiles
# --------------------------------------------------------------------------


@stream(name="profiles")
def profiles(
    stream_def: StreamDef, config: Config, cursor: Cursor, log: logging.Logger
) -> Iterator[Batch]:
    """Profiles updated since the cursor — ``GET /profiles?sort=updated``.

    Ascending sort, so the observed cursor never goes backwards and
    ``ordered: true`` holds: a mid-run flush is a safe resume point.
    """
    columns = _declared_columns(stream_def)
    batch_size = max(1, int(config.get("batch_size") or 2000))
    page_size = max(1, min(int(config.get("profiles_page_size") or 100), 100))
    start = cursor.start_value()

    params: dict[str, Any] = {"page[size]": page_size, "sort": "updated"}
    if start:
        # /profiles rejects greater-or-equal on `updated` ("Allowed operators
        # are greater-than, less-than"), unlike /events which accepts both.
        # Exclusive is fine here: `updated` moves on every change, and the 6h
        # lookback covers the boundary.
        params["filter"] = f"greater-than(updated,{_as_iso(start)})"
    # Consent and predictive analytics are OPT-IN fields. Without this the
    # every consent column lands NULL and the table cannot answer whether a
    # profile is contactable at all.
    additional = _csv(config.get("profile_additional_fields"))
    if additional:
        params["additional-fields[profile]"] = ",".join(additional)

    log.info(
        "klaviyo.profiles: from %s (additional-fields=%s)",
        start or "the beginning", ",".join(additional) or "none",
    )
    total = 0
    batch: list[dict[str, Any]] = []
    with _build_client(config, log) as client:
        for body in client.pages("profiles/", params):
            for item in body.get("data") or []:
                attrs = item.get("attributes") or {}
                record = {
                    "id": str(item.get("id")),
                    "updated": attrs.get("updated"),
                    "created": attrs.get("created"),
                    "email": attrs.get("email"),
                    "phone_number": attrs.get("phone_number"),
                    "external_id": attrs.get("external_id"),
                    "first_name": attrs.get("first_name"),
                    "last_name": attrs.get("last_name"),
                    "organization": attrs.get("organization"),
                    "title": attrs.get("title"),
                    "locale": attrs.get("locale"),
                    "last_event_date": attrs.get("last_event_date"),
                    "anonymous_id": attrs.get("anonymous_id"),
                    "location": attrs.get("location"),
                    "properties": attrs.get("properties"),
                }
                record.update(_consent_columns(attrs.get("subscriptions")))
                record.update(_predictive_columns(attrs.get("predictive_analytics")))
                cursor.observe(record["updated"])
                batch.append(_project(record, columns))
                total += 1
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
    if batch:
        yield batch
    log.info("klaviyo.profiles: extract complete records=%d", total)
