"""RevenueCat v2 source — eight @streams (see register.yaml for the why).

Streams run in declared order inside one ``dtex run -p`` and hand off through
the LANDED tables (``landed.py``), because dtex State is per stream and far
too small for a subscription snapshot:

    products
    entitlement_products
    customers                 full /customers walk -> first_seen_at / last_seen_at
    subscriptions             targets.plan_targets -> per-customer /subscriptions
    customer_details          aliases + attributes for customers with subscriptions
    subscription_transactions /subscriptions/{id}/transactions for changed subs
    reconciliation_daily      landed transactions per day vs RC's own chart
    metrics_daily             RC charts, long format

The diff streams read their previous snapshot at stream start, build the new
row with ``records.py`` and let the destination's merge overwrite the landed
row. First-seen values and ``*_detected_at`` stamps are carried forward in
the row itself, so nothing is lost when a later pull shows post-refund
amounts. What each stream remembers in State is a WATERMARK, advanced only
over work that actually finished, never a list of things to skip.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Any
from urllib.parse import quote

import requests

from dtex import Batch, Config, Cursor, State, stream

from .client import RevenueCatClient
from .landed import LandedReader, make_reader
from .records import (
    customer_details_record,
    customer_record,
    product_record,
    subscription_record,
    transaction_record,
)
from .targets import (
    HEAL_LOOKBEHIND,
    SWEEP_HORIZON,
    KnownCustomer,
    SeenCustomer,
    plan_targets,
)

ACTIVE_STATUSES = frozenset(
    {"active", "trialing", "in_grace_period", "in_billing_retry", "paused", "incomplete"}
)

_SWEEP_WATERMARK_KEY = "sweep_watermark"
_RECENTLY_EMPTY_KEY = "recently_empty"
_RECENTLY_EMPTY_CAP = 20000
_CHANGED_WATERMARK_KEY = "changed_watermark"
_NO_TXNS_KEY = "subscriptions_without_transactions"
_NO_TXNS_CAP = 20000
# State keys of connector versions before 2.0. The skip list is the bug this
# version exists to remove; it is dropped from State on the first run.
_LEGACY_STATE_KEYS = ("customers_without_subscriptions",)

_CHART_WINDOW_DAYS = 90


def _utcnow() -> datetime:
    """The clock. One seam, so tests can move time between runs: the engine
    loads connector modules under a synthetic name, which leaves nothing to
    monkeypatch, hence the environment variable. Not for production use."""
    frozen = os.environ.get("DTEX_REVENUECAT_NOW")
    if frozen:
        return datetime.fromisoformat(frozen)
    return datetime.now(tz=UTC)


def _client(config: Config) -> RevenueCatClient:
    return RevenueCatClient(
        api_key=str(config.secrets["api_key"]),
        project_id=str(config.project_id),
        base_url=str(config.base_url),
        rate_per_second=float(config.rate_per_second),
        workers=int(config.workers),
        max_retries=int(config.max_retries),
    )


def _reader(config: Config, log: logging.Logger) -> LandedReader:
    return make_reader(
        str(config.get("landed_reader") or ""), str(config.get("landed_dataset") or ""), log
    )


def _cid(customer_id: str) -> str:
    """Path-safe customer id (``$RCAnonymousID:...`` carries ``$`` and ``:``)."""
    return quote(str(customer_id), safe="")


def _batches(rows: list[dict], size: int) -> Iterator[Batch]:
    for i in range(0, len(rows), size):
        yield rows[i : i + size]


def _aware(value: Any) -> datetime | None:
    """A landed TIMESTAMP (aware, naive-UTC, or ISO text) as an aware datetime."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _first_column(rows: list[dict[str, Any]]) -> list[str]:
    """Values of the first column of an operator-supplied query, as strings."""
    out: list[str] = []
    for row in rows:
        value = next(iter(row.values()), None)
        if value is not None and str(value) != "":
            out.append(str(value))
    return out


def _optional_sql(
    reader: LandedReader, statement: str, what: str, log: logging.Logger
) -> list[str]:
    """Run an optional operator query. A failure costs a hint, never the run:
    the sweep is the completeness guarantee, seeds only make it faster."""
    statement = (statement or "").strip()
    if not statement:
        return []
    try:
        return _first_column(reader.sql(statement))
    except Exception as exc:  # noqa: BLE001 - any driver error degrades to "no hints"
        log.warning("revenuecat: %s query failed and is ignored this run: %s", what, exc)
        return []


# ---------------------------------------------------------------------------
# products / entitlement_products
# ---------------------------------------------------------------------------


@stream(name="products")
def products(config: Config, log: logging.Logger) -> Iterator[Batch]:
    client = _client(config)
    now = _utcnow().isoformat()
    rows = [
        product_record(p, now)
        for p in client.paginate(client.project_path("/products"), {"limit": int(config.page_size)})
    ]
    log.info("revenuecat.products: %d products", len(rows))
    yield from _batches(rows, int(config.batch_size))


@stream(name="entitlement_products")
def entitlement_products(config: Config, log: logging.Logger) -> Iterator[Batch]:
    client = _client(config)
    now = _utcnow().isoformat()
    limit = {"limit": int(config.page_size)}
    rows: list[dict] = []
    for ent in client.paginate(client.project_path("/entitlements"), limit):
        for p in client.paginate(client.project_path(f"/entitlements/{ent['id']}/products"), limit):
            rows.append(
                {
                    "entitlement_id": ent["id"],
                    "entitlement_lookup_key": ent.get("lookup_key"),
                    "entitlement_display_name": ent.get("display_name"),
                    "product_id": p["id"],
                    "product_store_identifier": p.get("store_identifier"),
                    "extracted_at": now,
                }
            )
    log.info("revenuecat.entitlement_products: %d entitlement-product pairs", len(rows))
    yield from _batches(rows, int(config.batch_size))


# ---------------------------------------------------------------------------
# customers — the full walk, as concurrent chains that cannot leave a gap
# ---------------------------------------------------------------------------

_HEX = "0123456789abcdef"
_ANON = "$RCAnonymousID:"


def _chain_starts(chains: int) -> list[str | None]:
    """``starting_after`` values for the concurrent chains. ``None`` is the
    chain from the very beginning of the list; it alone is a complete walk,
    the others only split it up."""
    if chains <= 1:
        return [None]
    return [None, *_HEX, *(f"{_ANON}{h}" for h in _HEX)]


@stream(name="customers")
def customers(config: Config, log: logging.Logger) -> Iterator[Batch]:
    """Full /customers walk as concurrent cursor chains.

    One chain is sequential by construction (each page's URL comes from the
    previous response), so ~2,500 pages take over an hour; the same walk as 33
    chains takes a few minutes, which is what lets every run re-read
    ``last_seen_at`` for every customer.

    ``starting_after`` accepts any string as a boundary, not only an existing
    id, so a chain can start anywhere. How RC ORDERS the list is not
    documented (observed: by the customer's original app user id, uuids
    first, then the ``$RCAnonymousID`` block, while the row displays the
    canonical id), so nothing here compares ids. Each chain fetches its first
    page, the first id of every chain becomes a boundary, and a chain walks
    until it meets another chain's boundary id or the end of the list. One
    chain starts at the very beginning, so whatever the collation, the
    segments tile the whole list: a wrong guess about the order costs
    duplicate rows (harmless under merge), never a gap.
    """
    client = _client(config)
    now = _utcnow().isoformat()
    page_size = int(config.page_size)
    max_pages = int(config.customers_max_pages)
    base = f"{client.base_url}{client.project_path('/customers')}"

    def probe(start: str | None) -> dict | None:
        params: dict[str, Any] = {"limit": page_size}
        if start is not None:
            params["starting_after"] = start
        try:
            return client._get(base, params=params)
        except requests.HTTPError as exc:
            # The unbounded chain must work; a boundary RC rejects just means
            # one chain fewer.
            if start is None:
                raise
            log.warning(
                "revenuecat.customers: boundary %r rejected (%s), chain dropped", start, exc
            )
            return None

    chains: list[tuple[str | None, dict]] = []
    first_ids: set[str] = set()
    for start, page in client.map_concurrent(
        _chain_starts(int(config.customers_walk_chains)), probe
    ):
        items = (page or {}).get("items") or []
        if not page or not items:
            continue
        first_id = str(items[0]["id"])
        if first_id in first_ids:
            continue  # two boundaries landed on the same row: one chain is enough
        first_ids.add(first_id)
        chains.append((start, page))

    def walk(chain: tuple[str | None, dict]) -> tuple[list[dict], int]:
        start, page = chain
        own = str(page["items"][0]["id"])
        rows: list[dict] = []
        pages = 0
        data: dict | None = page
        while data is not None:
            pages += 1
            for c in data.get("items", []):
                cid = str(c["id"])
                if cid in first_ids and cid != own:
                    return rows, pages  # met the next chain's first row
                rows.append(customer_record(c, now))
            next_url = data.get("next_page")
            if not next_url:
                break
            if max_pages and pages >= max_pages:
                log.warning(
                    "revenuecat.customers: chain %r hit the %d-page cap, walk INCOMPLETE",
                    start,
                    max_pages,
                )
                break
            data = client._get(next_url)
        return rows, pages

    # Every yielded batch is one load + merge in the destination (~10 s on
    # BigQuery); buffer across chains so the walk is a dozen loads, not 500.
    buffer: list[dict] = []
    flush_at = int(config.customers_batch_size)
    total_rows = total_pages = 0
    for chain, (rows, pages) in client.map_concurrent(chains, walk):
        total_rows += len(rows)
        total_pages += pages
        log.info(
            "revenuecat.customers: chain %-18r %6d rows in %4d pages", chain[0], len(rows), pages
        )
        buffer.extend(rows)
        if len(buffer) >= flush_at:
            yield buffer
            buffer = []
    if buffer:
        yield buffer
    log.info(
        "revenuecat.customers: walk complete, %d customers in %d pages over %d chains, %d requests",
        total_rows,
        total_pages,
        len(chains),
        client.requests_made,
    )


# ---------------------------------------------------------------------------
# subscriptions — see targets.py for who is fetched and why
# ---------------------------------------------------------------------------

_SUBSCRIPTION_PREV_COLUMNS = [
    "id",
    "customer_id",
    "status",
    "auto_renewal_status",
    "current_period_ends_at",
    "ends_at",
    "product_id",
    "pending_payment",
    "total_revenue_gross_usd",
    "first_extracted_at",
    "extracted_at",
    "changed_at",
    "auto_renew_off_detected_at",
    "billing_issue_detected_at",
    "refund_detected_at",
]


def _known_customers(
    rows: list[dict], now: datetime, hot_window: timedelta
) -> dict[str, KnownCustomer]:
    known: dict[str, KnownCustomer] = {}
    for r in rows:
        cid = r.get("customer_id")
        if not cid:
            continue
        ends = _aware(r.get("ends_at")) or _aware(r.get("current_period_ends_at"))
        hot = r.get("status") in ACTIVE_STATUSES or (ends is not None and ends >= now - hot_window)
        extracted = _aware(r.get("extracted_at"))
        prev = known.get(cid)
        if prev is not None:
            hot = hot or prev.hot
            if prev.extracted_at is not None and (
                extracted is None or prev.extracted_at > extracted
            ):
                extracted = prev.extracted_at
        known[cid] = KnownCustomer(id=cid, extracted_at=extracted, hot=hot)
    return known


def _heal_days(reader: LandedReader, today: date, heal_days: int) -> list[date]:
    if heal_days <= 0:
        return []
    rows = reader.rows("reconciliation_daily", ["day", "missing", "incomplete"]) or []
    out: list[date] = []
    for r in rows:
        day = r.get("day")
        if isinstance(day, datetime):
            day = day.date()
        elif isinstance(day, str):
            day = date.fromisoformat(day[:10])
        if not isinstance(day, date) or r.get("incomplete") or not (r.get("missing") or 0) > 0:
            continue
        if today - timedelta(days=heal_days) <= day < today:
            out.append(day)
    return out


@stream(name="subscriptions")
def subscriptions(config: Config, state: State, log: logging.Logger) -> Iterator[Batch]:
    client = _client(config)
    reader = _reader(config, log)
    now_dt = _utcnow()
    now = now_dt.isoformat()

    for legacy in _LEGACY_STATE_KEYS:
        if legacy in state:
            del state[legacy]
            log.info("revenuecat.subscriptions: dropped legacy state key %r", legacy)

    landed = reader.rows("subscriptions", _SUBSCRIPTION_PREV_COLUMNS) or []
    prev = {r["id"]: r for r in landed}
    known = _known_customers(landed, now_dt, timedelta(days=int(config.hot_window_days)))

    watermark = _aware(state.get(_SWEEP_WATERMARK_KEY)) or now_dt - timedelta(
        days=int(config.sweep_initial_days)
    )
    heal = _heal_days(reader, now_dt.date(), int(config.reconcile_heal_days))

    # Only customers whose schedule can still produce a checkpoint (or who sit
    # inside a heal window) matter; the rest of the walk is dead weight here.
    floor = min(watermark, now_dt) - SWEEP_HORIZON
    if heal:
        heal_floor = datetime.combine(min(heal), datetime.min.time(), tzinfo=UTC) - HEAL_LOOKBEHIND
        floor = min(floor, heal_floor)
    seen: list[SeenCustomer] = []
    snapshot_at: datetime | None = None
    for r in (
        reader.rows("customers", ["id", "first_seen_at", "last_seen_at", "extracted_at"]) or []
    ):
        extracted = _aware(r.get("extracted_at"))
        if extracted is not None and (snapshot_at is None or extracted > snapshot_at):
            snapshot_at = extracted
        c = SeenCustomer(
            str(r["id"]), _aware(r.get("first_seen_at")), _aware(r.get("last_seen_at"))
        )
        if c.anchor is not None and c.anchor >= floor:
            seen.append(c)
    if snapshot_at is None:
        log.warning(
            "revenuecat.subscriptions: no landed customers snapshot. Run the `customers` stream "
            "in the "
            "same config: without it only hot, cold, seed and bootstrap customers are fetched."
        )

    seeds = _optional_sql(
        reader, str(config.get("seed_customers_sql") or ""), "seed_customers_sql", log
    )
    n_direct = len(seeds)
    store_ids = _optional_sql(
        reader, str(config.get("seed_store_ids_sql") or ""), "seed_store_ids_sql", log
    )
    for _sid, found in client.map_concurrent(
        store_ids,
        lambda sid: client.collect_or_none(
            client.project_path("/subscriptions"),
            {"store_subscription_identifier": sid, "limit": 5},
        ),
    ):
        seeds.extend(str(s["customer_id"]) for s in found or [] if s.get("customer_id"))
    bootstrap = _optional_sql(
        reader, str(config.get("bootstrap_customers_sql") or ""), "bootstrap_customers_sql", log
    )

    empty_recheck = timedelta(hours=int(config.empty_recheck_hours))
    recently_empty: dict[str, datetime] = {}
    for cid, stamp in (state.get(_RECENTLY_EMPTY_KEY) or {}).items():
        checked = _aware(stamp)
        if checked is not None and now_dt - checked < empty_recheck:
            recently_empty[cid] = checked

    plan = plan_targets(
        now=now_dt,
        watermark=watermark,
        snapshot_at=snapshot_at,
        customers=seen,
        known=known,
        seeds=seeds,
        heal_days=heal,
        bootstrap_ids=bootstrap,
        recently_empty=recently_empty,
        empty_recheck=empty_recheck,
        cap=int(config.max_customers_per_run),
        cold_per_run=int(config.cold_refresh_per_run),
    )
    log.info(
        "revenuecat.subscriptions: targets seed=%d (direct %d, store ids %d) bootstrap=%d "
        "sweep=%d hot=%d heal=%d cold=%d | sweep backlog %d, watermark %s -> %s, heal days %s",
        plan.counts["seed"],
        n_direct,
        len(store_ids),
        plan.counts["bootstrap"],
        plan.counts["sweep"],
        plan.counts["hot"],
        plan.counts["heal"],
        plan.counts["cold"],
        plan.sweep_backlog,
        watermark.isoformat(),
        plan.new_watermark.isoformat(),
        [d.isoformat() for d in heal],
    )

    def fetch(cid: str) -> list[dict] | None:
        return client.collect_or_none(
            client.project_path(f"/customers/{_cid(cid)}/subscriptions"),
            {"limit": int(config.page_size)},
        )

    batch: list[dict] = []
    n_subs = n_new = n_changed = n_unknown = 0
    found_in: dict[str, int] = {}
    for i, (cid, subs) in enumerate(client.map_concurrent(plan.ordered, fetch), start=1):
        if subs is None:
            n_unknown += 1
        if not subs:
            if cid in plan.remember_if_empty:
                recently_empty[cid] = now_dt
            continue
        recently_empty.pop(cid, None)
        for sub in subs:
            landed_row = prev.get(sub["id"])
            row = subscription_record(sub, landed_row, now)
            n_subs += 1
            if landed_row is None:
                n_new += 1
                tier = plan.tier_of[cid]
                found_in[tier] = found_in.get(tier, 0) + 1
            elif row["changed_at"] == now:
                n_changed += 1
            batch.append(row)
            if len(batch) >= int(config.batch_size):
                yield batch
                batch = []
        if i % 1000 == 0:
            log.info(
                "revenuecat.subscriptions: %d/%d customers, %d subscriptions",
                i,
                len(plan.ordered),
                n_subs,
            )
    if batch:
        yield batch

    # Persist only now, after every planned customer was fetched: a run that
    # dies above leaves the watermark where it was and the window reopens.
    state.set(_SWEEP_WATERMARK_KEY, plan.new_watermark.isoformat())
    newest = sorted(recently_empty.items(), key=lambda kv: kv[1])[-_RECENTLY_EMPTY_CAP:]
    state.set(_RECENTLY_EMPTY_KEY, {cid: stamp.isoformat() for cid, stamp in newest})
    log.info(
        "revenuecat.subscriptions: %d customers -> %d subscriptions (%d new %s, %d changed, "
        "%d unknown ids, "
        "%d requests)",
        len(plan.ordered),
        n_subs,
        n_new,
        found_in or "",
        n_changed,
        n_unknown,
        client.requests_made,
    )


# ---------------------------------------------------------------------------
# customer_details — aliases + attributes for customers with subscriptions
# ---------------------------------------------------------------------------


@stream(name="customer_details")
def customer_details(config: Config, log: logging.Logger) -> Iterator[Batch]:
    client = _client(config)
    reader = _reader(config, log)
    now = _utcnow().isoformat()

    have = {r["customer_id"] for r in reader.rows("customer_details", ["customer_id"]) or []}
    newest: dict[str, datetime] = {}
    for r in reader.rows("subscriptions", ["customer_id", "first_extracted_at"]) or []:
        cid, first = r.get("customer_id"), _aware(r.get("first_extracted_at"))
        if (
            cid
            and cid not in have
            and first is not None
            and (cid not in newest or first > newest[cid])
        ):
            newest[cid] = first
    targets = sorted(newest, key=lambda cid: newest[cid], reverse=True)[
        : int(config.max_details_per_run)
    ]
    log.info(
        "revenuecat.customer_details: %d customers to detail (%d already detailed)",
        len(targets),
        len(have),
    )
    if not targets:
        return

    def fetch(cid: str) -> tuple[list[dict] | None, list[dict] | None]:
        base = client.project_path(f"/customers/{_cid(cid)}")
        return (
            client.collect_or_none(f"{base}/aliases", {"limit": int(config.page_size)}),
            client.collect_or_none(f"{base}/attributes", {"limit": int(config.page_size)}),
        )

    batch: list[dict] = []
    for cid, (aliases, attributes) in client.map_concurrent(targets, fetch):
        batch.append(customer_details_record(cid, aliases, attributes, now))
        if len(batch) >= int(config.batch_size):
            yield batch
            batch = []
    if batch:
        yield batch


# ---------------------------------------------------------------------------
# subscription_transactions — period grain for new and changed subscriptions
# ---------------------------------------------------------------------------


@stream(name="subscription_transactions")
def subscription_transactions(config: Config, state: State, log: logging.Logger) -> Iterator[Batch]:
    client = _client(config)
    reader = _reader(config, log)
    now_dt = _utcnow()
    now = now_dt.isoformat()

    subs = [
        r
        for r in reader.rows(
            "subscriptions", ["id", "customer_id", "store", "environment", "changed_at"]
        )
        or []
        if r.get("store")
        != "promotional"  # promotional grants have no store transactions (RC answers 400)
    ]
    landed = (
        reader.rows(
            "subscription_transactions",
            [
                "id",
                "subscription_id",
                "gross_usd",
                "first_gross_usd",
                "first_gross_local",
                "first_extracted_at",
                "refund_detected_at",
            ],
        )
        or []
    )
    prev = {str(r["id"]): r for r in landed}
    with_txns = {r["subscription_id"] for r in landed}

    watermark = _aware(state.get(_CHANGED_WATERMARK_KEY)) or now_dt - timedelta(
        hours=int(config.txn_lookback_hours)
    )
    # Subscriptions RC returned no transactions for. Without this they would be
    # re-fetched on every run for ever; with it, once a day or when they change.
    no_txns: dict[str, datetime] = {}
    for sid, stamp in (state.get(_NO_TXNS_KEY) or {}).items():
        checked = _aware(stamp)
        if checked is not None:
            no_txns[sid] = checked
    recheck = timedelta(hours=int(config.empty_recheck_hours))

    fresh: list[dict] = []
    changed: list[tuple[datetime, dict]] = []
    for s in subs:
        changed_at = _aware(s.get("changed_at"))
        if s["id"] in with_txns:
            if changed_at is not None and changed_at > watermark:
                changed.append((changed_at, s))
            continue
        checked = no_txns.get(s["id"])
        if (
            checked is None
            or now_dt - checked >= recheck
            or (changed_at is not None and changed_at > checked)
        ):
            fresh.append(s)
    changed.sort(key=lambda pair: (pair[0], pair[1]["id"]))

    cap = int(config.max_subscriptions_per_run)
    targets = (fresh + [s for _, s in changed])[:cap]
    target_ids = {s["id"] for s in targets}
    unprocessed = [at for at, s in changed if s["id"] not in target_ids]
    if unprocessed:
        new_watermark = max(watermark, min(unprocessed) - timedelta(milliseconds=1))
    else:
        new_watermark = max([watermark, *(at for at, _ in changed)])
    log.info(
        "revenuecat.subscription_transactions: %d subscriptions (%d without landed transactions, "
        "%d changed "
        "since %s, %d deferred by the cap)",
        len(targets),
        len(fresh),
        len(changed),
        watermark.isoformat(),
        len(unprocessed),
    )

    def fetch(sub_row: dict) -> list[dict] | None:
        try:
            return client.collect_or_none(
                client.project_path(f"/subscriptions/{sub_row['id']}/transactions"),
                {"limit": int(config.page_size)},
            )
        except requests.HTTPError as exc:
            # RC answers 400, not an empty list, for a subscription without
            # store transactions.
            if exc.response is not None and exc.response.status_code == 400:
                log.warning(
                    "revenuecat.subscription_transactions: 400 for %s (store=%s), treated as no "
                    "transactions",
                    sub_row["id"],
                    sub_row.get("store"),
                )
                return None
            raise

    batch: list[dict] = []
    n_rows = n_refunds = 0
    for sub_row, txns in client.map_concurrent(targets, fetch):
        if not txns:
            no_txns[sub_row["id"]] = now_dt
            continue
        no_txns.pop(sub_row["id"], None)
        ordered = sorted(txns, key=lambda t: (t.get("purchased_at") or 0, str(t.get("id"))))
        for idx, txn in enumerate(ordered, start=1):
            row = transaction_record(txn, sub_row, prev.get(str(txn["id"])), idx, now)
            if row["refund_detected_at"] == now:
                n_refunds += 1
            batch.append(row)
            n_rows += 1
            if len(batch) >= int(config.batch_size):
                yield batch
                batch = []
    if batch:
        yield batch

    state.set(_CHANGED_WATERMARK_KEY, new_watermark.isoformat())
    live = {s["id"] for s in subs}
    kept = sorted(((sid, at) for sid, at in no_txns.items() if sid in live), key=lambda kv: kv[1])[
        -_NO_TXNS_CAP:
    ]
    state.set(_NO_TXNS_KEY, {sid: at.isoformat() for sid, at in kept})
    log.info(
        "revenuecat.subscription_transactions: %d rows for %d subscriptions, %d refunds detected, "
        "%d requests",
        n_rows,
        len(targets),
        n_refunds,
        client.requests_made,
    )


# ---------------------------------------------------------------------------
# charts — shared by reconciliation_daily and metrics_daily
# ---------------------------------------------------------------------------


def _chart_values(
    client: RevenueCatClient, chart_name: str, start: date, end: date
) -> Iterator[tuple[date, str, Any, bool]]:
    """``(cohort_date, measure_name, value, incomplete)`` for every daily value
    of a chart, requested in windows so a long backfill stays a small response."""
    window_start = start
    while window_start <= end:
        window_end = min(end, window_start + timedelta(days=_CHART_WINDOW_DAYS - 1))
        data = client.get(
            client.project_path(f"/charts/{chart_name}"),
            {
                "resolution": "day",
                "start_date": window_start.isoformat(),
                "end_date": window_end.isoformat(),
            },
        )
        names = {
            idx: str(m.get("display_name") or m.get("name") or f"measure_{idx}")
            for idx, m in enumerate(data.get("measures") or [])
        }
        for v in data.get("values") or []:
            if v.get("cohort") is None:
                continue
            cohort = datetime.fromtimestamp(int(v["cohort"]), tz=UTC).date()
            if not window_start <= cohort <= window_end:
                continue  # RC pads a window to whole periods; keep each day in exactly one window
            idx = int(v.get("measure", 0))
            yield (
                cohort,
                names.get(idx, f"measure_{idx}"),
                v.get("value"),
                bool(v.get("incomplete", False)),
            )
        window_start = window_end + timedelta(days=1)


# ---------------------------------------------------------------------------
# reconciliation_daily — does the landed data add up to what RC itself counts?
# ---------------------------------------------------------------------------


@stream(name="reconciliation_daily")
def reconciliation_daily(config: Config, log: logging.Logger) -> Iterator[Batch]:
    """Landed production transactions per UTC day against the Transactions
    measure of RC's revenue chart.

    The two are not the same definition (RC counts revenue-generating
    purchases; the landed count includes zero-price periods), and that is on
    purpose: on 77 clean production days RC's count was never above the landed
    one, so ``missing = max(rc - landed, 0)`` has no false alarms while a run
    of missed purchases shows up at once. It is a lower bound on what is
    missing, not an exact figure. ``subscriptions`` heals the recent short
    days; alert on older ones.
    """
    client = _client(config)
    reader = _reader(config, log)
    days = int(config.reconcile_days)
    if days <= 0:
        return
    now_dt = _utcnow()
    today = now_dt.date()
    start = today - timedelta(days=days)

    rc: dict[date, tuple[int, bool]] = {}
    for cohort, measure, value, incomplete in _chart_values(client, "revenue", start, today):
        if measure == "Transactions" and value is not None:
            rc[cohort] = (int(value), incomplete)
    if not rc:
        log.warning(
            "revenuecat.reconciliation_daily: the revenue chart returned no Transactions measure, "
            "skipped"
        )
        return

    landed: dict[date, set[str]] = {}
    for r in reader.rows("subscription_transactions", ["id", "purchased_at", "environment"]) or []:
        purchased = _aware(r.get("purchased_at"))
        if purchased is None or r.get("environment") == "sandbox":
            continue
        landed.setdefault(purchased.date(), set()).add(str(r["id"]))

    rows: list[dict] = []
    for day in sorted(rc):
        rc_count, incomplete = rc[day]
        landed_count = len(landed.get(day, ()))
        missing = max(rc_count - landed_count, 0)
        rows.append(
            {
                "day": day.isoformat(),
                "rc_transactions": rc_count,
                "landed_transactions": landed_count,
                "missing": missing,
                "incomplete": incomplete,
                "checked_at": now_dt.isoformat(),
            }
        )
        if missing and not incomplete:
            log.warning(
                "revenuecat.reconciliation_daily: %s RevenueCat counts %d transactions, %d landed "
                "(%d missing)",
                day,
                rc_count,
                landed_count,
                missing,
            )
    log.info(
        "revenuecat.reconciliation_daily: %d days, %d short, %d transactions missing in total",
        len(rows),
        sum(1 for r in rows if r["missing"]),
        sum(r["missing"] for r in rows),
    )
    yield from _batches(rows, int(config.batch_size))


# ---------------------------------------------------------------------------
# metrics_daily — RC charts, long format
# ---------------------------------------------------------------------------


@stream(name="metrics_daily")
def metrics_daily(config: Config, cursor: Cursor, log: logging.Logger) -> Iterator[Batch]:
    """One row per (cohort_date, chart, measure).

    First run: from ``metrics_initial_since_date``. Every later run re-pulls
    the trailing ``metrics_lookback_days``, because RC keeps revising a day
    after it stops being ``incomplete`` (refunds and late store notifications
    move revenue and transaction counts for days). The cursor marks the last
    complete day and only decides where a first or interrupted backfill
    resumes.
    """
    client = _client(config)
    charts = [c.strip() for c in str(config.metrics_charts).split(",") if c.strip()]
    if not charts:
        log.warning("revenuecat.metrics_daily: no charts configured, skipping")
        return

    today = _utcnow().date()
    cursor_value = cursor.start_value() or date.fromisoformat(
        str(config.metrics_initial_since_date)
    )
    if isinstance(cursor_value, datetime):
        cursor_value = cursor_value.date()
    if isinstance(cursor_value, str):
        cursor_value = date.fromisoformat(cursor_value[:10])
    # The manifest's cursor initial_value is what a first run starts from;
    # metrics_initial_since_date lets a config start later than that.
    earliest = date.fromisoformat(str(config.metrics_initial_since_date))
    start_date = min(
        max(cursor_value, earliest), today - timedelta(days=int(config.metrics_lookback_days))
    )
    log.info("revenuecat.metrics_daily: charts=%s start=%s end=%s", charts, start_date, today)

    pulled_at = _utcnow().isoformat()
    batch: list[dict] = []
    max_complete: date | None = None
    for chart_name in charts:
        for cohort, measure, value, incomplete in _chart_values(
            client, chart_name, start_date, today
        ):
            batch.append(
                {
                    "cohort_date": cohort.isoformat(),
                    "chart_name": chart_name,
                    "measure_name": measure,
                    "value": value,
                    "incomplete": incomplete,
                    "pulled_at": pulled_at,
                }
            )
            if not incomplete and (max_complete is None or cohort > max_complete):
                max_complete = cohort
            if len(batch) >= int(config.batch_size):
                yield batch
                batch = []
    if batch:
        yield batch
    # Once, at the end: a crash between charts must not move the cursor past
    # days the remaining charts never pulled.
    if max_complete is not None:
        cursor.observe(max_complete)
