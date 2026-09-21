"""Pure record shaping for the RevenueCat v2 source — no I/O.

Every ``*_record`` builds one output row from an API object plus the
row's previous landed snapshot (``prev``, a dict read back from the
destination, or None).
The *_detected_at columns follow one rule: they are stamped only on an
OBSERVED TRANSITION between two pulls, never on first sight. RC exposes
current state only (no cancel / refund timestamps), so a subscription
that was already cancelled or refunded when this source first saw it
keeps NULL here; whatever recorded the event before this source started
(a vendor export, a webhook log) is the place to look for that timestamp.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

BILLING_RETRY_STATUSES = frozenset({"in_billing_retry"})
# auto_renewal_status values that mean the customer turned renewal OFF. The
# enum also carries will_change_product, requires_price_increase_consent and
# has_already_renewed, none of which is a cancellation (spec: Subscription
# data model, checked 2026-09-16).
AUTO_RENEW_OFF = frozenset({"will_not_renew", "will_pause"})
_EPS = 0.005  # half a cent: money compares below this are noise


def ms_to_iso(ms: Any) -> str | None:
    """RC millisecond epoch -> ISO-8601 UTC string (or None)."""
    if ms is None or ms == "":
        return None
    return datetime.fromtimestamp(int(ms) / 1000, tz=UTC).isoformat()


def to_ms(value: Any) -> int | None:
    """Landed TIMESTAMP (aware or naive-UTC datetime) or ISO string -> ms epoch."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=UTC)
        return int(round(dt.timestamp() * 1000))
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return int(round(datetime.fromisoformat(text).timestamp() * 1000))


def iso_of(value: Any) -> str | None:
    """Landed TIMESTAMP -> ISO string, passing strings through."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=UTC)
        return dt.isoformat()
    return str(value)


def money(block: Any, key: str) -> float | None:
    """``revenue_in_usd``-style block -> one component as float."""
    if not isinstance(block, dict):
        return None
    value = block.get(key)
    return None if value is None else float(value)


def _f(value: Any) -> float | None:
    return None if value is None else float(value)


def _dropped(current: float | None, previous: Any) -> bool:
    """True when a money value fell by more than half a cent."""
    if current is None or previous is None:
        return False
    return float(current) < float(previous) - _EPS


# ---------------------------------------------------------------------------


def product_record(p: dict, now_iso: str) -> dict:
    sub = p.get("subscription") or {}
    return {
        "id": p["id"],
        "store_identifier": p.get("store_identifier"),
        "app_id": p.get("app_id"),
        "type": p.get("type"),
        "display_name": p.get("display_name"),
        "duration": sub.get("duration"),
        "trial_duration": sub.get("trial_duration"),
        "grace_period_duration": sub.get("grace_period_duration"),
        "extracted_at": now_iso,
    }


def customer_record(c: dict, now_iso: str) -> dict:
    return {
        "id": c["id"],
        "first_seen_at": ms_to_iso(c.get("first_seen_at")),
        "last_seen_at": ms_to_iso(c.get("last_seen_at")),
        "last_seen_app_version": c.get("last_seen_app_version"),
        "last_seen_country": c.get("last_seen_country"),
        "last_seen_platform": c.get("last_seen_platform"),
        "extracted_at": now_iso,
    }


def _billing_issue(status: str | None, pending_payment: Any) -> bool:
    return bool(pending_payment) or (status in BILLING_RETRY_STATUSES)


def subscription_fingerprint(
    status: Any,
    auto_renewal_status: Any,
    current_period_ends_ms: Any,
    ends_ms: Any,
    gross_usd: Any,
    product_id: Any,
    pending_payment: Any,
) -> tuple:
    return (
        status,
        auto_renewal_status,
        to_ms(current_period_ends_ms),
        to_ms(ends_ms),
        None if gross_usd is None else round(float(gross_usd), 2),
        product_id,
        bool(pending_payment),
    )


def subscription_record(sub: dict, prev: dict | None, now_iso: str) -> dict:
    """One ``subscriptions`` row. ``prev`` is the landed row for the same id."""
    revenue = sub.get("total_revenue_in_usd") or {}
    gross = money(revenue, "gross")
    entitlements = ((sub.get("entitlements") or {}).get("items")) or []
    lookup_keys = (
        ",".join(str(e.get("lookup_key")) for e in entitlements if e.get("lookup_key")) or None
    )
    status = sub.get("status")
    renewal = sub.get("auto_renewal_status")
    pending = sub.get("pending_payment")

    fp_now = subscription_fingerprint(
        status,
        renewal,
        sub.get("current_period_ends_at"),
        sub.get("ends_at"),
        gross,
        sub.get("product_id"),
        pending,
    )
    if prev is None:
        changed = True
        first_extracted_at = now_iso
        auto_off = billing = refund = None
    else:
        fp_prev = subscription_fingerprint(
            prev.get("status"),
            prev.get("auto_renewal_status"),
            prev.get("current_period_ends_at"),
            prev.get("ends_at"),
            prev.get("total_revenue_gross_usd"),
            prev.get("product_id"),
            prev.get("pending_payment"),
        )
        changed = fp_now != fp_prev
        first_extracted_at = iso_of(prev.get("first_extracted_at")) or now_iso
        auto_off = iso_of(prev.get("auto_renew_off_detected_at"))
        if (
            auto_off is None
            and prev.get("auto_renewal_status") not in AUTO_RENEW_OFF
            and renewal in AUTO_RENEW_OFF
        ):
            auto_off = now_iso
        billing = iso_of(prev.get("billing_issue_detected_at"))
        if (
            billing is None
            and not _billing_issue(prev.get("status"), prev.get("pending_payment"))
            and _billing_issue(status, pending)
        ):
            billing = now_iso
        refund = iso_of(prev.get("refund_detected_at"))
        if refund is None and _dropped(gross, prev.get("total_revenue_gross_usd")):
            refund = now_iso

    return {
        "id": sub["id"],
        "customer_id": sub.get("customer_id"),
        "original_customer_id": sub.get("original_customer_id"),
        "product_id": sub.get("product_id"),
        "entitlement_lookup_keys": lookup_keys,
        "status": status,
        "gives_access": sub.get("gives_access"),
        "auto_renewal_status": renewal,
        "pending_payment": pending,
        "ownership": sub.get("ownership"),
        "environment": sub.get("environment"),
        "store": sub.get("store"),
        "store_subscription_identifier": sub.get("store_subscription_identifier"),
        "country": sub.get("country"),
        "presented_offering_id": sub.get("presented_offering_id"),
        "starts_at": ms_to_iso(sub.get("starts_at")),
        "ends_at": ms_to_iso(sub.get("ends_at")),
        "current_period_starts_at": ms_to_iso(sub.get("current_period_starts_at")),
        "current_period_ends_at": ms_to_iso(sub.get("current_period_ends_at")),
        "total_revenue_gross_usd": gross,
        "total_revenue_tax_usd": money(revenue, "tax"),
        "total_revenue_commission_usd": money(revenue, "commission"),
        "total_revenue_proceeds_usd": money(revenue, "proceeds"),
        "pending_changes": sub.get("pending_changes"),
        "first_extracted_at": first_extracted_at,
        "extracted_at": now_iso,
        "changed_at": now_iso
        if changed or prev is None
        else (iso_of(prev.get("changed_at")) or now_iso),
        "auto_renew_off_detected_at": auto_off,
        "billing_issue_detected_at": billing,
        "refund_detected_at": refund,
    }


def customer_details_record(
    customer_id: str, aliases: list[dict] | None, attributes: list[dict] | None, now_iso: str
) -> dict:
    alias_ids = [str(a.get("id")) for a in (aliases or []) if a.get("id")]
    anon = next((a for a in alias_ids if a.startswith("$RCAnonymousID")), None)
    attrs = {str(a.get("name")): a.get("value") for a in (attributes or []) if a.get("name")}
    # `$email` is RC's reserved attribute; many apps set their own `email`
    # instead (or did before adopting the reserved key). Everything else a
    # tenant sets is theirs to name, so it lands whole in `attributes`.
    return {
        "customer_id": customer_id,
        "original_app_user_id": anon or customer_id,
        "aliases": ",".join(alias_ids) or None,
        "email": attrs.get("$email") or attrs.get("email"),
        "attributes": attrs or None,
        "extracted_at": now_iso,
    }


def transaction_record(
    txn: dict, sub_row: dict, prev: dict | None, period_index: int, now_iso: str
) -> dict:
    """One ``subscription_transactions`` row. ``sub_row`` is the landed
    subscription (id, customer_id, store, environment); ``prev`` the landed
    transaction row for the same store transaction id."""
    usd = txn.get("revenue_in_usd") or {}
    local = txn.get("revenue_in_local_currency") or {}
    gross_usd = money(usd, "gross")
    gross_local = money(local, "gross")
    if prev is None:
        first_gross_usd, first_gross_local = gross_usd, gross_local
        first_extracted_at = now_iso
        refund = None
    else:
        first_gross_usd = (
            _f(prev.get("first_gross_usd"))
            if prev.get("first_gross_usd") is not None
            else gross_usd
        )
        first_gross_local = (
            _f(prev.get("first_gross_local"))
            if prev.get("first_gross_local") is not None
            else gross_local
        )
        first_extracted_at = iso_of(prev.get("first_extracted_at")) or now_iso
        refund = iso_of(prev.get("refund_detected_at"))
        if refund is None and _dropped(gross_usd, prev.get("gross_usd")):
            refund = now_iso
    return {
        "id": str(txn["id"]),
        "subscription_id": sub_row["id"],
        "customer_id": sub_row["customer_id"],
        "store": sub_row.get("store"),
        "environment": sub_row.get("environment"),
        "product_store_identifier": txn.get("product_store_identifier"),
        "period_index": period_index,
        "purchased_at": ms_to_iso(txn.get("purchased_at")),
        "expiration_date": ms_to_iso(txn.get("expiration_date")),
        "effective_expiration_date": ms_to_iso(txn.get("effective_expiration_date")),
        "currency": local.get("currency") or usd.get("currency"),
        "gross_local": gross_local,
        "tax_local": money(local, "tax"),
        "commission_local": money(local, "commission"),
        "proceeds_local": money(local, "proceeds"),
        "gross_usd": gross_usd,
        "tax_usd": money(usd, "tax"),
        "commission_usd": money(usd, "commission"),
        "proceeds_usd": money(usd, "proceeds"),
        "first_gross_usd": first_gross_usd,
        "first_gross_local": first_gross_local,
        "first_extracted_at": first_extracted_at,
        "extracted_at": now_iso,
        "refund_detected_at": refund,
    }
