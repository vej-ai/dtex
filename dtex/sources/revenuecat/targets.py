"""Which customers get their subscriptions fetched this run — pure, no I/O.

RevenueCat v2 has no "changed since" filter and its /customers list carries
no purchase signal at all: only ``first_seen_at`` / ``last_seen_at``. Worse,
``last_seen_at`` does NOT reliably move when a customer buys. Measured on a
production project (2026-09): of 2,104 first purchases, 20 happened more than
an hour after the customer's final ``last_seen_at`` and the longest gap was
20 hours, i.e. the customer was seen, fetched while still subscription-less,
bought later, and was never "seen" again.

So a rule of the form "fetch whoever was seen since we last looked" loses
purchases, and any memory of the form "this customer has no subscriptions,
skip them" loses them permanently. (An earlier version of this connector had
exactly that skip list; it dropped about 13% of new subscriptions.)

The plan below therefore never trusts a negative result:

1. **seeds**      operator-supplied change hints (optional). Always fetched.
2. **bootstrap**  operator-supplied ids that are KNOWN to have transacted (e.g.
                  a vendor export) and have no landed subscription: each one is
                  a proven gap, so they go ahead of everything that is merely
                  plausible. Seeds history on a first run, safety net after.
3. **sweep**      every customer that is not hot is re-checked on a fixed,
                  decaying schedule after the last time RC saw them
                  (``SWEEP_OFFSETS_HOURS``: hourly at first, thinning out to 30
                  days). The schedule is a pure function of the customer's
                  timestamps, so it needs no per-customer memory: a customer is
                  due when one of their checkpoints falls between the sweep
                  watermark and now. The watermark only advances over
                  checkpoints that were actually processed, so a failed run,
                  an outage or a capped backlog widens the next window instead
                  of dropping customers.
4. **hot**        customers with a subscription that is active or ended inside
                  the hot window: every run, stalest first (renewals,
                  cancellations, refunds and billing issues happen server-side
                  with no app session).
5. **heal**       when the reconciliation stream found a day on which RC counts
                  more transactions than were landed, every plausible customer
                  around that day is re-checked.
6. **cold**       a slow stale-first rotation over customers whose
                  subscriptions ended long ago (a resubscription made outside
                  the app moves no timestamp this source can see).

Bootstrap and heal are the only tiers that honour ``recently_empty`` (a
short-lived memory of ids that came back empty), purely to stop an
unresolvable id from being re-fetched every run. Seeds, sweep, hot and cold
never consult it.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

# Hours after a customer's anchor (the later of first_seen_at / last_seen_at)
# at which a customer without a hot subscription is re-checked. Dense while a
# purchase is likely, sparse later: 22 fetches per sighting over 30 days
# instead of one per run. The longest observed purchase-after-last-sighting
# gap was 20 h; the tail to 30 days is margin, and the cold rotation plus the
# reconciliation heal cover whatever lies beyond it.
SWEEP_OFFSETS_HOURS: tuple[int, ...] = (
    0, 1, 2, 3, 4, 6, 8, 12, 16, 20, 24, 30, 36, 48, 72, 96, 120, 168, 240, 336, 504, 720,
)  # fmt: skip
SWEEP_HORIZON = timedelta(hours=SWEEP_OFFSETS_HOURS[-1])
_EPSILON = timedelta(milliseconds=1)
# A heal re-checks customers seen from this long before the short day: a
# purchase can trail the last sighting by most of a day (see module docstring).
HEAL_LOOKBEHIND = timedelta(days=2)


@dataclass(frozen=True)
class SeenCustomer:
    """One row of the landed customers table."""

    id: str
    first_seen_at: datetime | None
    last_seen_at: datetime | None

    @property
    def anchor(self) -> datetime | None:
        stamps = [s for s in (self.first_seen_at, self.last_seen_at) if s is not None]
        return max(stamps) if stamps else None


@dataclass(frozen=True)
class KnownCustomer:
    """A customer with at least one landed subscription."""

    id: str
    extracted_at: datetime | None
    hot: bool


@dataclass
class Plan:
    """The ordered fetch list plus what the stream must persist afterwards."""

    ordered: list[str]
    tier_of: dict[str, str]
    counts: dict[str, int]
    new_watermark: datetime
    sweep_backlog: int = 0
    remember_if_empty: set[str] = field(default_factory=set)


def sweep_due_at(anchor: datetime, watermark: datetime, now: datetime) -> datetime | None:
    """Earliest checkpoint of ``anchor`` inside ``(watermark, now]``, if any."""
    if anchor > now or anchor + SWEEP_HORIZON <= watermark:
        return None
    for hours in SWEEP_OFFSETS_HOURS:
        checkpoint = anchor + timedelta(hours=hours)
        if checkpoint > now:
            return None
        if checkpoint > watermark:
            return checkpoint
    return None


def _stalest_first(customers: Iterable[KnownCustomer]) -> list[KnownCustomer]:
    return sorted(
        customers, key=lambda k: (k.extracted_at is not None, k.extracted_at or datetime.min, k.id)
    )


def plan_targets(
    *,
    now: datetime,
    watermark: datetime,
    snapshot_at: datetime | None,
    customers: Iterable[SeenCustomer],
    known: Mapping[str, KnownCustomer],
    seeds: Iterable[str] = (),
    heal_days: Iterable[date] = (),
    bootstrap_ids: Iterable[str] = (),
    recently_empty: Mapping[str, datetime] | None = None,
    empty_recheck: timedelta = timedelta(hours=24),
    cap: int = 8000,
    cold_per_run: int = 300,
) -> Plan:
    """Build the fetch plan for one run. See the module docstring for the tiers.

    ``watermark``   every sweep checkpoint at or before it has been processed.
    ``snapshot_at`` when the landed customers snapshot was taken (the latest
                    walk's start). The new watermark never passes it: customers
                    seen after the snapshot are not in ``customers`` yet, and
                    must still be due on the next run.
    """
    recently_empty = recently_empty or {}
    customers = list(customers)
    ordered: list[str] = []
    tier_of: dict[str, str] = {}

    def add(cid: str | None, tier: str) -> None:
        if cid and cid not in tier_of:
            tier_of[cid] = tier
            ordered.append(cid)

    def skipped_as_empty(cid: str) -> bool:
        checked = recently_empty.get(cid)
        return checked is not None and now - checked < empty_recheck

    for cid in seeds:
        add(cid, "seed")
    for cid in bootstrap_ids:
        if cid and cid not in known and not skipped_as_empty(cid):
            add(cid, "bootstrap")
    n_ahead = len(ordered)

    hot = _stalest_first(k for k in known.values() if k.hot)
    hot_ids = {k.id for k in hot}

    # Sweep: everyone who is not hot, known or not. A known-cold customer who
    # resubscribes in the app is caught here exactly like a first purchase.
    due: list[tuple[datetime, str]] = []
    for c in customers:
        if c.id in hot_ids or c.anchor is None:
            continue
        due_at = sweep_due_at(c.anchor, watermark, now)
        if due_at is not None:
            due.append((due_at, c.id))
    due.sort()
    # The hot set must not starve behind a sweep backlog (a first run, or the
    # run after an outage), and the sweep must not starve behind a hot set
    # larger than the cap: the sweep always gets at least a quarter of the cap.
    sweep_budget = max(cap // 4, cap - n_ahead - len(hot))
    for _, cid in due[:sweep_budget]:
        add(cid, "sweep")

    for k in hot:
        add(k.id, "hot")

    heal_days = sorted(set(heal_days))
    if heal_days:
        candidates: list[tuple[datetime, str]] = []
        for c in customers:
            if c.id in tier_of or c.anchor is None or skipped_as_empty(c.id):
                continue
            first_seen = c.first_seen_at or c.anchor
            for day in heal_days:
                start = datetime.combine(day, datetime.min.time(), tzinfo=now.tzinfo)
                if first_seen < start + timedelta(days=1) and c.anchor >= start - HEAL_LOOKBEHIND:
                    candidates.append((c.anchor, c.id))
                    break
        for _, cid in sorted(candidates, reverse=True):
            add(cid, "heal")

    cold = _stalest_first(k for k in known.values() if not k.hot and k.id not in tier_of)
    for k in cold[: max(0, cold_per_run)]:
        add(k.id, "cold")

    final = ordered[: max(0, cap)]
    final_set = set(final)

    # Advance the watermark only over sweep checkpoints that made it into this
    # run. Anything cut (by the sweep budget or by the cap) keeps its
    # checkpoint ahead of the watermark and is due again next run.
    unprocessed = [due_at for due_at, cid in due if cid not in final_set]
    ceiling = min(now, snapshot_at) if snapshot_at is not None else now
    if unprocessed:
        new_watermark = min(min(unprocessed) - _EPSILON, ceiling)
    else:
        new_watermark = ceiling
    new_watermark = max(new_watermark, watermark)

    counts = {t: 0 for t in ("seed", "bootstrap", "sweep", "hot", "heal", "cold")}
    for cid in final:
        counts[tier_of[cid]] += 1
    return Plan(
        ordered=final,
        tier_of={cid: tier_of[cid] for cid in final},
        counts=counts,
        new_watermark=new_watermark,
        sweep_backlog=len(unprocessed),
        remember_if_empty={cid for cid in final if tier_of[cid] in ("heal", "bootstrap")},
    )
