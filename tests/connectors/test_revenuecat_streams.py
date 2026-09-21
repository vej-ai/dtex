# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Stream-level tests for the baked RevenueCat v2 connector (2.x).

``test_revenuecat.py`` covers the HTTP client against a scripted response
queue. The streams fan out concurrently, so a queue cannot serve them; this
file stands up ``FakeRC``, a small routed model of the v2 API (customers list
with ``starting_after``, per-customer subscriptions, per-subscription
transactions, charts), and drives the real engine into DuckDB.

The centre of it is ``test_purchase_after_last_sighting_is_found``: a customer
is seen, fetched while still subscription-less, buys later, and RevenueCat
never moves ``last_seen_at`` again. A connector that remembers "this customer
has no subscriptions" loses that purchase for ever; this one must land it.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import duckdb
import pytest

import dtex
from dtex.sources.revenuecat.landed import LandedReaderError, make_reader
from dtex.sources.revenuecat.targets import (
    SWEEP_OFFSETS_HOURS,
    KnownCustomer,
    SeenCustomer,
    plan_targets,
    sweep_due_at,
)

T0 = datetime(2026, 9, 21, 12, 0, tzinfo=UTC)


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


# --------------------------------------------------------------------------
# targets.py — pure planning
# --------------------------------------------------------------------------


def _seen(cid: str, last_seen: datetime, first_seen: datetime | None = None) -> SeenCustomer:
    return SeenCustomer(cid, first_seen or last_seen, last_seen)


def test_sweep_checkpoints_are_a_pure_function_of_the_anchor() -> None:
    anchor = T0
    # Seen since the watermark: due at once (offset 0).
    assert sweep_due_at(anchor, T0 - timedelta(minutes=5), T0 + timedelta(minutes=1)) == anchor
    # Already checked at offset 0, the 1 h checkpoint is still ahead: not due.
    assert sweep_due_at(anchor, T0 + timedelta(minutes=1), T0 + timedelta(minutes=59)) is None
    # An hour later the 1 h checkpoint falls inside the window.
    assert sweep_due_at(
        anchor, T0 + timedelta(minutes=1), T0 + timedelta(minutes=61)
    ) == anchor + timedelta(hours=1)
    # Past the horizon nothing is due any more.
    last = timedelta(hours=SWEEP_OFFSETS_HOURS[-1])
    assert sweep_due_at(anchor, anchor + last, anchor + last + timedelta(days=1)) is None


def test_every_purchase_gap_up_to_the_horizon_meets_a_later_checkpoint() -> None:
    """Whatever the delay between the last sighting and the purchase, a
    checkpoint follows it, so hourly runs with an advancing watermark fetch
    the customer again after they bought."""
    anchor = T0
    for gap_hours in (0.2, 0.9, 5, 11.5, 20, 47, 100, 400, 700):
        purchase = anchor + timedelta(hours=gap_hours)
        fetched_after_purchase = False
        watermark = anchor - timedelta(hours=1)
        now = anchor
        while now <= anchor + timedelta(hours=SWEEP_OFFSETS_HOURS[-1] + 1):
            if sweep_due_at(anchor, watermark, now) is not None and now >= purchase:
                fetched_after_purchase = True
                break
            watermark = now
            now += timedelta(hours=1)
        assert fetched_after_purchase, gap_hours


def test_plan_never_skips_a_subscriptionless_customer_in_the_sweep() -> None:
    """``recently_empty`` silences only the heal and bootstrap tiers."""
    a = _seen("a", T0 - timedelta(minutes=10))
    plan = plan_targets(
        now=T0,
        watermark=T0 - timedelta(hours=1),
        snapshot_at=T0,
        customers=[a],
        known={},
        recently_empty={"a": T0 - timedelta(minutes=30)},
    )
    assert plan.ordered == ["a"]
    assert plan.tier_of["a"] == "sweep"


def test_plan_orders_tiers_and_reports_counts() -> None:
    known = {
        "hot_stale": KnownCustomer("hot_stale", T0 - timedelta(hours=5), True),
        "hot_fresh": KnownCustomer("hot_fresh", T0 - timedelta(hours=1), True),
        "cold": KnownCustomer("cold", T0 - timedelta(days=200), False),
    }
    customers = [
        _seen("new", T0 - timedelta(minutes=5)),
        _seen("hot_stale", T0 - timedelta(minutes=5)),  # hot: fetched as hot, not twice
        _seen("cold", T0 - timedelta(days=300)),  # past the sweep horizon
    ]
    plan = plan_targets(
        now=T0,
        watermark=T0 - timedelta(hours=1),
        snapshot_at=T0,
        customers=customers,
        known=known,
        seeds=["seeded", "new"],
        bootstrap_ids=["boot", "cold", "boot_empty"],
        recently_empty={"boot_empty": T0 - timedelta(hours=2)},
    )
    assert plan.ordered == ["seeded", "new", "boot", "hot_stale", "hot_fresh", "cold"]
    assert plan.counts == {"seed": 2, "bootstrap": 1, "sweep": 0, "hot": 2, "heal": 0, "cold": 1}
    assert plan.remember_if_empty == {"boot"}
    assert plan.new_watermark == T0


def test_watermark_stops_before_the_first_checkpoint_that_did_not_fit() -> None:
    customers = [_seen(f"c{i:02d}", T0 - timedelta(minutes=50 - i)) for i in range(40)]
    plan = plan_targets(
        now=T0,
        watermark=T0 - timedelta(hours=1),
        snapshot_at=T0,
        customers=customers,
        known={},
        cap=10,
    )
    assert plan.ordered == [f"c{i:02d}" for i in range(10)]
    assert plan.sweep_backlog == 30
    first_cut = T0 - timedelta(minutes=40)
    assert plan.new_watermark < first_cut
    # The next run picks up exactly where this one stopped.
    again = plan_targets(
        now=T0 + timedelta(minutes=1),
        watermark=plan.new_watermark,
        snapshot_at=T0,
        customers=customers,
        known={},
        cap=100,
    )
    assert again.ordered == [f"c{i:02d}" for i in range(10, 40)]


def test_watermark_never_passes_the_customers_snapshot() -> None:
    """Customers seen after the walk are not in the snapshot yet; the window
    that hides them must stay open."""
    snapshot = T0 - timedelta(hours=3)
    plan = plan_targets(
        now=T0, watermark=T0 - timedelta(hours=5), snapshot_at=snapshot, customers=[], known={}
    )
    assert plan.new_watermark == snapshot


def test_sweep_keeps_a_quarter_of_the_cap_when_the_hot_set_is_larger() -> None:
    known = {f"h{i}": KnownCustomer(f"h{i}", T0 - timedelta(hours=1), True) for i in range(100)}
    customers = [_seen(f"n{i}", T0 - timedelta(minutes=5)) for i in range(50)]
    plan = plan_targets(
        now=T0,
        watermark=T0 - timedelta(hours=1),
        snapshot_at=T0,
        customers=customers,
        known=known,
        cap=40,
    )
    assert plan.counts["sweep"] == 10
    assert plan.counts["hot"] == 30


def test_heal_rechecks_customers_around_a_short_day_once_per_recheck_window() -> None:
    day = date(2026, 9, 18)
    customers = [
        _seen("around", datetime(2026, 9, 17, 9, tzinfo=UTC)),
        _seen("checked", datetime(2026, 9, 18, 9, tzinfo=UTC)),
        _seen("too_old", datetime(2026, 9, 10, 9, tzinfo=UTC)),
        _seen(
            "too_new",
            datetime(2026, 9, 19, 9, tzinfo=UTC),
            first_seen=datetime(2026, 9, 19, 8, tzinfo=UTC),
        ),
    ]
    # A watermark at `now` keeps the sweep out of the picture.
    plan = plan_targets(
        now=T0,
        watermark=T0,
        snapshot_at=T0,
        customers=customers,
        known={},
        heal_days=[day],
        recently_empty={"checked": T0 - timedelta(hours=3)},
    )
    assert plan.ordered == ["around"]
    assert plan.remember_if_empty == {"around"}


# --------------------------------------------------------------------------
# landed.py
# --------------------------------------------------------------------------


def test_reader_is_required_and_named(tmp_path: Path) -> None:
    import logging

    log = logging.getLogger("t")
    with pytest.raises(LandedReaderError, match="landed_reader"):
        make_reader("", "", log)
    with pytest.raises(LandedReaderError, match="unknown"):
        make_reader("snowflake", "x", log)
    with pytest.raises(LandedReaderError, match="project.dataset"):
        make_reader("bigquery", "just_a_dataset", log)

    db = tmp_path / "w.duckdb"
    conn = duckdb.connect(str(db))
    conn.execute("CREATE SCHEMA s; CREATE TABLE s.subscriptions (id VARCHAR, n INTEGER)")
    conn.execute("INSERT INTO s.subscriptions VALUES ('a', 1)")
    conn.close()
    reader = make_reader("duckdb", f"{db}#s", log)
    assert reader.rows("subscriptions", ["id", "n"]) == [{"id": "a", "n": 1}]
    assert reader.rows("not_there", ["id"]) is None
    assert reader.sql("SELECT 'x' AS customer_id") == [{"customer_id": "x"}]


# --------------------------------------------------------------------------
# FakeRC — a routed model of the v2 API
# --------------------------------------------------------------------------


class FakeRC:
    def __init__(self) -> None:
        self.customers: dict[str, dict[str, Any]] = {}
        self.subscriptions: dict[str, list[dict[str, Any]]] = {}
        self.transactions: dict[str, list[dict[str, Any]]] = {}
        self.chart_transactions: dict[date, int] = {}
        self.sort_key: Callable[[str], Any] = lambda cid: cid
        self.requests: list[str] = []
        self._lock = threading.Lock()
        self.base_url = ""

    # -- building the world ------------------------------------------------

    def add_customer(
        self, cid: str, first_seen: datetime, last_seen: datetime | None = None
    ) -> None:
        self.customers[cid] = {
            "object": "customer",
            "id": cid,
            "first_seen_at": _ms(first_seen),
            "last_seen_at": _ms(last_seen or first_seen),
            "last_seen_platform": "iOS",
            "last_seen_country": "US",
            "last_seen_app_version": "1.0",
        }

    def add_purchase(
        self,
        cid: str,
        sub_id: str,
        txn_id: str,
        at: datetime,
        gross: float = 10.0,
        store: str = "app_store",
    ) -> None:
        self.subscriptions.setdefault(cid, []).append(
            {
                "id": sub_id,
                "customer_id": cid,
                "product_id": "prod1",
                "status": "active",
                "gives_access": True,
                "auto_renewal_status": "will_renew",
                "pending_payment": False,
                "environment": "production",
                "store": store,
                "store_subscription_identifier": txn_id,
                "starts_at": _ms(at),
                "current_period_starts_at": _ms(at),
                "current_period_ends_at": _ms(at + timedelta(days=30)),
                "ends_at": None,
                "total_revenue_in_usd": {
                    "gross": gross,
                    "tax": 0,
                    "commission": 3,
                    "proceeds": gross - 3,
                },
                "entitlements": {"items": [{"lookup_key": "pro"}]},
            }
        )
        self.transactions[sub_id] = [
            {
                "id": txn_id,
                "product_store_identifier": "pro_monthly",
                "purchased_at": _ms(at),
                "expiration_date": _ms(at + timedelta(days=30)),
                "effective_expiration_date": _ms(at + timedelta(days=30)),
                "revenue_in_usd": {
                    "gross": gross,
                    "tax": 0,
                    "commission": 3,
                    "proceeds": gross - 3,
                    "currency": "USD",
                },
                "revenue_in_local_currency": {
                    "gross": gross,
                    "tax": 0,
                    "commission": 3,
                    "proceeds": gross - 3,
                    "currency": "USD",
                },
            }
        ]
        self.chart_transactions[at.date()] = self.chart_transactions.get(at.date(), 0) + 1

    def refund(self, cid: str, sub_id: str) -> None:
        for sub in self.subscriptions[cid]:
            if sub["id"] == sub_id:
                sub["total_revenue_in_usd"] = {"gross": 0, "tax": 0, "commission": 0, "proceeds": 0}
        for txn in self.transactions[sub_id]:
            for block in ("revenue_in_usd", "revenue_in_local_currency"):
                txn[block] = {**txn[block], "gross": 0, "commission": 0, "proceeds": 0}

    # -- serving -------------------------------------------------------------

    def _list(self, items: list[dict], path: str, query: dict[str, list[str]]) -> dict:
        limit = int(query.get("limit", ["20"])[0])
        page = items[:limit]
        next_page = None
        if len(items) > limit:
            next_page = f"{self.base_url}{path}?starting_after={page[-1]['id']}&limit={limit}"
        return {"object": "list", "items": page, "next_page": next_page}

    def handle(self, raw_path: str) -> tuple[int, dict]:
        with self._lock:
            self.requests.append(raw_path)
        url = urlparse(raw_path)
        query = parse_qs(url.query)
        parts = [unquote(p) for p in url.path.strip("/").split("/")]
        assert parts[:2] == ["projects", "proj_test"], raw_path
        rest = parts[2:]
        if rest == ["customers"]:
            ordered = sorted(self.customers.values(), key=lambda c: self.sort_key(c["id"]))
            after = query.get("starting_after", [None])[0]
            if after is not None:
                ordered = [c for c in ordered if self.sort_key(c["id"]) > self.sort_key(after)]
            return 200, self._list(ordered, url.path, query)
        if len(rest) == 3 and rest[0] == "customers":
            cid, what = rest[1], rest[2]
            if cid not in self.customers:
                return 404, {"type": "resource_missing"}
            if what == "subscriptions":
                return 200, self._list(self.subscriptions.get(cid, []), url.path, query)
            if what == "aliases":
                return 200, self._list(
                    [{"id": cid}, {"id": f"$RCAnonymousID:{cid}"}], url.path, query
                )
            if what == "attributes":
                return 200, self._list(
                    [
                        {"id": "1", "name": "$email", "value": f"{cid}@example.com"},
                        {"id": "2", "name": "plan_hint", "value": "x"},
                    ],
                    url.path,
                    query,
                )
        if len(rest) == 3 and rest[0] == "subscriptions" and rest[2] == "transactions":
            if rest[1] not in self.transactions:
                return 400, {"type": "parameter_error"}
            return 200, self._list(self.transactions[rest[1]], url.path, query)
        if rest == ["products"]:
            return 200, self._list(
                [
                    {
                        "id": "prod1",
                        "store_identifier": "pro_monthly",
                        "type": "subscription",
                        "display_name": "Pro",
                        "subscription": {"duration": "P1M"},
                    }
                ],
                url.path,
                query,
            )
        if rest == ["entitlements"]:
            return 200, self._list(
                [{"id": "ent1", "lookup_key": "pro", "display_name": "Pro"}], url.path, query
            )
        if rest == ["entitlements", "ent1", "products"]:
            return 200, self._list(
                [{"id": "prod1", "store_identifier": "pro_monthly"}], url.path, query
            )
        if len(rest) == 2 and rest[0] == "charts":
            start = date.fromisoformat(query["start_date"][0])
            end = date.fromisoformat(query["end_date"][0])
            values = []
            day = start
            while day <= end:
                cohort = int(datetime.combine(day, datetime.min.time(), tzinfo=UTC).timestamp())
                count = self.chart_transactions.get(day, 0)
                values.append(
                    {"cohort": cohort, "incomplete": False, "measure": 0, "value": count * 10.0}
                )
                values.append(
                    {"cohort": cohort, "incomplete": False, "measure": 1, "value": float(count)}
                )
                day += timedelta(days=1)
            return 200, {
                "measures": [{"display_name": "Revenue"}, {"display_name": "Transactions"}],
                "values": values,
            }
        return 404, {"type": "resource_missing", "path": raw_path}


@pytest.fixture
def fake_rc() -> Iterator[FakeRC]:
    fake = FakeRC()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args: Any) -> None:
            return

        def do_GET(self) -> None:  # noqa: N802 - required by stdlib
            status, body = fake.handle(self.path)
            payload = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    fake.base_url = f"http://127.0.0.1:{server.server_address[1]}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield fake
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5.0)


def _project(tmp_path: Path, fake: FakeRC, streams: str = "all", extra_params: str = "") -> str:
    db_path = str(tmp_path / "warehouse.duckdb")
    (tmp_path / "dtex_project.yml").write_text(
        "name: t\nversion: '0.1'\nsource_paths: []\ndestination_paths: []\n"
        "config_paths:\n  - configs\n"
    )
    (tmp_path / "profiles.yml").write_text(
        "duckdb:\n  default_target: dev\n  targets:\n    dev:\n"
        "      path: '.dtex/warehouse.duckdb'\n"
    )
    (tmp_path / "configs").mkdir(exist_ok=True)
    (tmp_path / "configs" / "rc_test.yml").write_text(
        "name: rc_test\nsource: revenuecat\ndestination: duckdb\ntarget: dev\n"
        "params:\n  project_id: 'proj_test'\n"
        f"  base_url: '{fake.base_url}'\n  rate_per_second: 0\n  max_retries: 0\n  page_size: 3\n"
        f"  landed_reader: duckdb\n  landed_dataset: '{db_path}'\n"
        "  metrics_initial_since_date: '2026-09-01'\n"
        f"  metrics_charts: 'revenue'\n{extra_params}"
        f"streams: {streams}\n"
    )
    return db_path


def _run(tmp_path: Path, db_path: str, monkeypatch: pytest.MonkeyPatch, at: datetime) -> None:
    monkeypatch.setenv("REVENUECAT_API_KEY", "sk_test_unit")
    monkeypatch.setenv("DTEX_REVENUECAT_NOW", at.isoformat())
    result = dtex.run(
        config="rc_test", project_dir=str(tmp_path), destination_params_override={"path": db_path}
    )
    assert result.status.value == "succeeded", result.error


def _query(db_path: str, statement: str) -> list[tuple]:
    conn = duckdb.connect(db_path)
    try:
        return conn.execute(statement).fetchall()
    finally:
        conn.close()


# --------------------------------------------------------------------------
# customers walk
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sort_key",
    [
        lambda cid: cid,  # bytewise
        lambda cid: cid.lower(),  # case-insensitive collation
        lambda cid: (cid.startswith("$"), cid),  # anonymous block last, as observed on RC
        lambda cid: tuple(-ord(ch) for ch in cid),  # an order the connector knows nothing about
    ],
)
def test_customers_walk_is_complete_under_any_list_order(
    fake_rc: FakeRC, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sort_key: Callable[[str], Any]
) -> None:
    ids = [f"{h}{i:02d}" for h in "0123456789abcdefXYZ-" for i in range(4)]
    ids += [f"$RCAnonymousID:{h}{i}" for h in "05af" for i in range(3)]
    for cid in ids:
        fake_rc.add_customer(cid, T0 - timedelta(days=1))
    fake_rc.sort_key = sort_key
    db_path = _project(tmp_path, fake_rc, streams="\n  customers:")
    _run(tmp_path, db_path, monkeypatch, T0)
    landed = {r[0] for r in _query(db_path, "SELECT id FROM customers")}
    assert landed == set(ids)


def test_customers_walk_single_chain(
    fake_rc: FakeRC, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for i in range(10):
        fake_rc.add_customer(f"c{i}", T0)
    db_path = _project(
        tmp_path, fake_rc, streams="\n  customers:", extra_params="  customers_walk_chains: 1\n"
    )
    _run(tmp_path, db_path, monkeypatch, T0)
    assert _query(db_path, "SELECT COUNT(*) FROM customers") == [(10,)]
    assert all("starting_after=0" not in r for r in fake_rc.requests)


# --------------------------------------------------------------------------
# end to end
# --------------------------------------------------------------------------


def test_purchase_after_last_sighting_is_found(
    fake_rc: FakeRC, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_rc.add_customer("buyer_later", T0 - timedelta(minutes=10))
    fake_rc.add_customer("subscriber", T0 - timedelta(days=40), T0 - timedelta(days=2))
    fake_rc.add_purchase("subscriber", "sub_1", "txn_1", T0 - timedelta(days=2))
    db_path = _project(tmp_path, fake_rc)

    _run(tmp_path, db_path, monkeypatch, T0)
    assert _query(db_path, "SELECT id FROM subscriptions") == [("sub_1",)]
    assert _query(db_path, "SELECT id, first_gross_usd FROM subscription_transactions") == [
        ("txn_1", 10.0)
    ]
    assert _query(db_path, "SELECT SUM(missing) FROM reconciliation_daily") == [(0,)]
    assert any("/customers/buyer_later/subscriptions" in r for r in fake_rc.requests)

    # The customer buys 20 minutes later. RevenueCat does NOT move last_seen_at.
    fake_rc.add_purchase("buyer_later", "sub_2", "txn_2", T0 + timedelta(minutes=20), gross=57.68)

    # Five minutes on, nothing is due yet for them (their 1 h checkpoint is
    # ahead), and the reconciliation stream says so out loud.
    fake_rc.requests.clear()
    _run(tmp_path, db_path, monkeypatch, T0 + timedelta(minutes=25))
    assert not any("/customers/buyer_later/subscriptions" in r for r in fake_rc.requests)
    assert _query(
        db_path, f"SELECT missing FROM reconciliation_daily WHERE day = '{T0.date()}'"
    ) == [(1,)]

    # The next hourly run crosses the checkpoint and lands the purchase.
    _run(tmp_path, db_path, monkeypatch, T0 + timedelta(minutes=65))
    assert _query(db_path, "SELECT id FROM subscriptions ORDER BY id") == [("sub_1",), ("sub_2",)]
    assert _query(db_path, "SELECT id, gross_usd FROM subscription_transactions ORDER BY id") == [
        ("txn_1", 10.0),
        ("txn_2", 57.68),
    ]
    assert _query(db_path, "SELECT SUM(missing) FROM reconciliation_daily") == [(0,)]
    assert _query(db_path, "SELECT customer_id, email FROM customer_details ORDER BY 1") == [
        ("buyer_later", "buyer_later@example.com"),
        ("subscriber", "subscriber@example.com"),
    ]

    state = dict(_query(db_path, "SELECT stream, CAST(state_blob AS VARCHAR) FROM _dtex_state"))
    assert "sweep_watermark" in state["subscriptions"]
    assert "customers_without_subscriptions" not in state["subscriptions"]
    assert "changed_watermark" in state["subscription_transactions"]


def test_reconciliation_gap_heals_a_customer_the_sweep_is_done_with(
    fake_rc: FakeRC, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A purchase lands on a day RC reports, but the buyer's checkpoints are
    all behind the watermark. The short day alone must bring them back."""
    fake_rc.add_customer("ghost", T0 - timedelta(days=2))
    db_path = _project(tmp_path, fake_rc, extra_params="  sweep_initial_days: 0\n")
    _run(tmp_path, db_path, monkeypatch, T0)  # watermark = T0; ghost never fetched
    assert not any("/customers/ghost/subscriptions" in r for r in fake_rc.requests)

    fake_rc.add_purchase("ghost", "sub_g", "txn_g", T0 - timedelta(days=1))
    _run(
        tmp_path, db_path, monkeypatch, T0 + timedelta(minutes=5)
    )  # reconciliation finds the short day
    assert _query(db_path, "SELECT SUM(missing) FROM reconciliation_daily") == [(1,)]
    assert _query(db_path, "SELECT COUNT(*) FROM subscriptions") == [(0,)]

    _run(tmp_path, db_path, monkeypatch, T0 + timedelta(minutes=10))  # the heal tier fetches ghost
    assert _query(db_path, "SELECT id FROM subscription_transactions") == [("txn_g",)]
    assert _query(db_path, "SELECT SUM(missing) FROM reconciliation_daily") == [(0,)]


def test_refund_keeps_first_gross_and_stamps_detection(
    fake_rc: FakeRC, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_rc.add_customer("c", T0 - timedelta(days=3))
    fake_rc.add_purchase("c", "sub_1", "txn_1", T0 - timedelta(days=3), gross=25.0)
    db_path = _project(tmp_path, fake_rc)
    _run(tmp_path, db_path, monkeypatch, T0)

    fake_rc.refund("c", "sub_1")
    later = T0 + timedelta(hours=1)
    _run(tmp_path, db_path, monkeypatch, later)
    row = _query(
        db_path,
        "SELECT gross_usd, first_gross_usd, refund_detected_at IS NOT NULL "
        "FROM subscription_transactions",
    )
    assert row == [(0.0, 25.0, True)]
    assert _query(db_path, "SELECT refund_detected_at IS NOT NULL FROM subscriptions") == [(True,)]

    # An unchanged subscription is not re-pulled: the watermark moved past it.
    fake_rc.requests.clear()
    _run(tmp_path, db_path, monkeypatch, later + timedelta(hours=1))
    assert not any("/transactions" in r for r in fake_rc.requests)


def test_diff_streams_refuse_to_run_without_a_reader(
    fake_rc: FakeRC, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = _project(
        tmp_path, fake_rc, streams="\n  subscriptions:", extra_params="  landed_reader: ''\n"
    )
    monkeypatch.setenv("REVENUECAT_API_KEY", "sk_test_unit")
    result = dtex.run(
        config="rc_test", project_dir=str(tmp_path), destination_params_override={"path": db_path}
    )
    assert result.status.value != "succeeded"
    assert "landed_reader" in str(result.error)


def test_metrics_daily_backfills_then_repulls_the_trailing_window(
    fake_rc: FakeRC, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_rc.chart_transactions[date(2026, 9, 10)] = 4
    db_path = _project(tmp_path, fake_rc, streams="\n  metrics_daily:")
    _run(tmp_path, db_path, monkeypatch, T0)
    first = (
        "SELECT value FROM metrics_daily "
        "WHERE cohort_date = '2026-09-10' AND measure_name = 'Transactions'"
    )
    assert _query(db_path, first) == [(4.0,)]
    assert _query(
        db_path, "SELECT MIN(cohort_date), COUNT(DISTINCT cohort_date) FROM metrics_daily"
    ) == [
        (date(2026, 9, 1), 21),
    ]

    # RC revises a day that was already complete; the next run must see it.
    fake_rc.chart_transactions[date(2026, 9, 19)] = 9
    fake_rc.requests.clear()
    _run(tmp_path, db_path, monkeypatch, T0 + timedelta(hours=1))
    revised = (
        "SELECT value FROM metrics_daily "
        "WHERE cohort_date = '2026-09-19' AND measure_name = 'Transactions'"
    )
    assert _query(db_path, revised) == [(9.0,)]
    assert any("start_date=2026-09-14" in r for r in fake_rc.requests)
