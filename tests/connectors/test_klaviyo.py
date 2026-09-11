# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Klaviyo connector tests — the folds that make this connector worth having.

Payload shapes here are copied from live ``a.klaviyo.com`` responses
(2026-09-11), not invented: the attribution object really does carry an empty
``attributes`` and put everything in ``relationships``, and the consent block
really is nested ``subscriptions.email.marketing``.
"""

from __future__ import annotations

from typing import Any

import pytest
import yaml

from dtex.sources.klaviyo.source import (
    _attribution_index,
    _consent_columns,
    _emit,
    _emit_stream,
    _event_record,
    _page_size,
    _predictive_columns,
    _profile_index,
    _rel_ids,
)
from dtex.types import ConnectorManifest

# --------------------------------------------------------------------------
# Fixtures — real response shapes
# --------------------------------------------------------------------------

FLOW_ATTRIBUTION: dict[str, Any] = {
    "type": "attribution",
    "id": "7rVW3gbFLw3",
    "attributes": {},
    "relationships": {
        "event": {"data": {"type": "event", "id": "7rVW3gbFLw3"}},
        "attributed-event": {"data": {"type": "event", "id": "7rVvdYN6Xat"}},
        "flow": {"data": {"type": "flow", "id": "UzF98V"}},
        "flow-message": {"data": {"type": "flow-message", "id": "RUkrXD"}},
    },
}

CAMPAIGN_ATTRIBUTION: dict[str, Any] = {
    "type": "attribution",
    "id": "7rWdBBsuCZG",
    "attributes": {},
    "relationships": {
        "event": {"data": {"type": "event", "id": "7rWdBBsuCZG"}},
        "attributed-event": {"data": {"type": "event", "id": "7rQuLCCagKN"}},
        "campaign": {"data": {"type": "campaign", "id": "01M1Q2Y9VSV6AH0MBX9K943HBK"}},
        "campaign-message": {"data": {"type": "campaign-message", "id": "01M1Q2"}},
    },
}

PROFILE: dict[str, Any] = {
    "type": "profile",
    "id": "P1",
    "attributes": {"email": "a@b.com", "external_id": "user_42"},
}


def _event(event_id: str, attribution_id: str | None, profile_id: str = "P1") -> dict[str, Any]:
    relationships: dict[str, Any] = {
        "metric": {"data": {"type": "metric", "id": "T7Ywek"}},
        "profile": {"data": {"type": "profile", "id": profile_id}},
    }
    if attribution_id:
        relationships["attributions"] = {
            "data": [{"type": "attribution", "id": attribution_id}]
        }
    return {
        "type": "event",
        "id": event_id,
        "attributes": {
            "datetime": "2026-08-30T10:57:14+00:00",
            "timestamp": 1787082558,
            "uuid": "bafaeb80-856d-11ee-8001-47fc5d8979c5",
            "event_properties": {
                "$event_id": "ch_3U3zc9KECY1q7gSO0otFKAP3",
                "$value": 70.8,
                "Currency": "usd",
                "$extra": {"PaymentIntent": "pi_3U3zc9KECY1q7gSO0TfiqD1Z"},
                "Invoice": {"ID": "in_1U3yfnKECY1q7gSOW3UQyMpf", "Total": 70.8},
            },
        },
        "relationships": relationships,
    }


@pytest.fixture
def page() -> dict[str, Any]:
    return {
        "data": [
            _event("7rVW3gbFLw3", "7rVW3gbFLw3"),
            _event("7rWdBBsuCZG", "7rWdBBsuCZG"),
            _event("unattributed", None),
        ],
        "included": [FLOW_ATTRIBUTION, CAMPAIGN_ATTRIBUTION, PROFILE],
    }


# --------------------------------------------------------------------------
# The attribution fold — the connector's reason to exist
# --------------------------------------------------------------------------


def test_flow_attribution_resolves(page: dict[str, Any]) -> None:
    record = _event_record(page["data"][0], _attribution_index(page))
    assert record["attributed_channel"] == "flow"
    assert record["attributed_flow_id"] == "UzF98V"
    assert record["attributed_flow_message_id"] == "RUkrXD"
    assert record["attributed_event_id"] == "7rVvdYN6Xat"
    # A flow-attributed row must not carry campaign ids.
    assert record["attributed_campaign_id"] is None
    assert record["attributed_campaign_message_id"] is None


def test_campaign_attribution_resolves(page: dict[str, Any]) -> None:
    record = _event_record(page["data"][1], _attribution_index(page))
    assert record["attributed_channel"] == "campaign"
    assert record["attributed_campaign_id"] == "01M1Q2Y9VSV6AH0MBX9K943HBK"
    assert record["attributed_campaign_message_id"] == "01M1Q2"
    assert record["attributed_flow_id"] is None


def test_unattributed_event_is_all_null(page: dict[str, Any]) -> None:
    record = _event_record(page["data"][2], _attribution_index(page))
    assert record["attribution_id"] is None
    assert record["attributed_channel"] is None
    assert record["attributed_flow_id"] is None


def test_attribution_paginated_out_keeps_the_id() -> None:
    """A relationship whose object missed the page is not silently 'unattributed'.

    The id survives with a NULL channel — the tell that attribution exists but
    was not resolved, rather than that none was credited.
    """
    orphan = {"data": [_event("orphan", "MISSING")], "included": []}
    record = _event_record(orphan["data"][0], _attribution_index(orphan))
    assert record["attribution_id"] == "MISSING"
    assert record["attributed_channel"] is None


def test_stripe_join_keys_are_promoted(page: dict[str, Any]) -> None:
    record = _event_record(page["data"][0], _attribution_index(page))
    assert record["event_id"] == "ch_3U3zc9KECY1q7gSO0otFKAP3"
    assert record["invoice_id"] == "in_1U3yfnKECY1q7gSOW3UQyMpf"
    assert record["payment_intent"] == "pi_3U3zc9KECY1q7gSO0TfiqD1Z"
    assert record["value"] == 70.8
    assert record["value_currency"] == "usd"


# --------------------------------------------------------------------------
# Inline identity
# --------------------------------------------------------------------------


def test_inline_profile_identity(page: dict[str, Any]) -> None:
    profiles = _profile_index(page)
    assert len(profiles) == 1
    record = _event_record(page["data"][0], _attribution_index(page), profiles)
    assert record["profile_email"] == "a@b.com"
    assert record["profile_external_id"] == "user_42"


def test_identity_present_on_unattributed_rows(page: dict[str, Any]) -> None:
    """Identity and attribution are independent — an unattributed event still joins."""
    record = _event_record(page["data"][2], _attribution_index(page), _profile_index(page))
    assert record["profile_external_id"] == "user_42"
    assert record["attributed_channel"] is None


def test_identity_null_when_include_disabled(page: dict[str, Any]) -> None:
    record = _event_record(page["data"][0], _attribution_index(page), None)
    assert record["profile_email"] is None
    assert record["profile_external_id"] is None


# --------------------------------------------------------------------------
# Consent — the opt-in field Airbyte never requests
# --------------------------------------------------------------------------

SUBSCRIPTIONS: dict[str, Any] = {
    "email": {
        "marketing": {
            "can_receive_email_marketing": False,
            "consent": "UNSUBSCRIBED",
            "consent_timestamp": "2024-05-30T13:51:17.095643+00:00",
            "method": "PREFERENCE_PAGE",
            "suppression": [
                {"reason": "UNSUBSCRIBE", "timestamp": "2024-05-30T13:51:17.095643+00:00"}
            ],
        }
    },
    "sms": {
        "marketing": {
            "can_receive_sms_marketing": True,
            "consent": "SUBSCRIBED",
            "consent_timestamp": "2025-01-01T00:00:00+00:00",
        }
    },
}


def test_consent_flattens() -> None:
    columns = _consent_columns(SUBSCRIPTIONS)
    assert columns["email_marketing_consent"] == "UNSUBSCRIBED"
    assert columns["email_marketing_can_receive"] is False
    assert columns["email_marketing_method"] == "PREFERENCE_PAGE"
    assert columns["email_marketing_suppressions"][0]["reason"] == "UNSUBSCRIBE"
    assert columns["sms_marketing_consent"] == "SUBSCRIBED"
    assert columns["sms_marketing_can_receive"] is True
    assert columns["subscriptions"] == SUBSCRIPTIONS


def test_consent_absent_yields_nulls_not_errors() -> None:
    """Without additional-fields[profile] the key is missing entirely.

    ``{}`` is a Mapping, so it takes the normal path and echoes itself back
    into ``subscriptions``; only a non-Mapping is treated as "absent".
    """
    absent: tuple[Any, ...] = (None, "not-a-mapping", 42)
    for value in absent:
        columns = _consent_columns(value)
        assert columns["email_marketing_consent"] is None
        assert columns["email_marketing_can_receive"] is None
        assert columns["subscriptions"] is None

    empty = _consent_columns({})
    assert empty["email_marketing_consent"] is None
    assert empty["email_marketing_can_receive"] is None


def test_predictive_analytics_flattens() -> None:
    columns = _predictive_columns(
        {
            "predicted_clv": 123.4,
            "historic_clv": 50.0,
            "total_clv": 173.4,
            "churn_probability": 0.2,
            "expected_date_of_next_order": "2026-10-01T00:00:00+00:00",
        }
    )
    assert columns["predicted_clv"] == 123.4
    assert columns["churn_probability"] == 0.2
    assert _predictive_columns(None)["predicted_clv"] is None


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def test_page_size_clamps_to_each_endpoints_ceiling() -> None:
    """Klaviyo's page[size] ceiling differs per endpoint and a breach is a hard 400.

    A single `catalog_page_size: 50` failed a whole run mid-flight on /lists
    ("Page size must be an integer between 1 and 10: 50"). Ceilings measured
    against the live API 2026-09-11.
    """
    # Over-asking is clamped down, never passed through.
    assert _page_size("lists/", 50) == 10
    assert _page_size("segments/", 50) == 10
    assert _page_size("templates/", 1000) == 10
    assert _page_size("flows/", 1000) == 50
    assert _page_size("tags/", 1000) == 50
    assert _page_size("campaigns/", 1000) == 100
    assert _page_size("forms/", 1000) == 100
    assert _page_size("profiles/", 1000, default=100) == 100
    assert _page_size("events/", 5000) == 1000

    # Under-asking is honoured — the ceiling is a maximum, not a target.
    assert _page_size("events/", 25) == 25
    assert _page_size("lists/", 5) == 5

    # Garbage falls back to the default, then clamps.
    assert _page_size("lists/", None) == 10
    assert _page_size("lists/", "not-a-number") == 10
    assert _page_size("events/", None, default=200) == 200

    # Never zero or negative — a page[size] of 0 is itself a 400.
    assert _page_size("lists/", 0) == 1
    assert _page_size("lists/", -5) == 1


def test_empty_snapshot_yields_one_empty_batch() -> None:
    """A `replace` stream that yields NOTHING never truncates the destination.

    The engine only calls write_batch for a yielded batch, so a catalog that
    legitimately emptied (a deleted list, an audience that drained) would
    keep its previous rows in the warehouse forever, looking current. One
    empty batch is the signal that the snapshot really is empty.
    """
    assert list(_emit([], ["id"], 2000)) == [[]]
    assert list(_emit_stream(iter([]), ["id"], 2000)) == [[]]


def test_emit_batches_and_projects() -> None:
    rows = [{"id": str(n), "extra": "dropped"} for n in range(5)]
    batches = list(_emit(rows, ["id"], 2))
    assert [len(b) for b in batches] == [2, 2, 1]
    assert batches[0][0] == {"id": "0"}  # undeclared columns are projected out


def test_emit_stream_batches_without_buffering_everything() -> None:
    """_emit_stream consumes lazily — memberships can run to millions of rows."""
    consumed = 0

    def rows() -> Any:
        nonlocal consumed
        for n in range(5):
            consumed += 1
            yield {"id": str(n)}

    batches = _emit_stream(rows(), ["id"], 2)
    first = next(batches)
    assert first == [{"id": "0"}, {"id": "1"}]
    # Only the first batch has been pulled, not the whole source.
    assert consumed == 2
    assert [len(b) for b in batches] == [2, 1]


def test_null_ids_are_skipped_not_stringified() -> None:
    """A missing id must never become the string "None".

    Every malformed sidecar would share that one key, so an event with an
    equally malformed relationship would match it and be assigned another
    message's flow — a wrong attribution, which is worse than none.
    """
    page = {
        "included": [
            {"type": "attribution", "relationships": {"flow": {"data": {"id": "WRONG"}}}},
            {"type": "profile", "attributes": {"email": "x@y.com"}},
        ]
    }
    assert _attribution_index(page) == {}
    assert _profile_index(page) == {}

    event: dict[str, Any] = {
        "attributes": {},
        "relationships": {"attributions": {"data": [{}]}},
    }
    record = _event_record(event, _attribution_index(page), _profile_index(page))
    assert record["attribution_id"] is None
    assert record["attributed_flow_id"] is None
    assert record["attributed_channel"] is None


def test_rel_ids_handles_to_many_and_missing() -> None:
    relationships = {"campaigns": {"data": [{"id": "c1"}, {"id": "c2"}]}, "flows": {"data": []}}
    assert _rel_ids(relationships, "campaigns") == ["c1", "c2"]
    assert _rel_ids(relationships, "flows") == []
    assert _rel_ids(relationships, "absent") == []


# --------------------------------------------------------------------------
# Manifest
# --------------------------------------------------------------------------


def test_manifest_loads_and_every_stream_has_a_function() -> None:
    """A manifest that will not construct makes the connector vanish SILENTLY.

    ``_walk_roots`` swallows the ValueError and skips the folder, so the only
    symptom is the connector missing from ``dtex list``. Worth a test.
    """
    from pathlib import Path

    import dtex.sources.klaviyo.source as source_module

    path = Path(source_module.__file__).parent / "register.yaml"
    manifest = ConnectorManifest.from_dict(yaml.safe_load(path.read_text()))

    assert manifest.name == "klaviyo"
    declared = {stream.name for stream in manifest.streams}
    # @stream stamps __dtex_stream_name__ (dunder both sides) onto the wrapper
    # — see dtex/registry.py. Matching the wrong spelling makes this test
    # claim every stream is missing while the connector is perfectly fine.
    implemented = {
        obj.__dtex_stream_name__
        for obj in vars(source_module).values()
        if callable(obj) and hasattr(obj, "__dtex_stream_name__")
    }
    missing = declared - implemented
    assert not missing, f"declared in register.yaml with no @stream function: {sorted(missing)}"


def test_events_stream_is_unordered_with_a_lookback() -> None:
    """The three declarations that make late attribution survivable.

    ``ordered: true`` here would let a mid-run flush advance the cursor past
    events whose attribution Klaviyo has not written yet — permanently.
    """
    from pathlib import Path

    import dtex.sources.klaviyo.source as source_module

    path = Path(source_module.__file__).parent / "register.yaml"
    raw = yaml.safe_load(path.read_text())
    events = next(s for s in raw["streams"] if s["name"] == "events")

    assert events["write_disposition"] == "merge"
    assert events["primary_key"] == "id"
    assert events["incremental"]["ordered"] is False
    assert events["incremental"]["lookback"] == "3d"
