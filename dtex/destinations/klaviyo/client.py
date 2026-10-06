# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys
"""Fixed-origin Klaviyo bulk event transport. Never logs payloads or responses."""

from __future__ import annotations

import json
import math
import time
from collections.abc import Mapping
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import requests

ENDPOINT = "https://a.klaviyo.com/api/event-bulk-create-jobs"
MAX_PAYLOAD_BYTES = 4_900_000
MAX_STRING_BYTES = 100_000
MAX_EVENTS = 1000


class DeliveryError(RuntimeError):
    """A delivery failed; messages contain neither payload nor response content."""


def _validate_json(value: Any) -> None:
    if isinstance(value, str):
        if len(value.encode("utf-8")) > MAX_STRING_BYTES:
            raise ValueError("Klaviyo event contains a string exceeding 100 KB")
    elif isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("Klaviyo event object keys must be strings")
            _validate_json(key)
            _validate_json(item)
    elif isinstance(value, list):
        for item in value:
            _validate_json(item)
    elif value is None or isinstance(value, (bool, int)):
        return
    elif isinstance(value, float) and math.isfinite(value):
        return
    else:
        raise ValueError("Klaviyo event contains an unsupported JSON value")


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Klaviyo event requires a non-empty {field}")
    return value


def event_entry(record: Mapping[str, Any], *, backfill: bool) -> tuple[str, bytes]:
    """Map a canonical event envelope; ignore unrelated warehouse columns."""
    unique_id = _text(record.get("unique_id"), "unique_id")
    metric_name = _text(record.get("metric_name"), "metric_name")
    event_time = record.get("time")
    if isinstance(event_time, str):
        try:
            event_time = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("Klaviyo event time must be an ISO timestamp with timezone") from None
    if not isinstance(event_time, datetime) or event_time.utcoffset() is None:
        raise ValueError("Klaviyo event time must be a timestamp with timezone")

    profile = record.get("profile")
    if not isinstance(profile, dict):
        raise ValueError("Klaviyo event requires a profile object")
    if not any(isinstance(profile.get(k), str) and profile[k].strip()
               for k in ("id", "email", "phone_number", "external_id")):
        raise ValueError("Klaviyo event profile requires an identifier")
    attributes = dict(profile)
    profile_data: dict[str, Any] = {"type": "profile"}
    if "id" in attributes:
        profile_data["id"] = _text(attributes.pop("id"), "profile id")
    if attributes:
        profile_data["attributes"] = attributes

    properties = record.get("properties", {})
    if not isinstance(properties, dict):
        raise ValueError("Klaviyo event properties must be an object")
    event: dict[str, Any] = {
        "unique_id": unique_id,
        "time": event_time.astimezone(UTC).isoformat(),
        "metric": {"data": {"type": "metric", "attributes": {"name": metric_name}}},
        "properties": properties,
    }
    if record.get("value") is not None:
        value = record["value"]
        if isinstance(value, bool) or not isinstance(value, (float, int)):
            raise ValueError("Klaviyo event value must be numeric")
        event["value"] = value
    if record.get("value_currency") is not None:
        currency = record["value_currency"]
        if not isinstance(currency, str) or len(currency) != 3 or not currency.isascii():
            raise ValueError("Klaviyo event value_currency must be a currency code")
        if not currency.isalpha() or currency != currency.upper():
            raise ValueError("Klaviyo event value_currency must be a currency code")
        event["value_currency"] = currency
    entry = {
        "type": "event-bulk-create",
        "attributes": {
            "backfill": backfill,
            "profile": {"data": profile_data},
            "events": {"data": [{"type": "event", "attributes": event}]},
        },
    }
    _validate_json(entry)
    return unique_id, json.dumps(entry, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


_PREFIX = b'{"data":{"type":"event-bulk-create-job","attributes":{"events-bulk-create":{"data":['
_SUFFIX = b']}}}}'


def prepare_requests(records: list[dict[str, Any]], *, backfill: bool) -> list[bytes]:
    """Validate a whole engine batch before sending any of it; split by API limits."""
    requests_out: list[bytes] = []
    entries: list[bytes] = []
    ids: set[str] = set()
    size = len(_PREFIX) + len(_SUFFIX)
    for record in records:
        unique_id, entry = event_entry(record, backfill=backfill)
        if len(entry) + len(_PREFIX) + len(_SUFFIX) > MAX_PAYLOAD_BYTES:
            raise ValueError("Klaviyo event exceeds the request size limit")
        added = len(entry) + bool(entries)
        # The bulk endpoint rejects repeated unique_id values in ONE request,
        # even across profiles. Separate requests preserve API idempotency.
        if entries and (len(entries) == MAX_EVENTS or unique_id in ids
                        or size + added > MAX_PAYLOAD_BYTES):
            requests_out.append(_PREFIX + b",".join(entries) + _SUFFIX)
            entries, ids = [], set()
            size = len(_PREFIX) + len(_SUFFIX)
        size += len(entry) + bool(entries)
        entries.append(entry)
        ids.add(unique_id)
    if entries:
        requests_out.append(_PREFIX + b",".join(entries) + _SUFFIX)
    return requests_out


class KlaviyoDeliveryClient:
    """Bounded retries of byte-identical, idempotent event submissions."""

    def __init__(self, api_key: str, *, revision: str = "2026-07-15",
                 max_attempts: int = 5, timeout: float = 60.0) -> None:
        if not api_key or any(ord(c) < 33 or ord(c) > 126 for c in api_key):
            raise ValueError("Klaviyo API key is missing or invalid")
        try:
            parsed_revision = date.fromisoformat(revision)
            if parsed_revision.isoformat() != revision:
                raise ValueError
        except (ValueError, TypeError):
            raise ValueError("Klaviyo API revision must be YYYY-MM-DD") from None
        if not 1 <= max_attempts <= 10 or not math.isfinite(timeout) or not 0 < timeout <= 300:
            raise ValueError("Klaviyo retry or timeout setting is outside the supported range")
        self.max_attempts = max_attempts
        self.timeout = timeout
        self._next_request = 0.0
        self._session = requests.Session()
        self._session.trust_env = False
        self._session.headers.update({
            "Authorization": f"Klaviyo-API-Key {api_key}",
            "revision": revision,
            "Content-Type": "application/vnd.api+json",
            "Accept": "application/vnd.api+json",
        })

    def close(self) -> None:
        self._session.close()

    def send(self, payload: bytes) -> None:
        for attempt in range(self.max_attempts):
            time.sleep(max(0.0, self._next_request - time.monotonic()))
            self._next_request = time.monotonic() + 0.5
            delay = min(2.0 ** attempt, 60.0)
            status = None
            try:
                response = self._session.post(
                    ENDPOINT, data=payload, timeout=(5.0, self.timeout), allow_redirects=False,
                )
            except requests.RequestException:
                # Raised outside the except below: even a chained traceback
                # must not contain requests' URL, headers, or response body.
                response = None
            if response is not None:
                try:
                    status = response.status_code
                    if status == 202:
                        return
                    if status != 429 and not 500 <= status < 600:
                        raise DeliveryError(f"Klaviyo request rejected (HTTP {status})")
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            delay = float(retry_after)
                        except ValueError:
                            try:
                                retry_at = parsedate_to_datetime(retry_after)
                                delay = (retry_at - datetime.now(UTC)).total_seconds()
                            except (TypeError, ValueError, OverflowError):
                                raise DeliveryError(
                                    "Klaviyo returned an invalid retry delay"
                                ) from None
                        if not math.isfinite(delay) or delay > 60:
                            raise DeliveryError("Klaviyo retry delay exceeds this attempt's budget")
                        delay = max(0.0, delay)
                finally:
                    response.close()
            if attempt + 1 == self.max_attempts:
                suffix = f" (HTTP {status})" if status is not None else " (network failure)"
                raise DeliveryError("Klaviyo request exhausted retries" + suffix) from None
            self._next_request = max(self._next_request, time.monotonic() + delay)
