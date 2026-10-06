# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys
"""Append-only event delivery; warehouse-backed checkpoints, leases and audit."""

from __future__ import annotations

import importlib
from collections.abc import Sequence
from dataclasses import dataclass
from types import ModuleType
from typing import Any

from dtex import (
    Batch,
    Capability,
    Config,
    LeaseRecord,
    RunRecord,
    StateRecord,
    StreamMeta,
    WriteDisposition,
    destination,
)

from .client import KlaviyoDeliveryClient, prepare_requests


@dataclass(repr=False)
class Connection:
    client: KlaviyoDeliveryClient
    backend: ModuleType
    state: Any
    backfill: bool


@destination.capabilities
def capabilities() -> set[Capability]:
    # State is managed by this destination in its configured warehouse.
    # An HTTP acceptance cannot participate in a warehouse transaction.
    return {Capability.STATE, Capability.RUN_RECORDS, Capability.LEASE}


@destination.max_concurrent_writes
def max_concurrent_writes(config: Config) -> int:
    return 1


@destination.open
def open(config: Config) -> Connection:
    backfill = config.get("backfill")
    if not isinstance(backfill, bool):
        raise ValueError("Klaviyo destination requires an explicit boolean backfill setting")
    backend_name = config.get("state_backend", "duckdb")
    if backend_name not in ("duckdb", "bigquery"):
        raise ValueError("Klaviyo state_backend must be duckdb or bigquery")
    params: dict[str, Any]
    if backend_name == "duckdb":
        state_path = config.get("state_path", ".dtex/klaviyo-state.duckdb")
        if not isinstance(state_path, str) or not state_path.strip() or state_path == ":memory:":
            raise ValueError("Klaviyo state_path must name a persistent DuckDB file")
        params = {"path": state_path}
    else:
        params = {
            "project": config.get("state_project"),
            "dataset": config.get("state_dataset"),
            "location": config.get("state_location", "US"),
            "staging_bucket": config.get("state_staging_bucket"),
            "auth_type": "oauth",
        }
    client = KlaviyoDeliveryClient(
        config.secrets.get("api_key", ""),
        revision=str(config.get("api_revision", "2026-07-15")),
        max_attempts=int(config.get("max_attempts", 5)),
        timeout=float(config.get("timeout_seconds", 60)),
    )
    try:
        # Lazy import avoids registering another destination's decorators
        # inside this connector's discovery scope.
        backend = importlib.import_module(f"dtex.destinations.{backend_name}.destination")
        state = backend.open(Config(params=params))
    except BaseException:
        client.close()
        raise
    return Connection(client, backend, state, backfill)


@destination.ensure_schema
def ensure_schema(conn: Connection, stream: StreamMeta) -> None:
    if stream.write_disposition is not WriteDisposition.APPEND:
        raise ValueError("Klaviyo destination supports only append event streams")
    required = {"unique_id", "metric_name", "time", "profile"}
    if not required.issubset(stream.schema.names):
        raise ValueError("Klaviyo event schema requires unique_id, metric_name, time and profile")


@destination.write_batch
def write_batch(conn: Connection, batch: Batch, stream: StreamMeta) -> int:
    ensure_schema(conn, stream)
    payloads = prepare_requests(batch, backfill=conn.backfill)
    for payload in payloads:
        conn.client.send(payload)
    # Count API-accepted records, not confirmed downstream processing.
    # A failure raises; it never claims the unsent remainder was accepted.
    return len(batch)


@destination.read_state
def read_state(conn: Connection, connector: str) -> list[StateRecord]:
    return conn.backend.read_state(conn.state, connector)


@destination.commit_state
def commit_state(conn: Connection, run_id: str, records: list[StateRecord]) -> None:
    conn.backend.commit_state(conn.state, run_id, records)


@destination.acquire_leases
def acquire_leases(conn: Connection, leases: Sequence[LeaseRecord]) -> set[str]:
    return conn.backend.acquire_leases(conn.state, leases)


@destination.read_leases
def read_leases(conn: Connection, connector: str) -> list[LeaseRecord]:
    return conn.backend.read_leases(conn.state, connector)


@destination.heartbeat_leases
def heartbeat_leases(conn: Connection, leases: Sequence[LeaseRecord]) -> None:
    conn.backend.heartbeat_leases(conn.state, leases)


@destination.release_leases
def release_leases(conn: Connection, leases: Sequence[LeaseRecord]) -> None:
    conn.backend.release_leases(conn.state, leases)


@destination.write_run_record
def write_run_record(conn: Connection, record: RunRecord) -> None:
    conn.backend.write_run_record(conn.state, record)


@destination.close
def close(conn: Connection) -> None:
    try:
        conn.client.close()
    finally:
        conn.backend.close(conn.state)
