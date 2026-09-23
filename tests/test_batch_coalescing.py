"""Tests for destination-driven batch coalescing (``@destination.min_batch_rows``).

A destination with a high fixed cost per write (BigQuery) declares a minimum
rows-per-write; the engine buffers small source batches up to it. These prove:

    * many small source batches become few writes of >= the threshold, plus a
      final partial write, and every row is written exactly once;
    * without the hook (or with 0) every source batch is written as yielded;
    * state is still flushed only after a write lands, and the flushed resume
      pointer never runs ahead of the rows that are durable;
    * lease heartbeats keep firing while the buffer fills;
    * a merge stream's duplicate keys across coalesced pages are de-duplicated;
    * the destination hook is resolved from its params and rejects bad values.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from dtex import Batch, Config
from dtex.destinations.bigquery import destination as bq
from dtex.engine import runner
from dtex.engine.runner import _min_batch_rows, _run_one_stream
from dtex.types import StreamDef, WriteDisposition

from .test_state_flush import (
    LOG,
    _hooks_with_log,
    _incremental_stream_def,
    _make_source,
    _pipeline,
    _run_config,
)


def _pages(n_pages: int, per_page: int, *, state_key: bool = True):
    def gen(config: Config, state: Any, cursor: Any, log: Any) -> Iterator[Batch]:
        for p in range(n_pages):
            base = p * per_page
            rows = [{"id": base + i, "updated_at": base + i} for i in range(per_page)]
            if state_key:
                state.set("pk", p + 1)  # resume pointer = pages fully yielded
            cursor.observe(rows[-1]["updated_at"])
            yield rows

    return gen


def _writes(events: list[tuple[str, Any]]) -> list[int]:
    return [n for kind, n in events if kind == "write_batch"]


def test_small_batches_are_coalesced_to_threshold() -> None:
    events: list[tuple[str, Any]] = []
    result = _run_one_stream(
        _incremental_stream_def(), _make_source(_pages(25, 100)), _hooks_with_log(events), object(),
        _run_config(), _pipeline(), None, LOG, min_batch_rows=1000,
    )
    assert _writes(events) == [1000, 1000, 500]
    assert result.rows_loaded == result.rows_extracted == 2500


def test_without_threshold_every_batch_is_written() -> None:
    events: list[tuple[str, Any]] = []
    _run_one_stream(
        _incremental_stream_def(), _make_source(_pages(5, 100)), _hooks_with_log(events), object(),
        _run_config(), _pipeline(), None, LOG,
    )
    assert _writes(events) == [100] * 5


def test_flushed_resume_pointer_never_runs_ahead_of_durable_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every mid-stream flush records exactly the pages whose rows are written."""
    monkeypatch.setattr(runner, "STATE_COMMIT_INTERVAL_SECONDS", 0)
    events: list[tuple[str, Any]] = []
    _run_one_stream(
        _incremental_stream_def(), _make_source(_pages(10, 100)), _hooks_with_log(events), object(),
        _run_config(), _pipeline(), None, LOG, min_batch_rows=300,
    )
    written = 0
    for kind, value in events:
        if kind == "write_batch":
            written += value
        elif kind == "commit_state":
            # pk = pages yielded; each page is 100 rows, all of which must be durable
            assert value * 100 <= written, (value, written)
    assert _writes(events) == [300, 300, 300, 100]


def test_crash_mid_buffer_loses_nothing_that_was_flushed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failing coalesced write leaves the last flushed pointer at durable rows only."""
    monkeypatch.setattr(runner, "STATE_COMMIT_INTERVAL_SECONDS", 0)
    events: list[tuple[str, Any]] = []
    with pytest.raises(RuntimeError):
        _run_one_stream(
            _incremental_stream_def(), _make_source(_pages(10, 100)),
            _hooks_with_log(events, write_batch_raises_on=2), object(),
            _run_config(), _pipeline(), None, LOG, min_batch_rows=400,
        )
    commits = [v for k, v in events if k == "commit_state"]
    assert commits == [4]            # first 400-row write landed after page 4
    assert _writes(events) == [400]  # the second write raised


def test_heartbeat_fires_while_buffering() -> None:
    beats: list[int] = []
    events: list[tuple[str, Any]] = []
    _run_one_stream(
        _incremental_stream_def(), _make_source(_pages(9, 100)), _hooks_with_log(events), object(),
        _run_config(), _pipeline(), None, LOG,
        heartbeat=lambda: beats.append(1), min_batch_rows=1000,
    )
    assert _writes(events) == [900]
    assert len(beats) >= 8           # one per buffered page, plus the write


def test_merge_duplicates_across_pages_are_deduplicated() -> None:
    def gen(config: Config, state: Any, cursor: Any, log: Any) -> Iterator[Batch]:
        yield [{"id": 1, "updated_at": 1}, {"id": 2, "updated_at": 2}]
        yield [{"id": 2, "updated_at": 3}, {"id": 3, "updated_at": 3}]

    sd = _incremental_stream_def()
    merge_def = StreamDef(
        name=sd.name,
        table=sd.table,
        primary_key=sd.primary_key,
        write_disposition=WriteDisposition.MERGE,
        incremental=sd.incremental,
        schema=sd.schema,
    )
    events: list[tuple[str, Any]] = []
    _run_one_stream(merge_def, _make_source(gen), _hooks_with_log(events), object(),
                    _run_config(), _pipeline(), None, LOG, min_batch_rows=10)
    assert _writes(events) == [3]


def test_bigquery_hook_default_and_overrides() -> None:
    assert bq.min_batch_rows(Config(params={})) == 10_000
    assert bq.min_batch_rows(Config(params={"min_batch_rows": "0"})) == 0
    assert bq.min_batch_rows(Config(params={"min_batch_rows": 2500})) == 2500
    with pytest.raises(ValueError):
        bq.min_batch_rows(Config(params={"min_batch_rows": "lots"}))
    with pytest.raises(ValueError):
        bq.min_batch_rows(Config(params={"min_batch_rows": -1}))


def test_engine_resolves_the_hook() -> None:
    cfg = Config(params={"min_batch_rows": 750})
    assert _min_batch_rows({}, cfg) == 0
    assert _min_batch_rows({"min_batch_rows": bq.min_batch_rows}, cfg) == 750
