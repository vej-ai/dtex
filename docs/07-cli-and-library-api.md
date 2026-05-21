# 07 — CLI and Library API

> Part of the simpl.E design handbook. See [README.md](./README.md) for the full table of contents.

simpl.E is invoked the way dbt is: `pip install`, then a CLI. But the CLI is a **thin shell over a real Python library** — every command is one function call away. This is deliberate. Orchestrators (Dagster, Airflow) and notebooks should never shell out; they import simpl.E. The CLI and the library are the *same engine*, exposed twice.

---

## 1. Installation

```bash
pip install simple-e
```

This installs the engine, the CLI entry point `simple-e`, and the pre-baked connectors/destinations (BigQuery, DuckDB; more in v2 — see [10 — Roadmap](./10-roadmap-and-scope.md)). Source connectors with heavy dependencies are installed per-connector via each connector's `requirements.txt`.

---

## 2. CLI surface

All commands share a project root: the directory containing `simple_e_project.yml` (see [06 — Project Anatomy](./06-project-anatomy.md)). simpl.E walks up from the CWD to find it, like dbt and git.

### `simple-e init` — scaffold a project

```bash
simple-e init my_pipelines
```

Creates a minimal project:

```
my_pipelines/
  simple_e_project.yml      # project name, version, connector paths
  profiles.yml              # connection config (gitignored)
  connectors/               # custom source connectors (kind: source)
  destinations/             # custom destination connectors (kind: destination)
  .simple_e/                # run state, logs, cache (gitignored)
  .gitignore                # pre-populated — see section 08
```

`connectors/` and `destinations/` are a readability convention, not a typed boundary — both are scanned via `connector_paths`. See [06 — Project Anatomy](./06-project-anatomy.md).

### `simple-e new connector <name>` — scaffold a connector

```bash
simple-e new connector stripe              # a source (default)
simple-e new connector my_warehouse --kind destination
```

Generates a connector folder with a `register.yaml` and a stub `connector.py` containing a commented `@stream` (or `@destination`) example. The fastest path from zero to a working connector.

### `simple-e list` — discover what exists

```bash
simple-e list                  # all connectors, their kind, streams, tags
simple-e list --connector stripe   # detail one connector
```

```
$ simple-e list
CONNECTOR        KIND         STREAMS                       TAGS
stripe           source       charges, customers, invoices  finance, daily
hubspot          source       contacts, deals               crm, daily
bigquery         destination  —                             (baked)
webhook_sink     destination  —                             custom
```

### `simple-e run` — extract and load

The core command. Runs **synchronously**: it blocks until the run succeeds or fails, streaming logs to stdout. "Wait until it succeeds" is the contract — no background jobs, no polling, no daemon.

```bash
simple-e run -c stripe                     # run one connector, all streams
simple-e run -c stripe --select charges,invoices   # only these streams
simple-e run --tag daily                   # run every connector tagged 'daily'
simple-e run -c stripe --target prod       # use the 'prod' profile target
simple-e run -c stripe --full-refresh      # ignore state, reload from scratch
simple-e run -c stripe --dry-run           # plan only: resolve config, schema,
                                           # dispositions — extract nothing
```

| Flag | Purpose |
|---|---|
| `-c, --connector <name>` | Run a single connector. Mutually exclusive with `--tag`. |
| `--tag <tag>` | Run every connector carrying this tag. Repeatable. |
| `--select <streams>` | Comma-separated stream subset within the connector(s). |
| `--target <name>` | Profile target from `profiles.yml`. Defaults to the profile's `default`. |
| `--full-refresh` | Discard state for the selected streams; reload from the beginning. For `replace`/`merge` streams this recreates the table. |
| `--dry-run` | Resolve and validate everything (config, credentials present, schema, disposition vs. destination capability) but extract/load nothing. Exit 0 if the plan is valid. |
| `--log-level <level>` | `debug`/`info`/`warn`/`error`. Default `info`. See [09 — Logging](./09-logging-and-observability.md). |

Example run output (human-readable on a TTY; JSON-lines when piped — see [09](./09-logging-and-observability.md)):

```
$ simple-e run -c stripe --tag daily
[info] run a1b9f3 started · connector=stripe · target=prod → bigquery
[info] stream charges: resuming from cursor created_at=2026-05-20T00:00:00Z
[info] stream charges: batch 1 written (5000 rows)
[info] stream charges: batch 2 written (2310 rows)
[info] stream charges: done — 7310 extracted, 7310 loaded
[info] stream customers: full refresh (replace)
[info] stream customers: done — 1840 extracted, 1840 loaded
[info] state committed · charges→2026-05-21T00:00:00Z · customers→(replace)
[info] run a1b9f3 succeeded in 41.2s — 9150 rows across 2 streams
```

### `simple-e test` — validate connectors

```bash
simple-e test -c stripe          # connectivity + schema test, no full load
```

`test` calls each source's `@stream` for a tiny sample and validates the destination connection and capabilities. It is what CI runs and what `--dry-run` extends. It never advances state.

### `simple-e state` — inspect and reset state

```bash
simple-e state list                          # all cursors across connectors
simple-e state show -c stripe                # cursors for one connector
simple-e state reset -c stripe --select charges   # clear one stream's cursor
simple-e state set -c stripe --select charges --cursor '2026-01-01T00:00:00Z'
```

```
$ simple-e state show -c stripe
STREAM      CURSOR FIELD   CURSOR VALUE              LAST RUN   ROWS TOTAL
charges     created_at     2026-05-21T00:00:00Z      a1b9f3     2,104,553
customers   —              (replace)                 a1b9f3     1,840
invoices    created_at     2026-05-19T00:00:00Z      9c2d10     88,201
```

`state reset` is the safe, surgical alternative to `--full-refresh`: it clears the cursor without touching loaded data, so the next run re-extracts the window. `state set` lets an operator pin a resume point (e.g. to re-pull a known-bad day). Both read/write the `_simple_e_state` table or sidecar described in [05 — Destinations and State](./05-destinations-and-state.md).

---

## 3. Exit codes and synchronous semantics

`simple-e run` blocks for the entire run. There is no async mode in the CLI — an orchestrator that wants concurrency runs multiple `simple-e` invocations, or uses the library. Exit codes are stable and scriptable:

| Code | Meaning |
|---|---|
| `0` | Run succeeded. All selected streams loaded; state committed. |
| `1` | Run failed — an extract/load error. State **not** advanced (see [05 §5.3](./05-destinations-and-state.md)). |
| `2` | Configuration error — bad `register.yaml`, missing credential, unknown connector/target. Nothing ran. |
| `3` | Planning error — a stream requests a disposition the destination cannot satisfy (caught by `--dry-run` too). |
| `130` | Interrupted (Ctrl-C / SIGTERM). Partial batches may be written; state not advanced. |

The distinction between `1`, `2`, and `3` lets CI and orchestrators react correctly: `2`/`3` are "fix your config", `1` is "retry may help".

A `--tag` run executes connectors sequentially. If one fails, remaining connectors **still run** (so one broken source doesn't block the rest); the overall exit code is the worst code observed. Per-connector results are in the run record ([09](./09-logging-and-observability.md)).

---

## 4. The Python library API

Everything the CLI does, the library does — because the CLI calls the library. The importable surface is small and stable.

```python
import simple_e

# Load the project (walks up for simple_e_project.yml, like the CLI).
project = simple_e.load_project("./my_pipelines")

# Run a connector. Blocks until done — same synchronous contract as the CLI.
result = project.run(
    connector="stripe",
    select=["charges", "invoices"],   # optional stream subset
    target="prod",                    # optional; defaults to profile default
    full_refresh=False,
    dry_run=False,
)

print(result.status)        # "succeeded" | "failed"
print(result.run_id)        # "a1b9f3"
print(result.rows_loaded)   # 9150
for s in result.streams:
    print(s.name, s.rows_extracted, s.rows_loaded, s.cursor_after)

if result.status == "failed":
    raise result.error      # the original exception, re-raisable
```

### 4.1 The `RunResult` object

`project.run(...)` returns a `RunResult` — the same structure persisted as the run record ([09](./09-logging-and-observability.md)):

```python
@dataclass
class StreamResult:
    name: str
    rows_extracted: int
    rows_loaded: int
    cursor_before: Any | None
    cursor_after: Any | None
    status: str                 # "succeeded" | "failed" | "skipped"

@dataclass
class RunResult:
    run_id: str
    connector: str
    target: str
    status: str                 # "succeeded" | "failed"
    started_at: datetime
    ended_at: datetime
    streams: list[StreamResult]
    rows_loaded: int
    error: Exception | None
    log_path: str               # .simple_e/logs/<run_id>/run.jsonl
```

`project.run()` does **not** raise on a failed run — it returns a `RunResult` with `status="failed"` and a populated `error`. This lets callers decide whether to raise, retry, or record. (The CLI translates `status` into the exit code.) A caller wanting exceptions uses `result.error` or a `project.run(...).raise_for_status()` helper.

### 4.2 Tag runs and listing from the library

```python
results = project.run_tag("daily")        # -> list[RunResult]
connectors = project.list_connectors()    # -> list[ConnectorInfo]
state = project.state("stripe")           # -> list[StateRecord], see 05
```

### 4.3 Calling simpl.E from an orchestrator (Dagster)

Because the library is synchronous and returns a plain result object, orchestrator integration is trivial — no SDK, no callbacks:

```python
# dagster_pipeline.py
from dagster import op, job, Failure
import simple_e

@op
def load_stripe():
    project = simple_e.load_project("/opt/pipelines")
    result = project.run(connector="stripe", target="prod")
    if result.status == "failed":
        raise Failure(
            description=f"simpl.E run {result.run_id} failed",
            metadata={"run_id": result.run_id, "log": result.log_path},
        )
    return {s.name: s.rows_loaded for s in result.streams}

@job
def daily_ingest():
    load_stripe()
```

The same pattern works for Airflow (`PythonOperator`), Prefect, or a bare cron + script. simpl.E does not ship orchestrator-specific adapters in v1 — the library *is* the adapter. A thin official `dagster-simple-e` helper package is a v2 nicety, not a requirement. See [10 — Roadmap](./10-roadmap-and-scope.md).

> [Open question: should the library expose a streaming/iterator API (`for batch in project.extract("stripe", "charges"): ...`) for users who want to handle loading themselves? It is a clean extension of the engine but widens the supported surface. Proposal: keep v1 to whole-run `project.run()`; revisit after real demand.]

---

## 5. Configuration precedence

simpl.E resolves every setting through a fixed precedence chain. Higher wins:

```
CLI flag  >  environment variable  >  profiles.yml  >  register.yaml default
```

1. **`register.yaml` default** — the connector author's baseline (e.g. `page_size: 100`). Lowest precedence; never secret.
2. **`profiles.yml`** — the operator's per-environment config and connection details, including `${ENV_VAR}` interpolation. Gitignored. Canonical format and security rules in [08 — Security](./08-security.md).
3. **Environment variable** — `SIMPLE_E_*` variables override matching keys. Useful in CI/containers where mounting a `profiles.yml` is awkward.
4. **CLI flag** — `--target`, `--full-refresh`, `--select`, `--log-level`, etc. Always wins; it is the explicit operator intent for *this invocation*.

Example: `page_size` defaults to `100` in `register.yaml`, is set to `500` in `profiles.yml` for the `prod` target, and `SIMPLE_E_STRIPE__PAGE_SIZE=1000` is exported in CI → the run uses `1000`. A `--dry-run` prints the fully resolved config (with secrets redacted — see [08](./08-security.md)) so the operator can confirm what *would* run.

The library honors the identical chain: `load_project()` reads `profiles.yml` and env vars; arguments to `project.run()` are the library's equivalent of CLI flags and take top precedence. CLI and library are genuinely equal.

### Reference

- Project layout, `simple_e_project.yml` → [06 — Project Anatomy](./06-project-anatomy.md)
- `profiles.yml` format, secrets, `${ENV_VAR}` → [08 — Security](./08-security.md)
- Run logs and the run record → [09 — Logging and Observability](./09-logging-and-observability.md)
- Destinations, targets, state → [05 — Destinations and State](./05-destinations-and-state.md)
