# 00 — Vision & Naming

> Part of the **simpl.E** design handbook. This file sets the north star. Files
> 01 (landscape) and 02 (architecture) build on the vocabulary and the design
> test defined here.

## What simpl.E is

**simpl.E** ("simple extract") is an open-source Python extract-load (EL) tool. It
moves data from a **source** (an API, a database, a file drop) into a
**destination** (a warehouse, a database, an object store) and nothing more.
Transformation is somebody else's job — dbt's, specifically.

The mental model is **"dlt meets dbt"**:

- From **dlt**: connectors are ordinary Python — generator functions decorated
  with `@stream` / `@resource` — and pipeline **state lives in the destination**,
  not in a control plane.
- From **dbt**: you work in a **project folder** of plain files, run it with a
  CLI (`simple-e run`), select work with **tags**, and configure environments
  with **profiles**. The engine is a pip-installable library; the project is
  yours and lives in your repo, under version control, reviewable in a PR.

simpl.E has three faces, and they are equal first-class citizens:

| Face | Analogy | What it is |
|------|---------|-----------|
| **Engine / library** | `dbt-core` | The `simple_e` Python package: the run loop, the connector contract, baked connectors, the importable API. |
| **CLI** | the `dbt` binary | `simple-e` — installs via pip, runs a project folder. |
| **Project** | a dbt project | A user-owned folder of connector definitions, profiles, and config. |

A connector — source **or** destination — is a **folder**: a mandatory
`register.yaml` manifest plus Python files using decorators on plain functions.
Sources and destinations share that *folder + manifest + decorator* contract; the
decorators differ by direction — sources expose `@stream` / `@resource`
generators that **yield** records, destinations expose `@destination`-hooked
functions that **accept** them. A class form is a documented escape hatch but is
never mandatory.

simpl.E **ships pre-baked** connectors and destinations inside the `simple_e`
package (`connector="meta_ads"`). Users also write **custom** ones in their own
project folder (`connector="custom"`). Same contract for both.

Runs are **synchronous** — "run, wait until it succeeds, exit." That makes
simpl.E trivially easy to wrap in an orchestrator (Dagster, Airflow, cron) later,
because a synchronous process with an exit code is the universal contract.

## What simpl.E is deliberately NOT — the anti-Airbyte stance

simpl.E is **explicitly not Airbyte**. The rejection is concrete, not a vibe:

- **No UI as the authoring surface.** Connectors are code in a repo, not rows in
  a database edited through a web form. The source of truth is the filesystem.
- **No JSON-over-stdio process protocol.** Airbyte (and Singer) shuttle `RECORD`
  / `SCHEMA` / `STATE` JSON messages between separate OS processes — often
  separate Docker images. simpl.E runs the connector **in-process** as Python.
  No serialization tax, no container-per-connector, stack traces you can read.
- **No declarative YAML for stream *logic*.** Airbyte's low-code CDK expresses
  pagination, auth, and record selection *as YAML*. simpl.E uses YAML only for a
  **manifest** — metadata, declared streams, config schema. The *logic* of a
  stream is a Python generator. YAML that needs an `if` is a programming
  language with the safety removed; we will not build one.
- **No blackbox.** Every connector is readable, debuggable, `pdb`-able Python.

What simpl.E *keeps* from that world: a **YAML manifest** (`register.yaml`) for
declarative *metadata and config* — analogous in spirit to Airbyte's
`metadata.yaml`, not its `manifest.yaml`. Declaring *what a connector is* in YAML
is good. Encoding *what a connector does* in YAML is the mistake.

### Non-goals (full list in "Non-goals for v1" below)

simpl.E is not an orchestrator, not a transformation tool, not a reverse-ETL
platform, not a catalog, not a managed SaaS. It does one job.

## The "simplest possible thing" north star

The owner's #1 principle: **keep it as simple as possible.** This is not a slogan
— it is a **test** applied to every design decision:

> **The simpl.E test.** For any proposed feature or abstraction, ask:
> 1. Can a competent data engineer who has never seen simpl.E read a connector
>    folder and understand it in under five minutes?
> 2. Does this add a *concept the user must learn*? If yes, does it remove at
>    least one other concept, or unlock something genuinely impossible without it?
> 3. Could the user do this with plain Python instead? If yes, the burden of
>    proof is on the abstraction, not on plain Python.
> 4. Does it require a new config file, a new CLI flag, or a new lifecycle stage?
>    Each of those is a cost paid by every user forever.

A feature that fails the test is cut or demoted to an escape hatch. When two
designs are both correct, **the one with fewer concepts wins** — even if it is
less powerful. Power that costs comprehension is, for simpl.E, a net loss.

This is why connectors are folders of plain Python: a data engineer already
knows folders, YAML, and generators. It is why state lives in the destination:
no extra state service to learn, deploy, or back up. It is why runs are
synchronous: no scheduler, no queue, no daemon — just a process.

## Naming & branding

| Thing | Form | Notes |
|-------|------|-------|
| Product name | **simpl.E** | Stylized lowercase with a dot before a capital E. "simple extract." |
| Plain-text name | simpl.E | Use the stylization in prose; never "Simple" or "SimplE". |
| CLI binary | `simple-e` | Hyphenated, lowercase. `simple-e run -c meta_ads`. |
| Python package / import | `simple_e` | `import simple_e` / `from simple_e import run`. PEP 8 underscore. |
| State table | `_simple_e_state` | Underscore-prefixed, lives in the destination. |

Rationale: the three identifiers (`simpl.E`, `simple-e`, `simple_e`) are each the
idiomatic form for their context (brand / shell / Python) and are unambiguously
the same product. The dot in "simpl.E" is brand only and never appears in code.

## Target user

The target user is a **data engineer who currently hand-writes one-off connector
scripts** — the person with a `scripts/` folder full of `pull_stripe.py`,
`sync_hubspot.py`, each re-implementing pagination, retries, incremental
cursors, and "where did I leave off" bookkeeping slightly differently.

simpl.E offers that person a deal: **keep writing Python generators — that part
was never the problem — and we will take the boilerplate.** State, schema
inference, batching, retries, the destination write, the run record: handled. In
exchange they adopt one contract (folder + `register.yaml` + decorators).

simpl.E is **not** aimed at non-technical users (that is Airbyte's UI market) or
at teams who want a managed SaaS with an SLA (that is Fivetran's market). It is
aimed at engineers who want their EL layer to look like their dbt layer:
plain files, in a repo, in a PR, run from a CLI.

## Non-goals for v1

v1 scope is **CLI + library only**. Explicitly out of scope for v1:

- **No UI.** No web app, no connector builder. (Possible far-future; not v1.)
- **No built-in scheduler / orchestrator.** simpl.E runs once and exits.
  Scheduling is cron's or Dagster's job. simpl.E's contribution is being a
  clean synchronous process that an orchestrator can wrap.
- **No transformation / T layer.** EL only. Hand off to dbt.
- **No reverse-ETL framing.** A warehouse→SaaS sync is just a connector pair, but
  v1 does not build first-class reverse-ETL ergonomics.
- **No CDC / log-based replication** as a built-in. Cursor-based incremental
  only in v1. [Open question: whether a connector author can implement CDC
  themselves within the `@stream` contract — likely yes, but not a v1 promise.]
- **No distributed / multi-node execution.** Single process, single host.
- **No data catalog, lineage graph, or column-level metadata store.** simpl.E
  emits a **run record**; it is not a metadata platform.
- **No connector marketplace.** Baked connectors ship in the package; everything
  else is custom and lives in the user's project. A registry may come later.

Each non-goal is a direct application of the simpl.E test: every one of these
would add concepts and surface area without doing the one job better.
