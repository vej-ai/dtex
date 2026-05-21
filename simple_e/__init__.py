"""simpl.E — a simple, open-source Python extract-load (EL) tool.

simpl.E moves data from a **source** into a **destination** and nothing more —
"dlt meets dbt". Connectors are folders of plain Python; projects are folders
of plain files run from the ``simple-e`` CLI or the importable ``simple_e``
library.

This module is the **public API** a connector author imports. After build
stage 3 it exposes the decorator surface a connector body binds to —
``stream``, ``resource``, ``destination``, ``Connector``, ``stream_method`` —
plus the contract types an author references (``Capability``, ``Schema``,
``Field``, ``Config``, ``State``, ``Cursor``, ``Batch``, ``StateRecord`` and
the enums). A connector body should need to import only from ``simple_e``::

    from simple_e import stream, destination, Capability, Schema

As of build stage 5 it also exposes the engine entry point :func:`run` — the
library front door onto the run lifecycle (docs/02 §The triad). The CLI (a
later stage) is a thin shell over this same function::

    import simple_e
    result = simple_e.run(connector="shiphero", target="prod")

``run`` returns a :class:`~simple_e.types.RunResult` and never raises on a
connector/destination failure (docs/07 §4.1).
"""

from __future__ import annotations

# Imported last: simple_e.engine pulls in simple_e.registry / simple_e.types,
# which are already bound above — no import cycle. The engine is the library's
# run entry point (docs/02).
from simple_e.engine import run
from simple_e.registry import (
    Connector,
    destination,
    resource,
    stream,
    stream_method,
)
from simple_e.types import (
    Batch,
    Capability,
    Config,
    ConnectorKind,
    Cursor,
    CursorType,
    Field,
    FieldMode,
    FieldType,
    Record,
    RunConfig,
    RunResult,
    RunStatus,
    Schema,
    SchemaContract,
    State,
    StateBackend,
    StateRecord,
    StreamMeta,
    StreamResult,
    StreamStatus,
    WriteDisposition,
)

__version__ = "0.1.0"

__all__ = [
    # Version
    "__version__",
    # Engine entry point (simple_e.engine) — the library front door
    "run",
    # Decorator API surface (simple_e.registry)
    "stream",
    "resource",
    "destination",
    "Connector",
    "stream_method",
    # Contract types a connector author references (simple_e.types)
    "Batch",
    "Capability",
    "Config",
    "ConnectorKind",
    "Cursor",
    "CursorType",
    "Field",
    "FieldMode",
    "FieldType",
    "Record",
    "RunConfig",
    "RunResult",
    "RunStatus",
    "Schema",
    "SchemaContract",
    "State",
    "StateBackend",
    "StateRecord",
    "StreamMeta",
    "StreamResult",
    "StreamStatus",
    "WriteDisposition",
]
