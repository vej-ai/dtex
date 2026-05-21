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

The engine-facing entry points (``run``, ``load_project``, …) are added in a
later build stage.
"""

from __future__ import annotations

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
    Schema,
    SchemaContract,
    State,
    StateBackend,
    StateRecord,
    StreamMeta,
    WriteDisposition,
)

__version__ = "0.1.0"

__all__ = [
    # Version
    "__version__",
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
    "Schema",
    "SchemaContract",
    "State",
    "StateBackend",
    "StateRecord",
    "StreamMeta",
    "WriteDisposition",
]
