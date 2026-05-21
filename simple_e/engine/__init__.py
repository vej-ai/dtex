"""The simpl.E engine — discovery, config resolution, the run lifecycle.

This package is stage 5 of the simpl.E build: the engine that turns a connector
NAME into a completed run. docs/02 §The triad places the engine alongside the
library and the CLI as one of three front doors onto the same machinery; the
engine is the machinery.

Module layout — one module per lifecycle concern, so each is small and testable
in isolation:

* :mod:`simple_e.engine.discovery` — stage 1 (DISCOVER): find the project root,
  resolve a connector NAME to a loaded ``register.yaml`` + populated
  :class:`~simple_e.registry.ConnectorRegistry` (project-local beats baked),
  resolve a ``--tag`` to a connector set, run discovery-time validation
  (docs/03 section 7).
* :mod:`simple_e.engine.config` — stage 2 (RESOLVE): parse
  ``simple_e_project.yml`` / ``profiles.yml``, merge the layered param
  precedence (docs/03 section 6), resolve ``${env.X}`` / ``${profile.X.Y}``
  secrets, produce the frozen :class:`~simple_e.types.RunConfig` and immutable
  per-connector :class:`~simple_e.types.Config` objects.
* :mod:`simple_e.engine.runner` — stages 3-6: INIT DEST, LOAD STATE, RUN STREAMS
  (per-stream commit, sequential), RUN RECORD. Exposes :func:`run`.
* :mod:`simple_e.engine.logger` — the redacting structured logger injected as
  the ``log`` parameter of a ``@stream`` function.

The single public entry point is :func:`run`, re-exported from the top-level
``simple_e`` package as :func:`simple_e.run` -- the CLI (a later stage) and the
library both call it.
"""

from __future__ import annotations

from simple_e.engine.config import ConfigError
from simple_e.engine.discovery import DiscoveryError
from simple_e.engine.runner import EngineError, run

__all__ = [
    "run",
    "ConfigError",
    "DiscoveryError",
    "EngineError",
]
