# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""The google_drive source — the ``@stream`` entry point.

One stream, ``files``: every file under ``folder`` whose name matches ``glob``,
unioned into one table and loaded incrementally by file. The logic lives in
:mod:`.extract` (decorator-free, so a project-local multi-stream copy can
import :func:`~dtex.sources.google_drive.extract.extract_files`).
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from dtex import Batch, Config, Cursor, stream
from dtex.sources.google_drive.extract import extract_files


@stream(name="files")
def files(config: Config, cursor: Cursor, log: Any) -> Iterator[Batch]:
    """Every matching file under ``folder``, as one table — see the module docstring."""
    yield from extract_files(config, cursor, log)
