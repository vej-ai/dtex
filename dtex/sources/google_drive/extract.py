# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""The google_drive extraction logic — no decorators, safe to import anywhere.

The ``files`` stream in ``source.py`` is a one-line wrapper over
:func:`extract_files`; a project-local copy of the connector imports it from
here (importing ``source.py`` instead would re-run its ``@stream`` decorator
inside the project connector's registration scope).

Modelled on the ``filesystem`` source: one ``files`` stream = the files under
``folder`` whose name matches ``glob``, unioned into one table, loaded
incrementally by file. Want several tables from one folder (``orders_*.csv``
here, ``refunds.xlsx`` there)? Declare one stream per pattern in a
project-local copy of this connector and call :func:`extract_files` from each
— see the README.

Incremental design
------------------
The cursor is the synthetic ``_dtex_file_cursor`` field, ``<modifiedTime>|<fileId>``
(``2026-09-29T10:00:00.123Z|1AbC…``) — a string whose order is the order the
files were last changed, tie-broken by id. Files are read in that order; a
file whose key is at or below the committed cursor is skipped, so a run loads
only files added or modified since the last one. A modified file is loaded
again in full (``append``), so its old rows stay: every record carries
``_dtex_file_id`` and ``_dtex_file_cursor`` to keep only each file's latest
load downstream. The cursor advances by whole files (observed after a file's
last record), which is why the stream can declare ``ordered: true``.
"""

from __future__ import annotations

import tempfile
from collections.abc import Iterator
from fnmatch import fnmatchcase
from typing import Any

from dtex import Batch, Config, Cursor
from dtex.sources.google_drive.client import DriveClient, DriveFile
from dtex.sources.google_drive.readers import (
    FORMATS,
    detect_format,
    read_csv,
    read_google_sheet,
    read_xlsx,
)
from dtex.sources.google_sheets import auth
from dtex.sources.google_sheets.a1 import parse_a1, parse_folder_id, parse_list
from dtex.sources.google_sheets.client import SheetsClient
from dtex.sources.google_sheets.grid import GridOptions

FILE_CURSOR_FIELD = "_dtex_file_cursor"
FILE_ID_FIELD = "_dtex_file_id"
FILE_PATH_FIELD = "_dtex_file_path"

# Files larger than this spill from memory to a temporary file while read.
_SPOOL_BYTES = 64 * 1024 * 1024


def extract_files(
    config: Config,
    cursor: Cursor | None,
    log: Any,
    **overrides: Any,
) -> Iterator[Batch]:
    """The ``files`` stream body, reusable from a project-local multi-stream connector.

    Reads ``folder`` / ``glob`` / ``recursive`` / ``format`` / ``sheet`` /
    ``range`` / ``header_row`` / ``columns`` / ``csv_delimiter`` /
    ``parse_dates`` / ``batch_size`` and the auth params from ``config``;
    keyword ``overrides`` win over ``config`` — that is how each stream of a
    project-local copy pins its own pattern::

        from dtex.sources.google_drive.extract import extract_files

        @stream(name="orders")
        def orders(config, cursor, log):
            yield from extract_files(config, cursor, log, glob="orders_*.csv")

    ``cursor`` may be ``None`` (a non-incremental stream reads every file).
    """
    if overrides:
        config = Config(params={**config.params, **overrides}, secrets=config.secrets)
    folder_id = parse_folder_id(str(config.get("folder") or ""))
    glob = str(config.get("glob") or "*")
    recursive = bool(config.get("recursive", False))
    fmt_param = str(config.get("format") or "auto").strip().lower()
    if fmt_param != "auto" and fmt_param not in FORMATS:
        raise ValueError(
            f"google_drive: format must be 'auto' or one of {', '.join(FORMATS)}, "
            f"got {fmt_param!r}"
        )
    sheet = str(config.get("sheet") or "")
    range_text = str(config.get("range") or "").strip() or None
    bounds = parse_a1(range_text) if range_text else None
    header_row = _header_row(config)
    columns = parse_list(str(config.get("columns") or ""))
    delimiter = str(config.get("csv_delimiter") or ",")
    parse_dates = bool(config.get("parse_dates", True))
    batch_size = max(1, int(config.get("batch_size", 1000)))
    options = GridOptions(bounds=bounds, header_row=header_row, columns=tuple(columns))

    session = auth.authorized_session(
        config, [auth.DRIVE_READONLY_SCOPE, auth.SHEETS_READONLY_SCOPE]
    )
    drive = DriveClient(session)

    candidates: list[tuple[DriveFile, str]] = []
    for file in drive.walk(folder_id, recursive=recursive, log=log):
        if not fnmatchcase(file.name, glob):
            continue
        fmt = detect_format(file, fmt_param)
        if fmt is None:
            log.info(
                "google_drive: skipping %r — not CSV, XLSX or a Google Sheet "
                "(mimeType %s); set `glob` or `format` to change what is read",
                file.path,
                file.mime_type,
            )
            continue
        candidates.append((file, fmt))
    candidates.sort(key=lambda item: item[0].cursor_key)

    start = cursor.start_value() if cursor is not None else None
    if isinstance(start, str) and start:
        # Re-observe the resume point so a run with nothing new still commits
        # the same cursor (never an empty one).
        cursor.observe(start)  # type: ignore[union-attr]
        candidates = [(f, fmt) for f, fmt in candidates if f.cursor_key > start]
    log.info(
        "google_drive: %d file(s) to read under folder %s (glob %r)",
        len(candidates),
        folder_id,
        glob,
    )

    sheets_client: SheetsClient | None = None
    batch: list[dict[str, Any]] = []
    for file, fmt in candidates:
        if fmt == "google_sheets":
            sheets_client = sheets_client or SheetsClient(session)
            records: Iterator[dict[str, Any]] = read_google_sheet(
                sheets_client,
                file,
                sheet=sheet,
                range_text=range_text,
                header_row=header_row,
                columns=columns,
                parse_dates=parse_dates,
            )
            spool = None
        else:
            spool = tempfile.SpooledTemporaryFile(max_size=_SPOOL_BYTES)
            drive.download(file, spool)
            if fmt == "csv":
                records = read_csv(spool, file, delimiter=delimiter, options=options)
            else:
                records = read_xlsx(spool, file, sheet=sheet, options=options)
        try:
            for record in records:
                record[FILE_CURSOR_FIELD] = file.cursor_key
                record[FILE_ID_FIELD] = file.id
                record[FILE_PATH_FIELD] = file.path
                batch.append(record)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
        finally:
            if spool is not None:
                spool.close()
        if cursor is not None:
            cursor.observe(file.cursor_key)
    if batch:
        yield batch


def _header_row(config: Config) -> int | None:
    value = config.get("header_row")
    if value is None or value == "":
        return None
    row = int(value)
    if row < 0:
        raise ValueError(f"header_row must be 0 (no header) or a row number, got {row}")
    return row
