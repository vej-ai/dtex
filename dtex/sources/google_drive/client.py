# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""A thin Google Drive API v3 client — folder listing (incl. shared drives) + download.

* :meth:`DriveClient.folder` — ``files.get`` on the folder: confirms it is a
  folder the credentials can see and returns its ``driveId`` (set only when
  the folder lives in a shared drive).
* :meth:`DriveClient.walk` — ``files.list`` with
  ``q="'<folder>' in parents and trashed=false"``, following
  ``nextPageToken``; ``supportsAllDrives`` / ``includeItemsFromAllDrives`` are
  always on, and a shared-drive folder is listed with ``corpora=drive`` +
  ``driveId`` (the ``user`` corpus does not reliably include shared-drive
  items). With ``recursive`` it descends into sub-folders.
* :meth:`DriveClient.download` — ``files.get?alt=media`` streamed to a file.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import IO, Any
from urllib.parse import quote

from dtex.sources.google_sheets import http

DRIVE_FILES_API = "https://www.googleapis.com/drive/v3/files"
FOLDER_MIME = "application/vnd.google-apps.folder"
SHORTCUT_MIME = "application/vnd.google-apps.shortcut"
GOOGLE_SHEET_MIME = "application/vnd.google-apps.spreadsheet"

_LIST_FIELDS = "nextPageToken,files(id,name,mimeType,modifiedTime,size)"
_PAGE_SIZE = 1000


@dataclass(frozen=True)
class DriveFile:
    """One file found under the folder.

    ``path`` is the file's path relative to the configured folder
    (``2026/q3/orders.csv``; just the name for a direct child).
    ``modified_time`` is normalized to fixed-width UTC ISO 8601 with
    milliseconds (``2026-09-29T10:00:00.123Z``) so that plain string
    comparison is chronological — the cursor relies on it.
    """

    id: str
    name: str
    path: str
    mime_type: str
    modified_time: str
    size: int | None = None

    @property
    def cursor_key(self) -> str:
        """``<modifiedTime>|<fileId>`` — the stream's cursor value for this file."""
        return f"{self.modified_time}|{self.id}"


def normalize_modified_time(value: str) -> str:
    """Drive RFC 3339 time → ``YYYY-MM-DDTHH:MM:SS.mmmZ`` (UTC, fixed width)."""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    parsed = parsed.astimezone(UTC)
    return parsed.strftime("%Y-%m-%dT%H:%M:%S.") + f"{parsed.microsecond // 1000:03d}Z"


class DriveClient:
    """Read-only access to one Google identity's view of Drive."""

    def __init__(self, session: Any) -> None:
        self._session = session

    def folder(self, folder_id: str) -> dict[str, Any]:
        """The folder's ``id`` / ``name`` / ``mimeType`` / ``driveId``; raises if not a folder."""
        meta = http.request_json(
            self._session,
            f"{DRIVE_FILES_API}/{quote(folder_id)}",
            {"fields": "id,name,mimeType,driveId", "supportsAllDrives": "true"},
            what=f"reading Drive folder {folder_id}",
        )
        if meta.get("mimeType") != FOLDER_MIME:
            raise ValueError(
                f"google_drive: {folder_id} ({meta.get('name')!r}) is not a folder "
                f"(mimeType {meta.get('mimeType')!r}); `folder` must point at a folder"
            )
        return meta

    def list_children(self, folder_id: str, drive_id: str | None) -> Iterator[dict[str, Any]]:
        """Every non-trashed direct child of ``folder_id``, across all result pages."""
        params: dict[str, Any] = {
            "q": f"'{folder_id}' in parents and trashed=false",
            "fields": _LIST_FIELDS,
            "pageSize": _PAGE_SIZE,
            "supportsAllDrives": "true",
            "includeItemsFromAllDrives": "true",
        }
        if drive_id:
            params["corpora"] = "drive"
            params["driveId"] = drive_id
        page_token: str | None = None
        while True:
            page = dict(params)
            if page_token:
                page["pageToken"] = page_token
            body = http.request_json(
                self._session, DRIVE_FILES_API, page, what=f"listing Drive folder {folder_id}"
            )
            yield from body.get("files") or []
            page_token = body.get("nextPageToken")
            if not page_token:
                return

    def walk(self, folder_id: str, *, recursive: bool, log: Any = None) -> Iterator[DriveFile]:
        """Files under ``folder_id`` (sub-folders too with ``recursive``), paths relative."""
        root = self.folder(folder_id)
        drive_id = root.get("driveId") or None
        pending: list[tuple[str, str]] = [(folder_id, "")]
        visited: set[str] = set()
        while pending:
            current, prefix = pending.pop(0)
            if current in visited:
                continue  # a folder reachable twice (Drive allows several parents)
            visited.add(current)
            for item in self.list_children(current, drive_id):
                mime = str(item.get("mimeType", ""))
                name = str(item.get("name", ""))
                path = f"{prefix}{name}"
                if mime == FOLDER_MIME:
                    if recursive:
                        pending.append((str(item["id"]), f"{path}/"))
                    continue
                if mime == SHORTCUT_MIME:
                    if log is not None:
                        log.info(
                            "google_drive: skipping shortcut %r (shortcuts are not followed)",
                            path,
                        )
                    continue
                size = item.get("size")
                yield DriveFile(
                    id=str(item["id"]),
                    name=name,
                    path=path,
                    mime_type=mime,
                    modified_time=normalize_modified_time(str(item["modifiedTime"])),
                    size=int(size) if size is not None else None,
                )

    def download(self, file: DriveFile, sink: IO[bytes]) -> None:
        """Stream a (non-native) file's bytes into ``sink`` and rewind it."""
        http.download(
            self._session,
            f"{DRIVE_FILES_API}/{quote(file.id)}",
            {"alt": "media", "supportsAllDrives": "true"},
            sink,
            what=f"downloading Drive file {file.path!r}",
        )
