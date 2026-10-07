# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Google credentials for the Sheets / Drive sources — ADC or a service account.

Three params, shared by ``google_sheets`` and ``google_drive``, mirror the
BigQuery destination's ``auth_type`` / ``credentials_path`` pair:

* ``auth_type: auto`` (default) — a service account when ``credentials_path``
  or ``credentials_json`` is set, otherwise Application Default Credentials.
* ``auth_type: oauth`` — Application Default Credentials only (the
  ``GOOGLE_APPLICATION_CREDENTIALS`` env var, ``gcloud auth
  application-default login``, or the metadata server on GCP).
* ``auth_type: service_account`` — a service-account JSON key, read from
  ``credentials_path`` (a file) or ``credentials_json``.

``impersonate_service_account`` (optional, any ``auth_type``) mints a
short-lived token for that service account with exactly the Sheets / Drive
scopes through the IAM Credentials API. It is the way to get a scoped token
where the ambient one cannot carry these scopes — Cloud Build and GCE hand
the attached identity a ``cloud-platform``-only token, which the Sheets API
refuses — and it works for the identity itself (the account needs
``roles/iam.serviceAccountTokenCreator`` on the target, its own account
included). No key is involved.

``credentials_json`` is a **reference**, never the key itself: ``${env.VAR}``
(an environment variable holding the key JSON) or a ``secret://`` URL resolved
through dtex's secret-manager resolvers (docs/08 §3). A literal JSON value is
rejected so a private key never sits in a config file. The resolved key is
handed straight to google-auth and never logged.

Everything Google is imported lazily; ``google-auth`` ships with the base
install (the BigQuery destination depends on it).
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Sequence
from typing import Any

from dtex.types import Config

SHEETS_READONLY_SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"
DRIVE_READONLY_SCOPE = "https://www.googleapis.com/auth/drive.readonly"

_AUTH_TYPES = ("auto", "oauth", "service_account")
_SERVICE_ACCOUNT_EMAIL = re.compile(
    r"[a-z][a-z0-9-]{3,}@[a-z][a-z0-9-]*\.iam\.gserviceaccount\.com"
)


def authorized_session(config: Config, scopes: Sequence[str]) -> Any:
    """Build a ``google.auth.transport.requests.AuthorizedSession`` for ``scopes``.

    Tests monkeypatch this function to hand the connector a fake session; the
    connector only ever calls ``session.request(method, url, params=...,
    timeout=..., stream=...)`` on it.
    """
    credentials = load_credentials(config, scopes)
    from google.auth.transport.requests import AuthorizedSession

    return AuthorizedSession(credentials)


def load_credentials(config: Config, scopes: Sequence[str]) -> Any:
    """Resolve the credentials the ``auth_type`` / ``credentials_*`` params ask for,
    impersonating ``impersonate_service_account`` with ``scopes`` when set."""
    credentials = _source_credentials(config, scopes)
    target = str(config.get("impersonate_service_account") or "").strip()
    if not target:
        return credentials
    if not _SERVICE_ACCOUNT_EMAIL.fullmatch(target):
        raise ValueError(
            "impersonate_service_account must be a service-account email "
            "(name@project.iam.gserviceaccount.com)"
        )
    from google.auth import impersonated_credentials

    return impersonated_credentials.Credentials(
        source_credentials=credentials,
        target_principal=target,
        target_scopes=list(scopes),
        lifetime=3600,
    )


def _source_credentials(config: Config, scopes: Sequence[str]) -> Any:
    """ADC or a service-account key, per ``auth_type`` / ``credentials_*``."""
    auth_type = str(config.get("auth_type") or "auto").strip().lower()
    credentials_path = str(config.get("credentials_path") or "").strip()
    credentials_ref = str(config.get("credentials_json") or "").strip()
    if auth_type not in _AUTH_TYPES:
        raise ValueError(
            f"auth_type must be one of {', '.join(_AUTH_TYPES)}, got {auth_type!r}"
        )
    if credentials_path and credentials_ref:
        raise ValueError("set only one of credentials_path and credentials_json")
    has_key = bool(credentials_path or credentials_ref)
    if auth_type == "oauth" and has_key:
        raise ValueError(
            "auth_type is 'oauth' (Application Default Credentials) but a "
            "service-account key is configured; set auth_type: service_account "
            "(or auto) to use the key, or remove credentials_path/credentials_json"
        )
    if auth_type == "service_account" and not has_key:
        raise ValueError(
            "auth_type 'service_account' needs credentials_path (a key file) or "
            "credentials_json (a ${env.VAR} or secret:// reference to the key JSON)"
        )

    if not has_key:
        import google.auth

        credentials, _project = google.auth.default(scopes=list(scopes))
        return credentials

    from google.oauth2 import service_account

    if credentials_path:
        return service_account.Credentials.from_service_account_file(
            credentials_path, scopes=list(scopes)
        )
    info = _parse_key_json(resolve_credentials_reference(credentials_ref))
    return service_account.Credentials.from_service_account_info(info, scopes=list(scopes))


def resolve_credentials_reference(ref: str) -> str:
    """Resolve a ``${env.VAR}`` or ``secret://`` reference to the key JSON text."""
    text = ref.strip()
    if text.startswith("${env.") and text.endswith("}"):
        var = text[len("${env.") : -1].strip()
        if var not in os.environ:
            raise ValueError(
                f"credentials_json: environment variable {var!r} (referenced by "
                f"{text}) is not set"
            )
        return os.environ[var]
    if text.startswith("secret://"):
        from dtex.secrets import resolve_secret_url

        return resolve_secret_url(text)
    raise ValueError(
        "credentials_json must be a reference — ${env.VAR} or secret://<scheme>/<path> — "
        "not the key itself; keep service-account keys out of config files"
    )


def _parse_key_json(text: str) -> dict[str, Any]:
    try:
        info = json.loads(text)
    except json.JSONDecodeError as exc:
        # Never echo the text: it is (a malformed copy of) a private key.
        raise ValueError(
            f"credentials_json did not resolve to valid JSON (parse error at "
            f"line {exc.lineno}, column {exc.colno})"
        ) from None
    if not isinstance(info, dict) or info.get("type") != "service_account":
        raise ValueError(
            "credentials_json must resolve to a service-account key "
            "(a JSON object with \"type\": \"service_account\")"
        )
    return info
