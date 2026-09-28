# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Google Sheets baked source connector — every tab of a spreadsheet as a stream.

See ``register.yaml`` for the manifest and ``source.py`` for the entry points.
The helper modules (``a1``, ``auth``, ``grid``, ``http``, ``client``,
``reader``) carry no decorators, so the ``google_drive`` connector imports
them to read native Google Sheets files and to share the grid → record rules.
"""
