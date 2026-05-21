"""simpl.E — a simple, open-source Python extract-load (EL) tool.

simpl.E moves data from a **source** into a **destination** and nothing more —
"dlt meets dbt". Connectors are folders of plain Python; projects are folders
of plain files run from the ``simple-e`` CLI or the importable ``simple_e``
library.

This module currently exposes only the package version. The public API
(``run``, ``load_project``, ...) is added in a later build stage.
"""

__version__ = "0.1.0"
