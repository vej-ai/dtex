"""Engine tests for discovered streams — ``discover: true`` + ``@discover`` (docs/03 §2.2.3).

A throwaway project-local source (``shelves``) declares one template stream
and a discovery hook that returns whatever the ``shelves`` param lists, so
each test controls the discovered set through config alone. Covered:

* ``streams: all`` expands the template into one stream / table / state row
  per discovered stream, with the template's declaration inherited;
* explicit ``streams:`` naming discovered streams, per-stream ``params``,
  ``--select``; an unknown name fails listing the discovered names;
* incremental discovered streams keep independent cursors;
* hard errors: name collisions, a non-DiscoveredStream return, nothing
  discovered at all;
* discovery-time validation: a template without a hook, a hook without a
  template, bad injectables.
"""

from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Any

import duckdb
import pytest

import dtex
from dtex.engine import discovery as disc
from dtex.engine.discovery import DiscoveryError

_REGISTER = """\
name: shelves
kind: source
version: "1.0.0"
params:
  shelves: {type: string, default: ""}
  scale: {type: int, default: 1}
  mode: {type: string, default: "ok"}
streams:
  - name: fixed
    table: fixed
    write_disposition: replace
  - name: shelf
    table: shelf
    discover: true
    write_disposition: append
    incremental:
      cursor_field: n
      cursor_type: int
      initial_value: "0"
"""

_SOURCE = """\
from dtex import DiscoveredStream, discover, stream


@discover(stream="shelf")
def find_shelves(config, log):
    mode = config.get("mode")
    if mode == "bad_type":
        return ["not-a-DiscoveredStream"]
    names = [s.strip() for s in str(config.get("shelves") or "").split(",") if s.strip()]
    if mode == "collide":
        names.append("fixed")
    return [
        DiscoveredStream(name=n, table=f"shelf_{n}", context={"label": n.upper()})
        for n in names
    ]


@stream(name="shelf")
def shelf(config, cursor, stream_def):
    start = int(cursor.start_value() or 0)
    scale = int(config.get("scale"))
    rows = [
        {"n": i, "label": stream_def.context["label"], "scaled": i * scale}
        for i in range(start + 1, start + 3)
    ]
    for r in rows:
        cursor.observe(r["n"])
    yield rows


@stream(name="fixed")
def fixed():
    yield [{"k": 1}]
"""


def _project(tmp_path: Path, config: str, register: str = _REGISTER, source: str = _SOURCE) -> str:
    (tmp_path / "dtex_project.yml").write_text(
        "name: t\nversion: '0.1'\nsource_paths: [sources]\n"
        "destination_paths: []\nconfig_paths: [configs]\n"
    )
    (tmp_path / "profiles.yml").write_text(
        "duckdb:\n  default_target: dev\n  targets:\n    dev:\n      path: 'w.duckdb'\n"
    )
    src = tmp_path / "sources" / "shelves"
    src.mkdir(parents=True, exist_ok=True)
    (src / "register.yaml").write_text(register)
    (src / "source.py").write_text(source)
    (tmp_path / "configs").mkdir(exist_ok=True)
    (tmp_path / "configs" / "c.yml").write_text(
        "name: c\nsource: shelves\ndestination: duckdb\n" + textwrap.dedent(config)
    )
    return str(tmp_path / "w.duckdb")


def _run(tmp_path: Path, db: str, **kwargs: Any) -> Any:
    return dtex.run(
        config="c", project_dir=str(tmp_path), destination_params_override={"path": db}, **kwargs
    )


def test_streams_all_expands_the_template(tmp_path: Path) -> None:
    db = _project(tmp_path, "params: {shelves: 'a, b'}\nstreams: all\n")
    result = _run(tmp_path, db)
    assert result.status.value == "succeeded", result.error
    assert [s.name for s in result.streams] == ["fixed", "a", "b"]
    conn = duckdb.connect(db)
    a = conn.execute("SELECT n, label FROM shelf_a ORDER BY n").fetchall()
    states = conn.execute(
        "SELECT stream, cursor_value FROM _dtex_state WHERE connector = 'shelves' ORDER BY stream"
    ).fetchall()
    conn.close()
    assert a == [(1, "A"), (2, "A")]
    assert [(s, str(v)) for s, v in states] == [("a", "2"), ("b", "2"), ("fixed", "None")]

    # Each discovered stream resumes from its own cursor.
    (tmp_path / "configs" / "c.yml").write_text(
        "name: c\nsource: shelves\ndestination: duckdb\n"
        "params: {shelves: 'a, b, c'}\nstreams: all\n"
    )
    result = _run(tmp_path, db)
    assert result.status.value == "succeeded", result.error
    conn = duckdb.connect(db)
    assert conn.execute("SELECT max(n) FROM shelf_a").fetchone() == (4,)
    assert conn.execute("SELECT max(n) FROM shelf_c").fetchone() == (2,)
    conn.close()


def test_explicit_streams_with_per_stream_params_and_select(tmp_path: Path) -> None:
    db = _project(
        tmp_path,
        "params: {shelves: 'a, b'}\nstreams:\n  a:\n    params: {scale: 10}\n  b:\n",
    )
    result = _run(tmp_path, db, select=("a",))
    assert result.status.value == "succeeded", result.error
    assert {s.name: s.status.value for s in result.streams} == {
        "fixed": "skipped",
        "a": "succeeded",
        "b": "skipped",
    }
    conn = duckdb.connect(db)
    assert conn.execute("SELECT scaled FROM shelf_a ORDER BY n").fetchall() == [(10,), (20,)]
    conn.close()


def test_unknown_stream_name_lists_discovered_streams(tmp_path: Path) -> None:
    db = _project(tmp_path, "params: {shelves: 'a, b'}\nstreams:\n  z:\n")
    result = _run(tmp_path, db)
    assert result.status.value == "failed"
    message = str(result.error)
    assert "'z'" in message and "'a'" in message and "'fixed'" in message


@pytest.mark.parametrize(
    ("mode", "shelves", "match"),
    [
        ("collide", "a", "collides with a declared stream"),
        ("ok", "a, a", "two streams named 'a'"),
        ("bad_type", "a", "must return DiscoveredStream"),
    ],
)
def test_discovery_errors_fail_the_run(tmp_path: Path, mode: str, shelves: str, match: str) -> None:
    db = _project(tmp_path, f"params: {{shelves: '{shelves}', mode: {mode}}}\nstreams: all\n")
    result = _run(tmp_path, db)
    assert result.status.value == "failed"
    assert match in str(result.error)


def test_template_only_source_with_nothing_discovered_fails(tmp_path: Path) -> None:
    register = _REGISTER.replace(
        "  - name: fixed\n    table: fixed\n    write_disposition: replace\n", ""
    )
    source = _SOURCE.replace('@stream(name="fixed")\ndef fixed():\n    yield [{"k": 1}]\n', "")
    db = _project(tmp_path, "streams: all\n", register=register, source=source)
    result = _run(tmp_path, db)
    assert result.status.value == "failed"
    assert "found no streams" in str(result.error)


def test_validation_requires_a_hook_per_template_and_a_template_per_hook(tmp_path: Path) -> None:
    _project(tmp_path, "streams: all\n", source=_SOURCE.replace('@discover(stream="shelf")', ""))
    with pytest.raises(DiscoveryError, match="discover: true but has no matching @discover"):
        disc.resolve_source("shelves", tmp_path, ["sources"])

    _project(
        tmp_path,
        "streams: all\n",
        register=_REGISTER.replace("    discover: true\n", ""),
        source=_SOURCE,
    )
    with pytest.raises(DiscoveryError, match="has no matching streams\\[\\] entry"):
        disc.resolve_source("shelves", tmp_path, ["sources"])


def test_discover_rejects_non_injectable_parameters() -> None:
    with pytest.raises(TypeError, match="cannot inject"):

        @dtex.discover(stream="x")
        def hook(config: Any, cursor: Any) -> list[Any]:
            return []

    with pytest.raises(TypeError, match="non-empty string"):
        dtex.discover(stream="")


def test_discover_flag_must_be_boolean() -> None:
    from dtex.types import StreamDef

    with pytest.raises(ValueError, match="'discover' must be a boolean"):
        StreamDef.from_dict({"name": "s", "discover": "yes"})
    with pytest.raises(ValueError, match="non-empty name"):
        dtex.DiscoveredStream(name=" ")
