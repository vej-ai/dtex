"""Typed promoted columns stay stable across null batches and independent runs."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pytest
import yaml

import dtex
from dtex.engine.discovery import resolve_source
from dtex.types import FieldType, StreamRunConfig


@pytest.mark.parametrize('raw', [
    'FLOAT', {'invoice_total': 'FLOAT'}, [None],
    [{'name': 'invoice_total'}], [{'name': 'invoice_total', 'type': 'nonsense'}],
    [{'name': 'invoice_total', 'type': 'FLOAT'}] * 2,
    [{'name': '', 'type': 'FLOAT'}],
    [{'name': '_dtex_synced_at', 'type': 'STRING'}],
])
def test_configured_schema_rejects_invalid_fields(raw: Any) -> None:
    with pytest.raises(ValueError, match='schema'):
        StreamRunConfig.from_yaml_value({'schema': raw}, stream_name='items', config_name='test')


def test_configured_schema_parses_portable_types() -> None:
    config = StreamRunConfig.from_yaml_value(
        {'schema': [{'name': 'invoice_total', 'type': 'FLOAT'}]},
        stream_name='items', config_name='test',
    )
    assert config.schema is not None
    assert config.schema.fields[0].type is FieldType.FLOAT


def _project(root: Path, *, declared: bool, configured: list[dict[str, Any]]) -> Path:
    (root / 'dtex_project.yml').write_text('name: schema_test\nversion: "1.0.0"\n')
    (root / 'profiles.yml').write_text('duckdb:\n  default_target: dev\n  targets:\n    dev: {}\n')
    folder = root / 'sources' / 'schema_test'
    folder.mkdir(parents=True)
    stream: dict[str, Any] = {
        'name': 'items', 'table': 'items', 'primary_key': 'id', 'write_disposition': 'merge',
    }
    if declared:
        stream['schema'] = [
            {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'counter', 'type': 'INTEGER'},
        ]
    (folder / 'register.yaml').write_text(yaml.safe_dump({
        'name': 'schema_test', 'kind': 'source', 'version': '1.0.0',
        'summary': 'Typed promotion regression fixture', 'streams': [stream],
    }))
    (folder / 'source.py').write_text('''from dtex import stream

@stream(name="items")
def items(stream_def):
    for row in [
        {"id": 1, "counter": 7, "invoice_total": None},
        {"id": 2, "counter": 8, "invoice_total": "42.5"},
        {"id": 3, "counter": 9, "invoice_total": 7.0},
    ]:
        if stream_def.schema is not None:
            yield [{name: row.get(name) for name in stream_def.schema.names}]
        else:
            yield [row]
''')
    (root / 'configs').mkdir()
    (root / 'configs' / 'test.yml').write_text(yaml.safe_dump({
        'name': 'test', 'source': 'schema_test', 'destination': 'duckdb',
        'streams': {'items': {'schema': configured}},
    }))
    return root / 'warehouse.duckdb'


@pytest.mark.parametrize('declared', [True, False])
@pytest.mark.parametrize('existing_table', [True, False])
def test_null_first_batch_uses_configured_type_without_losing_other_columns(
    tmp_path: Path, declared: bool, existing_table: bool,
) -> None:
    path = _project(
        tmp_path, declared=declared, configured=[{'name': 'invoice_total', 'type': 'FLOAT'}],
    )
    if existing_table:
        with duckdb.connect(str(path)) as conn:
            conn.execute('CREATE TABLE items (id BIGINT, invoice_total DOUBLE)')
    for _ in range(2):
        result = dtex.run(config='test', project_dir=str(tmp_path),
                          destination_params_override={'path': str(path)})
        assert result.status.value == 'succeeded', result.error
    with duckdb.connect(str(path)) as conn:
        rows = conn.execute('SELECT id, counter, invoice_total FROM items ORDER BY id').fetchall()
        assert rows == [
            (1, 7, None), (2, 8, 42.5), (3, 9, 7.0),
        ]
        types = conn.execute(
            'SELECT typeof(invoice_total), typeof(counter) FROM items LIMIT 1'
        ).fetchone()
        assert types == (
            'DOUBLE', 'BIGINT',
        )
    source = resolve_source('schema_test', tmp_path, ['sources'])
    original = source.manifest.streams[0].schema
    if declared:
        assert original is not None and not original.has('invoice_total')
    else:
        assert original is None


@pytest.mark.parametrize('field', [
    {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'id', 'type': 'INTEGER'},
])
def test_configured_schema_cannot_change_declared_type_or_mode(
    tmp_path: Path, field: dict[str, Any],
) -> None:
    path = _project(tmp_path, declared=True, configured=[field])
    result = dtex.run(config='test', project_dir=str(tmp_path),
                      destination_params_override={'path': str(path)})
    assert result.status.value == 'failed'
    assert 'cannot change declared types or modes' in str(result.error)
    assert not path.exists()
