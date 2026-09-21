"""Read-back of the tables this source landed on earlier runs.

RevenueCat v2 exposes CURRENT state only: no change feed, no cancel or
refund timestamps, no original price once a refund rewrites a transaction.
Every such fact is the DIFFERENCE between two pulls, so the diff streams
need their previous snapshot. dtex ``State`` is a small per-stream JSON
blob; tens of thousands of subscription and transaction rows do not belong
there. They already sit in the destination, so the streams read them back.

The reader is deliberately tiny and dialect-free: ``rows`` is a plain
``SELECT columns FROM table`` with no predicates, parameters or functions,
and every filter happens in Python. That is what lets one connector run
against both Tier-A destinations:

    landed_reader: bigquery   landed_dataset: my-project.my_dataset
    landed_reader: duckdb     landed_dataset: /abs/path/warehouse.duckdb
                              (or ``/abs/path/warehouse.duckdb#schema``)

``sql`` runs an operator-supplied statement verbatim (the optional seed and
bootstrap queries); its dialect is the operator's business.

A missing table is not an error. It is the first run: ``rows`` returns
``None`` and the caller treats the snapshot as empty.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any, Protocol


class LandedReaderError(RuntimeError):
    """The landed-table reader is not configured, or is configured wrongly."""


class LandedReader(Protocol):
    def rows(self, table: str, columns: list[str]) -> list[dict[str, Any]] | None:
        """All rows of ``table`` (``None`` when the table does not exist)."""
        ...

    def sql(self, statement: str) -> list[dict[str, Any]]:
        """Run an operator-supplied statement and return its rows."""
        ...


class BigQueryReader:
    """``landed_dataset`` = ``project.dataset``; auth is the run's ADC."""

    def __init__(self, dataset: str, log: logging.Logger) -> None:
        parts = dataset.split(".")
        if len(parts) != 2 or not all(parts):
            raise LandedReaderError(
                "revenuecat: landed_dataset must be 'project.dataset' for bigquery, "
                f"got {dataset!r}"
            )
        self._project, self._dataset = parts
        self._log = log
        self._client: Any = None

    def _bq(self) -> Any:
        if self._client is None:
            from google.cloud import bigquery

            self._client = bigquery.Client(project=self._project)
        return self._client

    def rows(self, table: str, columns: list[str]) -> list[dict[str, Any]] | None:
        from google.api_core.exceptions import NotFound

        cols = ", ".join(f"`{c}`" for c in columns)
        statement = f"SELECT {cols} FROM `{self._project}.{self._dataset}.{table}`"
        try:
            result = self._bq().query(statement).result()
        except NotFound:
            self._log.info(
                "revenuecat: landed table %s does not exist yet, treating as empty", table
            )
            return None
        return [dict(row) for row in result]

    def sql(self, statement: str) -> list[dict[str, Any]]:
        return [dict(row) for row in self._bq().query(statement).result()]


class DuckDBReader:
    """``landed_dataset`` = a database path, optionally ``path#schema``.

    Opens its own connection per read and closes it again. Inside a dtex run
    the destination holds the same file open in this process; DuckDB hands a
    second in-process connection the same database instance, and a read only
    sees committed rows, which is exactly the previous streams' output.
    """

    def __init__(self, dataset: str, log: logging.Logger) -> None:
        path, _, schema = dataset.partition("#")
        if not path:
            raise LandedReaderError(
                "revenuecat: landed_dataset must be a DuckDB file path for duckdb"
            )
        self._path, self._schema = path, schema or None
        self._log = log

    def _table(self, table: str) -> str:
        return f'"{self._schema}"."{table}"' if self._schema else f'"{table}"'

    def _run(self, statement: str, timestamps: tuple[str, ...] = ()) -> list[dict[str, Any]]:
        import duckdb

        conn = duckdb.connect(self._path)
        try:
            cursor = conn.execute(statement)
            names = [d[0] for d in cursor.description]
            out = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
        finally:
            conn.close()
        for row in out:
            for name in timestamps:
                if row[name] is not None:
                    row[name] = datetime.fromtimestamp(row[name] / 1_000_000, tz=UTC)
        return out

    def rows(self, table: str, columns: list[str]) -> list[dict[str, Any]] | None:
        import duckdb

        cols = ", ".join(f'"{c}"' for c in columns)
        try:
            described = self._run(f"DESCRIBE SELECT {cols} FROM {self._table(table)}")
        except duckdb.CatalogException:
            self._log.info(
                "revenuecat: landed table %s does not exist yet, treating as empty", table
            )
            return None
        # The DuckDB destination binds aware datetimes into timezone-naive
        # TIMESTAMP columns, and DuckDB converts such a bind through the
        # session time zone. Reading the column back as an instant has to take
        # the same road in reverse (naive -> TIMESTAMPTZ in the same session
        # zone), or every landed timestamp comes back shifted by the machine's
        # UTC offset. epoch_us keeps pytz out of the picture.
        stamps = tuple(
            str(d["column_name"]) for d in described if str(d["column_type"]) == "TIMESTAMP"
        )
        select = ", ".join(
            f'epoch_us(CAST("{c}" AS TIMESTAMPTZ)) AS "{c}"' if c in stamps else f'"{c}"'
            for c in columns
        )
        return self._run(f"SELECT {select} FROM {self._table(table)}", stamps)

    def sql(self, statement: str) -> list[dict[str, Any]]:
        return self._run(statement)


def make_reader(kind: str, dataset: str, log: logging.Logger) -> LandedReader:
    """Build the reader named by the ``landed_reader`` param."""
    kind = (kind or "").strip().lower()
    dataset = (dataset or "").strip()
    if not kind:
        raise LandedReaderError(
            "revenuecat: this stream diffs against the rows landed by earlier runs and needs "
            "`landed_reader` (bigquery | duckdb) and `landed_dataset` set to the destination "
            "this config writes to. Streams that need no read-back: products, "
            "entitlement_products, customers, metrics_daily."
        )
    if kind == "bigquery":
        return BigQueryReader(dataset, log)
    if kind == "duckdb":
        return DuckDBReader(dataset, log)
    raise LandedReaderError(f"revenuecat: unknown landed_reader {kind!r} (bigquery | duckdb)")
