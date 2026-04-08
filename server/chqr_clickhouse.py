# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""ClickHouse HTTP client helpers (clickhouse-connect). Used only at episode boundaries."""

from __future__ import annotations

import os
from typing import Any, Optional, Sequence, Tuple

import clickhouse_connect

_QUERY_SETTINGS = {
    "max_execution_time": 45,
    "max_result_rows": 2000,
}


def get_client():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "127.0.0.1"),
        port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )


def run_setup_statements(setup_sql: str) -> None:
    """Execute DDL/DML for a task (semicolon-separated)."""
    client = get_client()
    from .sql_utils import split_statements

    for stmt in split_statements(setup_sql):
        client.command(stmt)


def run_select_query(sql: str) -> Tuple[Optional[Sequence[Tuple[Any, ...]]], Optional[str]]:
    """
    Run a single SELECT; return (rows, error_message).

    On success error_message is None. Rows may be empty.
    """
    client = get_client()
    try:
        result = client.query(sql, settings=_QUERY_SETTINGS)
        return result.result_rows, None
    except Exception as exc:  # noqa: BLE001 — surface CH errors to the agent
        return None, str(exc)


def rows_fingerprint(rows: Optional[Sequence[Sequence[Any]]]) -> Tuple[int, str]:
    """Cheap structural comparison token for two result sets."""
    if rows is None:
        return (-1, "")
    text = "\n".join(repr(r) for r in rows)
    return (len(rows), text)
