# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

from clickhouse_query_repair.server import sql_utils


is_safe_select = sql_utils.is_safe_select
local_sql_feedback = sql_utils.local_sql_feedback
normalize_sql = sql_utils.normalize_sql


def test_normalize_sql_casefold() -> None:
    a = normalize_sql("SELECT  a   FROM t WHERE x = 1")
    b = normalize_sql("select a from t where x = 1")
    assert a == b


def test_safe_select_accepts_simple_select() -> None:
    assert is_safe_select("SELECT 1")


def test_safe_select_rejects_insert() -> None:
    assert not is_safe_select("INSERT INTO t VALUES (1)")


def test_local_feedback_multi_statement() -> None:
    fb = local_sql_feedback("SELECT 1; SELECT 2")
    assert "one SQL" in fb
