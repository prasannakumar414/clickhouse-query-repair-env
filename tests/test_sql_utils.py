# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

from clickhouse_query_repair.server import sql_utils


check_required_terms = sql_utils.check_required_terms
is_safe_select = sql_utils.is_safe_select
local_sql_feedback = sql_utils.local_sql_feedback
normalize_sql = sql_utils.normalize_sql
required_terms_fraction = sql_utils.required_terms_fraction
result_set_similarity = sql_utils.result_set_similarity
sql_token_similarity = sql_utils.sql_token_similarity


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


def test_check_required_terms_none() -> None:
    assert check_required_terms("SELECT 1", None) is None


def test_check_required_terms_empty() -> None:
    assert check_required_terms("SELECT 1", []) is None


def test_check_required_terms_present() -> None:
    assert check_required_terms("SELECT x FROM t PREWHERE y = 1", ["PREWHERE"]) is None


def test_check_required_terms_missing() -> None:
    msg = check_required_terms("SELECT x FROM t WHERE y = 1", ["PREWHERE"])
    assert msg is not None
    assert "PREWHERE" in msg


def test_check_required_terms_case_insensitive() -> None:
    assert check_required_terms("select avgmerge(x) from t", ["avgMerge"]) is None


# --- sql_token_similarity ---


def test_token_similarity_identical() -> None:
    q = "SELECT a, b FROM t WHERE x = 1"
    assert sql_token_similarity(q, q) == 1.0


def test_token_similarity_disjoint() -> None:
    assert sql_token_similarity("SELECT a FROM t", "INSERT INTO z VALUES") < 0.3


def test_token_similarity_partial() -> None:
    sim = sql_token_similarity(
        "SELECT a, b FROM t WHERE x = 1",
        "SELECT a, c FROM t WHERE x = 2",
    )
    assert 0.4 < sim < 0.9


# --- required_terms_fraction ---


def test_terms_fraction_none() -> None:
    assert required_terms_fraction("SELECT 1", None) == 1.0


def test_terms_fraction_partial() -> None:
    frac = required_terms_fraction(
        "SELECT avgMerge(x) FROM t", ["avgMerge", "countMerge"]
    )
    assert frac == 0.5


def test_terms_fraction_all() -> None:
    frac = required_terms_fraction(
        "SELECT avgMerge(x), countMerge(y) FROM t", ["avgMerge", "countMerge"]
    )
    assert frac == 1.0


# --- result_set_similarity ---


def test_result_sim_identical() -> None:
    rows = [(1, "a"), (2, "b")]
    assert result_set_similarity(rows, rows) == 1.0


def test_result_sim_empty_both() -> None:
    assert result_set_similarity([], []) == 1.0


def test_result_sim_one_empty() -> None:
    assert result_set_similarity([], [(1,)]) == 0.0


def test_result_sim_partial_overlap() -> None:
    gold = [(1, "a"), (2, "b"), (3, "c")]
    cand = [(1, "a"), (2, "x"), (3, "c")]
    sim = result_set_similarity(cand, gold)
    assert 0.4 < sim < 0.95


def test_result_sim_different_row_count() -> None:
    gold = [(1,), (2,), (3,)]
    cand = [(1,)]
    sim = result_set_similarity(cand, gold)
    assert 0.0 < sim < 0.5
