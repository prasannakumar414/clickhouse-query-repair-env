# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Lightweight SQL normalization and local (non-ClickHouse) checks."""

from __future__ import annotations

import re
from collections import Counter
from typing import Any, List, Optional, Sequence


def normalize_sql(sql: str) -> str:
    """Collapse whitespace and strip terminators for comparison."""
    s = sql.strip().rstrip(";").strip()
    s = re.sub(r"\s+", " ", s)
    return s.casefold()


def split_statements(sql: str) -> List[str]:
    """Split on semicolons; ignore empty fragments."""
    return [p.strip() for p in sql.split(";") if p.strip()]


def local_sql_feedback(sql: str) -> str:
    """
    Return a short human-readable note for intermediate steps (no DB calls).

    This is intentionally shallow: the real grade happens once per episode.
    """
    if not sql.strip():
        return "Draft is empty; submit a SELECT when ready (or set submit_final on the last step)."
    parts = split_statements(sql)
    if len(parts) > 1:
        return "Only one SQL statement is allowed for validation. Remove extra statements."
    stmt = parts[0]
    upper = stmt.upper()
    if not upper.startswith("SELECT"):
        return "Validation expects a single SELECT. Rewrite as a SELECT query."
    blocked = (
        " INTO OUTFILE",
        " FORMAT ",
        "SYSTEM ",
        " DROP ",
        " ATTACH ",
        " DETACH ",
        " TRUNCATE ",
        " CREATE ",
        " ALTER ",
        " INSERT ",
    )
    for b in blocked:
        if b in upper:
            return f"Disallowed fragment detected ({b.strip()}). Use read-only SELECT only."
    return "Draft recorded locally; ClickHouse runs only on the final step of the episode."


def is_safe_select(sql: str) -> bool:
    """Whether SQL is eligible for server-side execution."""
    fb = local_sql_feedback(sql)
    return fb.startswith("Draft recorded")


def check_required_terms(
    sql: str, terms: Optional[Sequence[str]]
) -> Optional[str]:
    """Return a feedback message if any required term is absent, else None."""
    if not terms:
        return None
    upper = sql.upper()
    missing = [t for t in terms if t.upper() not in upper]
    if missing:
        return (
            f"Query is missing required ClickHouse concept(s): {', '.join(missing)}. "
            "The task expects you to use these in your solution."
        )
    return None


def required_terms_fraction(
    sql: str, terms: Optional[Sequence[str]]
) -> float:
    """Return the fraction of required terms present in *sql* (1.0 if none required)."""
    if not terms:
        return 1.0
    upper = sql.upper()
    present = sum(1 for t in terms if t.upper() in upper)
    return present / len(terms)


# ---------------------------------------------------------------------------
# Scoring helpers for dynamic reward
# ---------------------------------------------------------------------------

_TOKEN_RE = re.compile(r"[A-Za-z_]\w*|\d+(?:\.\d+)?|[<>=!]+")


def sql_token_similarity(candidate: str, gold: str) -> float:
    """Jaccard similarity over SQL tokens (identifiers, keywords, numbers)."""
    c_tok = set(_TOKEN_RE.findall(candidate.lower()))
    g_tok = set(_TOKEN_RE.findall(gold.lower()))
    if not c_tok and not g_tok:
        return 1.0
    if not c_tok or not g_tok:
        return 0.0
    return len(c_tok & g_tok) / len(c_tok | g_tok)


def result_set_similarity(
    candidate_rows: Optional[Sequence[Sequence[Any]]],
    gold_rows: Optional[Sequence[Sequence[Any]]],
) -> float:
    """
    Multi-signal comparison of two result sets.

    Returns a float in [0.0, 1.0] combining:
      - row count proximity   (weight 0.15)
      - row-level exact match (weight 0.35, order-independent)
      - cell-level overlap    (weight 0.50, on sorted rows)
    """
    if candidate_rows is None or gold_rows is None:
        return 0.0
    if not gold_rows and not candidate_rows:
        return 1.0
    if not gold_rows or not candidate_rows:
        return 0.0

    g_n, c_n = len(gold_rows), len(candidate_rows)

    row_count_score = 1.0 - min(abs(g_n - c_n) / max(g_n, 1), 1.0)

    g_strs = Counter(repr(tuple(r)) for r in gold_rows)
    c_strs = Counter(repr(tuple(r)) for r in candidate_rows)
    matched = sum((g_strs & c_strs).values())
    row_match_score = matched / g_n

    g_sorted = sorted(gold_rows, key=lambda r: repr(r))
    c_sorted = sorted(candidate_rows, key=lambda r: repr(r))
    total_cells = 0
    matching_cells = 0
    for i in range(min(g_n, c_n)):
        g_row, c_row = g_sorted[i], c_sorted[i]
        for j in range(min(len(g_row), len(c_row))):
            total_cells += 1
            if repr(g_row[j]) == repr(c_row[j]):
                matching_cells += 1
        total_cells += abs(len(g_row) - len(c_row))
    for i in range(min(g_n, c_n), g_n):
        total_cells += len(g_sorted[i]) if i < len(g_sorted) else 1
    cell_score = matching_cells / max(total_cells, 1)

    return 0.15 * row_count_score + 0.35 * row_match_score + 0.50 * cell_score

