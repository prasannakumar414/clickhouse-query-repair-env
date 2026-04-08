# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Lightweight SQL normalization and local (non-ClickHouse) checks."""

from __future__ import annotations

import re
from typing import List


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
    """Whether SQL is eligible for server-side execution (terminal validation)."""
    fb = local_sql_feedback(sql)
    return fb.startswith("Draft recorded")

