# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""ClickHouse-backed query repair: CH validates every step, gated by local checks."""

from __future__ import annotations

import os
import random
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from openenv.core.env_server.interfaces import Environment
from openenv.core.env_server.types import State

try:
    from ..models import ClickhouseQueryRepairAction, ClickhouseQueryRepairObservation
except ImportError:
    from models import ClickhouseQueryRepairAction, ClickhouseQueryRepairObservation

try:
    from .chqr_clickhouse import rows_fingerprint, run_select_query, run_setup_statements
    from .sql_utils import (
        check_required_terms,
        is_safe_select,
        local_sql_feedback,
        normalize_sql,
        required_terms_fraction,
        result_set_similarity,
        sql_token_similarity,
    )
    from .task_loader import load_all_tasks, validate_task_schema
except ImportError:
    from chqr_clickhouse import rows_fingerprint, run_select_query, run_setup_statements
    from sql_utils import (
        check_required_terms, is_safe_select, local_sql_feedback, normalize_sql,
        required_terms_fraction, result_set_similarity, sql_token_similarity,
    )
    from task_loader import load_all_tasks, validate_task_schema


_W_SQL_SIM = 0.15
_W_TERMS = 0.05
_W_EXEC = 0.10
_W_RESULT = 0.70


class ClickhouseQueryRepairEnvironment(Environment):
    """
    Sample a repair task on reset.  Every step is evaluated against ClickHouse
    (gated by local safety checks).  Episode auto-terminates on exact gold match
    (reward 1.0) or when max steps are hit.

    Reward is a continuous decimal in [0.0, 1.0] computed from four signals:

        sql_token_similarity   (weight 0.15) -- Jaccard overlap of SQL tokens
        required_terms_fraction(weight 0.05) -- fraction of task-mandated CH keywords
        execution_success      (weight 0.10) -- binary: runs without error
        result_set_similarity  (weight 0.70) -- row-count, row-match, cell overlap

    Exact result match => reward = 1.0 (episode ends).
    """

    SUPPORTS_CONCURRENT_SESSIONS: bool = True

    def __init__(self) -> None:
        self._state = State(episode_id=str(uuid4()), step_count=0)
        self._tasks = load_all_tasks()
        for task in self._tasks:
            validate_task_schema(task)
        if not self._tasks:
            raise RuntimeError("No tasks found under clickhouse_query_repair/tasks/*.json")
        self._rng = random.Random()
        self._max_steps = int(os.environ.get("CHQR_MAX_STEPS_PER_EPISODE", "8"))
        self._current_task: Optional[Dict[str, Any]] = None
        self._steps_taken = 0
        self._last_submitted_sql = ""

    def _task_meta(self) -> Dict[str, Any]:
        assert self._current_task is not None
        return {
            "task_id": self._current_task["id"],
            "difficulty": self._current_task["difficulty"],
            "episode_id": self._state.episode_id,
        }

    def reset(self) -> ClickhouseQueryRepairObservation:
        self._state = State(episode_id=str(uuid4()), step_count=0)
        self._steps_taken = 0
        self._last_submitted_sql = ""
        self._current_task = self._rng.choice(self._tasks)
        assert self._current_task is not None

        run_setup_statements(str(self._current_task["setup_sql"]))

        return ClickhouseQueryRepairObservation(
            broken_query=str(self._current_task["broken_query"]),
            instruction=str(self._current_task["instruction"]),
            schema_hint=str(self._current_task.get("schema_hint", "")),
            simulated_error=str(self._current_task.get("simulated_error", "")),
            feedback_message="Episode started. Submit repaired SQL each step; ClickHouse validates if local checks pass.",
            last_submitted_sql="",
            step_index=0,
            max_steps=self._max_steps,
            terminal=False,
            reward=0.0,
            done=False,
            metadata=self._task_meta(),
        )

    def step(self, action: ClickhouseQueryRepairAction) -> ClickhouseQueryRepairObservation:  # type: ignore[override]
        self._state.step_count += 1
        self._steps_taken += 1
        sql = action.repaired_query.strip()
        self._last_submitted_sql = sql

        assert self._current_task is not None
        task = self._current_task
        gold = str(task["gold_query"])
        required_terms: Optional[List[str]] = task.get("required_terms")

        reward, exec_ok, ch_err, gold_match, result_match, feedback = (
            self._evaluate_step(sql, gold, required_terms)
        )

        is_last_step = self._steps_taken >= self._max_steps
        done = (reward >= 1.0) or is_last_step

        return ClickhouseQueryRepairObservation(
            broken_query=str(task["broken_query"]),
            instruction=str(task["instruction"]),
            schema_hint=str(task.get("schema_hint", "")),
            simulated_error=str(task.get("simulated_error", "")),
            feedback_message=feedback,
            last_submitted_sql=sql,
            step_index=self._steps_taken,
            max_steps=self._max_steps,
            terminal=done,
            execution_ok=exec_ok,
            clickhouse_error=ch_err,
            gold_match=gold_match,
            result_match=result_match,
            reward=reward,
            done=done,
            metadata={**self._task_meta(), "normalized_match": gold_match},
        )

    def _evaluate_step(
        self,
        candidate: str,
        gold: str,
        required_terms: Optional[List[str]],
    ) -> Tuple[float, Optional[bool], Optional[str], Optional[bool], Optional[bool], str]:
        """
        Compute a continuous reward in [0.0, 1.0]:

            reward = W_SQL * sql_sim + W_TERMS * terms_frac
                   + W_EXEC * exec_ok + W_RESULT * result_sim

        Exact result match overrides to 1.0.
        Returns (reward, execution_ok, ch_error, gold_match, result_match, feedback).
        """
        tok_sim = sql_token_similarity(candidate, gold)
        terms_frac = required_terms_fraction(candidate, required_terms)

        if not is_safe_select(candidate):
            fb = local_sql_feedback(candidate)
            reward = round(_W_SQL_SIM * tok_sim, 4)
            return reward, False, fb, False, False, fb

        terms_msg = check_required_terms(candidate, required_terms)
        feedback_parts: List[str] = []
        if terms_msg:
            feedback_parts.append(terms_msg)

        c_rows, c_err = run_select_query(candidate)
        if c_err:
            reward = round(_W_SQL_SIM * tok_sim + _W_TERMS * terms_frac, 4)
            return reward, False, c_err, False, False, f"ClickHouse error: {c_err}"

        g_rows, g_err = run_select_query(gold)
        if g_err:
            fb = f"Reference query failed (task misconfigured): {g_err}"
            return 0.0, False, fb, False, False, fb

        gm = normalize_sql(candidate) == normalize_sql(gold)
        rm = rows_fingerprint(g_rows) == rows_fingerprint(c_rows)

        if gm or rm:
            return 1.0, True, None, gm, rm, "Correct! Result matches the gold query."

        res_sim = result_set_similarity(c_rows, g_rows)
        reward = (
            _W_SQL_SIM * tok_sim
            + _W_TERMS * terms_frac
            + _W_EXEC * 1.0
            + _W_RESULT * res_sim
        )
        reward = round(min(max(reward, 0.0), 0.99), 4)

        feedback_parts.append(
            f"Query executed but result differs from expected "
            f"(sql_sim={tok_sim:.2f}, result_sim={res_sim:.2f}, reward={reward:.2f})."
        )
        return reward, True, None, False, False, " ".join(feedback_parts)

    @property
    def state(self) -> State:
        return self._state
