# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""ClickHouse-backed query repair: one validation per episode (terminal step)."""

from __future__ import annotations

import os
import random
from typing import Any, Dict, Optional, Tuple
from uuid import uuid4

from openenv.core.env_server.interfaces import Environment
from openenv.core.env_server.types import State

try:
    from ..models import ClickhouseQueryRepairAction, ClickhouseQueryRepairObservation
except ImportError:
    from models import ClickhouseQueryRepairAction, ClickhouseQueryRepairObservation

try:
    from .chqr_clickhouse import rows_fingerprint, run_select_query, run_setup_statements
    from .sql_utils import is_safe_select, local_sql_feedback, normalize_sql
    from .task_loader import load_all_tasks, validate_task_schema
except ImportError:
    from chqr_clickhouse import rows_fingerprint, run_select_query, run_setup_statements
    from sql_utils import is_safe_select, local_sql_feedback, normalize_sql
    from task_loader import load_all_tasks, validate_task_schema


class ClickhouseQueryRepairEnvironment(Environment):
    """
    Sample a repair task on reset; intermediate steps only record drafts locally.
    ClickHouse executes the candidate SQL once on the terminal step.
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
            feedback_message="Episode started. Revise SQL locally; ClickHouse grades the final step only.",
            last_submitted_sql="",
            step_index=0,
            max_steps=self._max_steps,
            terminal=False,
            reward=0.0,
            done=False,
            metadata={
                "task_id": self._current_task["id"],
                "episode_id": self._state.episode_id,
            },
        )

    def step(self, action: ClickhouseQueryRepairAction) -> ClickhouseQueryRepairObservation:  # type: ignore[override]
        self._state.step_count += 1
        self._steps_taken += 1
        sql = action.repaired_query.strip()
        self._last_submitted_sql = sql

        assert self._current_task is not None
        task = self._current_task
        gold = str(task["gold_query"])

        terminal = bool(action.submit_final) or (self._steps_taken >= self._max_steps)

        if not terminal:
            fb = local_sql_feedback(sql)
            return ClickhouseQueryRepairObservation(
                broken_query=str(task["broken_query"]),
                instruction=str(task["instruction"]),
                schema_hint=str(task.get("schema_hint", "")),
                simulated_error=str(task.get("simulated_error", "")),
                feedback_message=fb,
                last_submitted_sql=sql,
                step_index=self._steps_taken,
                max_steps=self._max_steps,
                terminal=False,
                reward=0.0,
                done=False,
                metadata={"task_id": task["id"], "episode_id": self._state.episode_id},
            )

        reward, exec_ok, ch_err, gold_match, result_match = self._validate_terminal(
            sql, gold
        )

        return ClickhouseQueryRepairObservation(
            broken_query=str(task["broken_query"]),
            instruction=str(task["instruction"]),
            schema_hint=str(task.get("schema_hint", "")),
            simulated_error=str(task.get("simulated_error", "")),
            feedback_message="Terminal validation complete.",
            last_submitted_sql=sql,
            step_index=self._steps_taken,
            max_steps=self._max_steps,
            terminal=True,
            execution_ok=exec_ok,
            clickhouse_error=ch_err,
            gold_match=gold_match,
            result_match=result_match,
            reward=reward,
            done=True,
            metadata={
                "task_id": task["id"],
                "episode_id": self._state.episode_id,
                "normalized_match": gold_match,
            },
        )

    def _validate_terminal(self, candidate: str, gold: str) -> Tuple[float, bool, Optional[str], Optional[bool], Optional[bool]]:
        """Returns reward, execution_ok, clickhouse_error, gold_match, result_match."""
        if not is_safe_select(candidate):
            return (
                0.0,
                False,
                "Query failed local safety checks (single SELECT only).",
                False,
                False,
            )

        c_rows, c_err = run_select_query(candidate)
        if c_err:
            return 0.0, False, c_err, False, False

        g_rows, g_err = run_select_query(gold)
        if g_err:
            return (
                0.0,
                False,
                f"Reference query failed (task misconfigured): {g_err}",
                False,
                False,
            )

        gold_match = normalize_sql(candidate) == normalize_sql(gold)
        result_match = rows_fingerprint(g_rows) == rows_fingerprint(c_rows)
        reward = 1.0 if (gold_match or result_match) else 0.0
        return reward, True, None, gold_match, result_match

    @property
    def state(self) -> State:
        return self._state
