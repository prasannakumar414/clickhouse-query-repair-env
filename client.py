# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""HTTP/WebSocket client for the ClickHouse query repair environment."""

from typing import Any, Dict

from openenv.core import EnvClient
from openenv.core.client_types import StepResult
from openenv.core.env_server.types import State

from .models import ClickhouseQueryRepairAction, ClickhouseQueryRepairObservation


class ClickhouseQueryRepairEnv(
    EnvClient[
        ClickhouseQueryRepairAction,
        ClickhouseQueryRepairObservation,
        State,
    ]
):
    """Client for repair episodes (each step validated against ClickHouse when safe)."""

    def _step_payload(
        self, action: ClickhouseQueryRepairAction
    ) -> Dict[str, Any]:
        return {
            "repaired_query": action.repaired_query,
            "submit_final": action.submit_final,
        }

    def _parse_result(
        self, payload: Dict[str, Any]
    ) -> StepResult[ClickhouseQueryRepairObservation]:
        obs_data = payload.get("observation", {})
        observation = ClickhouseQueryRepairObservation(
            broken_query=obs_data.get("broken_query", ""),
            instruction=obs_data.get("instruction", ""),
            schema_hint=obs_data.get("schema_hint", ""),
            simulated_error=obs_data.get("simulated_error", ""),
            feedback_message=obs_data.get("feedback_message", ""),
            last_submitted_sql=obs_data.get("last_submitted_sql", ""),
            step_index=obs_data.get("step_index", 0),
            max_steps=obs_data.get("max_steps", 8),
            terminal=obs_data.get("terminal", False),
            execution_ok=obs_data.get("execution_ok"),
            clickhouse_error=obs_data.get("clickhouse_error"),
            gold_match=obs_data.get("gold_match"),
            result_match=obs_data.get("result_match"),
            done=payload.get("done", False),
            reward=payload.get("reward"),
            metadata=obs_data.get("metadata", {}),
        )

        return StepResult(
            observation=observation,
            reward=payload.get("reward"),
            done=payload.get("done", False),
        )

    def _parse_state(self, payload: Dict[str, Any]) -> State:
        return State(
            episode_id=payload.get("episode_id"),
            step_count=payload.get("step_count", 0),
        )
