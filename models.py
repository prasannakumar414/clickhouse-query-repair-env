# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Pydantic models for ClickHouse query repair (OpenEnv Action / Observation)."""

from typing import Optional

from openenv.core.env_server.types import Action, Observation
from pydantic import Field


class ClickhouseQueryRepairAction(Action):
    """Agent submits a SQL string; set submit_final when grading should run (once per episode)."""

    repaired_query: str = Field(
        ...,
        description="Draft or final repaired ClickHouse SQL (SELECT for validation)",
    )
    submit_final: bool = Field(
        default=False,
        description="If true, run ClickHouse validation for this step (episode terminal)",
    )


class ClickhouseQueryRepairObservation(Observation):
    """Observation after ``reset``/``step``: task text, feedback, and grader fields.

    Uses the base ``metadata`` dict for structured extras (task id, ``raw_reward``,
    episode id)—the OpenEnv pattern for step ``info``-style payloads.
    """

    broken_query: str = Field(default="", description="Broken query shown to the agent")
    instruction: str = Field(default="", description="Task-specific instructions")
    schema_hint: str = Field(default="", description="Schema or CREATE TABLE excerpt")
    simulated_error: str = Field(
        default="",
        description="Synthetic error text for the broken query (no CH round-trip)",
    )
    feedback_message: str = Field(
        default="",
        description="Local hints for intermediate steps (no execution)",
    )
    last_submitted_sql: str = Field(
        default="",
        description="Last SQL string received from the agent this episode",
    )
    step_index: int = Field(default=0, description="1-based step count in the episode")
    max_steps: int = Field(default=8, description="Steps allowed before forced terminal")
    terminal: bool = Field(
        default=False,
        description="True when this observation follows episode validation",
    )
    execution_ok: Optional[bool] = Field(
        default=None,
        description="Set on terminal step: whether the candidate SELECT succeeded",
    )
    clickhouse_error: Optional[str] = Field(
        default=None,
        description="ClickHouse error message on terminal validation, if any",
    )
    gold_match: Optional[bool] = Field(
        default=None,
        description="Terminal: normalized SQL matched reference gold query",
    )
    result_match: Optional[bool] = Field(
        default=None,
        description="Terminal: result set matched gold query execution",
    )
