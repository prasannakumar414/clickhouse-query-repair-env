# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Benchmark driver for the ClickHouse query repair environment.

Emits [START], one [STEP] per env.step, and [END] lines to stdout (see below).
"""

from __future__ import annotations

import asyncio
import os
import textwrap
from typing import List, Optional

from openai import OpenAI

from clickhouse_query_repair import ClickhouseQueryRepairAction
from clickhouse_query_repair.client import ClickhouseQueryRepairEnv

IMAGE_NAME = os.getenv("IMAGE_NAME")
API_KEY = os.getenv("HF_TOKEN") or os.getenv("API_KEY")

API_BASE_URL = os.getenv("API_BASE_URL") or "https://router.huggingface.co/v1"
MODEL_NAME = os.getenv("MODEL_NAME") or "Qwen/Qwen2.5-72B-Instruct"
TASK_NAME = os.getenv("CHQR_TASK", "queryrepair")
BENCHMARK = os.getenv("CHQR_BENCHMARK", "clickhouse_query_repair")
MAX_STEPS = int(os.getenv("CHQR_MAX_STEPS", "6"))
TEMPERATURE = 0.35
MAX_TOKENS = 700

SYSTEM_PROMPT = textwrap.dedent(
    """
    You fix broken ClickHouse SQL for tasks that use MergeTree tables, partition keys,
    and typical ClickHouse aggregates. Reply with exactly one SELECT statement — no
    markdown fences, no commentary. Intermediate turns are drafts; only the final turn
    should be your best fix (the harness sets submit_final on the last step).
    """
).strip()


def log_start(task: str, env: str, model: str) -> None:
    print(f"[START] task={task} env={env} model={model}", flush=True)


def log_step(step: int, action: str, reward: float, done: bool, error: Optional[str]) -> None:
    error_val = error if error else "null"
    done_val = str(done).lower()
    print(
        f"[STEP] step={step} action={action!r} reward={reward:.2f} done={done_val} error={error_val}",
        flush=True,
    )


def log_end(success: bool, steps: int, score: float, rewards: List[float]) -> None:
    rewards_str = ",".join(f"{r:.2f}" for r in rewards)
    print(
        f"[END] success={str(success).lower()} steps={steps} score={score:.3f} rewards={rewards_str}",
        flush=True,
    )


def build_user_prompt(
    broken_query: str,
    instruction: str,
    schema_hint: str,
    simulated_error: str,
    feedback: str,
    step: int,
) -> str:
    return textwrap.dedent(
        f"""
        Instruction: {instruction}
        Broken query:
        {broken_query}
        Simulated issue: {simulated_error}
        Schema hint: {schema_hint}
        Environment feedback (no DB run yet): {feedback}
        Step {step} of {MAX_STEPS}. Output one SELECT only.
        """
    ).strip()


def get_model_sql(client: OpenAI, user_prompt: str) -> str:
    completion = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        temperature=TEMPERATURE,
        max_tokens=MAX_TOKENS,
        stream=False,
    )
    text = (completion.choices[0].message.content or "").strip()
    for fence in ("```sql", "```"):
        text = text.replace(fence, "")
    return text.strip()


async def main() -> None:
    client = OpenAI(base_url=API_BASE_URL, api_key=API_KEY)

    if IMAGE_NAME:
        env = await ClickhouseQueryRepairEnv.from_docker_image(IMAGE_NAME)
    else:
        base = os.getenv("CHQR_BASE_URL", "http://127.0.0.1:8000")
        env = ClickhouseQueryRepairEnv(base_url=base)

    rewards: List[float] = []
    steps_taken = 0
    score = 0.0
    success = False
    last_error: Optional[str] = None

    log_start(task=TASK_NAME, env=BENCHMARK, model=MODEL_NAME)

    try:
        result = await env.reset()
        obs = result.observation
        last_feedback = obs.feedback_message

        for step in range(1, MAX_STEPS + 1):
            user_prompt = build_user_prompt(
                obs.broken_query,
                obs.instruction,
                obs.schema_hint,
                obs.simulated_error,
                last_feedback,
                step,
            )
            sql = get_model_sql(client, user_prompt)
            is_last = step == MAX_STEPS
            action = ClickhouseQueryRepairAction(repaired_query=sql, submit_final=is_last)

            result = await env.step(action)
            obs = result.observation
            reward = float(result.reward or 0.0)
            done = bool(result.done)
            rewards.append(reward)
            steps_taken = step
            last_error = obs.clickhouse_error

            log_step(
                step=step,
                action=sql[:500],
                reward=reward,
                done=done,
                error=last_error,
            )

            last_feedback = obs.feedback_message
            if done:
                break

        score = 1.0 if rewards and rewards[-1] >= 0.99 else (sum(rewards) / max(len(rewards), 1))
        score = min(max(score, 0.0), 1.0)
        success = bool(rewards and rewards[-1] >= 0.99)

    finally:
        try:
            await env.close()
        except Exception as exc:  # noqa: BLE001
            print(f"[DEBUG] env.close(): {exc}", flush=True)
        log_end(success=success, steps=steps_taken, score=score, rewards=rewards)


if __name__ == "__main__":
    asyncio.run(main())
