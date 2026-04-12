# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Inference Script — ClickHouse query repair
============================================
MANDATORY (environment configuration)

- ``API_BASE_URL`` — OpenAI-compatible API base URL for the LLM.
- ``MODEL_NAME`` — Model id for chat completions.
- ``HF_TOKEN`` — Hugging Face / provider API key (also ``API_KEY`` is accepted).
- ``LOCAL_IMAGE_NAME`` — Docker image for ``EnvClient.from_docker_image()`` (alias: ``IMAGE_NAME``).

Defaults may be set for API_BASE_URL and MODEL_NAME for local testing only.

- The inference script must be named ``inference.py`` at the project root.
- Use the OpenAI client for all LLM calls (``OpenAI`` + ``chat.completions``).

STDOUT FORMAT
-------------
Emit exactly three line types to stdout, in order:

  [START] task=<task_name> env=<benchmark> model=<model_name>
  [STEP] step=<n> action=<action_str> reward=<0.00> done=<true|false> error=<msg|null>
  [END] success=<true|false> steps=<n> score=<score> rewards=<r1,r2,...,rn>

Rules:

- One [START] at episode begin.
- One [STEP] per ``env.step()``, immediately after it returns.
- One [END] after ``env.close()``, always (including on errors).
- ``reward`` and each reward in ``rewards`` use two decimal places.
- ``done`` and ``success`` are lowercase ``true`` or ``false``.
- ``error`` is the last error string, or the literal ``null``.
- Single line per record; no embedded newlines in fields.
- Normalized ``score`` is in [0, 1].

Infra: default wall-clock budget under 20 minutes; tune ``INFERENCE_MAX_SECONDS``; targets ~2 vCPU / 8 GiB.

Optional:

- ``CHQR_NUM_EPISODES`` — number of **random** episodes (each ``env.reset()`` samples a task with replacement). Default ``1``.
  Ignored when ``CHQR_EVAL_ALL_TASKS`` is set.
- ``CHQR_EVAL_ALL_TASKS`` — if ``1`` / ``true``, run **every** task JSON once (sorted by id): ``env.reset(task_id=...)`` per episode.
  This is the only mode that guarantees full task coverage.
"""

from __future__ import annotations

import asyncio
import os
import sys
import textwrap
import time
from typing import List, Optional

from openai import OpenAI

from clickhouse_query_repair import ClickhouseQueryRepairAction
from clickhouse_query_repair.client import ClickhouseQueryRepairEnv

IMAGE_NAME = os.getenv("IMAGE_NAME") or os.getenv("LOCAL_IMAGE_NAME") or "prasannakumar08/clickhouse_query_repair_env:latest"
API_KEY = os.getenv("HF_TOKEN") or os.getenv("API_KEY")

API_BASE_URL = os.getenv("API_BASE_URL") or "https://router.huggingface.co/v1"
MODEL_NAME = os.getenv("MODEL_NAME") or "Qwen/Qwen2.5-72B-Instruct"
TASK_NAME = os.getenv("CHQR_TASK", "queryrepair")
BENCHMARK = os.getenv("CHQR_BENCHMARK", "clickhouse_query_repair")
# How many env.reset() episodes when not using CHQR_EVAL_ALL_TASKS (random sampling).
NUM_EPISODES = int(os.getenv("CHQR_NUM_EPISODES", "1"))
# If true, run one episode per task JSON (sorted by id); overrides NUM_EPISODES.
EVAL_ALL_TASKS = True
MAX_STEPS = int(os.getenv("CHQR_MAX_STEPS", "6"))
# Whole-run wall time (seconds). Default 1140s (< 20 min) for evaluator limits.
INFERENCE_MAX_SECONDS = int(os.getenv("INFERENCE_MAX_SECONDS", "1140"))
TEMPERATURE = 0.35
MAX_TOKENS = 700

SYSTEM_PROMPT = textwrap.dedent(
    """
    You fix broken ClickHouse SQL for tasks that use MergeTree tables, partition keys,
    and typical ClickHouse aggregates. Reply with exactly one SELECT statement — no
    markdown fences, no commentary. Use environment feedback each step to improve the query.
    """
).strip()


def log_start(task: str, env: str, model: str) -> None:
    print(f"[START] task={task} env={env} model={model}", flush=True)


def log_step(step: int, action: str, reward: float, done: bool, error: Optional[str]) -> None:
    error_val = error if error else "null"
    done_val = str(done).lower()
    safe_action = " ".join(str(action).split())
    print(
        f"[STEP] step={step} action={safe_action} reward={reward:.2f} done={done_val} error={error_val}",
        flush=True,
    )


def log_end(success: bool, steps: int, score: float, rewards: List[float]) -> None:
    rewards_str = ",".join(f"{r:.2f}" for r in rewards)
    print(
        f"[END] success={str(success).lower()} steps={steps} score={score:.2f} rewards={rewards_str}",
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
        Environment feedback (last step): {feedback}
        Step {step} of {MAX_STEPS}. Output one SELECT only.
        """
    ).strip()


def get_model_sql(client: OpenAI, user_prompt: str) -> str:
    try:
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
    except Exception as exc:  # noqa: BLE001
        print(f"[DEBUG] LLM call failed: {exc}", file=sys.stderr, flush=True)
        return "SELECT 1"


def _warn_if_missing_auth() -> None:
    if API_KEY:
        return
    if "huggingface.co" in API_BASE_URL or "hf.co" in API_BASE_URL:
        print(
            "WARNING: HF_TOKEN (or API_KEY) is unset; Hugging Face router calls may fail.",
            file=sys.stderr,
            flush=True,
        )


def _episode_task_ids() -> List[Optional[str]]:
    """
    Per-episode task selection: fixed id for full-coverage mode, else None (random on server).
    """
    if EVAL_ALL_TASKS:
        from clickhouse_query_repair.server.task_loader import load_all_tasks

        return [str(t["id"]) for t in sorted(load_all_tasks(), key=lambda x: str(x["id"]))]
    return [None] * max(1, NUM_EPISODES)


async def _run_episodes() -> None:
    _warn_if_missing_auth()
    client = OpenAI(
        base_url=API_BASE_URL,
        api_key=API_KEY,
        timeout=120.0,
        max_retries=2,
    )

    if IMAGE_NAME:
        env = await ClickhouseQueryRepairEnv.from_docker_image(IMAGE_NAME)
    else:
        base = os.getenv("CHQR_BASE_URL", "http://127.0.0.1:8000")
        env = ClickhouseQueryRepairEnv(base_url=base)

    all_rewards: List[float] = []
    steps_taken = 0
    score = 0.0
    success = False
    last_error: Optional[str] = None
    deadline = time.monotonic() + float(INFERENCE_MAX_SECONDS)
    plan = _episode_task_ids()
    num_episodes = len(plan)
    episode_scores: List[float] = []
    solved_episodes = 0

    try:
        for ep in range(1, num_episodes + 1):
            if time.monotonic() > deadline:
                print(
                    "[DEBUG] INFERENCE_MAX_SECONDS budget exceeded; stopping run.",
                    file=sys.stderr,
                    flush=True,
                )
                break

            fixed_id = plan[ep - 1]
            log_start(task=TASK_NAME, env=BENCHMARK, model=MODEL_NAME)

            reset_kw: dict[str, str] = {}
            if fixed_id is not None:
                reset_kw["task_id"] = fixed_id
            result = await env.reset(**reset_kw)
            obs = result.observation
            last_feedback = obs.feedback_message
            ep_rewards: List[float] = []

            for step in range(1, MAX_STEPS + 1):
                if time.monotonic() > deadline:
                    print(
                        "[DEBUG] INFERENCE_MAX_SECONDS budget exceeded; stopping episode.",
                        file=sys.stderr,
                        flush=True,
                    )
                    break

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
                reward = min(max(reward, 0.0), 1.0)
                done = bool(result.done)
                ep_rewards.append(reward)
                all_rewards.append(reward)
                steps_taken += 1
                last_error = obs.clickhouse_error

                log_step(
                    step=step,
                    action=sql,
                    reward=reward,
                    done=done,
                    error=last_error,
                )

                last_feedback = obs.feedback_message
                if done:
                    break

            ep_score = (
                1.0
                if ep_rewards and ep_rewards[-1] >= 0.99
                else (sum(ep_rewards) / max(len(ep_rewards), 1))
            )
            ep_score = min(max(ep_score, 0.0), 1.0)
            episode_scores.append(ep_score)
            if ep_rewards and ep_rewards[-1] >= 0.99:
                solved_episodes += 1

        if episode_scores:
            score = sum(episode_scores) / len(episode_scores)
            score = min(max(score, 0.0), 1.0)
        success = solved_episodes == len(episode_scores) and len(episode_scores) > 0

    finally:
        try:
            await env.close()
        except Exception as exc:  # noqa: BLE001
            print(f"[DEBUG] env.close(): {exc}", file=sys.stderr, flush=True)
        log_end(success=success, steps=steps_taken, score=score, rewards=all_rewards)


async def main() -> None:
    await _run_episodes()


if __name__ == "__main__":
    asyncio.run(main())
