# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""
Baseline inference — ClickHouse query repair (OpenEnv)
======================================================
diagnose and repair broken ClickHouse SQL using execution feedback
(data/analytics engineering–style work).

- ``API_BASE_URL`` — OpenAI-compatible API base URL for the LLM.
- ``MODEL_NAME`` — Model id for chat completions.
- ``HF_TOKEN`` — Hugging Face / provider API key (also ``API_KEY`` is accepted).
- ``LOCAL_IMAGE_NAME`` — Docker image for ``EnvClient.from_docker_image()`` (alias: ``IMAGE_NAME``).

Defaults may be set for API_BASE_URL and MODEL_NAME for local testing only.

- The inference script must be named ``inference.py`` at the project root.
- Use the OpenAI client for all LLM calls (``OpenAI`` + ``chat.completions``).

STDOUT FORMAT
-------------
Emit exactly three line types to stdout, in order::

    [START] task=<task_name> env=<benchmark> model=<model_name>
    [STEP]  step=<n> action=<action_str> reward=<0.42> done=<true|false> error=<msg|null>
    [END]   success=<true|false> steps=<n> score=<score> rewards=<r1,r2,...,rn>

Rules:

- One ``[START]`` at episode begin.
- One ``[STEP]`` per ``env.step()``, immediately after it returns.
- One ``[END]`` after ``env.close()``, always (including on exception).
- ``reward``, ``rewards``, and aggregate ``score`` use two decimal places and stay in **[0.10, 0.90]** (strictly between 0 and 1; never ``0.00`` or ``1.00``).
- ``done`` and ``success`` are lowercase ``true`` or ``false``.
- ``error`` is the last ClickHouse / validation error string, or ``null``.
- Single line per record; no embedded newlines in fields.

Optional env:

- ``CHQR_NUM_EPISODES`` — random episodes when not evaluating all tasks (default ``1``).
- ``CHQR_EVAL_ALL_TASKS`` — if set, run every task JSON once (sorted by id).
- ``INFERENCE_MAX_SECONDS`` — wall-clock budget (default 1140).
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

IMAGE_NAME = os.getenv("IMAGE_NAME")
API_KEY = (
    os.getenv("OPENAI_API_KEY")
    or os.getenv("HF_TOKEN")
    or os.getenv("API_KEY")
)

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

# Reported rewards use ``0.1 + 0.8 * raw`` with ``raw`` in ``[0, 1]`` (stdout in ``[0.10, 0.90]``).
_OPEN_LO = 0.1
_OPEN_HI = 0.9


def _clamp_reported(x: Optional[float]) -> float:
    """Clamp API ``reward`` to reported range ``[0.1, 0.9]``."""
    if x is None:
        return _OPEN_LO
    try:
        v = float(x)
    except (TypeError, ValueError):
        return _OPEN_LO
    if v != v:  # NaN
        return _OPEN_LO
    return min(max(v, _OPEN_LO), _OPEN_HI)


def _reported_from_raw(raw: float) -> float:
    """Same mapping as the environment: internal ``[0, 1]`` -> reported ``[0.1, 0.9]``."""
    x = min(max(float(raw), 0.0), 1.0)
    return _clamp_reported(0.1 + 0.8 * x)


def _raw_from_reported(rep: float) -> float:
    """Invert reported reward when ``metadata.raw_reward`` is missing."""
    o = _clamp_reported(rep)
    return min(max((o - 0.1) / 0.8, 0.0), 1.0)


def _fmt_stdout_reward(v: float) -> str:
    """Two decimals; never print ``0.00`` or ``1.00``."""
    x = _clamp_reported(v)
    s = f"{x:.2f}"
    if s == "0.00":
        return "0.10"
    if s == "1.00":
        return "0.90"
    return s

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
        f"[STEP] step={step} action={safe_action} reward={_fmt_stdout_reward(reward)} "
        f"done={done_val} error={error_val}",
        flush=True,
    )


def log_end(success: bool, steps: int, score: float, rewards: List[float]) -> None:
    if not rewards:
        rewards_str = _fmt_stdout_reward(_OPEN_LO)
    else:
        rewards_str = ",".join(_fmt_stdout_reward(r) for r in rewards)
    print(
        f"[END] success={str(success).lower()} steps={steps} score={_fmt_stdout_reward(score)} "
        f"rewards={rewards_str}",
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
    except Exception:  # noqa: BLE001
        return "SELECT 1"


def _warn_if_missing_auth() -> None:
    if API_KEY:
        return
    if "openai.com" in (API_BASE_URL or ""):
        print(
            "WARNING: OPENAI_API_KEY is unset; OpenAI API calls will fail.",
            file=sys.stderr,
            flush=True,
        )
    elif "huggingface.co" in API_BASE_URL or "hf.co" in API_BASE_URL:
        print(
            "WARNING: HF_TOKEN or API_KEY is unset; Hugging Face router calls may fail.",
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
    all_rewards: List[float] = []
    steps_taken = 0
    score = _reported_from_raw(0.0)
    success = False
    last_error: Optional[str] = None
    env: Optional[ClickhouseQueryRepairEnv] = None

    client = OpenAI(
        base_url=API_BASE_URL,
        api_key=API_KEY,
        timeout=120.0,
        max_retries=2,
    )

    try:
        try:
            if IMAGE_NAME:
                env = await ClickhouseQueryRepairEnv.from_docker_image(IMAGE_NAME)
            else:
                base = "https://prasannakumar414-clickhouse-query-repair.hf.space"
                env = ClickhouseQueryRepairEnv(base_url=base)
        except Exception:  # noqa: BLE001
            env = None

        if env is None:
            return

        deadline = time.monotonic() + float(INFERENCE_MAX_SECONDS)
        plan = _episode_task_ids()
        num_episodes = len(plan)
        episode_raw_scores: List[float] = []
        solved_episodes = 0

        for ep in range(1, num_episodes + 1):
            if time.monotonic() > deadline:
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
                md = obs.metadata or {}
                raw_reward = min(
                    max(
                        float(
                            md.get(
                                "raw_reward",
                                _raw_from_reported(_clamp_reported(obs.reward)),
                            )
                        ),
                        0.0,
                    ),
                    1.0,
                )
                reward = _clamp_reported(obs.reward)
                done = bool(result.done)
                ep_rewards.append(raw_reward)
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

            solved_this_ep = bool(ep_rewards) and bool(
                obs.gold_match or obs.result_match
            )
            raw_ep = (
                1.0
                if solved_this_ep
                else (sum(ep_rewards) / max(len(ep_rewards), 1))
            )
            raw_ep = min(max(raw_ep, 0.0), 1.0)
            episode_raw_scores.append(raw_ep)
            if solved_this_ep:
                solved_episodes += 1

        if episode_raw_scores:
            score = _reported_from_raw(
                sum(episode_raw_scores) / len(episode_raw_scores)
            )
        success = solved_episodes == len(episode_raw_scores) and len(episode_raw_scores) > 0
    except Exception:  # noqa: BLE001
        pass
    finally:
        if env is not None:
            try:
                await env.close()
            except Exception:  # noqa: BLE001
                pass
        log_end(success=success, steps=steps_taken, score=score, rewards=all_rewards)


async def main() -> None:
    await _run_episodes()


if __name__ == "__main__":
    asyncio.run(main())
