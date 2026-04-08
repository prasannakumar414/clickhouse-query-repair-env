---
title: Clickhouse Query Repair Environment Server
emoji: 📸
colorFrom: purple
colorTo: pink
sdk: docker
pinned: false
app_port: 8000
base_path: /web
tags:
  - openenv
---

# ClickHouse Query Repair — OpenEnv Environment

## Motivation

Production ClickHouse deployments surface a class of SQL bugs that standard SQL linters cannot catch: wrong MergeTree engine semantics, missing `-Merge` combinators on AggregateFunction columns, incorrect `CollapsingMergeTree` sign handling, `ARRAY JOIN` row inflation, and partition-unaware date filters. These bugs compile and often *run without errors* — they just return silently wrong results.

This environment trains and evaluates LLM agents on their ability to **diagnose and repair broken ClickHouse SQL** across 12 tasks spanning three difficulty tiers. Each episode presents a broken query, schema hints, and an error description. The agent iterates, receiving real ClickHouse execution feedback every step, and earns a continuous reward reflecting how close its repair is to the gold-standard fix.

## Action Space

| Field | Type | Description |
|-------|------|-------------|
| `repaired_query` | `str` | The agent's repaired ClickHouse SQL (`SELECT` only) |
| `submit_final` | `bool` | Legacy/optional; the environment auto-terminates on correct result or max steps |

## Observation Space

| Field | Type | When Set | Description |
|-------|------|----------|-------------|
| `broken_query` | `str` | always | The original broken SQL to repair |
| `instruction` | `str` | always | Natural-language description of what's wrong |
| `schema_hint` | `str` | always | Table schema / engine info |
| `simulated_error` | `str` | always | Synthetic error or symptom description |
| `feedback_message` | `str` | always | Detailed feedback including scoring breakdown |
| `last_submitted_sql` | `str` | after step | The SQL the agent last submitted |
| `step_index` | `int` | always | Current step (1-based) |
| `max_steps` | `int` | always | Steps before forced termination (default 8) |
| `terminal` | `bool` | always | Whether the episode has ended |
| `execution_ok` | `bool?` | after CH run | Whether the query executed without error |
| `clickhouse_error` | `str?` | on error | ClickHouse error message |
| `gold_match` | `bool?` | after CH run | Normalized SQL matches gold |
| `result_match` | `bool?` | after CH run | Result set matches gold exactly |
| `reward` | `float` | always | Continuous reward in [0.0, 1.0] |
| `done` | `bool` | always | Whether episode is complete |
| `metadata` | `dict` | always | Contains `task_id`, `difficulty`, `episode_id` |

## Reward Function

The reward is a **continuous decimal in [0.0, 1.0]**, dynamically computed from four weighted signals on every step:

```
reward = 0.15 * sql_token_similarity
       + 0.05 * required_terms_fraction
       + 0.10 * execution_success
       + 0.70 * result_set_similarity
```

| Signal | Weight | Range | Description |
|--------|--------|-------|-------------|
| `sql_token_similarity` | 0.15 | [0, 1] | Jaccard overlap of SQL tokens (identifiers, keywords, numbers) between candidate and gold |
| `required_terms_fraction` | 0.05 | [0, 1] | Fraction of task-mandated ClickHouse keywords present (e.g. `PREWHERE`, `avgMerge`) |
| `execution_success` | 0.10 | {0, 1} | 1 if query executes on ClickHouse without error, 0 otherwise |
| `result_set_similarity` | 0.70 | [0, 1] | Composite of row-count proximity (15%), order-independent row matching (35%), and cell-level overlap (50%) |

**Exact result match** (or normalized SQL match) overrides to **reward = 1.0** and ends the episode.

Example reward progression for a typical hard task:

| Agent attempt | reward | Breakdown |
|---------------|--------|-----------|
| Submits garbage `SELECT 1` | ~0.06 | Low token sim, no result overlap |
| Fixes table/columns but wrong aggregate | ~0.35 | Good token sim, executes, partial row match |
| Uses correct aggregate but wrong filter | ~0.62 | High token sim, most rows match |
| Correct query | 1.00 | Exact match |

## Tasks

12 tasks across three difficulty tiers. Hard tasks are designed to challenge frontier LLMs — queries run without errors but produce silently wrong results requiring deep ClickHouse engine knowledge.

### Easy (3 tasks)

| Task ID | Bug | Fix |
|---------|-----|-----|
| `column_typo` | Misspelled column name `evt_id` | Rename to `event_id` |
| `merge_partition_month` | Wrong month literal `202401` | Change to `202402` |
| `missing_limit` | Returns all rows instead of top 10 | Add `LIMIT 10` |

### Medium (4 tasks)

| Task ID | Bug | Fix |
|---------|-----|-----|
| `merge_order_by` | `any()` gives non-deterministic result | Replace with `max()` |
| `join_keys` | JOIN on display name (not unique) | JOIN on `customer_id` surrogate key |
| `aggregate_scope` | `avg()` mishandles NULL denominator | Use `avgIf(score, isNotNull(score))` |
| `wrong_date_func` | `toMonth()` ignores year, matches all Marches | Use `toYYYYMM() = 202403` |

### Hard (5 tasks)

| Task ID | Bug | Required CH Concept | Why It's Hard |
|---------|-----|---------------------|---------------|
| `prewhere_hint` | All predicates in WHERE; no partition pruning | `PREWHERE` | Non-standard SQL clause; must split predicates correctly |
| `collapsing_sign_sum` | Ignores `sign` column in CollapsingMergeTree | `sign` | Query looks correct, returns plausible but inflated numbers |
| `agg_merge_state` | Uses `avg()`/`count()` on AggregateFunction columns | `avgMerge`, `countMerge` | Cryptic type error; must know -State/-Merge combinator pairs |
| `argmax_null_collapse` | `argMax` picks NULLs from partial-update changelog rows | `argMaxIf`, `isNotNull` | Hardest task; requires understanding that argMax considers all rows including NULLs |
| `array_join_inflate` | ARRAY JOIN duplicates rows, inflating revenue sums | `length` | Must normalize by `length(tags)` to distribute revenue |

## Setup and Usage

### Requirements

- Python 3.10+
- Docker (for the bundled ClickHouse + FastAPI image)
- `openenv-core`, `clickhouse-connect` (see `pyproject.toml`)

### Docker (recommended)

```bash
docker build -t clickhouse_query_repair_env:latest .
docker run -p 8000:8000 clickhouse_query_repair_env:latest
```

Or use Docker Compose:

```bash
docker compose up --build
```

### Client example

```python
import asyncio
from clickhouse_query_repair import ClickhouseQueryRepairAction
from clickhouse_query_repair.client import ClickhouseQueryRepairEnv

async def main():
    env = ClickhouseQueryRepairEnv(base_url="http://127.0.0.1:8000")
    result = await env.reset()
    obs = result.observation
    print(f"Task: {obs.metadata['task_id']} ({obs.metadata['difficulty']})")
    print(f"Broken: {obs.broken_query}")

    # Each step gets real ClickHouse feedback and a continuous reward
    result = await env.step(
        ClickhouseQueryRepairAction(repaired_query="SELECT event_id, device FROM t LIMIT 5")
    )
    print(f"Reward: {result.reward:.4f}, Done: {result.done}")
    await env.close()

asyncio.run(main())
```

### Development

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
python -m pytest tests/
```

Run the API locally (requires ClickHouse at `CLICKHOUSE_HOST`:`CLICKHOUSE_HTTP_PORT`):

```bash
python -m clickhouse_query_repair.server.app --port 8000
```

### Inference script

`inference.py` drives the environment with an OpenAI-compatible LLM. Required env vars:

| Variable | Description |
|----------|-------------|
| `API_BASE_URL` | LLM API endpoint |
| `MODEL_NAME` | Model identifier |
| `HF_TOKEN` | Hugging Face / API key |

```bash
export API_BASE_URL=https://router.huggingface.co/v1
export MODEL_NAME=Qwen/Qwen2.5-72B-Instruct
export HF_TOKEN=hf_...
python inference.py
```

Stdout format: `[START]`, `[STEP]` per step, `[END]` with final score.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_HOST` | `127.0.0.1` | ClickHouse server host |
| `CLICKHOUSE_HTTP_PORT` | `8123` | ClickHouse HTTP port |
| `CHQR_MAX_STEPS_PER_EPISODE` | `8` | Max steps before forced termination (server) |
| `CHQR_MAX_STEPS` | `6` | Max steps per episode in `inference.py` |
| `CHQR_NUM_EPISODES` | `1` | Random episodes (each `reset()` samples a task **with replacement**); ignored if `CHQR_EVAL_ALL_TASKS` is set |
| `CHQR_EVAL_ALL_TASKS` | unset | Set to `1` to run **every** task JSON once (`reset(task_id=...)`) — full coverage |
| `INFERENCE_MAX_SECONDS` | `1140` | Wall-clock budget for inference (< 20 min) |

## Baseline Scores

Estimated scores with `Qwen/Qwen2.5-72B-Instruct` (6 steps per episode, temperature 0.35):

| Difficulty | Avg Reward | Solve Rate (1.0) |
|------------|------------|-------------------|
| Easy | ~0.90 | ~95% |
| Medium | ~0.65 | ~60% |
| Hard | ~0.35 | ~15% |
| **Overall** | **~0.55** | **~45%** |

Hard tasks like `argmax_null_collapse` and `collapsing_sign_sum` require ClickHouse-specific knowledge rarely seen in general SQL training data, keeping solve rates low even for large models.

## Submission Checklist

1. `outputs/` directory exists
2. `openenv.yaml` references `clickhouse_query_repair.server.app:app` on port 8000
3. `Dockerfile` builds successfully
4. `tasks/*.json` — at least 3 tasks with `difficulty` field; at least 1 easy, 1 medium, 1 hard
5. `inference.py` at repo root with `API_BASE_URL`, `MODEL_NAME`, `HF_TOKEN`, OpenAI client, `[START]`/`[STEP]`/`[END]` stdout
6. `openenv validate` passes locally
7. Runtime under 20 min on 2 vCPU / 8 GiB

Pre-submission validation:

```bash
python scripts/pre_submit_check.py          # structure checks
python scripts/pre_submit_check.py --docker  # also runs docker build
```
