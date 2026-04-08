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

# ClickHouse query repair (OpenEnv)

This environment trains or evaluates agents that **repair broken ClickHouse SQL**. Each **episode** samples a task (JSON under `tasks/`), applies DDL/DML on reset, and exposes the broken query plus hints. **Intermediate steps never execute SQL on the server**—they only perform local checks. **Exactly one** server-side validation runs on the **terminal** step (`submit_final=True` or when the step budget is exhausted), comparing your `SELECT` to the reference query (text and/or result set).

## Requirements

- Python 3.10+
- `openenv-core` and `clickhouse-connect` (see `pyproject.toml`)
- Docker: image bundles **ClickHouse + FastAPI** (see `Dockerfile` and `docker-entrypoint.sh`)

## Quick start (client)

```python
import asyncio
from clickhouse_query_repair import ClickhouseQueryRepairAction, ClickhouseQueryRepairEnv

async def main():
    env = ClickhouseQueryRepairEnv(base_url="http://127.0.0.1:8000")
    r = await env.reset()
    obs = r.observation
    # Draft steps: reward stays 0; ClickHouse is not called
    r = await env.step(ClickhouseQueryRepairAction(repaired_query="SELECT 1", submit_final=False))
    # Final step: validates against ClickHouse once
    r = await env.step(ClickhouseQueryRepairAction(repaired_query=obs.broken_query, submit_final=True))
    await env.close()

asyncio.run(main())
```

Docker image (build from this directory):

```bash
docker build -t clickhouse_query_repair_env:latest .
docker run -p 8000:8000 clickhouse_query_repair_env:latest
```

Or `docker compose up --build` using `docker-compose.yml`.

## Action and observation fields

- **Action:** `repaired_query` (string), `submit_final` (bool). When `submit_final` is true—or the configured max steps is reached—the environment runs **terminal validation** (single round-trip to ClickHouse for the candidate `SELECT`).
- **Observation:** `broken_query`, `instruction`, `schema_hint`, `simulated_error`, `feedback_message`, `last_submitted_sql`, `step_index`, `max_steps`, `terminal`, and on terminal steps `execution_ok`, `clickhouse_error`, `gold_match`, `result_match`.

## Environment variables

| Variable | Meaning |
|----------|---------|
| `CLICKHOUSE_HOST` | Default `127.0.0.1` (in-container) |
| `CLICKHOUSE_HTTP_PORT` | HTTP interface (default `8123`) |
| `CHQR_MAX_STEPS_PER_EPISODE` | Forced terminal after this many steps (default `8`) |

## Tasks

JSON files in `tasks/` define `setup_sql`, `broken_query`, `gold_query`, and narrative fields. Examples cover **MergeTree** partitioning, aggregates, joins, and `PREWHERE`—all evaluated with the same once-per-episode rule.

## Inference / benchmark script

`inference.py` drives the env with an OpenAI-compatible chat model, prints `[START]` / `[STEP]` / `[END]` lines, and uses `IMAGE_NAME` for `from_docker_image` or `CHQR_BASE_URL` for a running server.

## Development

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
python -m pytest tests/
```

Run the API locally **with ClickHouse available** at `CLICKHOUSE_HOST` / `CLICKHOUSE_HTTP_PORT` (for example the same Docker image), then:

```bash
python -m clickhouse_query_repair.server.app --port 8000
```

## Deploying to Hugging Face Spaces

Use `openenv push` from this directory (see OpenEnv docs). Cold start may take longer while ClickHouse becomes ready inside the single container.
