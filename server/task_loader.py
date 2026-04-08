# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Load JSON task specs shipped under clickhouse_query_repair/tasks/."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


def _tasks_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "tasks"


def load_all_tasks() -> List[Dict[str, Any]]:
    directory = _tasks_dir()
    if not directory.is_dir():
        return []
    tasks: List[Dict[str, Any]] = []
    for path in sorted(directory.glob("*.json")):
        with path.open(encoding="utf-8") as handle:
            payload = json.load(handle)
        payload["_file"] = path.name
        tasks.append(payload)
    return tasks


def validate_task_schema(task: Dict[str, Any]) -> None:
    required = ("id", "instruction", "broken_query", "gold_query", "setup_sql")
    missing = [k for k in required if k not in task or not str(task[k]).strip()]
    if missing:
        raise ValueError(f"Task missing fields: {missing}")
