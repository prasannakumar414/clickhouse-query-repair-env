#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# Pre-submission checks aligned with OpenEnv / hackathon validation expectations.

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _fail(msg: str) -> None:
    print(f"FAIL: {msg}", file=sys.stderr)


def _ok(msg: str) -> None:
    print(f"OK: {msg}")


def check_outputs_dir() -> bool:
    out = ROOT / "outputs"
    if not out.is_dir():
        _fail(
            "Directory outputs/ is missing "
            "(create it; openenv recommends it for Spaces)."
        )
        return False
    _ok("outputs/ exists")
    return True


def check_openenv_yaml() -> bool:
    path = ROOT / "openenv.yaml"
    if not path.is_file():
        _fail("openenv.yaml missing")
        return False
    text = path.read_text(encoding="utf-8")
    need = (
        "spec_version:",
        "clickhouse_query_repair.server.app:app",
        "port:",
        "8000",
    )
    for n in need:
        if n not in text:
            _fail(f"openenv.yaml should contain {n!r}")
            return False
    _ok("openenv.yaml references app and port")
    return True


def check_tasks_min_three() -> bool:
    tasks_dir = ROOT / "tasks"
    if not tasks_dir.is_dir():
        _fail("tasks/ missing")
        return False
    paths = sorted(tasks_dir.glob("*.json"))
    if len(paths) < 3:
        _fail(f"Need at least 3 task JSON files, found {len(paths)}")
        return False
    required = (
        "id",
        "instruction",
        "broken_query",
        "gold_query",
        "setup_sql",
        "difficulty",
    )
    valid_diff = {"easy", "medium", "hard"}
    tiers: dict[str, int] = {"easy": 0, "medium": 0, "hard": 0}
    for p in paths:
        data = json.loads(p.read_text(encoding="utf-8"))
        missing = [k for k in required if k not in data]
        if missing:
            _fail(f"{p.name} missing keys: {missing}")
            return False
        d = data["difficulty"]
        if d not in valid_diff:
            _fail(f"{p.name} invalid difficulty: {d!r}")
            return False
        tiers[d] += 1
    missing_tiers = [t for t in valid_diff if tiers[t] == 0]
    if missing_tiers:
        _fail(f"No tasks for tier(s): {', '.join(sorted(missing_tiers))}")
        return False
    summary = ", ".join(f"{k}={v}" for k, v in sorted(tiers.items()))
    _ok(f"{len(paths)} tasks ({summary})")
    return True


def check_inference_module() -> bool:
    inf = ROOT / "inference.py"
    if not inf.is_file():
        _fail("inference.py must exist at repository root")
        return False
    text = inf.read_text(encoding="utf-8")
    tokens = (
        "API_BASE_URL",
        "MODEL_NAME",
        "HF_TOKEN",
        "OpenAI",
        "[START]",
        "[STEP]",
        "[END]",
    )
    for token in tokens:
        if token not in text:
            _fail(f"inference.py should mention {token!r} (submission rules)")
            return False
    _ok("inference.py present with required symbols")
    return True


def check_dockerfile() -> bool:
    df = ROOT / "Dockerfile"
    if not df.is_file():
        _fail("Dockerfile missing")
        return False
    text = df.read_text(encoding="utf-8")
    has_app = "clickhouse_query_repair.server.app" in text
    if not has_app and "uvicorn" not in text:
        _fail("Dockerfile should start the OpenEnv app (uvicorn + module path)")
        return False
    _ok("Dockerfile present")
    return True


def try_docker_build() -> None:
    print(
        "Running: docker build -t clickhouse_query_repair_env:pre_submit .",
        flush=True,
    )
    r = subprocess.run(
        ["docker", "build", "-t", "clickhouse_query_repair_env:pre_submit", "."],
        cwd=ROOT,
    )
    if r.returncode != 0:
        _fail("docker build failed (fix Dockerfile before submit)")
    else:
        _ok("docker build succeeded")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Pre-submission validation helper",
    )
    parser.add_argument(
        "--docker",
        action="store_true",
        help="Also run docker build (requires Docker daemon)",
    )
    args = parser.parse_args()

    all_ok = True
    all_ok &= check_outputs_dir()
    all_ok &= check_openenv_yaml()
    all_ok &= check_tasks_min_three()
    all_ok &= check_inference_module()
    all_ok &= check_dockerfile()

    print("\nManual steps (cannot be fully automated here):", flush=True)
    print("  - openenv validate", flush=True)
    print(
        "  - openenv validate --url https://<your-space>.hf.space",
        flush=True,
    )
    print("    (after deploy)", flush=True)
    print(
        "  - huggingface-cli login  # if push fails with SSL, see README",
        flush=True,
    )
    print(
        "  - export API_BASE_URL MODEL_NAME HF_TOKEN; python inference.py",
        flush=True,
    )

    if args.docker:
        try_docker_build()

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
