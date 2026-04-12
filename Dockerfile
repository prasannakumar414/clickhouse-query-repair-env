# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Multi-stage build using openenv-base
# This Dockerfile is flexible and works for both:
# - In-repo environments (with local OpenEnv sources)
# - Standalone environments (with openenv from PyPI/Git)
# The build script (openenv build) handles context detection and sets appropriate build args.

ARG BASE_IMAGE=ghcr.io/meta-pytorch/openenv-base:latest
FROM ${BASE_IMAGE} AS builder

WORKDIR /app

# Ensure git is available (required for installing dependencies from VCS)
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

# Build argument to control whether we're building standalone or in-repo
ARG BUILD_MODE=in-repo
ARG ENV_NAME=clickhouse_query_repair

# Copy environment code (always at root of build context)
COPY . /app/env

# For in-repo builds, openenv is already vendored in the build context
# For standalone builds, openenv will be installed via pyproject.toml
WORKDIR /app/env

# Ensure uv is available (for local builds where base image lacks it)
RUN if ! command -v uv >/dev/null 2>&1; then \
        curl -LsSf https://astral.sh/uv/install.sh | sh && \
        mv /root/.local/bin/uv /usr/local/bin/uv && \
        mv /root/.local/bin/uvx /usr/local/bin/uvx; \
    fi
    
# Install dependencies using uv sync
# If uv.lock exists, use it; otherwise resolve on the fly
RUN --mount=type=cache,target=/root/.cache/uv \
    if [ -f uv.lock ]; then \
        uv sync --frozen --no-install-project --no-editable; \
    else \
        uv sync --no-install-project --no-editable; \
    fi

RUN --mount=type=cache,target=/root/.cache/uv \
    if [ -f uv.lock ]; then \
        uv sync --frozen --no-editable; \
    else \
        uv sync --no-editable; \
    fi

# Final runtime stage
FROM ${BASE_IMAGE}

WORKDIR /app

# Set PATH to use the virtual environment
ENV PATH="/app/.venv/bin:$PATH"

# Set PYTHONPATH so imports work correctly
ENV PYTHONPATH="/app/env:$PYTHONPATH"

#enables web interface
ENV ENABLE_WEB_INTERFACE=true

# Avoid apt/dpkg interactive prompts during image build (and noisy gpg pinentry).
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies and GPG key (batch gpg avoids "enter password" / pinentry hangs).
RUN apt-get update && \
    apt-get install -y --no-install-recommends apt-transport-https ca-certificates curl gnupg && \
    curl -fsSL 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' | \
    gpg --batch --no-tty --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg && \
    rm -rf /var/lib/apt/lists/*

# Add repository and install ClickHouse (redirect, not tee — no TTY assumptions).
RUN echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb stable main" \
        > /etc/apt/sources.list.d/clickhouse.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends clickhouse-server clickhouse-client && \
    rm -rf /var/lib/apt/lists/* && \
    # Pre-create dirs with correct ownership so entrypoint needs no CAP_CHOWN
    mkdir -p /var/lib/clickhouse \
             /var/log/clickhouse-server \
             /var/run/clickhouse-server \
             /var/lib/clickhouse/access && \
    chown -R clickhouse:clickhouse \
        /var/lib/clickhouse \
        /var/log/clickhouse-server \
        /var/run/clickhouse-server \
        /etc/clickhouse-server


# Lower ClickHouse memory/concurrency defaults (merged via config.d) so the container
# keeps RAM for uvicorn + OS; limits are min(3GiB, 45% RAM) to reduce OOM risk.
COPY docker/clickhouse-resource-limits.xml /etc/clickhouse-server/config.d/99-resource-limits.xml

RUN chown clickhouse:clickhouse /etc/clickhouse-server/config.d/99-resource-limits.xml

# Copy the virtual environment from builder
COPY --from=builder /app/env/.venv /app/.venv

# Copy the environment code
COPY --from=builder /app/env /app/env

# OpenEnv HTTP API + ClickHouse interfaces (map 8123/9000 only if you need host access).
EXPOSE 8000 8123 9000

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENV CLICKHOUSE_HOST=127.0.0.1 \
    CLICKHOUSE_HTTP_PORT=8123 \
    CHQR_MAX_STEPS_PER_EPISODE=8 \
    CLICKHOUSE_WATCHDOG_ENABLE=0

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
