#!/usr/bin/env bash
set -euo pipefail

export PYTHONUNBUFFERED=1
export CLICKHOUSE_WATCHDOG_ENABLE=0

CH_HTTP_PORT="${CLICKHOUSE_HTTP_PORT:-8123}"
CH_CONFIG="${CLICKHOUSE_CONFIG:-/etc/clickhouse-server/config.xml}"

rm -f /var/run/clickhouse-server/clickhouse-server.pid 2>/dev/null || true

# Tail logs to stdout (dirs and files already exist with correct ownership)
tail -F \
  /var/log/clickhouse-server/clickhouse-server.log \
  /var/log/clickhouse-server/clickhouse-server.err.log 2>/dev/null &

# Remove the > redirects from all three branches — let CH manage its own files
# Try to switch to clickhouse user; fall back to running as current user (root)
# in sandboxed envs where setuid/setgid caps are dropped
CLICKHOUSE_CMD="clickhouse-server --config-file=${CH_CONFIG}"

if id clickhouse &>/dev/null && command -v runuser >/dev/null 2>&1; then
  if runuser -u clickhouse -- echo ok >/dev/null 2>&1; then
    runuser -u clickhouse -- ${CLICKHOUSE_CMD} &
  else
    echo "Warning: cannot switch to clickhouse user, running as $(whoami)" >&2
    ${CLICKHOUSE_CMD} &
  fi
else
  ${CLICKHOUSE_CMD} &
fi

echo "Waiting for ClickHouse on port ${CH_HTTP_PORT}..."
READY=0
for i in $(seq 1 120); do
  if curl -fsS "http://127.0.0.1:${CH_HTTP_PORT}/ping" >/dev/null 2>&1; then
    READY=1
    echo "ClickHouse ready after ${i}s."
    break
  fi
  sleep 1
done

if [[ "${READY}" -ne 1 ]]; then
  echo "ClickHouse did not respond within 120s." >&2
  tail -n 80 /var/log/clickhouse-server/clickhouse-server.err.log >&2 || true
  tail -n 40 /var/log/clickhouse-server/clickhouse-server.log >&2 || true
  exit 1
fi

cd /app/env
echo "Starting uvicorn server..."
exec uvicorn clickhouse_query_repair.server.app:app --host 0.0.0.0 --port 8000