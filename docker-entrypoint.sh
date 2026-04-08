#!/usr/bin/env bash
set -euo pipefail

CH_HTTP_PORT="${CLICKHOUSE_HTTP_PORT:-8123}"
CH_CONFIG="${CLICKHOUSE_CONFIG:-/etc/clickhouse-server/config.xml}"

# Stale PID files can make a new server exit immediately.
rm -f /var/run/clickhouse-server/clickhouse-server.pid 2>/dev/null || true

mkdir -p /var/lib/clickhouse /var/log/clickhouse-server /var/run/clickhouse-server
if id clickhouse &>/dev/null; then
  chown -R clickhouse:clickhouse /var/lib/clickhouse /var/log/clickhouse-server /var/run/clickhouse-server 2>/dev/null || true
fi

dump_ch_logs() {
  echo "--- tail clickhouse-server.err.log ---" >&2
  tail -n 80 /var/log/clickhouse-server/clickhouse-server.err.log 2>/dev/null || true
  echo "--- tail clickhouse-server.log ---" >&2
  tail -n 40 /var/log/clickhouse-server/clickhouse-server.log 2>/dev/null || true
}

# Start server (no --daemon=0: many builds reject it; default daemon fork exits the parent).
# Do not use $! for health: after fork the parent PID is gone. Wait only on HTTP /ping.
if id clickhouse &>/dev/null; then
  if command -v runuser >/dev/null 2>&1; then
    runuser -u clickhouse -- clickhouse-server --config-file="${CH_CONFIG}" &
  else
    su -s /bin/sh clickhouse -c "exec clickhouse-server --config-file=${CH_CONFIG}" &
  fi
else
  clickhouse-server --config-file="${CH_CONFIG}" &
fi

READY=0
for i in $(seq 1 120); do
  if curl -fsS "http://127.0.0.1:${CH_HTTP_PORT}/ping" >/dev/null 2>&1; then
    READY=1
    break
  fi
  sleep 1
done

if [[ "${READY}" -ne 1 ]]; then
  echo "ClickHouse did not respond on http://127.0.0.1:${CH_HTTP_PORT}/ping within 120s." >&2
  dump_ch_logs
  exit 1
fi

cd /app/env
exec uvicorn clickhouse_query_repair.server.app:app --host 0.0.0.0 --port 8000
