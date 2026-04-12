#!/usr/bin/env bash
set -euo pipefail

CH_HTTP_PORT="${CLICKHOUSE_HTTP_PORT:-8123}"
CH_CONFIG="${CLICKHOUSE_CONFIG:-/etc/clickhouse-server/config.xml}"

export PYTHONUNBUFFERED=1   # ← fix #2: forces Python/uvicorn to flush immediately

rm -f /var/run/clickhouse-server/clickhouse-server.pid 2>/dev/null || true
mkdir -p /var/lib/clickhouse /var/log/clickhouse-server /var/run/clickhouse-server

if id clickhouse &>/dev/null; then
  chown -R clickhouse:clickhouse \
    /var/lib/clickhouse /var/log/clickhouse-server /var/run/clickhouse-server 2>/dev/null || true
fi

dump_ch_logs() {
  echo "--- tail clickhouse-server.err.log ---" >&2
  tail -n 80 /var/log/clickhouse-server/clickhouse-server.err.log 2>/dev/null || true
  echo "--- tail clickhouse-server.log ---" >&2
  tail -n 40 /var/log/clickhouse-server/clickhouse-server.log 2>/dev/null || true
}

# ← fix #1: stream ClickHouse logs to docker stdout in the background
stream_ch_logs() {
  mkdir -p /var/log/clickhouse-server
  touch /var/log/clickhouse-server/clickhouse-server.log \
        /var/log/clickhouse-server/clickhouse-server.err.log

  # ← ADD THIS: give clickhouse user ownership before tail locks the files
  if id clickhouse &>/dev/null; then
    chown clickhouse:clickhouse \
      /var/log/clickhouse-server/clickhouse-server.log \
      /var/log/clickhouse-server/clickhouse-server.err.log
  fi

  tail -F \
    /var/log/clickhouse-server/clickhouse-server.log \
    /var/log/clickhouse-server/clickhouse-server.err.log 2>/dev/null &
}

stream_ch_logs

if id clickhouse &>/dev/null; then
  if command -v runuser >/dev/null 2>&1; then
    runuser -u clickhouse -- clickhouse-server --config-file="${CH_CONFIG}" \
      >/var/log/clickhouse-server/clickhouse-server.log \
      2>/var/log/clickhouse-server/clickhouse-server.err.log &
  else
    su -s /bin/sh clickhouse -c \
      "exec clickhouse-server --config-file=${CH_CONFIG} \
       >>/var/log/clickhouse-server/clickhouse-server.log \
       2>>/var/log/clickhouse-server/clickhouse-server.err.log" &
  fi
else
  clickhouse-server --config-file="${CH_CONFIG}" \
    >/var/log/clickhouse-server/clickhouse-server.log \
    2>/var/log/clickhouse-server/clickhouse-server.err.log &
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
  dump_ch_logs
  exit 1
fi

cd /app/env
echo "Starting uvicorn server..."
exec uvicorn clickhouse_query_repair.server.app:app --host 0.0.0.0 --port 8000