#!/usr/bin/env bash
# Permanentes Server-Watchdog-Script
# Startet Server neu wenn er abstürzt

set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

PID_FILE=".kmu-server.pid"
LOG_FILE="server.log"
PORT="8000"

find_listening_pid() {
  netstat -tlnp 2>/dev/null | awk -v p=":${PORT}$" '$4 ~ p {print $7}' | head -n1 | cut -d/ -f1
}

pid_is_alive() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

cleanup() {
  if [[ -f "$PID_FILE" ]]; then
    PID="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$PID" ]]; then
      kill "$PID" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
  fi
  exit 0
}

trap cleanup EXIT INT TERM

while true; do
  LISTEN_PID="$(find_listening_pid || true)"

  # Alte PID aufräumen falls existent
  if [[ -f "$PID_FILE" ]]; then
    OLD_PID="$(cat "$PID_FILE" 2>/dev/null || true)"
    if ! pid_is_alive "$OLD_PID"; then
      rm -f "$PID_FILE"
    fi
  fi
  
  # Falls bereits ein Server auf dem Port läuft, übernimm dessen PID und starte nicht neu.
  if pid_is_alive "$LISTEN_PID"; then
    echo "$LISTEN_PID" > "$PID_FILE"
    sleep 10
    continue
  fi
  
  # Server starten
  CURRENT_PID="$(cat "$PID_FILE" 2>/dev/null || true)"
  if ! pid_is_alive "$CURRENT_PID"; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Server starten..." >&2
    source .venv/bin/activate
    nohup python3 kmu_tool.py serve --host 0.0.0.0 --port 8000 >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    sleep 2
  fi
  
  sleep 10
done
