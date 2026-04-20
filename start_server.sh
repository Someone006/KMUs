#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

source .venv/bin/activate

PID_FILE=".kmu-server.pid"
LOG_FILE="server.log"
PORT="8000"

find_listening_pid() {
  netstat -tlnp 2>/dev/null | awk -v p=":${PORT}$" '$4 ~ p {print $7}' | head -n1 | cut -d/ -f1
}

# Wenn bereits ein Server auf 8000 läuft, nichts neu starten.
LISTEN_PID="$(find_listening_pid || true)"
if [[ -n "$LISTEN_PID" ]] && kill -0 "$LISTEN_PID" 2>/dev/null; then
  echo "$LISTEN_PID" > "$PID_FILE"
  echo "✓ KMU Server läuft bereits (PID $LISTEN_PID)"
  echo "  URL: http://localhost:8000"
  echo "  Logs: $LOG_FILE"
  exit 0
fi

# Veraltete PID ggf. aufräumen
if [[ -f "$PID_FILE" ]]; then
  OLD_PID="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$OLD_PID" ]] && ! kill -0 "$OLD_PID" 2>/dev/null; then
    rm -f "$PID_FILE"
  fi
fi

# Server im Hintergrund starten
nohup python3 kmu_tool.py serve --host 0.0.0.0 --port 8000 >> "$LOG_FILE" 2>&1 &
PID=$!
echo "$PID" > "$PID_FILE"

echo "✓ KMU Server gestartet (PID $PID)"
echo "  URL: http://localhost:8000"
echo "  Logs: $LOG_FILE"
