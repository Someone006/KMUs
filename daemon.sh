#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

PID_FILE=".kmu-worker.pid"
LOG_FILE="kmu-worker.log"

start_worker() {
  if [[ -f "$PID_FILE" ]]; then
    PID="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "${PID}" ]] && kill -0 "$PID" 2>/dev/null; then
      echo "Worker läuft bereits (PID $PID)."
      return 0
    fi
    rm -f "$PID_FILE"
  fi

  nohup "$ROOT_DIR/worker_loop.sh" >> "$LOG_FILE" 2>&1 &
  PID=$!
  echo "$PID" > "$PID_FILE"
  echo "Worker gestartet (PID $PID). Log: $LOG_FILE"
}

stop_worker() {
  if [[ ! -f "$PID_FILE" ]]; then
    echo "Kein PID-File gefunden. Worker scheint nicht zu laufen."
    return 0
  fi

  PID="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -z "${PID}" ]]; then
    rm -f "$PID_FILE"
    echo "PID-File war leer und wurde entfernt."
    return 0
  fi

  if kill -0 "$PID" 2>/dev/null; then
    kill "$PID" 2>/dev/null || true
    for _ in {1..20}; do
      if ! kill -0 "$PID" 2>/dev/null; then
        break
      fi
      sleep 0.2
    done
    if kill -0 "$PID" 2>/dev/null; then
      kill -9 "$PID" 2>/dev/null || true
    fi
    echo "Worker gestoppt (PID $PID)."
  else
    echo "Prozess $PID lief nicht mehr."
  fi

  rm -f "$PID_FILE"
}

status_worker() {
  if [[ ! -f "$PID_FILE" ]]; then
    echo "Worker läuft nicht."
    return 1
  fi

  PID="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "${PID}" ]] && kill -0 "$PID" 2>/dev/null; then
    echo "Worker läuft (PID $PID)."
    return 0
  fi

  echo "Worker läuft nicht (stale PID-File)."
  return 1
}

logs_worker() {
  tail -n 100 -f "$LOG_FILE"
}

case "${1:-}" in
  start)
    start_worker
    ;;
  stop)
    stop_worker
    ;;
  restart)
    stop_worker || true
    start_worker
    ;;
  status)
    status_worker
    ;;
  logs)
    logs_worker
    ;;
  *)
    echo "Nutzung: $0 {start|stop|restart|status|logs}"
    exit 1
    ;;
esac
