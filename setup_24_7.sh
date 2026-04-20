#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -eq 0 ]]; then
  echo "Bitte nicht direkt als root ausfuehren. Mit normalem Benutzer starten."
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="/opt/kmus"
WORKER_SERVICE_NAME="kmu-worker.service"
WORKER_SERVICE_FILE="/etc/systemd/system/${WORKER_SERVICE_NAME}"
WEB_SERVICE_NAME="kmu-web.service"
WEB_SERVICE_FILE="/etc/systemd/system/${WEB_SERVICE_NAME}"

if ! command -v systemctl >/dev/null 2>&1; then
  echo "systemd nicht gefunden. Dieses Setup braucht einen Linux-Server mit systemd."
  exit 1
fi

sudo mkdir -p "$APP_DIR"
sudo rsync -a --delete --exclude ".git" --exclude ".venv" "$ROOT_DIR/" "$APP_DIR/"

if [[ ! -d "$APP_DIR/.venv" ]]; then
  sudo python3 -m venv "$APP_DIR/.venv"
fi

sudo "$APP_DIR/.venv/bin/pip" install --upgrade pip >/dev/null

sudo chmod +x "$APP_DIR/worker_loop.sh" "$APP_DIR/daemon.sh" "$APP_DIR/setup_24_7.sh"

sudo install -m 0644 "$APP_DIR/kmu-worker.service" "$WORKER_SERVICE_FILE"
sudo install -m 0644 "$APP_DIR/kmu-web.service" "$WEB_SERVICE_FILE"
sudo systemctl daemon-reload
sudo systemctl enable "$WORKER_SERVICE_NAME" "$WEB_SERVICE_NAME"
sudo systemctl restart "$WORKER_SERVICE_NAME" "$WEB_SERVICE_NAME"

echo "24/7 Worker + Web installiert und gestartet."
echo "Status Worker: sudo systemctl status ${WORKER_SERVICE_NAME}"
echo "Status Web: sudo systemctl status ${WEB_SERVICE_NAME}"
echo "Logs Worker: sudo journalctl -u ${WORKER_SERVICE_NAME} -f"
echo "Logs Web: sudo journalctl -u ${WEB_SERVICE_NAME} -f"
