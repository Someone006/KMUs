#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

DATA_FILE="companies.json"
LIMIT=10000
EMAIL_SCAN=800
DISCOVER_WEBSITES=1
HOST="127.0.0.1"
PORT=8000
ONLY_SEED=0
ONLY_SERVE=0

print_help() {
  cat <<'EOF'
KMUs Schnellstart

Nutzung:
  ./start.sh [optionen]

Optionen:
  --data FILE            Ziel-Datei fuer Daten (default: companies.json)
  --limit N              Anzahl Firmen fuer Zefix-Seed (default: 10000)
  --email-scan N         Anzahl Firmen fuer E-Mail-Anreicherung (default: 800)
  --no-discover          Keine Website-Suche fuer Firmen ohne URL
  --host HOST            Webserver Host (default: 127.0.0.1)
  --port PORT            Webserver Port (default: 8000)
  --only-seed            Nur Seed + Enrichment, keinen Webserver starten
  --only-serve           Nur Webserver starten (nutzt existierende Daten)
  -h, --help             Hilfe anzeigen

Beispiele:
  ./start.sh
  ./start.sh --limit 20000 --email-scan 2000
  ./start.sh --only-seed --limit 10000 --email-scan 0
  ./start.sh --only-serve --data companies.json --port 8080
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --data)
      DATA_FILE="$2"
      shift 2
      ;;
    --limit)
      LIMIT="$2"
      shift 2
      ;;
    --email-scan)
      EMAIL_SCAN="$2"
      shift 2
      ;;
    --no-discover)
      DISCOVER_WEBSITES=0
      shift
      ;;
    --host)
      HOST="$2"
      shift 2
      ;;
    --port)
      PORT="$2"
      shift 2
      ;;
    --only-seed)
      ONLY_SEED=1
      shift
      ;;
    --only-serve)
      ONLY_SERVE=1
      shift
      ;;
    -h|--help)
      print_help
      exit 0
      ;;
    *)
      echo "Unbekannte Option: $1"
      echo "Nutze --help fuer Hilfe."
      exit 1
      ;;
  esac
done

if [[ "$ONLY_SEED" -eq 1 && "$ONLY_SERVE" -eq 1 ]]; then
  echo "--only-seed und --only-serve koennen nicht zusammen verwendet werden."
  exit 1
fi

if [[ "$ONLY_SERVE" -ne 1 ]]; then
  DISCOVER_ARG=()
  if [[ "$DISCOVER_WEBSITES" -eq 1 ]]; then
    DISCOVER_ARG=(--discover-websites)
  fi

  echo "[1/2] Seed + Enrichment starten..."
  python3 -u kmu_tool.py bootstrap \
    --out "$DATA_FILE" \
    --limit "$LIMIT" \
    --email-scan "$EMAIL_SCAN" \
    --timeout 10 \
    "${DISCOVER_ARG[@]}"

  if [[ "$ONLY_SEED" -eq 1 ]]; then
    echo "Fertig. Daten liegen in: $DATA_FILE"
    exit 0
  fi
fi

echo "[2/2] Webserver starten..."
python3 -u kmu_tool.py serve --data "$DATA_FILE" --host "$HOST" --port "$PORT"
