#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

DATA_FILE="${DATA_FILE:-companies.json}"
LIMIT="${LIMIT:-10000}"
EMAIL_SCAN="${EMAIL_SCAN:-1200}"
TIMEOUT="${TIMEOUT:-12}"
SLEEP_SECONDS="${SLEEP_SECONDS:-120}"
SEED_SOURCES="${SEED_SOURCES:-zefix,search.ch,tel.search.ch,local.ch,local.ch.ch,moneyhouse,swissguide}"
DISCOVER_WEBSITES="${DISCOVER_WEBSITES:-1}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if [[ "$DISCOVER_WEBSITES" == "1" ]]; then
  DISCOVER_FLAG=(--discover-websites)
else
  DISCOVER_FLAG=()
fi

while true; do
  date '+[%Y-%m-%d %H:%M:%S] worker cycle start' >&2

  if ! "$PYTHON_BIN" -u kmu_tool.py bootstrap \
    --out "$DATA_FILE" \
    --limit "$LIMIT" \
    --email-scan "$EMAIL_SCAN" \
    --timeout "$TIMEOUT" \
    --seed-sources "$SEED_SOURCES" \
    "${DISCOVER_FLAG[@]}"; then
    date '+[%Y-%m-%d %H:%M:%S] worker cycle failed, retry after sleep' >&2
  fi

  date '+[%Y-%m-%d %H:%M:%S] worker cycle done' >&2
  sleep "$SLEEP_SECONDS"
done
