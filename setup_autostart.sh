#!/usr/bin/env bash
# Richte Autostart für Watchdog beim Codespace-Start ein

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STARTUP_SCRIPT="$HOME/.startup-kmu-services.sh"

cat > "$STARTUP_SCRIPT" << 'STARTUP'
#!/usr/bin/env bash
# KMU Services Autostart

cd /workspaces/KMUs 2>/dev/null || exit 0

# Aktiviere Python venv
if [[ -f .venv/bin/activate ]]; then
  source .venv/bin/activate
fi

# Starte Watchdog (falls nicht bereits laufen)
if [[ ! -f .watchdog.pid ]] || ! kill -0 "$(cat .watchdog.pid 2>/dev/null)" 2>/dev/null; then
  nohup ./keep_server_alive.sh > watchdog.log 2>&1 &
  echo $! > .watchdog.pid
  echo "[$(date)] KMU Watchdog gestartet"
fi
STARTUP

chmod +x "$STARTUP_SCRIPT"

# Für VS Code Remote/Codespace crontab
if command -v crontab &>/dev/null; then
  CURRENT_CRON="$(crontab -l 2>/dev/null || true)"
  if ! printf '%s\n' "$CURRENT_CRON" | grep -Fq "@reboot $STARTUP_SCRIPT"; then
    (printf '%s\n' "$CURRENT_CRON"; echo "@reboot $STARTUP_SCRIPT") | crontab -
  fi
  echo "✓ Crontab-Eintrag hinzugefügt"
else
  echo "✓ Startup-Script erstellt: $STARTUP_SCRIPT"
  echo "  Manuell starten: $STARTUP_SCRIPT"
fi
