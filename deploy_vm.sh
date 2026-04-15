#!/bin/bash
# Deploy script für GCP VM - Führe dieses Script auf der VM aus
set -e

echo "=== VM Fresh Deploy ==="
echo

# 1. Alte Repos löschen
echo "1. Ältere Daten bereinigen..."
cd /opt/kmus
git status || echo "Kein git repo, ok"
rm -rf .git node_modules __pycache__ *.pyc .pytest_cache
rm -rf companies*.json tmp-*.json visited_seed_pages.json
echo "✓ Alte Dateien gelöscht"
echo

# 2. Git neu initialisieren
echo "2. Git neu initialisieren..."
cd /opt/kmus
git init
git remote add origin https://github.com/Someone006/KMUs.git
git fetch origin main
git reset --hard origin/main
echo "✓ Git aktualisiert"
echo

# 3. Leere companies.json für schnellen Start
echo "3. Initialisiere companies.json..."
echo '[]' > /opt/kmus/companies.json
chmod 666 /opt/kmus/companies.json
echo "✓ companies.json erstellt"
echo

# 4. Services neustarten
echo "4. Services neustarten..."
sudo systemctl restart kmu-worker.service kmu-web.service
sleep 2
sudo systemctl status kmu-web.service --no-pager | head -3
echo "✓ Services gestartet"
echo

# 5. Tests
echo "5. Lokale Tests..."
curl -s http://127.0.0.1:8000 | head -3
echo
echo "✓ Webserver antwortet"
echo

echo "=== DEPLOY ERFOLGREICH ==="
echo
echo "Website: http://$(curl -s ifconfig.me):8000"
echo "Status: $(sudo systemctl is-active kmu-web.service)"
echo
echo "Nächste Schritte:"
echo "1. Browser öffnen: http://$(curl -s ifconfig.me):8000"
echo "2. Auf 'Unbegrenzte Suche starten' klicken"
echo "3. Beobachte die Statistiken"
echo "4. Stop mit 'Job stoppen' Button"
