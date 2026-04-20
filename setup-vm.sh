#!/bin/bash
# setup-vm.sh - Direkt auf Google VM ausführen
# Verwendung: bash setup-vm.sh

set -e

echo "🚀 KMU-Tool Setup auf Google Cloud VM"
echo "======================================"
echo ""

# 1. Repository clonen
echo "📦 1. Repository vorbereiten..."
if [ ! -d "KMUs" ]; then
    git clone https://github.com/Someone006/KMUs.git
    cd KMUs
else
    cd KMUs
    git pull
fi

# 2. Python-Umgebung
echo "🐍 2. Python-Umgebung einrichten..."
if [ ! -d ".venv" ]; then
    python3 -m venv .venv
fi
source .venv/bin/activate

# 3. Website-Server starten
echo "🌐 3. Website-Server starten..."
pkill -f "kmu_tool.py serve" || true
sleep 2

nohup python3 kmu_tool.py serve --host 0.0.0.0 --port 8000 > server.log 2>&1 &
sleep 3

# 4. Überprüfe ob Server läuft
if curl -s http://127.0.0.1:8000/api/job > /dev/null 2>&1; then
    echo "✅ Server läuft!"
else
    echo "❌ Server-Start fehlgeschlagen!"
    tail -20 server.log
    exit 1
fi

echo ""
echo "======================================"
echo "✅ SETUP ABGESCHLOSSEN"
echo "======================================"
echo ""
echo "🌐 Website verfügbar unter:"
echo "   http://$(hostname -I | awk '{print $1}'):8000/"
echo ""
echo "📊 Datenbank:"
companies_count=$(python3 -c "import json; print(len(json.loads(open('companies.json').read())))" 2>/dev/null || echo "?")
echo "   Firmen geladen: $companies_count"
echo ""
echo "🎯 Nächste Schritte:"
echo "   1. Öffne http://<IP>:8000/ im Browser"
echo "   2. Klicke 'Seed + Email-Scan starten'"
echo "   3. Wähle Worker: 4-8 und Timeout: 12"
echo "   4. Website-Suche: AN"
echo ""
echo "📋 Logs überprüfen:"
echo "   tail -f server.log"
echo ""
