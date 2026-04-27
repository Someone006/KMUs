#!/bin/bash
# Deployment skript für Speed Mode auf der VM
# Führe aus: bash deploy_speed_mode.sh

set -e

echo "=== Speed Mode Deployment ==="
echo ""

# 1. Repository aktualisieren
echo "1. Repository aktualisieren..."
cd /home/ubuntu/kmu
git pull origin main
echo "   ✓ Git pull erfolgreich"
echo ""

# 2. Alten Batch-Loop stoppen
echo "2. Alten Batch-Loop stoppen..."
pkill -f enrich_batch_loop.py || true
sleep 2
echo "   ✓ Batch-Loop gestoppt"
echo ""

# 3. Neuen Batch-Loop starten
echo "3. Neuen Batch-Loop mit Speed Mode starten..."
nohup python enrich_batch_loop.py > batch_loop.log 2>&1 &
sleep 3
echo "   ✓ Batch-Loop gestartet (PID: $!)"
echo ""

# 4. Status prüfen
echo "4. Status prüfen..."
STATUS=$(curl -s http://127.0.0.1:8000/api/job)
echo "   API Response:"
echo "   $STATUS" | jq .
echo ""

echo "=== Deployment komplett! ==="
echo ""
echo "Speed Mode aktiviert mit:"
echo "  • 1000 Worker (statt 3)"
echo "  • 12x Crawl-Tiefe (statt 6)"
echo "  • 32x HTTP-Concurrency (statt 4)"
echo "  • 6 neue Suchterme"
echo ""
echo "Live-Status: curl http://127.0.0.1:8000/api/job | jq ."
