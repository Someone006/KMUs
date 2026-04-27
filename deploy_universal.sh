#!/bin/bash
# Universal KMU Speed Mode Deployment
# Führe aus: bash ~/deploy_universal.sh

set -e

echo "=== Universal Speed Mode Deployment ==="
echo ""

# Finde das KMU-Projekt
echo "Suche KMU-Projekt..."
KMU_PATH=$(find ~ /opt /home -name "kmu_tool.py" -type f 2>/dev/null | head -1 | xargs dirname)

if [ -z "$KMU_PATH" ]; then
  echo "❌ KMU-Projekt nicht gefunden!"
  echo "Versuche manuelle Eingabe:"
  read -p "Gib KMU-Projektpfad ein: " KMU_PATH
fi

if [ ! -f "$KMU_PATH/kmu_tool.py" ]; then
  echo "❌ kmu_tool.py nicht gefunden in: $KMU_PATH"
  exit 1
fi

echo "✓ Gefunden: $KMU_PATH"
cd "$KMU_PATH"
echo ""

# Git Pull
echo "Aktualisiere Code..."
git pull origin main
echo "✓ Code aktualisiert"
echo ""

# Check if deploy script exists
if [ ! -f "deploy_speed_mode.sh" ]; then
  echo "❌ deploy_speed_mode.sh nicht gefunden!"
  exit 1
fi

# Deploy
echo "Starte Speed Mode Deployment..."
bash deploy_speed_mode.sh
echo ""
echo "✅ Deployment komplett!"
