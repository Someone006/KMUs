#!/bin/bash
# Einfaches One-Line Deployment für Speed Mode
# Führe aus: bash ~/quick_deploy.sh

set -e

echo "=== Speed Mode Quick Deploy ==="
echo ""

# Finde Projekt
PROJECT=$(find ~ -name "kmu_tool.py" 2>/dev/null | head -1 | xargs dirname)

if [ -z "$PROJECT" ]; then
  echo "❌ kmu_tool.py nicht gefunden!"
  exit 1
fi

echo "✓ Projekt found: $PROJECT"
cd "$PROJECT"

# Pull
echo "Pull neuesten Code..."
git pull origin main

# Deploy
echo ""
echo "Starte Speed Mode..."
bash deploy_speed_mode.sh

# Status
echo ""
echo "=== Live-Status ==="
curl -s http://127.0.0.1:8000/api/job | python3 -m json.tool | grep -E '"running"|"processed"|"emails_found"|"websites_found"'

echo ""
echo "✅ Deployment komplett!"
