#!/bin/bash
# Deployment-Script für Google Cloud VM
# Verwendung: ./deploy_to_google_vm.sh [VM_IP] [SSH_USER] [SSH_KEY_PATH]
# Beispiel: ./deploy_to_google_vm.sh 34.158.19.57 ubuntu ~/.ssh/google-vm-key

set -e

VM_IP="${1:-34.158.19.57}"
SSH_USER="${2:-ubuntu}"
SSH_KEY="${3:-$HOME/.ssh/id_rsa}"

echo "🚀 Deploying to Google VM: $VM_IP"
echo "User: $SSH_USER"
echo "Key: $SSH_KEY"
echo ""

# Überprüfe SSH-Zugang
echo "📡 Überprüfe SSH-Zugang..."
if ! ssh -i "$SSH_KEY" -o ConnectTimeout=5 "$SSH_USER@$VM_IP" "echo '✓ SSH OK'" 2>/dev/null; then
    echo "❌ SSH-Zugang fehlgeschlagen!"
    echo "Bitte überprüfe:"
    echo "  1. VM läuft und ist erreichbar"
    echo "  2. SSH-Key richtig: $SSH_KEY"
    echo "  3. SSH-Benutzer richtig: $SSH_USER"
    exit 1
fi

echo "✓ SSH OK"
echo ""

# Kopiere companies.json zur VM
echo "📦 Kopiere 574k Firmen-Datenbank (149.5MB)..."
scp -i "$SSH_KEY" "companies.json" "$SSH_USER@$VM_IP:~/KMUs/companies.json" 2>&1 | grep -E "(transferred|companies\.json)" || true

# Starte Website-Server auf VM
echo ""
echo "🌐 Starte Website-Server auf VM..."
ssh -i "$SSH_KEY" "$SSH_USER@$VM_IP" << 'REMOTE_SCRIPT'
cd ~/KMUs
pkill -f "kmu_tool.py serve" || true
sleep 2
nohup python3 kmu_tool.py serve --host 0.0.0.0 --port 8000 > server.log 2>&1 &
sleep 3
echo "✓ Server gestartet"
REMOTE_SCRIPT

echo ""
echo "✅ Deployment abgeschlossen!"
echo ""
echo "🌐 Website verfügbar unter:"
echo "   http://$VM_IP:8000/"
echo ""
echo "📊 Was zu tun ist:"
echo "   1. Im Browser: http://$VM_IP:8000/ öffnen"
echo "   2. Button 'Seed + Email-Scan starten' klicken"
echo "   3. Worker: 4-8 | Timeout: 12 | discover: AN"
echo "   4. Warten Sie auf Websites + Emails..."
echo ""
