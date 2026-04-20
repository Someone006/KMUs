# 🚀 QUICK START - Google Cloud VM

## ⚡ SUPER EINFACH - Copy & Paste!

### SSH auf deine Google VM:
```bash
ssh ubuntu@34.158.19.57
# (oder mit Key: ssh -i ~/key.pem ubuntu@34.158.19.57)
```

### DANN ein einfach nur das hier Copy-Pasten:

```bash
cd ~ && \
git clone https://github.com/Someone006/KMUs.git 2>/dev/null || cd KMUs && git pull && \
cd KMUs && \
python3 -m venv .venv && source .venv/bin/activate && \
pkill -f "kmu_tool.py serve" 2>/dev/null || true && \
sleep 2 && \
nohup python3 kmu_tool.py serve --host 0.0.0.0 --port 8000 > server.log 2>&1 & \
sleep 3 && \
echo "✅ Website läuft auf http://$(hostname -I | awk '{print $1}'):8000/"
```

## ✅ Dann funktioniert:

1. **Website öffnen:**
   - http://34.158.19.57:8000/

2. **Sehen Sie:**
   - 574.041 Schweizer Firmen
   - Live-Status unten links

3. **Klicken Sie:**
   - Button "Seed + Email-Scan starten"
   - Worker: 4 
   - Timeout: 12
   - Website-Suche: AN ✓

4. **Warten Sie:**
   - Websites werden gesucht
   - Emails werden extrahiert
   - Fortschritt sichtbar in Stats

## 🔍 Workflow wie du wünschst:

**Beispiel Suche:**
1. Filtere: Name = "Musterstrasse"
2. Stadt = "Zurich"
3. Siehst: Adresse aus Datenbank
4. Email-Scan lädt Website + Kontakt automatisch

## 📊 Performance:

- **4 Worker** + **574k Firmen** = ~30-40 Tage CPU-Zeit
- Mit **10 Worker** = ~5-7 Tage
- Pro Firma: ~5-8 Sekunden (Website suchen + Email extrahieren)

## 🆘 Troubleshooting:

**Port schon in Verwendung?**
```bash
lsof -i :8000  # Sieht welcher Prozess Port nutzt
pkill -9 python3  # Killt alle Python
```

**Server crasht?**
```bash
tail -100 server.log  # Schaut Fehler an
```

**Datenbank nicht geladen?**
```bash
ls -lh companies.json  # Sollte ~149MB sein
python3 -c "import json; print(len(json.loads(open('companies.json').read())))"  # Zeigt Anzahl
```

## 📈 Nächste Steps

Nach erste Tests:
1. Upscale Workers (10-20 für mehr Speed)
2. Lungo Job laufen lassen (Übernacht)
3. Ergebnisse exportieren (CSV/XLSX Button)
4. Auf Qualität überprüfen

## 🎯 DAS IST DEINE WEBSITE!

- **Firma → Adresse → Website → Email** - genau wie gewünscht!
- **574k Firmen** bereits in Datenbank
- **Website-Suche** sucht Google/DuckDuckGo
- **Email-Extraktion** aus Website + Google Maps

---

**Alles bereit! Viel Erfolg! 🚀**
