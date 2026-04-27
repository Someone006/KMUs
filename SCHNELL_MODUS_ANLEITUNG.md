# ⚡ SCHNELL-MODUS - AKTIVIERUNG

## 🎯 Was wurde geändert?

Alle Änderungen sind **bereits zu GitHub gepusht**:
- ✅ `enrich_batch_loop.py`: 1000 Worker, `discover=true`, Timeout 20s
- ✅ `kmu_tool.py`: Maximale E-Mail-Erfassung, .ch bevorzugt aber auch .com/.org akzeptiert
- ✅ Website-Validierung gelockert: Akzeptiert fast alle .ch Domains

## 🚀 SO AKTIVIERST DU DEN SCHNELL-MODUS AUF DER VM

### Option 1: SSH (empfohlen, wenn du SSH-Zugriff hast)

```bash
# SSH zur VM
ssh ubuntu@34.158.19.57

# Wechsel zum KMU-Verzeichnis
cd /opt/kmus

# Hole neuesten Code von GitHub
git pull origin main

# Stoppe den alten Batch Loop (falls läuft)
pkill -f "enrich_batch_loop.py"

# Starte den neuen Batch Loop mit Schnell-Modus
nohup python3 enrich_batch_loop.py > batch_loop.log 2>&1 &

# Beobachte den Fortschritt
tail -f batch_loop.log
```

### Option 2: Weboberfläche (wenn du nur Webzugriff hast)

1. **Website öffnen:** http://34.158.19.57:8000/
2. **Warte bis der Status sichtbar ist** (unten links)
3. **Klicke auf "Stop" Button** um alte Loop zu stoppen
4. **Aktualisiere die Seite** (F5)
5. **Der neue Batch Loop startet automatisch** nach kurzer Zeit

## ✅ FORTSCHRITT ÜBERPRÜFEN

### Live-Status ansehen (via curl):
```bash
curl -s http://34.158.19.57:8000/api/job | python3 -m json.tool
```

### Erwartete Ausgabe:
```json
{
  "running": true,
  "type": "enrich",
  "processed": <zahl>,
  "accepted": <zahl>,
  "emails_found": <SOLLTE JETZT STEIGEN>,
  "websites_found": <SOLLTE JETZT STEIGEN>,
  "workers": 1000,
  "total": 100
}
```

### Logs auf der VM live ansehen (SSH):
```bash
ssh ubuntu@34.158.19.57

# Standard-Output des Batch Loop
tail -f ~/batch_loop.log

# Oder systemd logs wenn über Service
sudo journalctl -u kmu-worker.service -f
```

## 🎯 ZIELE IM SCHNELL-MODUS

- **1000 Worker** statt 10 = Maximale Parallelität
- **discover=true** = Website-Erkennung aktiv
- **Timeout 20s** = Mehr Zeit für Website-Crawling
- **Lockere E-Mail-Validierung** = Akzeptiert auch nicht-perfekte E-Mails
- **Priorität auf .ch** = Schweizer E-Mails bevorzugt, aber auch .com/.org OK

## ⚠️ FEHLERBEHEBUNG

### "emails_found" bleibt 0?

1. **Check ob discover=true ist:**
   ```bash
   grep "discover" ~/batch_loop.log
   ```

2. **Check ob Worker laufen:**
   ```bash
   ps aux | grep enrich_batch_loop
   ```

3. **Erhöhe timeout noch mehr (falls nötig):**
   - Ändere "REQUEST_TIMEOUT" in `enrich_batch_loop.py` auf 30s

### Worker sind blockiert?

```bash
# Alle KMU-Prozesse killen
pkill -f "kmu_tool.py"
pkill -f "enrich_batch_loop.py"

# Neu starten
cd /opt/kmus
nohup python3 enrich_batch_loop.py > batch_loop.log 2>&1 &
```

## 💡 TIPPS

- Der erste Batch braucht mehr Zeit (10-20 Minuten mit 1000 Writern und Website-Crawling)
- Nach dem ersten Batch sollten **mails_found > 0** sein und steigen
- Websites brauchen zeitaufwendiges Crawling, daher sind 20s Timeout wichtig
- Wenn viele Worker aber keine E-Mails gefunden = Check HTML-Extraktion, nicht Worker-Problem

## ✨ ERFOLGSZEICHEN

✅ Du solltest sehen:
- `processed` Zahl steigt
- `websites_found` > 0 
- `emails_found` > 0 und steigt
- `workers` = 1000
- Keine Fehler im Log

Viel Erfolg! 🚀
