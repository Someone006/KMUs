# VM Deployment - Speed Mode aktivieren

## SSH-Befehl zur VM:
```bash
ssh ubuntu@34.158.19.57
```

## Auf der VM ausführen:

### 1. Repository aktualisieren
```bash
cd /home/ubuntu/kmu
git pull origin main
```

### 2. Batch-Loop neu starten mit neuem Code
```bash
pkill -f enrich_batch_loop.py
sleep 2
python enrich_batch_loop.py &
```

### 3. Status prüfen
```bash
curl http://127.0.0.1:8000/api/job | jq .
```

## Was wurde geändert (Speed Mode):

### ✅ Crawl-Tiefe erhöht
- `MAX_CRAWL_PAGES`: 6 → 12
- Kontaktseiten pro Website: 10 → 16
- Crawl-Links pro Seite: 4 → 8
- Crawl-Tiefe: Depth 1 → Depth 2

### ✅ Suchbegriffe erweitert
- Email-Suche: +4 neue Query-Varianten (email, info, impressum, support, office)
- Kandidaten pro Suche: 6 → 12
- Frühe Suchabbrüche entfernt

### ✅ Worker massiv erhöht
- `DEFAULT_WORKERS`: 3 → 1000
- `MAX_INTERNAL_WORKERS`: 64 → 1000
- `HTTP_CONCURRENCY_LIMIT`: 4 → 32
- Worker-Ceiling: min(4, MAX) → MAX

### ✅ Email-Scan-Limit erhöht
- Default: 800 → 1,000,000,000 (unbegrenzt)

### ✅ Host-Filterung gelockert
- Entfernte strenge .ch-Only-Filterung in Suchresultaten
- Akzeptiert jetzt mehr internationale Domains

---

## Erwartete Verbesserungen:
- Mehr E-Mails pro Firma gefunden
- Breitere Suchdeckung
- Schnellere Verarbeitung durch 1000 Worker
- Keine frühen Abbrüche mehr

## Rückgängig machen:
```bash
cd /home/ubuntu/kmu
git checkout HEAD~1 -- kmu_tool.py
pkill -f enrich_batch_loop.py
python enrich_batch_loop.py &
```
