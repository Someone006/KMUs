# Deployment auf Google Cloud VM

## Status (20.04.2026)

✅ **Lokale Codespace-Website funktioniert** mit 574.041 Firmen
✅ **API funktioniert**: `/api/search`, `/api/job`
✅ **HTML/JavaScript-UI lädt** korrekt

## Daten verfügbar

- **companies.json**: 574.041 Schweizer Firmen (149.5MB)
  - Quelle: `artifacts/export_compact_live.csv`
  - Contains: Name, Stadt, teilweise rechtliche Form
  - Brauchen: Website-Suche + Email-Extraktion (via Google/DuckDuckGo)

## Google VM Setup

```bash
# 1. SSH zur VM
ssh -i ~/.ssh/id_rsa ubuntu@34.158.19.57

# 2. Repository klonen
cd ~
git clone https://github.com/Someone006/KMUs.git
cd KMUs

# 3. Python-Umgebung einrichten
python3 -m venv .venv
source .venv/bin/activate

# 4. Daten kopieren
cp /pfad/zu/companies.json .

# 5. Web-Server starten
python3 kmu_tool.py serve --host 0.0.0.0 --port 8000

# 6. Browser öffnen
# http://34.158.19.57:8000/
```

## Erforderliche Schritte im Web-UI

1. **Seed + Email-Scan**: Lädt Websites und Emails für Firmen
2. **Nur Seed**: Lädt aus Zefix (optional)
3. **Worker**: 4-8 parallel (Intel vCPU abhängig)
4. **Timeout**: 10-12 Sekunden pro Website

## Wichtige Filter in UI

- **Nach Name suchen**: `Firma muster`
- **Nach Stadt**: `Zurich`
- **Mit Website**: `ja`
- **Mit Email**: `ja`
- **Nach Kanton**: `ZH`

## Workflow (wie vom Benutzer gewünscht)

1. **Firma suchen**: Filter nutzen (z.B. Name "Musterstrasse")
2. **Adresse überprüfen**: "Musterstrasse 1, 8000 Zürich"
3. **Website suchen**: Die Website-Suche findet automatisch Website + Kontakt
4. **Email eintragen**: Aus Website oder Google Maps

## Performance

- 574.041 Firmen mit 4 Worker: ~30-60 Worker-Tage für komplette Email-Suche
- Empfehlung: Starten Sie mit 50-100 Firmen, testen dann upscale

## Troubleshooting

**Problem**: `Address already in use`
```bash
pkill -9 python3  # Alle Python-Prozesse killen
python3 kmu_tool.py serve --host 0.0.0.0 --port 8000
```

**Problem**: Keine Emails gefunden
- Prüfen Sie Worker-Count (sollte 2-4 sein)
- Timeout erhöhen (12-15 Sekunden)
- Website-Suche aktiviert? (Checkbox "Website-Suche an")

## Nächste Steps

1. Git-Push der neuesten `companies.json`
2. Auf Google VM deployen
3. Tests mit kleiner Slice (10-50 Firmen)
4. Upscale auf größere Jobs (1000+ Firmen)

## Datei-Größen

- `companies.json`: 149.5 MB (574k Firmen)
- Server Memory: ~500-700MB benötigt (beachte VM-Größe)
