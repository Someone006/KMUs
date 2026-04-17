# KMUs

Ein einfaches Tool für Schweizer Firmen-Recherche mit Filtern und öffentlicher E-Mail-Erkennung.

- Seed aus Zefix, search.ch und local.ch (10k+ Firmen)
- Filtern nach Name, Rechtsform, Mitarbeiterzahl, Ort, Kanton
- Zusätzliche Filter: hat Website, hat E-Mail
- Öffentliche E-Mails von Firmenwebseiten sammeln
- Export als CSV oder XLSX
- Weboberfläche mit denselben Filtern
- Gespeichert werden nur Firmen mit öffentlicher E-Mail; Schweizer Websites und E-Mails werden bevorzugt

## Voraussetzungen

- Python 3.11+ (3.12 getestet)

## Dauerlauf Im Cloud-Host (offline-sicher)

Wenn dein Laptop/Browser offline geht, soll der Crawl trotzdem weiterlaufen.
Nutze dafuer den Worker-Daemon auf einem laufenden Host (VPS, VM, Codespace):

```bash
cd /workspaces/KMUs
chmod +x daemon.sh worker_loop.sh

# Startet endlosen Hintergrundlauf (Seed + Email-Suche in Zyklen)
./daemon.sh start

# Status pruefen
./daemon.sh status

# Live-Logs ansehen
./daemon.sh logs

# Stoppen
./daemon.sh stop
```

Der Worker laeuft in Endlosschleife und schreibt nach jedem Zyklus in dieselbe Datei weiter.
Damit findet das System weiter Websites und E-Mails, auch wenn du offline bist.

Wichtige Umgebungsvariablen (optional):

```bash
DATA_FILE=companies.json
LIMIT=10000
EMAIL_SCAN=1200
TIMEOUT=12
SLEEP_SECONDS=120
SEED_SOURCES=zefix,search.ch,tel.search.ch,local.ch,local.ch.ch,moneyhouse,swissguide
DISCOVER_WEBSITES=1
```

Beispiel mit eigenen Werten:

```bash
EMAIL_SCAN=2500 SLEEP_SECONDS=60 SEED_SOURCES=zefix,local.ch,moneyhouse ./daemon.sh restart
```

## Echter 24/7 Betrieb (empfohlen)

Fuer echten Dauerbetrieb ohne Codespace-Suspend nutze einen Linux VPS mit systemd.

Im Repo ist alles vorbereitet:

- setup_24_7.sh: installiert den Worker nach /opt/kmus und aktiviert systemd
- kmu-worker.service: Auto-Restart Service
- worker_loop.sh: Endlosschleife fuer kontinuierliche Seed+Email-Suche

Schritte auf dem VPS:

1. Repo auf den VPS klonen
2. Script ausfuehrbar machen
3. Setup starten

Beispiel:

chmod +x setup_24_7.sh
./setup_24_7.sh

Danach:

- Status: sudo systemctl status kmu-worker.service
- Logs live: sudo journalctl -u kmu-worker.service -f
- Neustart: sudo systemctl restart kmu-worker.service

Wenn der Server rebootet, startet der Worker automatisch wieder.

## Schritt-fuer-Schritt

### 1. Projektordner oeffnen

```bash
cd /workspaces/KMUs
```

### 1b. Ein-Kommando-Start (empfohlen)

Startet den Webserver mit der neuen Klick-Oberflaeche:

```bash
./start.sh --only-serve --data companies.json --port 8000
```

Dann im Browser oeffnen:

- http://127.0.0.1:8000

In der Oberflaeche kannst du dann per Buttons starten:

- Seed + Email-Scan starten
- Nur Seed starten
- Nur Email-Scan starten

Alle Jobs laufen im Hintergrund, Status wird oben angezeigt.
Zusätzlich siehst du dort live Zahlen wie:

- wie viele Firmen schon gesucht wurden
- wie viele Firmen gespeichert wurden
- wie viele Websites gefunden wurden
- wie viele E-Mails gefunden wurden

Im Job-Panel kannst du auch die Zahl paralleler Worker einstellen. Mehr Worker bedeuten meist schnellere Ergebnisse bei gleicher Prüfqualität.
Du kannst im Panel jetzt zusätzlich pro Klick auswählen, welche Seed-Quellen genutzt werden (z. B. nur Zefix + local.ch).
Das Tool bevorzugt Schweizer .ch-Seiten und Schweizer E-Mail-Domains, blockiert aber gezielt problematische AuslandstLDs wie .de und .nl.
Wenn auf der Firmenwebsite keine E-Mail gefunden wird, prüft es zusätzlich lokale Schweizer Verzeichniseinträge und Suchtreffer als Fallback.

### 2. Schnellstart mit einem Befehl

Dieser Befehl laedt Firmen aus Zefix, search.ch und local.ch und speichert sie als `companies.json`.
Optional scannt er auch eine erste Teilmenge fuer E-Mails.
Der Lauf schreibt gefundene Firmen direkt in die Datei, damit du später erneut starten kannst, ohne die bisherigen Treffer zu verlieren.

```bash
python3 kmu_tool.py bootstrap --out companies.json --limit 10000 --email-scan 800 --discover-websites

# Nur bestimmte Seed-Quellen nutzen
python3 kmu_tool.py bootstrap --out companies.json --limit 10000 --email-scan 800 --discover-websites --seed-sources zefix,local.ch,moneyhouse
```

Wenn du zuerst nur Firmen laden willst (ohne E-Mail-Scan):

```bash
python3 kmu_tool.py bootstrap --out companies.json --limit 10000 --email-scan 0
```

### 3. Im Terminal filtern

Beispiele:

```bash
# Nach Kanton und Rechtsform
python3 kmu_tool.py search --data companies.json --canton ZH --legal-form GMBH

# Nur Firmen mit E-Mail
python3 kmu_tool.py search --data companies.json --has-email ja

# Nur Firmen ohne Website
python3 kmu_tool.py search --data companies.json --has-website nein

# Nach Mitarbeiterbereich
python3 kmu_tool.py search --data companies.json --employees 10-50
```

### 4. Zusaetzliche E-Mail-Anreicherung laufen lassen

Wenn du mehr E-Mails sammeln willst:

```bash
python3 kmu_tool.py enrich --data companies.json --discover-websites --limit 2000 --out companies.json
```

Hinweis: Das kann je nach Anzahl Firmen laenger dauern.

### 5. Exportieren

```bash
# CSV
python3 kmu_tool.py export --data companies.json --format csv --out export.csv --has-email ja

# XLSX
python3 kmu_tool.py export --data companies.json --format xlsx --out export.xlsx --canton BE
```

### 6. Weboberflaeche starten

```bash
python3 kmu_tool.py serve --data companies.json --host 127.0.0.1 --port 8000
```

Dann im Browser oeffnen:

- http://127.0.0.1:8000

Wenn `companies.json` noch nicht existiert, erstellt `serve` automatisch einen Seed mit mindestens 10.000 Firmen aus mehreren Quellen.

## Verfuegbare Filter

- `name`: Firmenname enthaelt
- `legal_form`: z. B. `AG`, `GMBH`, `Genossenschaft`
- `employees`: z. B. `1-10`, `10-50`, `100+`
- `city`: Ort
- `canton`: Kanton, z. B. `ZH`, `BE`, `GE`
- `website`: Teilstring in URL
- `has_email`: `ja` oder `nein`
- `has_website`: `ja` oder `nein`

## Alternative Datenquelle: CSV-Import

Die Importdatei kann diese Spalten enthalten:

- `name` oder `firma`
- `legal_form` oder `rechtsform`
- `employees` oder `mitarbeiter`
- `city` oder `ort`
- `canton` oder `kanton`
- `website` oder `url`

Import-Befehl:

```bash
python3 kmu_tool.py import-csv companies.example.csv --out companies.json
```

## Was das Tool macht

Das Tool baut Firmen standardmässig zuerst aus Zefix auf, übernimmt Zefix-Adressen in bestehende Datensätze und prüft danach Websites sowie Kontaktseiten auf sichtbare E-Mail-Adressen. Es greift nicht auf versteckte oder personenbezogene Quellen zu.

Wenn auf der Firmenwebsite keine passende E-Mail gefunden wird, nutzt das Tool zusätzlich Suchmaschinen-Treffer, inklusive Google als letzten Prüfpass, um offizielle Websites und Kontaktseiten zu finden. Dabei werden nur Firmenadressen akzeptiert; Verzeichnis- oder Serviceadressen werden verworfen.

Zusätzlich validiert das Tool Treffer streng:

- Website muss zur Firma passen (Name/Domain) ODER Name+Ort muessen klar auf der Seite vorkommen.
- E-Mail wird akzeptiert, wenn Domain zur Website passt.
- Falls Domain anders ist: Ausnahme nur bei .ch-Website mit .ch-E-Mail und klarem Name+Ort-Match; sonst wird verworfen.
- Typische Fremdseiten und Beispiel-/Wegwerf-Domains werden verworfen.