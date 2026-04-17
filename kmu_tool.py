#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import csv
import html
import json
import os
import re
import socket
import ssl
import sys
import tempfile
import threading
import time
import traceback
import zipfile
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from datetime import date
from dataclasses import asdict, dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable, Iterable
from urllib.parse import parse_qs, quote_plus, urlencode, urljoin, urlparse
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)
MAILTO_RE = re.compile(r"mailto:([^\'\"\s>]+)", re.IGNORECASE)
CANTON_ALIASES = {
    "ag": "AG",
    "aargau": "AG",
    "ar": "AR",
    "appenzell ausserrhoden": "AR",
    "ai": "AI",
    "appenzell innerrhoden": "AI",
    "bl": "BL",
    "basel-landschaft": "BL",
    "baselland": "BL",
    "bs": "BS",
    "basel-stadt": "BS",
    "be": "BE",
    "bern": "BE",
    "fr": "FR",
    "freiburg": "FR",
    "ge": "GE",
    "genf": "GE",
    "gl": "GL",
    "glarus": "GL",
    "gr": "GR",
    "graubünden": "GR",
    "ju": "JU",
    "jura": "JU",
    "lu": "LU",
    "luzern": "LU",
    "ne": "NE",
    "neuenburg": "NE",
    "nw": "NW",
    "obwalden": "NW",
    "ow": "OW",
    "niedwalden": "NW",
    "sg": "SG",
    "st. gallen": "SG",
    "sh": "SH",
    "schaffhausen": "SH",
    "so": "SO",
    "solothurn": "SO",
    "sz": "SZ",
    "schwyz": "SZ",
    "tg": "TG",
    "thurgau": "TG",
    "ti": "TI",
    "tessin": "TI",
    "ur": "UR",
    "uri": "UR",
    "vd": "VD",
    "waadt": "VD",
    "vs": "VS",
    "wallis": "VS",
    "zg": "ZG",
    "zug": "ZG",
    "zh": "ZH",
    "zürich": "ZH",
    "zurich": "ZH",
}
KEYWORD_HINTS = ("kontakt", "contact", "impressum", "imprint", "about", "unternehmen", "company", "team")
USER_AGENT = "KMU-Mail-Finder/1.0"
MAX_CRAWL_PAGES = 6
MAX_PAGE_BYTES = 1_000_000
ZEFIX_QUERY_ENDPOINT = "https://lindas.admin.ch/query"
ZEFIX_PAGE_SIZE = 500
ZEFIX_DEFAULT_LIMIT = 10000
ZEFIX_MAX_ATTEMPTS_PER_PAGE = 12
ZEFIX_POST_TIMEOUT_SEC = 45
ZEFIX_POST_RETRIES = 2
DEFAULT_WORKERS = 1000
MAX_INTERNAL_WORKERS = 1000
KMU_MIN_EMPLOYEES = 10
KMU_MAX_EMPLOYEES: int | None = None
BFS_SNAPSHOT_URL = "https://www.agvchapp.bfs.admin.ch/api/communes/snapshot?date={date}"
DDG_SEARCH_URL = "https://duckduckgo.com/html/?q={query}"
BING_SEARCH_URL = "https://www.bing.com/search?q={query}"
GOOGLE_SEARCH_URL = "https://www.google.com/search?hl=de&num=10&q={query}"
LOCALCH_SEARCH_URL = "https://www.local.ch/en/q?what={query}&where={where}"
LOCALCH_SEED_URL = "https://www.local.ch/en/q?what={term}&where={where}"
MONEYHOUSE_SEARCH_URL = "https://www.moneyhouse.ch/de/search?q={query}"
SWISSGUIDE_SEARCH_URL = "https://www.swissguide.ch/suche?query={query}"
SEARCHCH_SEED_URL = "https://www.search.ch/tel/?all={term}&alltl=1"
TEL_SEARCHCH_SEED_URL = "https://tel.search.ch/?was={term}&wo={where}"
LOCALCH_SEED_URL = "https://www.local.ch/en/q?what={term}&where={where}"
MONEYHOUSE_SEARCH_URL = "https://www.moneyhouse.ch/de/search?q={query}"
SWISSGUIDE_SEARCH_URL = "https://www.swissguide.ch/suche?query={query}"
SEED_PAGES_MAX = 4
VISITED_PAGES_FILE = str(Path(__file__).resolve().with_name("visited_seed_pages.json"))
SEARCH_DISCOVERY_DISABLED = False
SEARCH_BLOCKLIST = (
    "duckduckgo.com",
    "zefix.admin.ch",
    "wikipedia.org",
    "linkedin.com",
    "facebook.com",
    "instagram.com",
    "github.com",
    "reddit.com",
    "youtube.com",
    "google.com",
    "googleusercontent.com",
    "local.ch",
    "search.ch",
    "moneyhouse.ch",
    "swissguide.ch",
    "business-monitor.ch",
    "allbiz.ch",
    "deviantart.com",
    "bluewin.ch",
    "gmail.com",
    "outlook.com",
    "hotmail.com",
    "yahoo.com",
    "gmx.ch",
    "gmx.net",
)
PREFERRED_TLDS = {"ch", "swiss"}
BLOCKED_WEBSITE_TLDS = {"de", "nl"}
EMAIL_DOMAIN_BLOCKLIST = {
    "example.com",
    "example.org",
    "example.net",
    "exemple.fr",
    "test.com",
    "invalid",
    "localhost",
    "mailinator.com",
    "tempmail.com",
    "guerrillamail.com",
    "local.ch",
    "localsearch.ch",
    "search.ch",
    "localcities.ch",
    "sentry.io",
    "sentry.wixpress.com",
    "sentry-next.wixpress.com",
}
FALSE_EMAIL_DOMAIN_SUFFIXES = {
    "png",
    "jpg",
    "jpeg",
    "webp",
    "gif",
    "svg",
    "ico",
    "pdf",
    "xml",
    "json",
    "js",
    "css",
    "woff",
    "woff2",
    "ttf",
    "otf",
}
FREE_EMAIL_DOMAINS = {
    "gmail.com",
    "googlemail.com",
    "outlook.com",
    "hotmail.com",
    "live.com",
    "yahoo.com",
    "yahoo.de",
    "yahoo.fr",
    "icloud.com",
    "proton.me",
    "protonmail.com",
    "bluewin.ch",
    "gmx.ch",
    "gmx.net",
    "hispeed.ch",
}
LEGAL_FORM_HINTS = (
    " ag",
    " gmbh",
    " sa",
    " sarl",
    " sàrl",
    " sagl",
    " genossenschaft",
    " stiftung",
    " verein",
)
JOB_STATE = {
    "running": False,
    "type": "",
    "started_at": 0.0,
    "finished_at": 0.0,
    "last_error": "",
    "last_message": "",
    "phase": "idle",
    "total": 0,
    "processed": 0,
    "accepted": 0,
    "websites_found": 0,
    "emails_found": 0,
    "skipped": 0,
    "errors": 0,
    "current": "",
    "stopped": False,
}
JOB_LOCK = threading.Lock()
JOB_STOP_EVENT = threading.Event()
LEGAL_FORM_KEYWORDS: dict[str, tuple[str, ...]] = {
    "AG": ("ag", "aktiengesellschaft", "societe anonyme", "société anonyme"),
    "GMBH": ("gmbh", "gesellschaft mit beschrankter haftung", "gesellschaft mit beschränkter haftung", "sarl", "sàrl", "srl"),
    "GENOSSENSCHAFT": ("genossenschaft", "cooperative", "coop"),
    "EINZELUNTERNEHMEN": ("einzelunternehmen", "raison individuelle", "ditta individuale"),
    "VEREIN": ("verein", "association"),
    "STIFTUNG": ("stiftung", "fondation", "fondazione"),
}
SEARCHCH_SEED_BASE_TERMS = (
    "ag",
    "gmbh",
    "treuhand",
    "immobilien",
    "restaurant",
    "hotel",
    "garage",
    "elektro",
    "sanitaer",
    "heizung",
    "arzt",
    "zahnarzt",
    "physio",
    "logistik",
    "transport",
    "bau",
    "architektur",
    "consulting",
)
SEARCHCH_SEED_CANTONS = (
    "ZH",
    "BE",
    "LU",
    "UR",
    "SZ",
    "OW",
    "NW",
    "GL",
    "ZG",
    "FR",
    "SO",
    "BS",
    "BL",
    "SH",
    "AR",
    "AI",
    "SG",
    "GR",
    "AG",
    "TG",
    "TI",
    "VD",
    "VS",
    "NE",
    "GE",
    "JU",
)
LOCALCH_SEED_WHERE_TERMS = (
    "zurich",
    "bern",
    "basel",
    "luzern",
    "st-gallen",
    "lausanne",
    "geneva",
    "winterthur",
    "lugano",
    "thun",
)
WEB_SEED_BASE_TERMS = SEARCHCH_SEED_BASE_TERMS
DEFAULT_SEED_SOURCES = (
    "zefix",
    "search.ch",
    "tel.search.ch",
    "local.ch",
    "local.ch.ch",
    "moneyhouse",
    "swissguide",
)
SEED_SOURCE_LABELS = {
    "zefix": "Zefix",
    "search.ch": "search.ch",
    "tel.search.ch": "tel.search.ch",
    "local.ch": "local.ch",
    "local.ch.ch": "local.ch (CH)",
    "moneyhouse": "moneyhouse",
    "swissguide": "swissguide",
}


def parse_seed_sources(raw_sources: str | Iterable[str] | None, default_on_empty: bool = True) -> list[str]:
    if raw_sources is None:
        return list(DEFAULT_SEED_SOURCES) if default_on_empty else []
    if isinstance(raw_sources, str):
        candidates = raw_sources.split(",")
    else:
        candidates = [str(value) for value in raw_sources]

    selected: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = candidate.strip().lower()
        if key in SEED_SOURCE_LABELS and key not in seen:
            selected.append(key)
            seen.add(key)
    return selected or (list(DEFAULT_SEED_SOURCES) if default_on_empty else [])


def format_seed_sources(seed_sources: str | Iterable[str] | None) -> str:
    selected = parse_seed_sources(seed_sources)
    return ", ".join(SEED_SOURCE_LABELS.get(source, source) for source in selected)


@dataclass
class Company:
    name: str
    legal_form: str = ""
    employee_min: int | None = None
    employee_max: int | None = None
    city: str = ""
    canton: str = ""
    website: str = ""
    emails: list[str] = field(default_factory=list)
    source: str = ""
    uid: str = ""

    @property
    def employee_label(self) -> str:
        if self.employee_min is None and self.employee_max is None:
            return ""
        if self.employee_min is not None and self.employee_max is not None and self.employee_min == self.employee_max:
            return str(self.employee_min)
        if self.employee_min is not None and self.employee_max is not None:
            return f"{self.employee_min}-{self.employee_max}"
        if self.employee_min is not None:
            return f"{self.employee_min}+"
        return f"bis {self.employee_max}"


class CompanyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Company):
            data = asdict(obj)
            data["employee_label"] = obj.employee_label
            return data
        return super().default(obj)


def company_to_row(company: Company) -> dict[str, str]:
    return {
        "name": company.name,
        "legal_form": company.legal_form,
        "employee_min": "" if company.employee_min is None else str(company.employee_min),
        "employee_max": "" if company.employee_max is None else str(company.employee_max),
        "employee_label": company.employee_label,
        "city": company.city,
        "canton": company.canton,
        "website": company.website,
        "emails": ", ".join(company.emails),
        "source": company.source,
        "uid": company.uid,
    }


def company_to_export_row(company: Company) -> dict[str, str]:
    row = company_to_row(company)
    row["emails"] = ", ".join(company.emails)
    return row


def company_to_export_rows(company: Company) -> list[dict[str, str]]:
    """
    Erzeugt Export-Zeilen mit genau einer E-Mail pro Zeile.
    Eine Firma mit mehreren E-Mails erscheint mehrfach.
    """
    base = company_to_row(company)
    emails = sorted({normalize_text(email).lower() for email in company.emails if normalize_text(email)})
    if not emails:
        row = dict(base)
        row["emails"] = ""
        return [row]
    rows: list[dict[str, str]] = []
    for email in emails:
        row = dict(base)
        row["emails"] = email
        rows.append(row)
    return rows


def clone_company(company: Company) -> Company:
    return Company(
        name=company.name,
        legal_form=company.legal_form,
        employee_min=company.employee_min,
        employee_max=company.employee_max,
        city=company.city,
        canton=company.canton,
        website=company.website,
        emails=list(company.emails),
        source=company.source,
        uid=company.uid,
    )


def resolve_worker_count(requested_workers: int | None, default_workers: int = DEFAULT_WORKERS) -> int:
    if requested_workers is not None and requested_workers > 0:
        return max(1, min(requested_workers, MAX_INTERNAL_WORKERS))
    cpu_count = os.cpu_count() or 4
    return max(2, min(default_workers, cpu_count * 2, MAX_INTERNAL_WORKERS))


def job_update(**updates: object) -> None:
    with JOB_LOCK:
        JOB_STATE.update(updates)


def job_increment(**deltas: int) -> None:
    with JOB_LOCK:
        for key, value in deltas.items():
            JOB_STATE[key] = int(JOB_STATE.get(key, 0)) + value


def job_set_current(text: str) -> None:
    with JOB_LOCK:
        JOB_STATE["current"] = text


def job_should_stop() -> bool:
    return JOB_STOP_EVENT.is_set()


def request_job_stop() -> bool:
    with JOB_LOCK:
        if not JOB_STATE.get("running"):
            return False
        JOB_STATE["stopped"] = True
    JOB_STOP_EVENT.set()
    return True


def make_job_summary() -> str:
    with JOB_LOCK:
        total = int(JOB_STATE.get("total", 0))
        processed = int(JOB_STATE.get("processed", 0))
        accepted = int(JOB_STATE.get("accepted", 0))
        websites = int(JOB_STATE.get("websites_found", 0))
        emails = int(JOB_STATE.get("emails_found", 0))
        skipped = int(JOB_STATE.get("skipped", 0))
        errors = int(JOB_STATE.get("errors", 0))
        current = str(JOB_STATE.get("current", ""))
    return f"{processed}/{total} geprüft | {accepted} gespeichert | {websites} Websites | {emails} E-Mails | {skipped} übersprungen | {errors} Fehler | {current}"


def safe_get_text(
    url: str,
    timeout: int = 20,
    retries: int = 3,
    tolerate_statuses: set[int] | None = None,
) -> str:
    headers = {"User-Agent": USER_AGENT}
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        request = Request(url, headers=headers)
        try:
            with urlopen(request, timeout=timeout) as response:
                return response.read().decode("utf-8", errors="ignore")
        except HTTPError as exc:
            last_error = exc
            if tolerate_statuses and exc.code in tolerate_statuses:
                return ""
            if exc.code not in {429, 500, 502, 503, 504} or attempt == retries:
                raise
        except (URLError, TimeoutError, ssl.SSLError) as exc:
            last_error = exc
            if attempt == retries:
                raise
        time.sleep(min(2 ** (attempt - 1), 8))
    if last_error:
        raise last_error
    raise RuntimeError("Unerwarteter Fehler beim GET-Request")


def safe_post_csv(url: str, data: dict[str, str], timeout: int = 8, retries: int = 1) -> str:
    body = urlencode(data).encode()
    headers = {"User-Agent": USER_AGENT, "Content-Type": "application/x-www-form-urlencoded"}
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        request = Request(url, data=body, headers=headers)
        try:
            with urlopen(request, timeout=timeout) as response:
                return response.read().decode("utf-8", errors="ignore")
        except HTTPError as exc:
            last_error = exc
            # Retry common transient upstream errors from SPARQL endpoint.
            if exc.code not in {429, 500, 502, 503, 504} or attempt == retries:
                raise
        except (URLError, TimeoutError, ssl.SSLError) as exc:
            last_error = exc
            if attempt == retries:
                raise
        time.sleep(min(2 ** (attempt - 1), 8))
    if last_error:
        raise last_error
    raise RuntimeError("Unerwarteter Fehler beim POST-Request")


def extract_bfs_code(value: str) -> int | None:
    match = re.search(r"(\d+)$", value or "")
    return int(match.group(1)) if match else None


def snapshot_date_candidates() -> list[str]:
    today = date.today().strftime("%d-%m-%Y")
    return [today, "31-12-2025"]


def load_bfs_canton_map(cache_path: Path = Path(".cache/bfs-communes-snapshot.csv")) -> dict[int, str]:
    if cache_path.exists():
        raw = cache_path.read_text(encoding="utf-8")
    else:
        raw = ""
        last_error: Exception | None = None
        for candidate in snapshot_date_candidates():
            try:
                raw = safe_get_text(BFS_SNAPSHOT_URL.format(date=candidate))
                if raw.strip():
                    break
            except Exception as exc:  # pragma: no cover - network fallback
                last_error = exc
        if not raw.strip():
            raise RuntimeError("BFS-Snapshot konnte nicht geladen werden") from last_error
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(raw, encoding="utf-8")

    rows = list(csv.DictReader(raw.splitlines()))
    nodes_by_historical: dict[int, dict[str, str]] = {}
    current_municipalities: dict[int, dict[str, str]] = {}
    for row in rows:
        historical_code = extract_bfs_code(row.get("HistoricalCode", ""))
        bfs_code = extract_bfs_code(row.get("BfsCode", ""))
        if historical_code is not None:
            nodes_by_historical[historical_code] = row
        if bfs_code is not None and row.get("Level") == "3" and not (row.get("ValidTo") or "").strip():
            current_municipalities[bfs_code] = row

    canton_map: dict[int, str] = {}

    def canton_for(code: int) -> str:
        current = current_municipalities.get(code)
        visited: set[int] = set()
        while current:
            current_code = extract_bfs_code(current.get("HistoricalCode", ""))
            if current_code is None or current_code in visited:
                return ""
            visited.add(current_code)
            if current.get("Level") == "1":
                return (current.get("ShortName") or current.get("Name") or "").strip().upper()
            parent_code = extract_bfs_code(current.get("Parent", ""))
            if parent_code is None:
                return ""
            current = nodes_by_historical.get(parent_code)
        return ""

    for code, row in current_municipalities.items():
        if row.get("Level") == "3":
            canton = canton_for(code)
            if canton:
                canton_map[code] = canton

    return canton_map


# ==================== Visited Pages Tracking ====================


def load_visited_pages() -> dict[str, set[str]]:
    """
    Lädt die Datei mit besuchten Suchbegriffen.
    Format: {"search.ch": ["ag", "gmbh", "consulting"], "local.ch": [...], ...}
    """
    if not os.path.exists(VISITED_PAGES_FILE):
        return {}
    try:
        with open(VISITED_PAGES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return {k: set(v) for k, v in data.items()}
    except (json.JSONDecodeError, IOError):
        return {}


def save_visited_pages(visited: dict[str, set[str]]) -> None:
    """Speichert die Datei mit besuchten Suchbegriffen."""
    try:
        data = {k: sorted(list(v)) for k, v in visited.items()}
        with open(VISITED_PAGES_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except IOError as e:
        print(f"Warnung: visited_pages nicht gespeichert: {e}", file=sys.stderr)


def mark_term_visited(source: str, term: str, visited: dict[str, set[str]]) -> None:
    """Markiert einen Suchbegriff als besucht."""
    if source not in visited:
        visited[source] = set()
    visited[source].add(term.lower())


def get_unvisited_terms(
    source: str, all_terms: list[str], visited: dict[str, set[str]]
) -> list[str]:
    """
    Gibt nur die Suchbegriffe zurück, die noch nicht besucht wurden.
    """
    if source not in visited:
        return all_terms
    return [t for t in all_terms if t.lower() not in visited[source]]


def fetch_zefix_rows(
    limit: int = ZEFIX_DEFAULT_LIMIT,
    page_size: int = ZEFIX_PAGE_SIZE,
    max_duration_sec: int | None = None,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    offset = 0
    consecutive_page_failures = 0
    started_at = time.monotonic()
    query_template = """PREFIX schema: <http://schema.org/>
PREFIX admin: <https://schema.ld.admin.ch/>
SELECT ?company_uri (SAMPLE(?name) AS ?name) (SAMPLE(?company_type) AS ?company_type) (SAMPLE(?muni_id) AS ?municipality_uri) (SAMPLE(?municipality) AS ?municipality) (SAMPLE(?adresse) AS ?adresse) (SAMPLE(?locality) AS ?locality)
WHERE {{
  ?company_uri a admin:ZefixOrganisation ;
    schema:name ?name ;
    admin:municipality ?muni_id ;
    schema:additionalType ?type_id ;
    schema:address ?adr .
  ?muni_id schema:name ?municipality .
  ?type_id schema:name ?company_type .
  ?adr schema:streetAddress ?adresse ;
      schema:addressLocality ?locality .
}}
GROUP BY ?company_uri
ORDER BY ?company_uri
LIMIT {page_size}
OFFSET {offset}"""

    while len(rows) < limit:
        if job_should_stop():
            break
        if max_duration_sec is not None and time.monotonic() - started_at >= max_duration_sec:
            if on_progress is not None:
                on_progress(min(len(rows), limit), limit, "Zefix Zeitbudget erreicht")
            break
        page_number = offset // page_size + 1
        if on_progress is not None:
            on_progress(min(len(rows), limit), limit, f"Zefix Seite {page_number} lädt...")
        query = query_template.format(page_size=page_size, offset=offset)
        if job_should_stop():
            return rows[:limit]
        if max_duration_sec is not None and time.monotonic() - started_at >= max_duration_sec:
            if on_progress is not None:
                on_progress(min(len(rows), limit), limit, "Zefix Zeitbudget erreicht")
            return rows[:limit]
        try:
            csv_text = safe_post_csv(
                ZEFIX_QUERY_ENDPOINT,
                {"query": query},
                timeout=ZEFIX_POST_TIMEOUT_SEC,
                retries=ZEFIX_POST_RETRIES,
            )
        except Exception:
            consecutive_page_failures += 1
            retry_wait = min(30, 2 ** min(consecutive_page_failures - 1, 5))
            if on_progress is not None:
                on_progress(
                    min(len(rows), limit),
                    limit,
                    f"Zefix Seite {page_number} fehlgeschlagen (Versuch {consecutive_page_failures}), neuer Versuch in {retry_wait}s",
                )
            if consecutive_page_failures >= ZEFIX_MAX_ATTEMPTS_PER_PAGE:
                if on_progress is not None:
                    on_progress(
                        min(len(rows), limit),
                        limit,
                        f"Zefix Seite {page_number} übersprungen nach {consecutive_page_failures} Fehlversuchen",
                    )
                consecutive_page_failures = 0
                offset += page_size
                continue
            time.sleep(retry_wait)
            continue
        if not csv_text:
            consecutive_page_failures += 1
            retry_wait = min(30, 2 ** min(consecutive_page_failures - 1, 5))
            if on_progress is not None:
                on_progress(
                    min(len(rows), limit),
                    limit,
                    f"Zefix Seite {page_number} leer (Versuch {consecutive_page_failures}), neuer Versuch in {retry_wait}s",
                )
            if consecutive_page_failures >= ZEFIX_MAX_ATTEMPTS_PER_PAGE:
                if on_progress is not None:
                    on_progress(
                        min(len(rows), limit),
                        limit,
                        f"Zefix Seite {page_number} übersprungen nach {consecutive_page_failures} leeren Antworten",
                    )
                consecutive_page_failures = 0
                offset += page_size
                continue
            time.sleep(retry_wait)
            continue
        batch = list(csv.DictReader(csv_text.splitlines()))
        if not batch:
            break
        consecutive_page_failures = 0
        rows.extend(batch)
        if on_progress is not None:
            on_progress(min(len(rows), limit), limit, f"Zefix Seite {page_number} fertig")
        if len(batch) < page_size:
            break
        offset += page_size

    return rows[:limit]


def company_from_zefix_row(row: dict[str, str], canton_map: dict[int, str]) -> Company:
    municipality_uri = row.get("municipality_uri", "")
    municipality_code = extract_bfs_code(municipality_uri)
    city = (row.get("municipality") or "").strip()
    canton = canton_map.get(municipality_code or -1, "")
    company_uri = (row.get("company_uri") or "").strip()
    uid = company_uri.rsplit("/", 1)[-1] if company_uri else ""
    return Company(
        name=(row.get("name") or "").strip(),
        legal_form=(row.get("company_type") or "").strip(),
        city=city,
        canton=canton,
        source="Zefix/LINDAS",
        uid=uid,
    )


def seed_from_zefix(
    limit: int = ZEFIX_DEFAULT_LIMIT,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> list[Company]:
    canton_map = load_bfs_canton_map()
    raw_rows = fetch_zefix_rows(limit=limit, on_progress=on_progress)
    companies = [company_from_zefix_row(row, canton_map) for row in raw_rows if (row.get("name") or "").strip()]
    return companies


def parse_searchch_seed_companies(html_text: str) -> list[Company]:
    pattern = re.compile(
        r'<span class="locality">([^<]+)</span>\s*<span class="region">([^<]+)</span>.*?'
        r'<a class="tel-result-detail-link"[^>]*title="([^"]+)"[^>]*href="([^"]+)"',
        re.S,
    )
    companies: list[Company] = []
    for city, canton, name, href in pattern.findall(html_text):
        company_name = html.unescape(name).strip()
        if not looks_like_company_name(company_name):
            continue
        city_name = html.unescape(city).strip()
        canton_code = normalize_canton(canton)
        slug = href.rsplit("/", 1)[-1] if href else ""
        if not company_name:
            continue
        companies.append(
            Company(
                name=company_name,
                city=city_name,
                canton=canton_code,
                source="search.ch",
                uid=f"searchch:{slug}" if slug else "",
            )
        )
    return companies


def parse_localch_seed_companies(html_text: str) -> list[Company]:
    text = html_text.replace('\\"', '"')
    entry_pattern = re.compile(r'"entry":\{(.*?)\},"availabilities":\[\]', re.S)
    title_pattern = re.compile(r'"title":"([^"]+)"')
    city_pattern = re.compile(r'"city":"([^"]+)"')
    canton_pattern = re.compile(r'"cantonCode":"([A-Z]{2})"')
    website_pattern = re.compile(r'"__typename":"CustomerProvidedURLContact".*?"value":"(https?://[^"]+)"', re.S)
    companies: list[Company] = []
    seen: set[str] = set()
    for entry_text in entry_pattern.findall(text):
        title_match = title_pattern.search(entry_text)
        city_match = city_pattern.search(entry_text)
        canton_match = canton_pattern.search(entry_text)
        if not title_match:
            continue
        name = html.unescape(title_match.group(1)).strip()
        if not name or not looks_like_company_name(name):
            continue
        city = html.unescape(city_match.group(1)).strip() if city_match else ""
        canton = canton_match.group(1).strip().upper() if canton_match else ""
        website_match = website_pattern.search(entry_text)
        emails = extract_emails_from_text(entry_text)
        website = html.unescape(website_match.group(1)).strip() if website_match else ""
        if website:
            host = urlparse(website).netloc.lower()
            if not host or any(blocked in host for blocked in SEARCH_BLOCKLIST) or is_blocked_foreign_domain(host):
                website = ""
        company = Company(
            name=name,
            city=city,
            canton=canton,
            website=website,
            emails=emails,
            source="local.ch",
            uid=f"localch:{normalize_text(name)}:{normalize_text(city)}:{canton}",
        )
        key = company_key(company)
        if key in seen:
            continue
        seen.add(key)
        companies.append(company)
    return companies


def strip_html_tags(value: str) -> str:
    return re.sub(r"<[^>]+>", "", value or "")


def normalize_result_title(value: str) -> str:
    text = html.unescape(strip_html_tags(value)).strip()
    return re.sub(r"\s+", " ", text)


def looks_like_company_name(name: str) -> bool:
    text = normalize_result_title(name)
    lower = text.lower()
    if len(text) < 4:
        return False
    if any(token in lower for token in ("http", "www.", "zhihu", "baidu", "question", "answer", "wikipedia", "linkedin", "socialmedia", "kanäle", "kanaele", "firmendetails", "spontan besuch")):
        return False
    if re.search(r"\b\d{4,}\b", lower):
        return False
    if not re.search(r"[a-zA-ZäöüÄÖÜ]", text):
        return False
    if lower in {"ag", "gmbh", "sa", "sarl", "sàrl", "sagl", "verein", "stiftung", "genossenschaft"}:
        return False
    generic_tokens = {
        "schweiz",
        "swiss",
        "switzerland",
        "suisse",
        "svizzera",
        "site",
        "group",
        "holding",
        "service",
        "solutions",
        "services",
        "international",
    }
    words = [w for w in re.split(r"\s+", lower) if w]
    informative = [w for w in words if len(w) >= 4 and w not in generic_tokens]
    if not informative:
        return False
    if any(hint in f" {lower} " for hint in LEGAL_FORM_HINTS):
        words = [w for w in re.split(r"\s+", text) if w]
        if len(words) <= 1:
            return False
        return True
    words = [w for w in re.split(r"\s+", text) if w]
    return len(words) >= 2 and all(len(w) > 1 for w in words[:2])


def build_web_seed_terms(limit: int, max_queries: int | None = None) -> list[str]:
    target_queries = max(1, limit)
    if max_queries is not None:
        target_queries = min(target_queries, max_queries)
    terms: list[str] = []
    seen: set[str] = set()
    for base in WEB_SEED_BASE_TERMS:
        for suffix in ("", " schweiz", " site:.ch", " kontakt"):
            term = f"{base} {suffix}".strip().lower()
            if term not in seen:
                terms.append(term)
                seen.add(term)
            if len(terms) >= target_queries:
                return terms
    return terms[:target_queries]


def parse_moneyhouse_seed_companies(html_text: str) -> list[Company]:
    pattern = re.compile(
        r'<a href="(?P<link>/de/company/[^"]+)">(?P<name>[^<]+)</a>\s*'
        r'<div class="section--small">\s*<p class="minor">(?P<addr>[^<]+)</p>',
        re.S,
    )
    companies: list[Company] = []
    seen: set[str] = set()
    for match in pattern.finditer(html_text):
        link = match.group("link")
        if "/network" in link:
            continue
        name = normalize_result_title(match.group("name"))
        address = html.unescape(match.group("addr")).strip()
        if "|" in address:
            address = address.split("|")[-1].strip()
        address = re.sub(r"^\d{3,4}\s*", "", address)
        company = Company(
            name=name,
            city=address,
            source="moneyhouse",
            uid=link.rsplit("/", 1)[-1],
        )
        key = company_key(company)
        if not name or key in seen:
            continue
        seen.add(key)
        companies.append(company)
    return companies


def parse_swissguide_seed_companies(html_text: str, final_url: str = "") -> list[Company]:
    titles = [normalize_result_title(title) for title in re.findall(r'<h2 class="elementor-heading-title[^>]*>([^<]+)</h2>', html_text, re.S)]
    if not titles:
        titles = [normalize_result_title(title) for title in re.findall(r'<meta property="og:title" content="([^"]+)"', html_text, re.S)]
    companies: list[Company] = []
    seen: set[str] = set()
    path = urlparse(final_url).path if final_url else ""
    city = ""
    if "/firma/" in path:
        slug = path.split("/firma/", 1)[-1].strip("/")
        if "_" in slug:
            city = slug.rsplit("_", 1)[-1].replace("-", " ").strip().title()
    for title in titles:
        if not title or not looks_like_company_name(title):
            continue
        company = Company(
            name=title,
            city=city,
            source="swissguide",
            uid=path.rsplit("/", 2)[-2] if "/firma/" in path else normalize_text(title),
        )
        key = company_key(company)
        if key in seen:
            continue
        seen.add(key)
        companies.append(company)
    return companies


def parse_search_engine_seed_companies(provider: str, html_text: str, base_url: str) -> list[Company]:
    provider_key = provider.lower()
    if provider_key == "ddg":
        pattern = re.compile(r'<a[^>]*class="result__a"[^>]*href="([^"]+)"[^>]*>(.*?)</a>', re.S)
    elif provider_key == "bing":
        pattern = re.compile(r'<li class="b_algo".*?<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>', re.S)
    else:
        pattern = re.compile(r'<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>', re.S)

    companies: list[Company] = []
    seen: set[str] = set()
    for href, title_html in pattern.findall(html_text):
        title = normalize_result_title(title_html)
        if not title or len(title) < 3 or not looks_like_company_name(title):
            continue
        candidate = decode_ddg_url(href.replace("&amp;", "&")) if provider_key == "ddg" else decode_bing_url(href)
        if provider_key == "google" and candidate.startswith("/url?"):
            parsed = urlparse(candidate)
            candidate = (parse_qs(parsed.query).get("q") or [candidate])[0]
        parsed = urlparse(candidate if "://" in candidate else urljoin(base_url, candidate))
        host = parsed.netloc.lower()
        if host and (host in SEARCH_BLOCKLIST or is_blocked_foreign_domain(host)):
            continue
        if host and host_tld(host) not in PREFERRED_TLDS:
            continue
        company = Company(
            name=title,
            website=candidate if candidate.startswith(("http://", "https://")) and host and host not in SEARCH_BLOCKLIST else "",
            source=provider_key,
            uid=f"{provider_key}:{normalize_text(title)}",
        )
        key = company_key(company)
        if key in seen:
            continue
        seen.add(key)
        companies.append(company)
    return companies


def build_searchch_seed_terms(limit: int, max_queries: int | None = None) -> list[str]:
    target_queries = max(1, limit)
    if max_queries is not None:
        target_queries = min(target_queries, max_queries)
    terms: list[str] = []
    seen: set[str] = set()
    for base in SEARCHCH_SEED_BASE_TERMS:
        if base not in seen:
            terms.append(base)
            seen.add(base)
    for canton in SEARCHCH_SEED_CANTONS:
        for base in ("ag", "gmbh", "treuhand", "immobilien", "bau", "restaurant"):
            term = f"{base} {canton}".lower()
            if term not in seen:
                terms.append(term)
                seen.add(term)
            if len(terms) >= target_queries:
                return terms
    return terms[:target_queries]


def build_localch_seed_terms(limit: int, max_queries: int | None = None) -> list[str]:
    target_queries = max(1, limit)
    if max_queries is not None:
        target_queries = min(target_queries, max_queries)
    terms: list[str] = []
    seen: set[str] = set()
    for base in SEARCHCH_SEED_BASE_TERMS:
        if base not in seen:
            terms.append(base)
            seen.add(base)
    for where in LOCALCH_SEED_WHERE_TERMS:
        for base in ("ag", "gmbh", "treuhand", "immobilien", "bau", "restaurant", "hotel", "arzt", "physio", "consulting"):
            term = f"{base} {where}".lower()
            if term not in seen:
                terms.append(term)
                seen.add(term)
            if len(terms) >= target_queries:
                return terms
    return terms[:target_queries]


def fetch_searchch_seed_companies_for_term(term: str, timeout: int = 10, source_label: str = "search.ch") -> list[Company]:
    visited = load_visited_pages()
    
    # Versuche Variationen des Terms, um verschiedene Seiten zu erreichen
    term_variants = [
        term,
        f"{term} II",
        f"{term} 2",
        f"{term} page:2",
    ]
    
    all_companies = []
    for variant in term_variants:
        if variant.lower() in visited.get(source_label, set()):
            continue  # Diesen Term haben wir schon gecrawlt
        
        url = SEARCHCH_SEED_URL.format(term=quote_plus(variant))
        html_text = safe_get_text(url, timeout=timeout, retries=1, tolerate_statuses={403, 429})
        if not html_text:
            continue
        
        companies = parse_searchch_seed_companies(html_text)
        if not companies:
            continue
        
        for company in companies:
            company.source = source_label
        
        all_companies.extend(companies)
        mark_term_visited(source_label, variant, visited)
    
    save_visited_pages(visited)
    return all_companies


def fetch_telsearchch_seed_companies_for_term(
    term: str,
    where: str = "schweiz",
    timeout: int = 10,
    source_label: str = "tel.search.ch",
) -> list[Company]:
    visited = load_visited_pages()
    lookup_key = f"{term}|{where}"
    
    # Versuche Variationen des Terms
    term_variants = [
        term,
        f"{term} II",
        f"{term} 2",
        f"{term} page:2",
    ]
    
    all_companies = []
    for variant in term_variants:
        variant_key = f"{variant}|{where}"
        if variant_key.lower() in {k.lower() for k in visited.get(source_label, set())}:
            continue
        
        url = TEL_SEARCHCH_SEED_URL.format(term=quote_plus(variant), where=quote_plus(where))
        html_text = safe_get_text(url, timeout=timeout, retries=1, tolerate_statuses={403, 429})
        if not html_text:
            continue
        
        companies = parse_searchch_seed_companies(html_text)
        if not companies:
            continue
        
        for company in companies:
            company.source = source_label
        
        all_companies.extend(companies)
        mark_term_visited(source_label, variant_key, visited)
    
    save_visited_pages(visited)
    return all_companies


def fetch_localch_seed_companies_for_term(
    term: str,
    where: str = "zurich",
    timeout: int = 10,
    source_label: str = "local.ch",
) -> list[Company]:
    visited = load_visited_pages()
    
    # Versuche Variationen des Terms
    term_variants = [
        term,
        f"{term} II",
        f"{term} 2",
        f"{term} page:2",
    ]
    
    all_companies = []
    for variant in term_variants:
        variant_key = f"{variant}|{where}"
        if variant_key.lower() in {k.lower() for k in visited.get(source_label, set())}:
            continue
        
        url = LOCALCH_SEED_URL.format(term=quote_plus(variant), where=quote_plus(where))
        html_text = safe_get_text(url, timeout=timeout, retries=1, tolerate_statuses={403, 429})
        if not html_text:
            continue
        
        companies = parse_localch_seed_companies(html_text)
        if not companies:
            continue
        
        for company in companies:
            company.source = source_label
        
        all_companies.extend(companies)
        mark_term_visited(source_label, variant_key, visited)
    
    save_visited_pages(visited)
    return all_companies


def fetch_moneyhouse_seed_companies_for_term(term: str, timeout: int = 10) -> list[Company]:
    visited = load_visited_pages()
    source_label = "moneyhouse"
    
    # Versuche Variationen des Terms
    term_variants = [
        term,
        f"{term} II",
        f"{term} 2",
        f"{term} page:2",
    ]
    
    all_companies = []
    for variant in term_variants:
        if variant.lower() in visited.get(source_label, set()):
            continue
        
        url = MONEYHOUSE_SEARCH_URL.format(query=quote_plus(variant))
        html_text = safe_get_text(url, timeout=timeout, retries=1, tolerate_statuses={403, 429})
        if not html_text:
            continue
        
        companies = parse_moneyhouse_seed_companies(html_text)
        if not companies:
            continue
        
        all_companies.extend(companies)
        mark_term_visited(source_label, variant, visited)
    
    save_visited_pages(visited)
    return all_companies


def fetch_swissguide_seed_companies_for_term(term: str, timeout: int = 10) -> list[Company]:
    visited = load_visited_pages()
    source_label = "swissguide"
    
    # Versuche Variationen des Terms
    term_variants = [
        term,
        f"{term} II",
        f"{term} 2",
        f"{term} page:2",
    ]
    
    all_companies = []
    for variant in term_variants:
        if variant.lower() in visited.get(source_label, set()):
            continue
        
        url = SWISSGUIDE_SEARCH_URL.format(query=quote_plus(variant))
        html_text = safe_get_text(url, timeout=timeout, retries=1, tolerate_statuses={403, 429})
        if not html_text:
            continue
        
        companies = parse_swissguide_seed_companies(html_text, final_url=url)
        if not companies:
            continue
        
        all_companies.extend(companies)
        mark_term_visited(source_label, variant, visited)
    
    save_visited_pages(visited)
    return all_companies


def fetch_web_search_seed_companies_for_term(provider: str, term: str, timeout: int = 10) -> list[Company]:
    provider_key = provider.lower()
    if provider_key == "ddg":
        url = DDG_SEARCH_URL.format(query=quote_plus(term))
    elif provider_key == "bing":
        url = BING_SEARCH_URL.format(query=quote_plus(term))
    elif provider_key == "google":
        url = GOOGLE_SEARCH_URL.format(query=quote_plus(term))
    else:
        return []
    html_text = safe_get_text(url, timeout=timeout, retries=1, tolerate_statuses={403, 429})
    if not html_text:
        return []
    return parse_search_engine_seed_companies(provider_key, html_text, base_url=url)


def run_seed_jobs_incremental(
    jobs: list[tuple],
    fetcher: Callable[..., list[Company]],
    limit: int,
    max_workers: int,
    progress_prefix: str,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> list[Company]:
    if not jobs or limit <= 0:
        return []

    dedup: dict[str, Company] = {}
    total_jobs = len(jobs)
    finished = 0
    iterator = iter(jobs)
    in_flight: set = set()

    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
        future_args: dict = {}

        def submit_next() -> None:
            while len(in_flight) < max(1, max_workers):
                if job_should_stop():
                    return
                try:
                    args = next(iterator)
                except StopIteration:
                    return
                future = executor.submit(fetcher, *args)
                in_flight.add(future)
                future_args[future] = args

        submit_next()
        while in_flight:
            if job_should_stop():
                for pending_future in in_flight:
                    pending_future.cancel()
                break
            done, _ = wait(in_flight, timeout=0.5, return_when=FIRST_COMPLETED)
            if not done:
                continue
            for future in done:
                in_flight.discard(future)
                future_args.pop(future, None)
                finished += 1
                try:
                    companies = future.result()
                except Exception:
                    companies = []
                for company in companies:
                    key = company_key(company)
                    if key in dedup:
                        continue
                    dedup[key] = company
                    if len(dedup) >= limit:
                        for pending_future in in_flight:
                            pending_future.cancel()
                        return list(dedup.values())[:limit]
                if on_progress is not None:
                    on_progress(min(len(dedup), limit), limit, f"{progress_prefix} {finished}/{total_jobs}")
            submit_next()

    return list(dedup.values())[:limit]


def seed_from_searchch(
    limit: int,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> list[Company]:
    terms = build_searchch_seed_terms(limit)
    if not terms:
        return []

    jobs = [(term,) for term in terms]
    return run_seed_jobs_incremental(
        jobs=jobs,
        fetcher=fetch_searchch_seed_companies_for_term,
        limit=limit,
        max_workers=8,
        progress_prefix="search.ch Seed",
        on_progress=on_progress,
    )


def seed_from_telsearchch(
    limit: int,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> list[Company]:
    terms = build_searchch_seed_terms(limit)
    if not terms:
        return []

    jobs = [(term, "schweiz", 10, "tel.search.ch") for term in terms]
    return run_seed_jobs_incremental(
        jobs=jobs,
        fetcher=fetch_telsearchch_seed_companies_for_term,
        limit=limit,
        max_workers=6,
        progress_prefix="tel.search.ch Seed",
        on_progress=on_progress,
    )


def seed_from_localch(
    limit: int,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> list[Company]:
    terms = build_localch_seed_terms(limit)
    if not terms:
        return []

    where_terms = LOCALCH_SEED_WHERE_TERMS
    jobs = [(term, where, 10, "local.ch") for term in terms for where in where_terms]
    return run_seed_jobs_incremental(
        jobs=jobs,
        fetcher=fetch_localch_seed_companies_for_term,
        limit=limit,
        max_workers=6,
        progress_prefix="local.ch Seed",
        on_progress=on_progress,
    )


def seed_from_localch_switzerland(
    limit: int,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> list[Company]:
    terms = build_localch_seed_terms(limit)
    if not terms:
        return []

    jobs = [(term, "switzerland", 10, "local.ch (CH)") for term in terms]
    return run_seed_jobs_incremental(
        jobs=jobs,
        fetcher=fetch_localch_seed_companies_for_term,
        limit=limit,
        max_workers=5,
        progress_prefix="local.ch (CH) Seed",
        on_progress=on_progress,
    )


def seed_from_moneyhouse(
    limit: int,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> list[Company]:
    terms = build_web_seed_terms(limit)
    if not terms:
        return []

    jobs = [(term, 12) for term in terms]
    return run_seed_jobs_incremental(
        jobs=jobs,
        fetcher=fetch_moneyhouse_seed_companies_for_term,
        limit=limit,
        max_workers=6,
        progress_prefix="moneyhouse Seed",
        on_progress=on_progress,
    )


def seed_from_swissguide(
    limit: int,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> list[Company]:
    terms = build_web_seed_terms(limit)
    if not terms:
        return []

    jobs = [(term, 12) for term in terms]
    return run_seed_jobs_incremental(
        jobs=jobs,
        fetcher=fetch_swissguide_seed_companies_for_term,
        limit=limit,
        max_workers=5,
        progress_prefix="swissguide Seed",
        on_progress=on_progress,
    )


def seed_from_web_search(provider: str, limit: int, on_progress: Callable[[int, int, str], None] | None = None) -> list[Company]:
    terms = build_web_seed_terms(limit)
    if not terms:
        return []

    jobs = [(provider, term, 12) for term in terms]
    return run_seed_jobs_incremental(
        jobs=jobs,
        fetcher=fetch_web_search_seed_companies_for_term,
        limit=limit,
        max_workers=6,
        progress_prefix=f"{provider.upper()} Seed",
        on_progress=on_progress,
    )


def seed_from_multi_sources(
    limit: int = ZEFIX_DEFAULT_LIMIT,
    on_progress: Callable[[int, int, str], None] | None = None,
    on_company: Callable[[Company], None] | None = None,
    source_workers: int | None = None,
    enabled_sources: Iterable[str] | None = None,
) -> list[Company]:
    selected_sources = parse_seed_sources(enabled_sources)
    selected_source_set = set(selected_sources)

    search_limit = limit
    local_limit = limit
    web_limit = limit
    source_results: dict[str, list[Company]] = {source: [] for source in DEFAULT_SEED_SOURCES}

    def zefix_progress(processed: int, total: int, current: str) -> None:
        if on_progress is not None:
            on_progress(processed, limit, current)

    def source_progress(processed: int, total: int, current: str) -> None:
        if on_progress is not None:
            on_progress(processed, limit, current)

    merged: dict[str, Company] = {}
    emitted_keys: set[str] = set()

    def merge_available_sources() -> None:
        staged_sources = [
            source_results["zefix"],
            source_results["search.ch"],
            source_results["tel.search.ch"],
            source_results["local.ch"],
            source_results["local.ch.ch"],
            source_results["moneyhouse"],
            source_results["swissguide"],
        ]
        max_len = max((len(source) for source in staged_sources), default=0)
        for index in range(max_len):
            for source_list in staged_sources:
                if index >= len(source_list):
                    continue
                company = sanitize_seed_company(source_list[index])
                if not company.name:
                    continue
                key = company_key(company)
                existing = merged.get(key)
                merged[key] = merge_companies(existing, company) if existing is not None else company
                if key not in emitted_keys:
                    emitted_keys.add(key)
                    if on_company is not None:
                        on_company(merged[key])
                if len(merged) >= limit:
                    return

    source_executor_workers = max(1, min(7, resolve_worker_count(source_workers, default_workers=7)))
    with ThreadPoolExecutor(max_workers=source_executor_workers) as executor:
        futures: dict = {}
        if "zefix" in selected_source_set:
            futures[executor.submit(seed_from_zefix, limit, zefix_progress)] = "zefix"
        if "search.ch" in selected_source_set:
            futures[executor.submit(seed_from_searchch, search_limit, source_progress)] = "search.ch"
        if "tel.search.ch" in selected_source_set:
            futures[executor.submit(seed_from_telsearchch, search_limit, source_progress)] = "tel.search.ch"
        if "local.ch" in selected_source_set:
            futures[executor.submit(seed_from_localch, local_limit, source_progress)] = "local.ch"
        if "local.ch.ch" in selected_source_set:
            futures[executor.submit(seed_from_localch_switzerland, local_limit, source_progress)] = "local.ch.ch"
        if "moneyhouse" in selected_source_set:
            futures[executor.submit(seed_from_moneyhouse, web_limit, source_progress)] = "moneyhouse"
        if "swissguide" in selected_source_set:
            futures[executor.submit(seed_from_swissguide, web_limit, source_progress)] = "swissguide"
        pending = set(futures.keys())
        while pending:
            if job_should_stop():
                for future in pending:
                    future.cancel()
                break
            done, pending = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
            if not done:
                if len(merged) >= limit:
                    for future in pending:
                        future.cancel()
                    break
                continue
            for future in done:
                source = futures[future]
                try:
                    result = future.result()
                except Exception:
                    result = []
                    if on_progress is not None:
                        on_progress(0, limit, f"{SEED_SOURCE_LABELS.get(source, source)} temporär nicht erreichbar")
                source_results[source] = result
            merge_available_sources()
            if len(merged) >= limit:
                for future in pending:
                    future.cancel()
                break

    merge_available_sources()
    combined = list(merged.values())[:limit]
    if on_progress is not None:
        on_progress(min(len(combined), limit), limit, "Seed aus mehreren Quellen fertig")
    return combined


def decode_ddg_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.netloc.endswith("duckduckgo.com") and parsed.path == "/l/":
        params = parse_qs(parsed.query)
        target = (params.get("uddg") or [""])[0]
        if target:
            return target
    return url


def decode_bing_url(url: str) -> str:
    decoded = html.unescape(url)
    parsed = urlparse(decoded)
    params = parse_qs(parsed.query)
    target = (params.get("u") or [""])[0]
    if target.startswith("a1"):
        raw = target[2:]
        padding = "=" * (-len(raw) % 4)
        try:
            candidate = base64.b64decode(raw + padding).decode("utf-8", errors="ignore")
            if candidate.startswith(("http://", "https://")):
                return candidate
        except Exception:
            pass
    return decoded


def host_without_port(host: str) -> str:
    return host.split(":", 1)[0].strip().lower()


def root_domain(host: str) -> str:
    clean = host_without_port(host)
    if clean.startswith("www."):
        clean = clean[4:]
    parts = [part for part in clean.split(".") if part]
    if len(parts) < 2:
        return clean
    return ".".join(parts[-2:])


def host_tld(host: str) -> str:
    clean = host_without_port(host)
    parts = [part for part in clean.split(".") if part]
    return parts[-1].lower() if parts else ""


def is_blocked_foreign_domain(host: str) -> bool:
    return host_tld(host) in BLOCKED_WEBSITE_TLDS


def company_name_tokens(name: str) -> set[str]:
    text = normalize_text(name)
    tokens = set(re.findall(r"[a-z0-9]{3,}", text))
    common = {
        "ag",
        "gmbh",
        "sarl",
        "soci",
        "holding",
        "group",
        "company",
        "unternehmen",
        "service",
        "sa",
    }
    return {token for token in tokens if token not in common}


def company_search_name(company: Company) -> str:
    name_text = normalize_text(company.name)
    legal_text = normalize_text(company.legal_form)
    cleaned = name_text
    if legal_text:
        cleaned = re.sub(rf"\b{re.escape(legal_text)}\b", " ", cleaned)
    for token in company_name_tokens(company.legal_form):
        cleaned = re.sub(rf"\b{re.escape(token)}\b", " ", cleaned)
    for token in ("schweiz", "switzerland", "suisse", "svizzera", "swiss"):
        cleaned = re.sub(rf"\b{re.escape(token)}\b", " ", cleaned)
    cleaned = " ".join(cleaned.split())
    return cleaned or name_text


def city_tokens(city: str) -> set[str]:
    cleaned = re.sub(r"[()\-/,]", " ", normalize_text(city))
    return set(re.findall(r"[a-z0-9]{3,}", cleaned))


def company_core_name(company: Company) -> str:
    full_name = normalize_text(company.name)
    short_name = company_search_name(company)
    return short_name if len(short_name) >= 6 else full_name


def page_mentions_full_company_name(company: Company, page_text: str) -> bool:
    content = normalize_text(page_text)
    if not content:
        return False
    full_name = normalize_text(company.name)
    core_name = company_core_name(company)
    return (full_name and full_name in content) or (core_name and core_name in content)


def page_mentions_company_and_city(company: Company, page_text: str) -> bool:
    content = normalize_text(page_text)
    if not content:
        return False
    name_tokens = [token for token in company_name_tokens(company.name) if len(token) >= 4]
    if not name_tokens:
        return page_mentions_full_company_name(company, page_text)
    token_hits = sum(1 for token in name_tokens if token in content)
    required_hits = 2 if len(name_tokens) >= 2 else 1
    if token_hits < required_hits and not page_mentions_full_company_name(company, page_text):
        return False
    city_parts = city_tokens(company.city)
    if not city_parts:
        return True
    return any(token in content for token in city_parts)


def website_looks_like_company_site(company: Company, website: str, page_text: str = "") -> bool:
    parsed = urlparse(website if "://" in website else f"https://{website}")
    host = host_without_port(parsed.netloc)
    if not host:
        return False
    if is_blocked_foreign_domain(host):
        return False
    if any(blocked in host for blocked in SEARCH_BLOCKLIST):
        return False
    domain = root_domain(host)
    if domain in EMAIL_DOMAIN_BLOCKLIST:
        return False
    if domain in FREE_EMAIL_DOMAINS:
        return False
    token_blob = re.sub(r"[^a-z0-9]+", "", domain)
    content_match = page_mentions_company_and_city(company, page_text)
    full_name_match = page_mentions_full_company_name(company, page_text)
    domain_token_match = any(token in token_blob or token_blob in token for token in company_name_tokens(company.name))

    # Hohe Präzision: Domain-Hinweis allein reicht nicht, es braucht zusätzlich Inhalts-Match.
    if domain_token_match and content_match:
        return True
    # Alternative: klarer Vollname + Ortsbezug im Inhalt.
    if full_name_match and content_match:
        return True
    return False


def validate_company_emails(
    company: Company,
    website: str,
    emails: Iterable[str],
    page_text: str = "",
) -> list[str]:
    parsed = urlparse(website if "://" in website else f"https://{website}")
    host = host_without_port(parsed.netloc)
    if is_blocked_foreign_domain(host):
        return []
    allowed_domain = root_domain(host)
    valid: set[str] = set()
    for email in emails:
        candidate = normalize_text(email).lower()
        if not EMAIL_RE.fullmatch(candidate):
            continue
        domain = candidate.rsplit("@", 1)[-1]
        if domain.rsplit(".", 1)[-1] in FALSE_EMAIL_DOMAIN_SUFFIXES:
            continue
        if domain in EMAIL_DOMAIN_BLOCKLIST:
            continue
        if is_blocked_foreign_domain(domain):
            continue
        domain_matches_website = allowed_domain and (domain == allowed_domain or domain.endswith("." + allowed_domain))
        if domain_matches_website:
            valid.add(candidate)
            continue

        # Hohe Präzision: keine Cross-Domain-Akzeptanz.
        continue
    return sorted(valid)


def domain_resolves(domain: str) -> bool:
    try:
        socket.getaddrinfo(domain, None)
        return True
    except Exception:
        return False


def validate_company_emails_second_pass(
    company: Company,
    website: str,
    emails: Iterable[str],
    page_text: str = "",
) -> list[str]:
    parsed = urlparse(website if "://" in website else f"https://{website}")
    host = host_without_port(parsed.netloc)
    if not host or is_blocked_foreign_domain(host):
        return []

    allowed_domain = root_domain(host)
    strong_content_match = page_mentions_company_and_city(company, page_text)
    name_tokens = {token for token in company_name_tokens(company.name) if len(token) >= 4}
    checked: set[str] = set()
    for email in emails:
        candidate = normalize_text(email).lower()
        if not EMAIL_RE.fullmatch(candidate):
            continue
        domain = candidate.rsplit("@", 1)[-1]
        if domain in EMAIL_DOMAIN_BLOCKLIST:
            continue
        if domain in FREE_EMAIL_DOMAINS:
            continue
        if domain.rsplit(".", 1)[-1] in FALSE_EMAIL_DOMAIN_SUFFIXES:
            continue
        if is_blocked_foreign_domain(domain):
            continue
        if not domain_resolves(domain):
            continue

        same_website_domain = domain == allowed_domain or domain.endswith("." + allowed_domain)
        if same_website_domain:
            checked.add(candidate)
            continue

        # Ausnahme nur fuer starke Treffer auf .ch-Domains.
        if host_tld(domain) != "ch":
            continue
        if not strong_content_match:
            continue
        domain_blob = re.sub(r"[^a-z0-9]+", "", root_domain(domain))
        if not any(token in domain_blob for token in name_tokens):
            continue
        checked.add(candidate)

    return sorted(checked)


def double_validate_company_emails(
    company: Company,
    website: str,
    emails: Iterable[str],
    page_text: str = "",
) -> list[str]:
    first_pass = set(validate_company_emails(company, website, emails, page_text=page_text))
    if not first_pass:
        return []
    second_pass = set(validate_company_emails_second_pass(company, website, first_pass, page_text=page_text))
    if not second_pass:
        return []
    return sorted(first_pass.intersection(second_pass))


def discover_website(company: Company, timeout: int = 20) -> str:
    global SEARCH_DISCOVERY_DISABLED
    if job_should_stop():
        return ""
    search_name = company_search_name(company)
    full_name = normalize_text(company.name)
    city = company.city.strip()
    query_variants = [
        f'"{full_name}" "{city}" site:.ch' if city else f'"{full_name}" site:.ch',
        f'"{full_name}" impressum site:.ch',
        f'"{full_name}" kontakt site:.ch',
        f'"{search_name}" site:.ch',
        f'{search_name} site:.ch',
        f'"{search_name}" {city} site:.ch' if city else f'"{search_name}" site:.ch',
        f'{search_name} {city} site:.ch' if city else f'{search_name} site:.ch',
        f'"{search_name}" Kontakt site:.ch',
        f'"{search_name}" Impressum site:.ch',
    ]
    search_sources = [
        ("ddg", DDG_SEARCH_URL.format(query=quote_plus(query)), r'class="result__a" href="([^"]+)"')
        for query in query_variants
    ] + [
        ("bing", BING_SEARCH_URL.format(query=quote_plus(query)), r'<li class="b_algo".*?<a[^>]*href="([^"]+)"')
        for query in query_variants
    ]
    search_results: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=min(2, len(search_sources))) as executor:
        future_map = {}
        for provider, search_url, _ in search_sources:
            if provider == "ddg" and SEARCH_DISCOVERY_DISABLED:
                continue
            future_map[executor.submit(safe_get_text, search_url, max(6, min(timeout, 12)), 1)] = provider
        if not future_map:
            return ""
        wait(future_map)
        for future, provider in future_map.items():
            try:
                search_results[provider] = future.result()
            except HTTPError as exc:
                if provider == "ddg" and exc.code in {403, 429}:
                    SEARCH_DISCOVERY_DISABLED = True
            except Exception:
                pass

    candidates: list[str] = []
    for provider, _, pattern in search_sources:
        html_text = search_results.get(provider, "")
        if not html_text:
            continue
        result_urls = re.findall(pattern, html_text, flags=re.S)
        for raw_url in result_urls:
            candidate = decode_ddg_url(raw_url.replace("&amp;", "&")) if provider == "ddg" else decode_bing_url(raw_url)
            parsed = urlparse(candidate)
            host = parsed.netloc.lower()
            if not host or is_blocked_foreign_domain(host):
                continue
            if any(blocked in host for blocked in SEARCH_BLOCKLIST):
                continue
            if candidate not in candidates:
                candidates.append(candidate)

    candidates.sort(key=priority_score, reverse=True)
    top_candidates = candidates[:8]
    if not top_candidates:
        return ""

    with ThreadPoolExecutor(max_workers=min(4, len(top_candidates))) as executor:
        future_map = {
            candidate: executor.submit(verify_candidate_website, company, candidate, timeout)
            for candidate in top_candidates
        }
        for candidate in top_candidates:
            if job_should_stop():
                return ""
            try:
                verified = future_map[candidate].result()
            except Exception:
                continue
            if verified:
                return verified
    return ""


def enrich_company_record(company: Company, timeout: int = 10, discover_sites: bool = False) -> tuple[Company, dict[str, int]]:
    working = clone_company(company)
    stats = {"websites_found": 0, "emails_found": 0, "errors": 0}
    if job_should_stop():
        return working, stats
    website = working.website
    if discover_sites and not website:
        try:
            website = discover_website(working, timeout=timeout)
        except Exception:
            website = ""
        if website:
            working.website = website

    if website:
        try:
            homepage_text, _ = fetch_html(website, timeout=timeout)
        except Exception:
            homepage_text = ""
        if not website_looks_like_company_site(working, website, page_text=homepage_text):
            working.website = ""
        else:
            stats["websites_found"] = 1

            # Bereits vorhandene E-Mails immer neu pruefen (Pass 1 + Pass 2).
            existing_valid = double_validate_company_emails(working, website, working.emails, page_text=homepage_text)
            working.emails = keep_consistent_email_domains(existing_valid)

            def email_validator(email: str, page_text: str) -> bool:
                return bool(double_validate_company_emails(working, website, [email], page_text=page_text))

            contact_found = crawl_contact_pages(website, homepage_text, timeout=timeout, email_validator=email_validator)
            if contact_found:
                working.emails = sorted(set(working.emails).union(contact_found))
                working.emails = keep_consistent_email_domains(working.emails)
                stats["emails_found"] += len(contact_found)
                if not working.source:
                    working.source = "website contact"

            found = crawl_public_emails(website, timeout=timeout, email_validator=email_validator)
            found = double_validate_company_emails(working, website, found, page_text=homepage_text)
            if found:
                working.emails = sorted(set(working.emails).union(found))
                working.emails = keep_consistent_email_domains(working.emails)
                stats["emails_found"] += len(found)
                if not working.source:
                    working.source = "website crawl"

            # Finale doppelte Validierung fuer gesamten Satz.
            working.emails = double_validate_company_emails(working, website, working.emails, page_text=homepage_text)
            working.emails = keep_consistent_email_domains(working.emails)
    else:
        # Ohne verifizierte Firmen-Website behalten wir keine unsicheren Alt-E-Mails.
        working.emails = []

    if not working.website and working.emails:
        inferred_website = infer_website_from_emails(working.emails)
        if inferred_website:
            working.website = inferred_website
            stats["websites_found"] = 1

    return working, stats


def process_companies_parallel(
    companies: list[Company],
    timeout: int,
    discover_sites: bool,
    workers: int,
    on_result: Callable[[Company, dict[str, int]], None],
) -> None:
    if not companies:
        return
    if workers <= 1:
        for company in companies:
            if job_should_stop():
                break
            on_result(*enrich_company_record(company, timeout=timeout, discover_sites=discover_sites))
        return

    with ThreadPoolExecutor(max_workers=workers) as executor:
        iterator = iter(companies)
        in_flight = set()

        def submit_next() -> None:
            while len(in_flight) < workers:
                if job_should_stop():
                    return
                try:
                    company = next(iterator)
                except StopIteration:
                    return
                in_flight.add(executor.submit(enrich_company_record, company, timeout, discover_sites))

        submit_next()
        while in_flight:
            if job_should_stop():
                for future in in_flight:
                    future.cancel()
                break
            done, _ = wait(in_flight, timeout=0.2, return_when=FIRST_COMPLETED)
            if not done:
                continue
            for future in done:
                in_flight.discard(future)
                if future.cancelled():
                    continue
                on_result(*future.result())
            submit_next()


def normalize_obfuscated_email(text: str) -> str:
    candidate = text.lower()
    candidate = candidate.replace("[at]", "@").replace("(at)", "@").replace(" at ", "@")
    candidate = candidate.replace("[dot]", ".").replace("(dot)", ".").replace(" dot ", ".")
    candidate = candidate.replace("\u200b", "")
    return candidate


def normalize_text(value: str | None) -> str:
    return " ".join((value or "").strip().lower().split())


def parse_bool_filter(value: str | None) -> bool | None:
    text = normalize_text(value)
    if not text:
        return None
    if text in {"1", "true", "yes", "ja", "y"}:
        return True
    if text in {"0", "false", "no", "nein", "n"}:
        return False
    return None


def infer_legal_form_codes(value: str | None) -> set[str]:
    text = normalize_text(value)
    if not text:
        return set()
    compact = re.sub(r"[^a-z0-9]+", " ", text)
    codes: set[str] = set()
    for code, keywords in LEGAL_FORM_KEYWORDS.items():
        for keyword in keywords:
            if normalize_text(keyword) in compact:
                codes.add(code)
                break
    return codes


def legal_form_matches(filter_value: str | None, company_value: str | None) -> bool:
    desired = normalize_text(filter_value)
    if not desired:
        return True
    company_text = normalize_text(company_value)
    if desired in company_text:
        return True

    desired_codes = infer_legal_form_codes(desired) or {desired.upper()}
    company_codes = infer_legal_form_codes(company_text)
    return bool(desired_codes.intersection(company_codes))


def normalize_canton(value: str | None) -> str:
    normalized = normalize_text(value)
    if not normalized:
        return ""
    return CANTON_ALIASES.get(normalized, normalized.upper())


def parse_employee_range(value: str | None) -> tuple[int | None, int | None]:
    text = (value or "").strip()
    if not text:
        return None, None
    compact = text.replace(" ", "")
    numbers = [int(chunk) for chunk in re.findall(r"\d+", compact)]
    if not numbers:
        return None, None
    if "+" in compact:
        return numbers[0], None
    if len(numbers) == 1:
        return numbers[0], numbers[0]
    return min(numbers[0], numbers[1]), max(numbers[0], numbers[1])


def parse_employee_filter(value: str | None) -> tuple[int | None, int | None]:
    if not value:
        return None, None
    return parse_employee_range(value)


def company_key(company: Company) -> str:
    if company.uid:
        return f"uid:{company.uid}"
    # Verbesserte Deduplizierung: Website/Email-Domain als zusätzlicher Identifier
    # Dadurch werden Firmen "Immobilia AG" und "Immobilie ABB" nicht vermischt
    website_domain = ""
    if company.website:
        parsed = urlparse(company.website)
        website_domain = parsed.netloc.lower()
    elif company.emails:
        # Notfalls Email-Domain als Hint verwenden
        website_domain = company.emails[0].rsplit("@", 1)[-1].lower() if "@" in company.emails[0] else ""
    
    key_parts = [
        f"name:{normalize_text(company.name)}",
        f"city:{normalize_text(company.city)}",
        f"legal:{normalize_text(company.legal_form)}"
    ]
    if website_domain:
        key_parts.append(f"domain:{website_domain}")
    return "|".join(key_parts)


def company_summary(company: Company) -> str:
    email_text = ", ".join(company.emails) if company.emails else "-"
    website_text = company.website or "-"
    employee_text = company.employee_label or "?"
    return f"{company.name} | {company.city} | {employee_text} | {website_text} | {email_text}"


def company_has_email(company: Company) -> bool:
    return any(normalize_text(email) for email in company.emails)


def infer_website_from_emails(emails: Iterable[str]) -> str:
    for email in emails:
        candidate = normalize_text(email).lower()
        if not EMAIL_RE.fullmatch(candidate):
            continue
        domain = candidate.rsplit("@", 1)[-1]
        if domain in FREE_EMAIL_DOMAINS:
            continue
        if domain in EMAIL_DOMAIN_BLOCKLIST:
            continue
        if is_blocked_foreign_domain(domain):
            continue
        if any(blocked in domain for blocked in SEARCH_BLOCKLIST):
            continue
        return f"https://{root_domain(domain)}"
    return ""


def keep_consistent_email_domains(emails: Iterable[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for email in emails:
        candidate = normalize_text(email).lower()
        if not EMAIL_RE.fullmatch(candidate):
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        normalized.append(candidate)
    if not normalized:
        return []

    domain_counts: dict[str, int] = {}
    for email in normalized:
        domain = root_domain(email.rsplit("@", 1)[-1])
        if not domain:
            continue
        domain_counts[domain] = domain_counts.get(domain, 0) + 1
    if not domain_counts:
        return normalized

    primary_domain = max(domain_counts.items(), key=lambda item: (item[1], len(item[0])))[0]
    return [email for email in normalized if root_domain(email.rsplit("@", 1)[-1]) == primary_domain]


def company_is_kmu(
    company: Company,
    min_employees: int = KMU_MIN_EMPLOYEES,
    max_employees: int | None = KMU_MAX_EMPLOYEES,
) -> bool:
    # Unbekannte Mitarbeiterzahl wird zugelassen, damit valide Firmen-E-Mails nicht verloren gehen.
    if company.employee_min is None and company.employee_max is None:
        return True
    lower = company.employee_min if company.employee_min is not None else company.employee_max
    upper = company.employee_max if company.employee_max is not None else company.employee_min
    if upper is None:
        return False
    if min_employees is not None and upper < min_employees:
        return False
    if max_employees is not None and lower is not None and lower > max_employees:
        return False
    return True


def company_is_storable(
    company: Company,
    min_employees: int = KMU_MIN_EMPLOYEES,
    max_employees: int | None = KMU_MAX_EMPLOYEES,
) -> bool:
    return company_has_email(company) and company_is_kmu(company, min_employees, max_employees)


def merge_source_labels(existing: str, incoming: str) -> str:
    sources: list[str] = []
    for raw in (existing, incoming):
        for part in (raw or "").split(","):
            label = part.strip()
            if label and label not in sources:
                sources.append(label)
    return ", ".join(sources)


def merge_companies(existing: Company | None, incoming: Company) -> Company:
    if existing is None:
        return incoming
    emails = sorted({*existing.emails, *incoming.emails})
    return Company(
        name=existing.name or incoming.name,
        legal_form=existing.legal_form or incoming.legal_form,
        employee_min=existing.employee_min if existing.employee_min is not None else incoming.employee_min,
        employee_max=existing.employee_max if existing.employee_max is not None else incoming.employee_max,
        city=existing.city or incoming.city,
        canton=existing.canton or incoming.canton,
        website=existing.website or incoming.website,
        emails=emails,
        source=merge_source_labels(existing.source, incoming.source),
        uid=existing.uid or incoming.uid,
    )


def sanitize_seed_emails(company: Company) -> list[str]:
    cleaned: set[str] = set()
    for email in company.emails:
        candidate = normalize_text(email).lower().strip("<>'\"")
        if not EMAIL_RE.fullmatch(candidate):
            continue
        domain = candidate.rsplit("@", 1)[-1]
        if domain in EMAIL_DOMAIN_BLOCKLIST:
            continue
        if domain.rsplit(".", 1)[-1] in FALSE_EMAIL_DOMAIN_SUFFIXES:
            continue
        if domain in FREE_EMAIL_DOMAINS:
            continue
        if is_blocked_foreign_domain(domain):
            continue
        cleaned.add(candidate)
    return sorted(cleaned)


def sanitize_seed_company(company: Company) -> Company:
    if not looks_like_company_name(company.name):
        company.name = ""
        company.emails = []
        company.website = ""
        return company

    company.emails = sanitize_seed_emails(company)
    website = (company.website or "").strip()
    if website:
        parsed = urlparse(website if "://" in website else f"https://{website}")
        host = parsed.netloc.lower()
        if not host or any(blocked in host for blocked in SEARCH_BLOCKLIST) or is_blocked_foreign_domain(host):
            company.website = ""
        else:
            company.website = website
            if company.emails:
                company.emails = validate_company_emails(company, company.website, company.emails, page_text=company.name)
    return company


def overlaps(record_min: int | None, record_max: int | None, filter_min: int | None, filter_max: int | None) -> bool:
    if filter_min is None and filter_max is None:
        return True
    if record_min is None and record_max is None:
        return False
    low = record_min if record_min is not None else record_max
    high = record_max if record_max is not None else record_min
    if low is None or high is None:
        return False
    if filter_min is not None and high < filter_min:
        return False
    if filter_max is not None and low > filter_max:
        return False
    return True


def company_matches(company: Company, filters: dict[str, str | None]) -> bool:
    if not company_is_kmu(company):
        return False
    name = normalize_text(filters.get("name"))
    legal_form = normalize_text(filters.get("legal_form"))
    city = normalize_text(filters.get("city"))
    canton = normalize_canton(filters.get("canton"))
    website = normalize_text(filters.get("website"))
    has_email = parse_bool_filter(filters.get("has_email"))
    has_website = parse_bool_filter(filters.get("has_website"))
    employee_min, employee_max = parse_employee_filter(filters.get("employees"))

    if name and name not in normalize_text(company.name):
        return False
    if not legal_form_matches(legal_form, company.legal_form):
        return False
    if city and city not in normalize_text(company.city):
        return False
    if canton and normalize_canton(company.canton) != canton:
        return False
    if website and website not in normalize_text(company.website):
        return False
    if has_email is True and not company.emails:
        return False
    if has_email is False and company.emails:
        return False
    if has_website is True and not company.website:
        return False
    if has_website is False and company.website:
        return False
    if not overlaps(company.employee_min, company.employee_max, employee_min, employee_max):
        return False
    return True


def load_companies(path: Path) -> list[Company]:
    if not path.exists():
        return []

    def _load_json(candidate: Path) -> list[dict]:
        with candidate.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        if not isinstance(data, list):
            raise ValueError(f"Unerwartetes JSON-Format in {candidate}")
        return data

    raw: list[dict]
    try:
        raw = _load_json(path)
    except Exception:
        backup_path = path.with_suffix(f"{path.suffix}.bak")
        if backup_path.exists():
            raw = _load_json(backup_path)
        else:
            raise

    companies: list[Company] = []
    for item in raw:
        companies.append(
            Company(
                name=item.get("name", "").strip(),
                legal_form=item.get("legal_form", "").strip(),
                employee_min=item.get("employee_min"),
                employee_max=item.get("employee_max"),
                city=item.get("city", "").strip(),
                canton=item.get("canton", "").strip(),
                website=item.get("website", "").strip(),
                emails=list(item.get("emails", [])),
                source=item.get("source", "").strip(),
                uid=item.get("uid", "").strip(),
            )
        )
    return companies


def save_companies(path: Path, companies: Iterable[Company]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(f"{path.suffix}.tmp")
    backup_path = path.with_suffix(f"{path.suffix}.bak")
    payload = list(companies)

    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, cls=CompanyEncoder)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())

    if path.exists():
        os.replace(path, backup_path)
    os.replace(tmp_path, path)


def pick_field(row: dict[str, str], *names: str) -> str:
    normalized = {normalize_text(key): value for key, value in row.items()}
    for name in names:
        value = normalized.get(normalize_text(name))
        if value:
            return value.strip()
    return ""


def import_csv(path: Path) -> list[Company]:
    companies: list[Company] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            name = pick_field(row, "name", "firma", "company")
            if not name:
                continue
            employee_raw = pick_field(row, "employees", "mitarbeiter", "anzahl_mitarbeiter", "mitarbeiterzahl")
            employee_min, employee_max = parse_employee_range(employee_raw)
            company = Company(
                name=name,
                legal_form=pick_field(row, "legal_form", "rechtsform", "form"),
                employee_min=employee_min,
                employee_max=employee_max,
                city=pick_field(row, "city", "ort", "gemeinde"),
                canton=normalize_canton(pick_field(row, "canton", "kanton")),
                website=pick_field(row, "website", "url", "webseite"),
                source=pick_field(row, "source", "quelle"),
                uid=pick_field(row, "uid", "uidnr", "identifier"),
            )
            companies.append(company)
    return companies


def serialize_companies(companies: Iterable[Company]) -> list[dict]:
    output = []
    for company in companies:
        record = asdict(company)
        record["employee_label"] = company.employee_label
        output.append(record)
    return output


def extract_emails_from_text(text: str) -> list[str]:
    normalized_text = normalize_obfuscated_email(text)
    candidates = set(match.group(0).lower() for match in EMAIL_RE.finditer(normalized_text))
    for match in MAILTO_RE.finditer(text):
        candidates.add(match.group(1).split("?")[0].lower())
    for token in re.findall(r"[A-Za-z0-9._%+\-\[\]\(\)\s@]{6,}", text):
        normalized = normalize_obfuscated_email(token)
        if EMAIL_RE.fullmatch(normalized.strip()):
            candidates.add(normalized.strip())
    return sorted(candidates)


def fetch_html(url: str, timeout: int = 10) -> tuple[str, str]:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    context = ssl.create_default_context()
    with urlopen(request, timeout=timeout, context=context) as response:
        content_type = response.headers.get_content_type()
        if content_type not in {"text/html", "text/plain", "application/xhtml+xml"}:
            return "", response.geturl()
        body = response.read(MAX_PAGE_BYTES)
        charset = response.headers.get_content_charset() or "utf-8"
        return body.decode(charset, errors="ignore"), response.geturl()


def same_site(candidate: str, root: str) -> bool:
    try:
        candidate_host = urlparse(candidate).netloc.lower()
        root_host = urlparse(root).netloc.lower()
    except Exception:
        return False
    if not candidate_host or not root_host:
        return False
    return candidate_host == root_host or candidate_host.endswith("." + root_host)


def discover_links(html_text: str, base_url: str) -> list[str]:
    links: list[str] = []
    for raw_href in HREF_RE.findall(html_text):
        absolute = urljoin(base_url, raw_href.strip())
        parsed = urlparse(absolute)
        if parsed.scheme not in {"http", "https"}:
            continue
        links.append(absolute)
    return links


def priority_score(url: str) -> int:
    lower = url.lower()
    score = 0
    for index, keyword in enumerate(KEYWORD_HINTS):
        if keyword in lower:
            score += 20 - index
    if lower.endswith(("/kontakt", "/contact", "/impressum")):
        score += 15
    return score


def discover_contact_pages(website: str, html_text: str) -> list[str]:
    if not website or not html_text:
        return []
    root_url = website if "://" in website else f"https://{website}"
    parsed_root = urlparse(root_url)
    root_origin = f"{parsed_root.scheme}://{parsed_root.netloc}" if parsed_root.scheme and parsed_root.netloc else root_url
    contact_keywords = (
        "kontakt",
        "contact",
        "impressum",
        "about",
        "team",
        "support",
        "service",
        "legal",
    )
    candidates: list[str] = []

    for path in (
        "/kontakt",
        "/contact",
        "/impressum",
        "/imprint",
        "/about",
        "/ueber-uns",
        "/team",
        "/support",
    ):
        direct_url = urljoin(root_origin + "/", path.lstrip("/"))
        if same_site(direct_url, root_url) and direct_url not in candidates:
            candidates.append(direct_url)

    for link in discover_links(html_text, root_url):
        if not same_site(link, root_url):
            continue
        lower = link.lower()
        if not any(keyword in lower for keyword in contact_keywords):
            continue
        if link not in candidates:
            candidates.append(link)
    candidates.sort(key=priority_score, reverse=True)
    return candidates[:6]


def crawl_contact_pages(
    website: str,
    homepage_text: str,
    timeout: int = 10,
    email_validator: Callable[[str, str], bool] | None = None,
) -> list[str]:
    emails: set[str] = set()
    for candidate in discover_contact_pages(website, homepage_text):
        if job_should_stop():
            break
        try:
            page_text, _ = fetch_html(candidate, timeout=timeout)
        except Exception:
            continue
        if not page_text:
            continue
        for email in extract_emails_from_text(page_text):
            if email_validator is not None and not email_validator(email, page_text):
                continue
            emails.add(email)
    return sorted(emails)


def verify_candidate_website(company: Company, candidate: str, timeout: int = 20) -> str:
    if job_should_stop():
        return ""
    try:
        page_text, _ = fetch_html(candidate, timeout=timeout)
    except Exception:
        return ""
    if website_looks_like_company_site(company, candidate, page_text=page_text):
        return candidate
    return ""


def discover_localch_emails(company: Company, timeout: int = 12) -> list[str]:
    # Für hohe Datenqualität werden Verzeichnis-E-Mails nicht direkt übernommen.
    return []


def discover_company_emails_via_search(company: Company, timeout: int = 12) -> list[str]:
    global SEARCH_DISCOVERY_DISABLED
    if job_should_stop():
        return []

    search_name = company_search_name(company)
    full_name = normalize_text(company.name)
    city = company.city.strip()
    query_variants = [
        f'"{full_name}" "{city}" kontakt email' if city else f'"{full_name}" kontakt email',
        f'"{full_name}" impressum email',
        f'"{search_name}" email',
        f'"{search_name}" kontakt email',
        f'"{search_name}" {city} kontakt email' if city else f'"{search_name}" kontakt email',
    ]
    providers = [
        ("ddg", DDG_SEARCH_URL, r'class="result__a" href="([^"]+)"'),
        ("bing", BING_SEARCH_URL, r'<li class="b_algo".*?<a[^>]*href="([^"]+)"'),
    ]

    emails: set[str] = set()
    candidate_urls: list[str] = []
    with ThreadPoolExecutor(max_workers=min(3, len(providers) * len(query_variants))) as executor:
        future_map = {}
        for provider, search_url, pattern in providers:
            if provider == "ddg" and SEARCH_DISCOVERY_DISABLED:
                continue
            for query in query_variants:
                future = executor.submit(safe_get_text, search_url.format(query=quote_plus(query)), max(4, min(timeout, 7)), 1, {403, 429})
                future_map[future] = (provider, pattern)
        for future in as_completed(future_map):
            if job_should_stop():
                break
            provider, pattern = future_map[future]
            try:
                page_text = future.result()
            except HTTPError as exc:
                if provider == "ddg" and exc.code in {403, 429}:
                    SEARCH_DISCOVERY_DISABLED = True
                continue
            except Exception:
                continue
            if not page_text or not pattern:
                continue
            for raw_url in re.findall(pattern, page_text, flags=re.S):
                candidate = decode_ddg_url(raw_url.replace("&amp;", "&")) if provider == "ddg" else decode_bing_url(raw_url)
                if candidate not in candidate_urls:
                    candidate_urls.append(candidate)
            if len(candidate_urls) >= 6:
                for pending in future_map:
                    if not pending.done():
                        pending.cancel()
                break

    candidate_urls.sort(key=priority_score, reverse=True)
    top_candidates = candidate_urls[:6]

    def fetch_candidate_emails(candidate: str) -> list[str]:
        try:
            page_text, final_url = fetch_html(candidate, timeout=timeout)
        except Exception:
            return []
        if not page_text:
            return []
        target_url = final_url or candidate
        if not website_looks_like_company_site(company, target_url, page_text=page_text):
            return []
        parsed = urlparse(target_url if "://" in target_url else f"https://{target_url}")
        host = host_without_port(parsed.netloc)
        if host_tld(host) != "ch":
            host_blob = re.sub(r"[^a-z0-9]+", "", host)
            if not any(token in host_blob for token in company_name_tokens(company.name)):
                return []
        return double_validate_company_emails(company, target_url, extract_emails_from_text(page_text), page_text=page_text)

    if top_candidates:
        with ThreadPoolExecutor(max_workers=min(4, len(top_candidates))) as executor:
            futures = [executor.submit(fetch_candidate_emails, candidate) for candidate in top_candidates]
            for future in as_completed(futures):
                if job_should_stop():
                    break
                try:
                    result = future.result()
                except Exception:
                    continue
                if not result:
                    continue
                emails.update(result)
                if len(emails) >= 2:
                    for pending in futures:
                        if not pending.done():
                            pending.cancel()
                    break

    return sorted(emails)


def crawl_public_emails(
    website: str,
    max_pages: int = MAX_CRAWL_PAGES,
    timeout: int = 10,
    on_email: Callable[[str], None] | None = None,
    email_validator: Callable[[str, str], bool] | None = None,
) -> list[str]:
    if not website:
        return []
    parsed = urlparse(website if "://" in website else f"https://{website}")
    if parsed.scheme not in {"http", "https"}:
        return []
    if is_blocked_foreign_domain(parsed.netloc):
        return []
    root_url = parsed.geturl()
    queue: deque[tuple[str, int]] = deque([(root_url, 0)])
    seen: set[str] = set()
    emails: set[str] = set()

    while queue and len(seen) < max_pages:
        if job_should_stop():
            break
        current_url, depth = queue.popleft()
        if current_url in seen:
            continue
        seen.add(current_url)
        try:
            html_text, final_url = fetch_html(current_url, timeout=timeout)
        except Exception:
            continue
        if not html_text:
            continue
        for email in extract_emails_from_text(html_text):
            if email_validator is not None and not email_validator(email, html_text):
                continue
            if email not in emails:
                emails.add(email)
                if on_email is not None:
                    on_email(email)
        if depth >= 1:
            continue
        candidates = []
        for link in discover_links(html_text, final_url):
            if same_site(link, root_url):
                candidates.append(link)
        candidates.sort(key=priority_score, reverse=True)
        for link in candidates[:4]:
            if link not in seen:
                queue.append((link, depth + 1))

    return sorted(emails)


def enrich_companies(
    companies: list[Company],
    limit: int | None = None,
    timeout: int = 10,
    discover_sites: bool = False,
) -> list[Company]:
    updated: list[Company] = []
    for index, company in enumerate(companies):
        if limit is not None and index >= limit:
            updated.append(company)
            continue
        website = company.website
        if discover_sites and not website:
            try:
                website = discover_website(company, timeout=timeout)
            except Exception:
                website = ""
            if website:
                company.website = website
        if website:
            try:
                homepage_text, _ = fetch_html(website, timeout=timeout)
            except Exception:
                homepage_text = ""
            if not website_looks_like_company_site(company, website, page_text=homepage_text):
                website = ""
                company.website = ""
            def validator(email: str, page_text: str) -> bool:
                return bool(validate_company_emails(company, website, [email], page_text=page_text))
            contact_found = crawl_contact_pages(website, homepage_text, timeout=timeout, email_validator=validator) if website else []
            if contact_found:
                merged = set(company.emails)
                merged.update(contact_found)
                company.emails = sorted(merged)
                company.emails = keep_consistent_email_domains(company.emails)
                if not company.source:
                    company.source = "website contact"
            found = crawl_public_emails(website, timeout=timeout, email_validator=validator) if website else []
            if found:
                merged = set(company.emails)
                merged.update(found)
                company.emails = sorted(merged)
                company.emails = keep_consistent_email_domains(company.emails)
                if not company.source:
                    company.source = "website crawl"
        if not company.emails:
            search_emails = discover_company_emails_via_search(company, timeout=timeout)
            if search_emails:
                merged = set(company.emails)
                merged.update(search_emails)
                company.emails = sorted(merged)
                company.emails = keep_consistent_email_domains(company.emails)
                if not company.source:
                    company.source = "web search"
        if not company.website and company.emails:
            inferred_website = infer_website_from_emails(company.emails)
            if inferred_website:
                company.website = inferred_website
        updated.append(company)
    return updated


def html_escape_attr(value: str) -> str:
    return html.escape(value, quote=True)


def render_export_rows(companies: list[Company]) -> list[list[str]]:
    rows = []
    seen_emails: set[str] = set()
    for company in companies:
        for export_row in company_to_export_rows(company):
            email_value = export_row["emails"].strip().lower()
            if email_value and email_value in seen_emails:
                continue
            if email_value:
                seen_emails.add(email_value)
            rows.append(
                [
                    export_row["name"],
                    export_row["legal_form"],
                    export_row["employee_label"],
                    export_row["city"],
                    export_row["canton"],
                    export_row["website"],
                    export_row["emails"],
                    export_row["source"],
                    export_row["uid"],
                ]
            )
    return rows


def export_csv_text(companies: list[Company]) -> str:
    headers = ["name", "legal_form", "employees", "city", "canton", "website", "emails", "source", "uid"]
    buffer = tempfile.SpooledTemporaryFile(mode="w+", encoding="utf-8", newline="")
    writer = csv.writer(buffer)
    writer.writerow(headers)
    for row in render_export_rows(companies):
        writer.writerow(row)
    buffer.seek(0)
    return buffer.read()


def xlsx_cell(reference: str, value: str) -> str:
    escaped = html_escape_attr(value)
    return f'<c r="{reference}" t="inlineStr"><is><t>{escaped}</t></is></c>'


def column_name(index: int) -> str:
    name = ""
    while index >= 0:
        index, remainder = divmod(index, 26)
        name = chr(65 + remainder) + name
        index -= 1
    return name


def build_xlsx_bytes(companies: list[Company]) -> bytes:
    headers = ["name", "legal_form", "employees", "city", "canton", "website", "emails", "source", "uid"]
    rows = [headers] + render_export_rows(companies)

    sheet_rows = []
    for row_index, row in enumerate(rows, start=1):
        cells = []
        for col_index, value in enumerate(row):
            cells.append(xlsx_cell(f"{column_name(col_index)}{row_index}", value))
        sheet_rows.append(f'<row r="{row_index}">' + "".join(cells) + "</row>")

    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<sheetData>{"".join(sheet_rows)}</sheetData>'
        '</worksheet>'
    )
    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets><sheet name="KMUs" sheetId="1" r:id="rId1"/></sheets></workbook>'
    )
    rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
        '</Relationships>'
    )
    root_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
        '</Relationships>'
    )
    content_types_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '</Types>'
    )

    buffer = tempfile.SpooledTemporaryFile()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", content_types_xml)
        archive.writestr("_rels/.rels", root_rels_xml)
        archive.writestr("xl/workbook.xml", workbook_xml)
        archive.writestr("xl/_rels/workbook.xml.rels", rels_xml)
        archive.writestr("xl/worksheets/sheet1.xml", sheet_xml)
    buffer.seek(0)
    return buffer.read()


def format_table(rows: list[Company]) -> str:
    headers = ["Name", "Rechtsform", "Mitarbeiter", "Ort", "Kanton", "Website", "Emails"]
    columns = [headers[:]]
    for company in rows:
        columns.append(
            [
                company.name,
                company.legal_form,
                company.employee_label,
                company.city,
                company.canton,
                company.website,
                ", ".join(company.emails),
            ]
        )
    widths = [max(len(str(row[i])) for row in columns) for i in range(len(headers))]
    lines = []
    lines.append(" | ".join(header.ljust(widths[i]) for i, header in enumerate(headers)))
    lines.append("-|-".join("-" * width for width in widths))
    for row in columns[1:]:
        lines.append(" | ".join(str(value).ljust(widths[i]) for i, value in enumerate(row)))
    return "\n".join(lines)


def html_page(
    companies: list[Company],
    params: dict[str, str],
    title: str = "KMU Finder",
    job_state: dict[str, object] | None = None,
    total_count: int | None = None,
    page: int = 1,
    page_size: int = 200,
) -> str:
    def value(name: str, default: str = "") -> str:
        return html.escape(params.get(name, default))

    rows = []
    for company in companies:
        email_html = ", ".join(html.escape(email) for email in company.emails) or "-"
        website_html = html.escape(company.website) if company.website else "-"
        if company.website:
            website_html = f'<a href="{html.escape(company.website)}" target="_blank" rel="noreferrer">{html.escape(company.website)}</a>'
        rows.append(
            "<tr>"
            f"<td>{html.escape(company.name)}</td>"
            f"<td>{html.escape(company.legal_form)}</td>"
            f"<td>{html.escape(company.employee_label)}</td>"
            f"<td>{html.escape(company.city)}</td>"
            f"<td>{html.escape(company.canton)}</td>"
            f"<td>{website_html}</td>"
            f"<td>{email_html}</td>"
            "</tr>"
        )

    status = job_state or {}
    running = bool(status.get("running"))
    state_label = "Laeuft" if running else "Bereit"
    state_class = "running" if running else "idle"

    style = """
    :root {
      --bg-1: #071824;
      --bg-2: #0b2f3f;
      --accent: #00b3a4;
      --accent-2: #f2552c;
      --card: rgba(255, 255, 255, 0.92);
      --line: #cfdbe4;
      --ink: #122333;
      --muted: #486173;
    }
    body {
      margin: 0;
      font-family: "Segoe UI", "Noto Sans", system-ui, sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at 10% 10%, rgba(0, 179, 164, 0.25), transparent 25%),
        radial-gradient(circle at 90% 20%, rgba(242, 85, 44, 0.22), transparent 32%),
        linear-gradient(135deg, var(--bg-1), var(--bg-2));
      min-height: 100vh;
    }
    .wrap { max-width: 1280px; margin: 0 auto; padding: 28px 18px 36px; }
    .hero {
      border: 1px solid rgba(255, 255, 255, 0.16);
      border-radius: 24px;
      padding: 22px;
      background: rgba(255, 255, 255, 0.1);
      backdrop-filter: blur(6px);
      color: #f4fbff;
    }
    .hero h1 { margin: 0 0 8px; font-size: clamp(1.5rem, 2.8vw, 2.1rem); }
    .hero p { margin: 0; color: rgba(244, 251, 255, 0.86); }
    .status {
      margin-top: 14px;
      display: inline-flex;
      gap: 8px;
      align-items: center;
      border-radius: 999px;
      padding: 7px 12px;
      font-weight: 700;
      font-size: 0.9rem;
      border: 1px solid rgba(255, 255, 255, 0.32);
    }
    .status.idle { background: rgba(0, 179, 164, 0.16); }
    .status.running { background: rgba(242, 85, 44, 0.18); }
    .grid {
      margin-top: 14px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
      gap: 10px;
    }
    .panel {
      margin-top: 16px;
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 16px;
      box-shadow: 0 14px 38px rgba(5, 22, 34, 0.2);
    }
    .panel h2 { margin: 0 0 10px; font-size: 1.05rem; }
    input, button, select {
      width: 100%;
      border-radius: 12px;
      border: 1px solid var(--line);
      padding: 10px 12px;
      font: inherit;
      box-sizing: border-box;
      background: #fff;
    }
    .row { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; }
    .field { display: grid; gap: 4px; }
        .field-wide { grid-column: 1 / -1; }
    .field label { font-size: 0.82rem; color: var(--muted); font-weight: 700; }
        .checks {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
            gap: 6px;
            border: 1px solid var(--line);
            border-radius: 12px;
            padding: 10px;
            background: #fff;
        }
        .checks label {
            display: flex;
            gap: 8px;
            align-items: center;
            font-size: 0.9rem;
            color: var(--ink);
            font-weight: 600;
        }
        .checks input[type="checkbox"] {
            width: auto;
            padding: 0;
            margin: 0;
        }
    .btn-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px; margin-top: 10px; }
    button {
      border: none;
      cursor: pointer;
      font-weight: 700;
      color: #fff;
      background: linear-gradient(135deg, var(--accent), #0c8f86);
    }
    button[data-action="enrich"] { background: linear-gradient(135deg, #f2552c, #d43c16); }
    button[data-action="seed"] { background: linear-gradient(135deg, #2a7de1, #1f63b8); }
    button[data-action="unlimited"] { background: linear-gradient(135deg, #9333ea, #7e22ce); }
    button[data-action="stop"] { background: linear-gradient(135deg, #7f1d1d, #ef4444); }
    .hint { margin-top: 10px; color: var(--muted); font-size: 0.92rem; }
        .stats {
            margin-top: 14px;
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 10px;
        }
        .stat {
            background: rgba(255, 255, 255, 0.92);
            border: 1px solid var(--line);
            border-radius: 16px;
            padding: 12px;
            min-height: 72px;
            box-shadow: 0 10px 26px rgba(5, 22, 34, 0.10);
        }
        .stat .label { font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.06em; color: var(--muted); }
        .stat .value { font-size: 1.6rem; font-weight: 800; margin-top: 4px; }
        .stat .sub { color: var(--muted); font-size: 0.85rem; margin-top: 4px; }
    .actions { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 12px; }
    .actions a {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: #fff;
      padding: 8px 14px;
      color: var(--ink);
      text-decoration: none;
      font-weight: 700;
    }
    .tablewrap {
      margin-top: 14px;
      overflow: auto;
      border-radius: 18px;
      background: #fff;
      border: 1px solid var(--line);
      box-shadow: 0 14px 40px rgba(5, 22, 34, 0.16);
    }
    table { width: 100%; min-width: 980px; border-collapse: collapse; }
    th, td { padding: 11px 12px; text-align: left; border-bottom: 1px solid #e5edf3; }
    th { background: #eff5fa; position: sticky; top: 0; }
    tr:hover td { background: #f8fbfd; }
    @media (max-width: 680px) {
      .wrap { padding: 16px 10px 24px; }
      .panel { padding: 12px; }
      .actions { display: grid; grid-template-columns: 1fr 1fr; }
    }
    """

    shown_count = len(companies)
    all_count = shown_count if total_count is None else max(int(total_count), 0)
    safe_page_size = max(int(page_size), 1)
    total_pages = max(1, (all_count + safe_page_size - 1) // safe_page_size) if all_count else 1
    current_page = min(max(int(page), 1), total_pages)

    def _page_link(target_page: int, label: str) -> str:
        qp = {k: v for k, v in params.items() if k != "page"}
        qp["page"] = str(target_page)
        qp["page_size"] = str(safe_page_size)
        return f'<a href="/?{urlencode(qp)}">{label}</a>'

    pagination_parts = []
    if total_pages > 1:
        if current_page > 1:
            pagination_parts.append(_page_link(1, "Erste"))
            pagination_parts.append(_page_link(current_page - 1, "Zurueck"))
        pagination_parts.append(f"Seite {current_page} / {total_pages}")
        if current_page < total_pages:
            pagination_parts.append(_page_link(current_page + 1, "Weiter"))
            pagination_parts.append(_page_link(total_pages, "Letzte"))
    pagination_html = f'<div class="actions">{"".join(pagination_parts)}</div>' if pagination_parts else ""

    filter_form = f"""
    <form method="get" class="grid">
      <input type="hidden" name="page" value="1" />
      <input type="hidden" name="page_size" value="{safe_page_size}" />
      <input name="name" placeholder="Firmenname" value="{value('name')}" />
      <input name="city" placeholder="Ort" value="{value('city')}" />
      <input name="canton" placeholder="Kanton z.B. ZH" value="{value('canton')}" />
      <input name="legal_form" placeholder="Rechtsform" value="{value('legal_form')}" />
    <input name="employees" placeholder="Mitarbeiter z.B. 10+ oder 50-200" value="{value('employees')}" />
      <input name="has_email" placeholder="Email ja/nein" value="{value('has_email')}" />
      <button type="submit">Treffer filtern</button>
    </form>
    """

    export_params = {k: v for k, v in params.items() if k not in {"page", "page_size"}}
    export_query = f"?{urlencode(export_params)}" if export_params else ""
    table_rows = "".join(rows) if rows else '<tr><td colspan="7">Keine Treffer</td></tr>'
    seed_source_options = "".join(
        f'<label><input type="checkbox" class="seed-source" value="{html.escape(source)}" checked /> {html.escape(label)}</label>'
        for source, label in SEED_SOURCE_LABELS.items()
    )

    return f"""<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{html.escape(title)}</title>
  <style>{style}</style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>{html.escape(title)}</h1>
      <p>Zefix-inspirierter Workflow: Seed, Website-Pruefung, Email-Verifikation mit Name+Ort, alles per Klick.</p>
      <div id="jobStatus" class="status {state_class}">Status: {state_label}</div>
            <div class="hint" id="jobCurrent">Aktuell: {html.escape(str(status.get('current', '')))}</div>
            <div class="hint" id="jobSummary">{html.escape(make_job_summary())}</div>
            <div class="stats">
                <div class="stat"><div class="label">Firmen geprüft</div><div class="value" id="statProcessed">{int(status.get('processed', 0))}</div><div class="sub">von <span id="statTotal">{int(status.get('total', 0))}</span></div></div>
                <div class="stat"><div class="label">Firmen gespeichert</div><div class="value" id="statAccepted">{int(status.get('accepted', 0))}</div><div class="sub">KMU mit geprüfter E-Mail</div></div>
                <div class="stat"><div class="label">Websites verifiziert</div><div class="value" id="statWebsites">{int(status.get('websites_found', 0))}</div><div class="sub">passende Firmen-Websites</div></div>
                <div class="stat"><div class="label">E-Mails verifiziert</div><div class="value" id="statEmails">{int(status.get('emails_found', 0))}</div><div class="sub">nur passende Domains</div></div>
            </div>
    </section>

    <section class="panel">
      <h2>Datenaufbau per Klick</h2>
      <div class="row">
                <div class="field">
                    <label for="limit">Seed-Firmen (Anzahl)</label>
                    <input id="limit" type="number" min="1" value="10000" />
                </div>
                <div class="field">
                    <label for="emailScan">Email-Scan (Anzahl)</label>
                    <input id="emailScan" type="number" min="0" value="800" />
                </div>
                <div class="field">
                    <label for="workers">Parallele Worker</label>
                    <input id="workers" type="number" min="1" value="1000" />
                </div>
                <div class="field">
                    <label for="timeout">Timeout pro Request (Sek.)</label>
                    <input id="timeout" type="number" min="3" value="10" />
                </div>
                <div class="field">
                    <label for="discover">Website-Suche</label>
                    <select id="discover">
                        <option value="1">Website-Suche an</option>
                        <option value="0">Website-Suche aus</option>
                    </select>
                </div>
                <div class="field field-wide">
                    <label>Seed-Quellen</label>
                    <div id="seedSources" class="checks">{seed_source_options}</div>
                </div>
      </div>
      <div class="btn-row">
        <button data-action="bootstrap">Seed + Email-Scan starten</button>
        <button data-action="seed">Nur Seed starten</button>
        <button data-action="enrich">Seed stoppen + E-Mail-Scan starten</button>
                <button data-action="unlimited">Unbegrenzte Suche starten</button>
                <button data-action="stop">Job stoppen</button>
      </div>
      <div class="hint">Die Jobs laufen im Hintergrund. "Unbegrenzte Suche" läuft bis zum Stop-Button - perfekt für 24/7 Betrieb.</div>
    </section>

    <section class="panel">
      <h2>Treffer filtern</h2>
      {filter_form}
      <div class="actions">
        <a href="/export.csv{export_query}">CSV exportieren</a>
        <a href="/export.xlsx{export_query}">XLSX exportieren</a>
      </div>
            <div class="hint">Treffer gesamt: {all_count} | angezeigt: {shown_count} (Seite {current_page} / {total_pages})</div>
            {pagination_html}
    </section>

    <div class="tablewrap">
      <table>
        <thead>
          <tr><th>Name</th><th>Rechtsform</th><th>Mitarbeiter</th><th>Ort</th><th>Kanton</th><th>Website</th><th>Emails</th></tr>
        </thead>
        <tbody>{table_rows}</tbody>
      </table>
    </div>
  </div>

  <script>
    const statusEl = document.getElementById('jobStatus');
    const controls = {{
      limit: document.getElementById('limit'),
      emailScan: document.getElementById('emailScan'),
            workers: document.getElementById('workers'),
      timeout: document.getElementById('timeout'),
            discover: document.getElementById('discover'),
            seedSources: Array.from(document.querySelectorAll('input.seed-source'))
    }};

        const buttons = Array.from(document.querySelectorAll('button[data-action]'));

        function updateStats(data) {{
            document.getElementById('statProcessed').textContent = data.processed ?? 0;
            document.getElementById('statTotal').textContent = data.total ?? 0;
            document.getElementById('statAccepted').textContent = data.accepted ?? 0;
            document.getElementById('statWebsites').textContent = data.websites_found ?? 0;
            document.getElementById('statEmails').textContent = data.emails_found ?? 0;
            document.getElementById('jobCurrent').textContent = 'Aktuell: ' + (data.current || '-');
            document.getElementById('jobSummary').textContent =
                `${{data.processed ?? 0}}/${{data.total ?? 0}} geprueft | ` +
                `${{data.accepted ?? 0}} gespeichert | ` +
                `${{data.websites_found ?? 0}} Websites | ` +
                `${{data.emails_found ?? 0}} E-Mails | ` +
                `${{data.skipped ?? 0}} uebersprungen | ` +
                `${{data.errors ?? 0}} Fehler`;
        }}

        function setBusy(busy) {{
            buttons.forEach((btn) => {{
                btn.disabled = busy;
                btn.style.opacity = busy ? '0.7' : '1';
                btn.style.cursor = busy ? 'wait' : 'pointer';
            }});
        }}

    async function refreshStatus() {{
      try {{
        const response = await fetch('/api/job');
        const data = await response.json();
                updateStats(data);
        const running = Boolean(data.running);
        statusEl.className = 'status ' + (running ? 'running' : 'idle');
                if (running && data.stopped) {{
                    statusEl.textContent = 'Status: Stop angefordert...';
                }} else {{
                    statusEl.textContent = running
                        ? `Status: Laeuft (${{data.type || 'job'}})`
                        : `Status: Bereit${{data.last_error ? ' | Fehler: ' + data.last_error : ''}}`;
                }}
      }} catch (error) {{
        statusEl.className = 'status running';
        statusEl.textContent = 'Status: Verbindung fehlgeschlagen';
      }}
    }}

    async function startJob(action) {{
            if (action === 'stop') {{
                const response = await fetch('/api/stop', {{ method: 'GET' }});
                const data = await response.json();
                if (!response.ok) {{
                    alert(data.message || 'Job konnte nicht gestoppt werden');
                    return;
                }}
                statusEl.textContent = 'Status: Stop angefordert';
                await refreshStatus();
                return;
            }}
      const payload = new URLSearchParams();
      
      // Für unbegrenzte Suche: setze limit auf sehr hohe Zahl
            if (action === 'unlimited') {{
                payload.set('limit', '1000000000');
                payload.set('email_scan', '1000000000');
      }} else {{
        payload.set('limit', controls.limit.value || '10000');
        payload.set('email_scan', controls.emailScan.value || '800');
      }}
      
                        payload.set('workers', controls.workers.value || '1000');
      payload.set('timeout', controls.timeout.value || '10');
      payload.set('discover', controls.discover.value || '1');
            const selectedSeedSources = controls.seedSources.filter((item) => item.checked).map((item) => item.value);
            if ((action === 'seed' || action === 'bootstrap' || action === 'unlimited') && selectedSeedSources.length === 0) {{
                alert('Bitte mindestens eine Seed-Quelle auswaehlen.');
                return;
            }}
            payload.set('seed_sources', selectedSeedSources.join(','));
            statusEl.textContent = 'Status: starte unbegrenzte Suche...';
            setBusy(true);
      const response = await fetch('/api/run/' + (action === 'unlimited' ? 'bootstrap' : action), {{
        method: 'POST',
        headers: {{'Content-Type': 'application/x-www-form-urlencoded'}},
        body: payload.toString()
      }});
      if (!response.ok) {{
        const txt = await response.text();
                setBusy(false);
        alert('Job konnte nicht gestartet werden: ' + txt);
                return;
      }}
            setBusy(false);
      await refreshStatus();
    }}

        buttons.forEach((btn) => {{
      btn.addEventListener('click', async (event) => {{
        event.preventDefault();
        await startJob(btn.dataset.action);
      }});
    }});

    refreshStatus();
    setInterval(refreshStatus, 3000);
  </script>
</body>
</html>"""


def job_status_snapshot() -> dict[str, object]:
    with JOB_LOCK:
        return dict(JOB_STATE)


def start_background_job(job_type: str, task: Callable[[], None]) -> tuple[bool, str]:
    with JOB_LOCK:
        if JOB_STATE["running"]:
            return False, "Es laeuft bereits ein Job."
        JOB_STOP_EVENT.clear()
        JOB_STATE["running"] = True
        JOB_STATE["type"] = job_type
        JOB_STATE["started_at"] = time.time()
        JOB_STATE["finished_at"] = 0.0
        JOB_STATE["last_error"] = ""
        JOB_STATE["last_message"] = "gestartet"
        JOB_STATE["phase"] = job_type
        JOB_STATE["total"] = 0
        JOB_STATE["processed"] = 0
        JOB_STATE["accepted"] = 0
        JOB_STATE["websites_found"] = 0
        JOB_STATE["emails_found"] = 0
        JOB_STATE["skipped"] = 0
        JOB_STATE["errors"] = 0
        JOB_STATE["current"] = ""
        JOB_STATE["stopped"] = False

    def runner() -> None:
        error = ""
        message = "fertig"
        try:
            task()
        except Exception as exc:
            error = str(exc)
            message = "fehlgeschlagen"
        finally:
            with JOB_LOCK:
                JOB_STATE["running"] = False
                JOB_STATE["finished_at"] = time.time()
                JOB_STATE["last_error"] = error
                JOB_STATE["last_message"] = "gestoppt" if JOB_STATE.get("stopped") else message
                JOB_STATE["phase"] = "idle"
                JOB_STATE["current"] = "" if not error else f"Fehler: {error}"

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    return True, "Job gestartet"


class KMURequestHandler(BaseHTTPRequestHandler):
    dataset_path: Path = Path("companies.json")
    _dataset_cache_lock = threading.Lock()
    _dataset_cache_mtime_ns: int | None = None
    _dataset_cache_path: Path | None = None
    _dataset_cache_companies: list[Company] | None = None

    def _load(self) -> list[Company]:
        path = self.dataset_path
        try:
            mtime_ns = path.stat().st_mtime_ns
        except FileNotFoundError:
            return []

        with self._dataset_cache_lock:
            if (
                self._dataset_cache_path == path
                and self._dataset_cache_mtime_ns == mtime_ns
                and self._dataset_cache_companies is not None
            ):
                return list(self._dataset_cache_companies)

        companies = load_companies(path)
        with self._dataset_cache_lock:
            self._dataset_cache_path = path
            self._dataset_cache_mtime_ns = mtime_ns
            self._dataset_cache_companies = list(companies)
        return companies

    def _send(self, content: str, status: int = 200, content_type: str = "text/html; charset=utf-8") -> None:
        payload = content.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _send_error_response(self, status: int, message: str) -> None:
        body = json.dumps({"ok": False, "message": message}, ensure_ascii=False, indent=2)
        self._send(body, status=status, content_type="application/json; charset=utf-8")

    def _safe_handle(self, func: Callable[[], None]) -> None:
        try:
            func()
        except Exception as exc:
            error_text = f"{type(exc).__name__}: {exc}"
            print("[web-error]", error_text, file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)
            if not self.wfile.closed:
                try:
                    self._send_error_response(500, error_text)
                except Exception:
                    pass

    def _filtered_companies(self, params: dict[str, str]) -> list[Company]:
        return [company for company in self._load() if company_matches(company, params)]

    def _query_params(self, query: str) -> dict[str, str]:
        return {key: values[-1] for key, values in parse_qs(query).items() if values}

    def _post_params(self) -> dict[str, str]:
        raw_len = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(raw_len).decode("utf-8", errors="ignore")
        return {key: values[-1] for key, values in parse_qs(body, keep_blank_values=True).items() if values}

    def _to_int(self, payload: dict[str, str], key: str, default: int, min_value: int = 0) -> int:
        try:
            value = int(payload.get(key, str(default)).strip())
            return value if value >= min_value else default
        except Exception:
            return default

    def _to_bool(self, payload: dict[str, str], key: str, default: bool) -> bool:
        raw = normalize_text(payload.get(key))
        if not raw:
            return default
        return raw in {"1", "true", "yes", "ja", "y", "on"}

    def do_GET(self) -> None:  # noqa: N802
        def handler() -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/api/search":
                params = self._query_params(parsed.query)
                companies = self._filtered_companies(params)
                body = json.dumps(serialize_companies(companies), ensure_ascii=False, indent=2)
                self._send(body, content_type="application/json; charset=utf-8")
                return
            if parsed.path == "/api/job":
                body = json.dumps(job_status_snapshot(), ensure_ascii=False, indent=2)
                self._send(body, content_type="application/json; charset=utf-8")
                return
            if parsed.path == "/api/stop":
                ok = request_job_stop()
                body = json.dumps({"ok": ok, "message": "Stop angefordert" if ok else "Kein laufender Job"}, ensure_ascii=False, indent=2)
                status = 202 if ok else 409
                self._send(body, status=status, content_type="application/json; charset=utf-8")
                return
            if parsed.path == "/api/emails":
                params = parse_qs(parsed.query)
                website = (params.get("website") or [""])[0]
                body = json.dumps({"website": website, "emails": crawl_public_emails(website)}, ensure_ascii=False, indent=2)
                self._send(body, content_type="application/json; charset=utf-8")
                return
            if parsed.path in {"/export.csv", "/export.xlsx"}:
                params = self._query_params(parsed.query)
                companies = self._filtered_companies(params)
                if parsed.path == "/export.csv":
                    self._send(export_csv_text(companies), content_type="text/csv; charset=utf-8")
                else:
                    xlsx_bytes = build_xlsx_bytes(companies)
                    self.send_response(200)
                    self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                    self.send_header("Content-Length", str(len(xlsx_bytes)))
                    self.send_header("Content-Disposition", 'attachment; filename="kmus.xlsx"')
                    self.end_headers()
                    self.wfile.write(xlsx_bytes)
                return
            params = self._query_params(parsed.query)
            companies = self._filtered_companies(params)
            page = self._to_int(params, "page", 1, min_value=1)
            page_size = min(self._to_int(params, "page_size", 200, min_value=1), 1000)
            start = (page - 1) * page_size
            paged_companies = companies[start : start + page_size]
            self._send(
                html_page(
                    paged_companies,
                    params,
                    job_state=job_status_snapshot(),
                    total_count=len(companies),
                    page=page,
                    page_size=page_size,
                )
            )

        self._safe_handle(handler)

    def do_POST(self) -> None:  # noqa: N802
        def handler() -> None:
            parsed = urlparse(self.path)
            if not parsed.path.startswith("/api/run/"):
                self._send("Not found", status=404)
                return
            payload = self._post_params()
            limit = self._to_int(payload, "limit", ZEFIX_DEFAULT_LIMIT, min_value=1)
            email_scan = self._to_int(payload, "email_scan", 800, min_value=0)
            workers = self._to_int(payload, "workers", DEFAULT_WORKERS, min_value=1)
            timeout = self._to_int(payload, "timeout", 10, min_value=3)
            discover = self._to_bool(payload, "discover", True)
            if "seed_sources" in payload:
                seed_sources = parse_seed_sources(payload.get("seed_sources"), default_on_empty=False)
                if not seed_sources:
                    self._send("Bitte mindestens eine Seed-Quelle auswaehlen.", status=400)
                    return
            else:
                seed_sources = parse_seed_sources(None)

            def start_seed() -> None:
                args = argparse.Namespace(
                    data=str(self.dataset_path),
                    out=str(self.dataset_path),
                    limit=limit,
                    workers=workers,
                    seed_sources=seed_sources,
                )
                command_seed_zefix(args)

            def start_enrich() -> None:
                args = argparse.Namespace(
                    data=str(self.dataset_path),
                    out=str(self.dataset_path),
                    limit=email_scan,
                    timeout=timeout,
                    discover_websites=discover,
                    workers=workers,
                )
                command_enrich(args)

            def start_bootstrap() -> None:
                args = argparse.Namespace(
                    data=str(self.dataset_path),
                    out=str(self.dataset_path),
                    limit=limit,
                    email_scan=email_scan,
                    timeout=timeout,
                    discover_websites=discover,
                    workers=workers,
                    seed_sources=seed_sources,
                )
                command_bootstrap(args)

            action = parsed.path.removeprefix("/api/run/")
            task_map: dict[str, Callable[[], None]] = {
                "seed": start_seed,
                "enrich": start_enrich,
                "bootstrap": start_bootstrap,
            }
            if action not in task_map:
                self._send("Unbekannte Aktion", status=400)
                return
            ok, message = start_background_job(action, task_map[action])
            status = 202 if ok else 409
            body = json.dumps({"ok": ok, "message": message, "action": action}, ensure_ascii=False)
            self._send(body, status=status, content_type="application/json; charset=utf-8")

        self._safe_handle(handler)

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Schweizer KMU filtern und öffentliche E-Mails sammeln.")
    parser.add_argument("--data", default="companies.json", help="Pfad zur JSON-Datenbank")
    subparsers = parser.add_subparsers(dest="command", required=True)

    seed_parser = subparsers.add_parser("seed-zefix", help="Schweizer Firmen aus mehreren Quellen laden (u.a. Zefix, search.ch, local.ch, moneyhouse, swissguide, tel.search.ch)")
    seed_parser.add_argument("--limit", type=int, default=ZEFIX_DEFAULT_LIMIT)
    seed_parser.add_argument(
        "--seed-sources",
        default=",".join(DEFAULT_SEED_SOURCES),
        help="Kommagetrennte Seed-Quellen (zefix,search.ch,tel.search.ch,local.ch,local.ch.ch,moneyhouse,swissguide)",
    )
    seed_parser.add_argument("--out", default=None, help="Zielpfad für JSON")

    bootstrap_parser = subparsers.add_parser("bootstrap", help="Einfacher Start: Seed + optionale E-Mail-Anreicherung")
    bootstrap_parser.add_argument("--limit", type=int, default=ZEFIX_DEFAULT_LIMIT, help="Anzahl Firmen für den Seed")
    bootstrap_parser.add_argument("--email-scan", type=int, default=800, help="Wie viele Firmen für E-Mail-Crawling prüfen")
    bootstrap_parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Anzahl paralleler Worker")
    bootstrap_parser.add_argument("--timeout", type=int, default=10)
    bootstrap_parser.add_argument(
        "--seed-sources",
        default=",".join(DEFAULT_SEED_SOURCES),
        help="Kommagetrennte Seed-Quellen (zefix,search.ch,tel.search.ch,local.ch,local.ch.ch,moneyhouse,swissguide)",
    )
    bootstrap_parser.add_argument("--discover-websites", action="store_true", help="Webseiten für Firmen ohne URL automatisch suchen")
    bootstrap_parser.add_argument("--out", default=None, help="Zielpfad für JSON")

    import_parser = subparsers.add_parser("import-csv", help="Firmen aus CSV importieren")
    import_parser.add_argument("csv_path", help="CSV-Datei")
    import_parser.add_argument("--out", help="Zielpfad für JSON", default=None)

    search_parser = subparsers.add_parser("search", help="Firmen filtern")
    search_parser.add_argument("--name", default="")
    search_parser.add_argument("--legal-form", default="")
    search_parser.add_argument("--employees", default="")
    search_parser.add_argument("--city", default="")
    search_parser.add_argument("--canton", default="")
    search_parser.add_argument("--website", default="")
    search_parser.add_argument("--has-email", default="")
    search_parser.add_argument("--has-website", default="")

    enrich_parser = subparsers.add_parser("enrich", help="Öffentliche E-Mails von Websites crawlen")
    enrich_parser.add_argument("--limit", type=int, default=None, help="Optional nur die ersten N Firmen bearbeiten")
    enrich_parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Anzahl paralleler Worker")
    enrich_parser.add_argument("--timeout", type=int, default=10)
    enrich_parser.add_argument("--discover-websites", action="store_true", help="Vor dem Crawling nach Webseiten suchen")
    enrich_parser.add_argument("--out", default=None, help="Ausgabedatei für angereicherte Daten")

    export_parser = subparsers.add_parser("export", help="Firmen nach CSV oder XLSX exportieren")
    export_parser.add_argument("--format", choices=("csv", "xlsx"), default="csv")
    export_parser.add_argument("--out", required=True)
    export_parser.add_argument("--name", default="")
    export_parser.add_argument("--legal-form", default="")
    export_parser.add_argument("--employees", default="")
    export_parser.add_argument("--city", default="")
    export_parser.add_argument("--canton", default="")
    export_parser.add_argument("--website", default="")
    export_parser.add_argument("--has-email", default="")
    export_parser.add_argument("--has-website", default="")

    serve_parser = subparsers.add_parser("serve", help="Weboberfläche starten")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8000)
    serve_parser.add_argument("--reload-data", action="store_true", help="Nur Hinweis für Standard-JSON-Pfad")
    return parser


def normalize_argv(argv: list[str]) -> list[str]:
    if "--data" not in argv or not argv:
        return argv
    if argv[0].startswith("-"):
        return argv
    data_index = argv.index("--data")
    if data_index < 0 or data_index + 1 >= len(argv):
        return argv
    data_flag = argv[data_index : data_index + 2]
    remaining = argv[:data_index] + argv[data_index + 2 :]
    return [*data_flag, *remaining]


def command_import_csv(args: argparse.Namespace) -> int:
    source = Path(args.csv_path)
    destination = Path(args.out or args.data)
    companies = import_csv(source)
    save_companies(destination, companies)
    print(f"Importiert: {len(companies)} Firmen -> {destination}")
    return 0


def command_seed_zefix(args: argparse.Namespace) -> int:
    destination = Path(args.out or args.data)
    selected_sources = parse_seed_sources(getattr(args, "seed_sources", None))
    job_update(
        phase="seed",
        total=args.limit,
        processed=0,
        accepted=0,
        websites_found=0,
        emails_found=0,
        skipped=0,
        errors=0,
        current=f"Seed lädt (Quellen: {format_seed_sources(selected_sources)})...",
    )
    companies = seed_from_multi_sources(
        limit=args.limit,
        on_progress=lambda processed, total, current: job_update(processed=processed, total=total, current=current),
        enabled_sources=selected_sources,
    )
    save_companies(destination, companies)
    print(f"Seed geladen: {len(companies)} Firmen -> {destination}")
    job_update(current="Seed fertig", last_message="Seed fertig")
    return 0


def command_bootstrap(args: argparse.Namespace) -> int:
    destination = Path(args.out or args.data)
    selected_sources = parse_seed_sources(getattr(args, "seed_sources", None))
    job_update(
        phase="seed",
        total=args.limit,
        processed=0,
        accepted=0,
        websites_found=0,
        emails_found=0,
        skipped=0,
        errors=0,
        current=f"Seed lädt (Quellen: {format_seed_sources(selected_sources)})...",
    )
    companies = seed_from_multi_sources(
        limit=args.limit,
        on_progress=lambda processed, total, current: job_update(processed=processed, total=total, current=current),
        source_workers=resolve_worker_count(getattr(args, "workers", None)),
        enabled_sources=selected_sources,
    )
    print(f"Seed geladen: {len(companies)} Firmen")

    stored_companies = {company_key(company): company for company in load_companies(destination)}
    for company in companies:
        key = company_key(company)
        existing = stored_companies.get(key)
        stored_companies[key] = merge_companies(existing, company) if existing is not None else company
    # Seeds sofort sichern, damit ein späterer E-Mail-Scan auf dem vorhandenen Bestand weiterarbeiten kann.
    save_companies(destination, stored_companies.values())
    workers = resolve_worker_count(getattr(args, "workers", None))

    def persist() -> None:
        save_companies(destination, stored_companies.values())

    if args.email_scan > 0:
        scan_count = min(args.email_scan, len(companies))
        pending: list[Company] = []
        for company in companies[:scan_count]:
            existing = stored_companies.get(company_key(company))
            pending.append(merge_companies(existing, company))

        job_update(
            phase="bootstrap",
            total=len(pending),
            processed=0,
            accepted=len(stored_companies),
            websites_found=0,
            emails_found=0,
            skipped=0,
            errors=0,
            current="",
            last_message="Starte parallelen Email-Scan nach Seed",
        )
        print(f"Starte E-Mail-Anreicherung für {len(pending)} Firmen mit {workers} parallelen Prozessen...")

        processed_since_persist = 0
        persist_every = 25

        def handle_result(working: Company, stats: dict[str, int]) -> None:
            nonlocal processed_since_persist
            key = company_key(working)
            processed_since_persist += 1
            job_increment(
                processed=1,
                websites_found=int(stats.get("websites_found", 0)),
                emails_found=int(stats.get("emails_found", 0)),
                errors=int(stats.get("errors", 0)),
            )
            job_set_current(working.name)
            stored_companies[key] = merge_companies(stored_companies.get(key), working)
            job_increment(accepted=1)
            if not working.emails:
                job_increment(skipped=1)
            if processed_since_persist >= persist_every:
                persist()
                processed_since_persist = 0
            print(f"  Fortschritt: {make_job_summary()}")

        process_companies_parallel(
            pending,
            timeout=args.timeout,
            discover_sites=args.discover_websites,
            workers=workers,
            on_result=handle_result,
        )
        persist()

    persist()
    with_emails = sum(1 for company in stored_companies.values() if company.emails)
    with_websites = sum(1 for company in stored_companies.values() if company.website)
    print(f"Bootstrap fertig -> {destination}")
    print(f"Mit Website: {with_websites} | Mit E-Mail: {with_emails}")
    return 0


def command_search(args: argparse.Namespace) -> int:
    companies = load_companies(Path(args.data))
    filters = {
        "name": args.name,
        "legal_form": args.legal_form,
        "employees": args.employees,
        "city": args.city,
        "canton": args.canton,
        "website": args.website,
        "has_email": args.has_email,
        "has_website": args.has_website,
    }
    matches = [company for company in companies if company_matches(company, filters)]
    print(format_table(matches))
    print(f"\nTreffer: {len(matches)}")
    return 0


def command_enrich(args: argparse.Namespace) -> int:
    data_path = Path(args.data)
    target = Path(args.out or data_path)
    companies = load_companies(data_path)
    workers = resolve_worker_count(getattr(args, "workers", None))
    target_count = min(args.limit, len(companies)) if args.limit is not None else len(companies)
    job_update(phase="enrich", total=target_count, processed=0, accepted=0, websites_found=0, emails_found=0, skipped=0, errors=0, current="Email-Scan startet...")
    pending = companies[:target_count]
    job_update(
        phase="enrich",
        total=len(pending),
        processed=0,
        accepted=0,
        websites_found=0,
        emails_found=0,
        skipped=0,
        errors=0,
        current="",
        last_message="Starte parallelen Email-Scan",
    )

    stored_companies = {company_key(company): company for company in companies}

    def persist() -> None:
        save_companies(target, stored_companies.values())

    def handle_result(working: Company, stats: dict[str, int]) -> None:
        key = company_key(working)
        stored_companies[key] = merge_companies(stored_companies.get(key), working)
        job_increment(
            processed=1,
            websites_found=int(stats.get("websites_found", 0)),
            emails_found=int(stats.get("emails_found", 0)),
            errors=int(stats.get("errors", 0)),
        )
        job_set_current(working.name)
        job_increment(accepted=1)
        if not working.emails:
            job_increment(skipped=1)
        persist()

    process_companies_parallel(
        pending,
        timeout=args.timeout,
        discover_sites=args.discover_websites,
        workers=workers,
        on_result=handle_result,
    )
    persist()
    print(f"Angereichert: {len(stored_companies)} Firmen -> {target}")
    return 0


def command_export(args: argparse.Namespace) -> int:
    companies = load_companies(Path(args.data))
    filters = {
        "name": args.name,
        "legal_form": args.legal_form,
        "employees": args.employees,
        "city": args.city,
        "canton": args.canton,
        "website": args.website,
        "has_email": args.has_email,
        "has_website": args.has_website,
    }
    matches = [company for company in companies if company_matches(company, filters)]
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if args.format == "csv":
        out_path.write_text(export_csv_text(matches), encoding="utf-8")
    else:
        out_path.write_bytes(build_xlsx_bytes(matches))
    print(f"Exportiert: {len(matches)} Firmen -> {out_path}")
    return 0


def command_serve(args: argparse.Namespace) -> int:
    dataset_path = Path(args.data)
    if not dataset_path.exists():
        print("Keine Datendatei gefunden, ich lade automatisch einen Multi-Seed (Zefix + search.ch + local.ch).")
        seed_companies = seed_from_multi_sources(limit=ZEFIX_DEFAULT_LIMIT)
        save_companies(dataset_path, seed_companies)
        print(f"Seed geladen: {len(seed_companies)} Firmen -> {dataset_path}")
    KMURequestHandler.dataset_path = dataset_path
    server = ThreadingHTTPServer((args.host, args.port), KMURequestHandler)
    print(f"Server läuft auf http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer beendet.")
    finally:
        server.server_close()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    effective_argv = normalize_argv(list(argv) if argv is not None else sys.argv[1:])
    args = parser.parse_args(effective_argv)

    if args.command == "import-csv":
        return command_import_csv(args)
    if args.command == "seed-zefix":
        args.seed_sources = parse_seed_sources(getattr(args, "seed_sources", None))
        return command_seed_zefix(args)
    if args.command == "bootstrap":
        args.seed_sources = parse_seed_sources(getattr(args, "seed_sources", None))
        return command_bootstrap(args)
    if args.command == "search":
        return command_search(args)
    if args.command == "enrich":
        return command_enrich(args)
    if args.command == "export":
        return command_export(args)
    if args.command == "serve":
        return command_serve(args)
    parser.error("Unbekannter Befehl")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
