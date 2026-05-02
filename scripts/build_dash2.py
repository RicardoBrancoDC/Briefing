#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_dash2.py

Gera o arquivo site/dashboard_data2.json para o dashboard2.

Entrada principal:
  .cache/historico_alertas.json

A ideia é NÃO simplificar demais o CAP. O script preserva os campos já
extraídos pelo idap_daily_maps.py e acrescenta campos derivados para uso
do dashboard: data, hora, status de vigência, duração, tempo desde emissão,
nome curto do emissor, evento curto, localização e agregações.
"""

import json
import os
import re
import random
import shutil
import time
import unicodedata
from urllib.parse import urljoin
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo


DEFAULT_HISTORY_PATH = ".cache/historico_alertas.json"
DEFAULT_SITE_DIR = "site"
DEFAULT_WINDOW_HOURS = 24
DEFAULT_GEOJSON_SOURCE = "resources/br_uf.geojson"
DEFAULT_GEOJSON_TARGET = "site/data/br_uf.geojson"

# Bandeiras municipais, opcional e incremental.
# Se o site externo bloquear ou falhar, o dashboard continua funcionando.
DEFAULT_FLAG_DOWNLOAD_ENABLED = "1"
DEFAULT_FLAG_BASE_URL = "https://www.mbi.com.br"
DEFAULT_FLAG_MAX_DOWNLOADS = 5
DEFAULT_FLAG_MIN_SLEEP = 8
DEFAULT_FLAG_MAX_SLEEP = 15
DEFAULT_FLAG_DIR = "site/assets/flags/municipios"
DEFAULT_FLAG_FAILURES_PATH = ".cache/flag_failures.json"
DEFAULT_FLAG_FAILURE_RETRY_HOURS = 168

TZ_BRASILIA = ZoneInfo("America/Sao_Paulo")

NIVEL_ORDER = ["Baixo", "Médio", "Alto", "Severo", "Extremo", "Indefinido"]
STATUS_ORDER = ["vigente", "futuro", "expirado", "sem_validade"]

UF_TO_REGION = {
    "AC": "N", "AP": "N", "AM": "N", "PA": "N", "RO": "N", "RR": "N", "TO": "N",
    "AL": "NE", "BA": "NE", "CE": "NE", "MA": "NE", "PB": "NE", "PE": "NE",
    "PI": "NE", "RN": "NE", "SE": "NE",
    "DF": "CO", "GO": "CO", "MT": "CO", "MS": "CO",
    "ES": "SE", "MG": "SE", "RJ": "SE", "SP": "SE",
    "PR": "S", "RS": "S", "SC": "S",
}

STATE_NAME_TO_UF = {
    "ACRE": "AC", "ALAGOAS": "AL", "AMAPA": "AP", "AMAPÁ": "AP",
    "AMAZONAS": "AM", "BAHIA": "BA", "CEARA": "CE", "CEARÁ": "CE",
    "DISTRITO FEDERAL": "DF", "ESPIRITO SANTO": "ES", "ESPÍRITO SANTO": "ES",
    "GOIAS": "GO", "GOIÁS": "GO", "MARANHAO": "MA", "MARANHÃO": "MA",
    "MATO GROSSO": "MT", "MATO GROSSO DO SUL": "MS", "MINAS GERAIS": "MG",
    "PARA": "PA", "PARÁ": "PA", "PARAIBA": "PB", "PARAÍBA": "PB",
    "PARANA": "PR", "PARANÁ": "PR", "PERNAMBUCO": "PE", "PIAUI": "PI",
    "PIAUÍ": "PI", "RIO DE JANEIRO": "RJ", "RIO GRANDE DO NORTE": "RN",
    "RIO GRANDE DO SUL": "RS", "RONDONIA": "RO", "RONDÔNIA": "RO",
    "RORAIMA": "RR", "SANTA CATARINA": "SC", "SAO PAULO": "SP",
    "SÃO PAULO": "SP", "SERGIPE": "SE", "TOCANTINS": "TO",
}


ESTADOS_MBI = {
    "AC": "acre",
    "AL": "alagoas",
    "AP": "amapa",
    "AM": "amazonas",
    "BA": "bahia",
    "CE": "ceara",
    "ES": "espirito-santo",
    "GO": "goias",
    "MA": "maranhao",
    "MT": "mato-grosso",
    "MS": "mato-grosso-do-sul",
    "MG": "minas-gerais",
    "PA": "para",
    "PB": "paraiba",
    "PR": "parana",
    "PE": "pernambuco",
    "PI": "piaui",
    "RJ": "rio-de-janeiro",
    "RN": "rio-grande-do-norte",
    "RS": "rio-grande-do-sul",
    "RO": "rondonia",
    "RR": "roraima",
    "SC": "santa-catarina",
    "SP": "sao-paulo",
    "SE": "sergipe",
    "TO": "tocantins",
}


def load_json(path: Path, default: Any) -> Any:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None

    txt = str(value).strip()
    if not txt:
        return None

    if txt.endswith("Z"):
        txt = txt[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(TZ_BRASILIA)
    except Exception:
        return None


def normalize_text(value: Optional[str]) -> str:
    txt = (value or "").strip()
    if not txt:
        return ""

    txt = unicodedata.normalize("NFKD", txt)
    txt = "".join(c for c in txt if not unicodedata.combining(c))
    return txt.upper()


def short_event(value: Optional[str]) -> str:
    txt = (value or "").strip()
    if not txt:
        return "Não informado"

    if " - " in txt:
        txt = txt.split(" - ")[-1].strip()

    replacements = {
        "TEMPESTADE LOCAL/CONVECTIVA": "TEMPESTADE",
        "CORRIDA DE MASSA/SOLO/LAMA": "SOLO/LAMA",
        "CORRIDA DE MASSA/ROCHAS/DETRITOS": "ROCHAS/DETRITOS",
        "FRENTES FRIAS OU ZONAS DE CONVERGÊNCIA": "FRENTES FRIAS/ZONAS DE CONVERGÊNCIA",
    }

    for old, new in replacements.items():
        txt = txt.replace(old, new)

    return txt.title()


def short_emitter(value: Optional[str], max_len: int = 32) -> str:
    txt = (value or "Emissor não informado").strip()

    patterns = [
        r"^Defesa Civil Estadual de\s+(.+)$",
        r"^Defesa Civil Estadual do\s+(.+)$",
        r"^Defesa Civil Estadual da\s+(.+)$",
    ]

    for pat in patterns:
        m = re.match(pat, txt, flags=re.IGNORECASE)
        if m:
            out = f"DC {m.group(1).strip()}"
            return out if len(out) <= max_len else out[:max_len - 1].rstrip() + "…"

    m = re.match(r"^Defesa Civil de\s+(.+)$", txt, flags=re.IGNORECASE)
    if m:
        out = f"DC {m.group(1).strip()}"
        return out if len(out) <= max_len else out[:max_len - 1].rstrip() + "…"

    return txt if len(txt) <= max_len else txt[:max_len - 1].rstrip() + "…"


def guess_uf_from_text(value: Optional[str]) -> str:
    txt = (value or "").strip().upper()
    if not txt:
        return ""

    patterns = [
        r"/([A-Z]{2})\b",
        r"-([A-Z]{2})\b",
        r"\(([A-Z]{2})\)",
        r"\b([A-Z]{2})\b$",
    ]

    for pat in patterns:
        m = re.search(pat, txt)
        if m and m.group(1) in UF_TO_REGION:
            return m.group(1)

    n = normalize_text(txt)
    for state_name, uf in sorted(STATE_NAME_TO_UF.items(), key=lambda x: -len(x[0])):
        if normalize_text(state_name) in n:
            return uf

    return ""


def derive_uf(alert: Dict[str, Any]) -> str:
    return (
        (alert.get("uf_hint") or "").strip().upper()
        or guess_uf_from_text(alert.get("areaDesc"))
        or guess_uf_from_text(alert.get("senderName"))
    )


def derive_location(alert: Dict[str, Any]) -> str:
    area = (alert.get("areaDesc") or "").strip()
    uf = derive_uf(alert)

    if area and len(area) <= 80:
        return area

    sender = (alert.get("senderName") or "").strip()
    if sender and len(sender) <= 80:
        return sender

    return uf or "Não informado"


def classify_status(now_dt: datetime, onset_dt: Optional[datetime], expires_dt: Optional[datetime]) -> str:
    if onset_dt and onset_dt > now_dt:
        return "futuro"

    if expires_dt:
        return "vigente" if expires_dt >= now_dt else "expirado"

    return "sem_validade"


def duration_minutes(onset_dt: Optional[datetime], expires_dt: Optional[datetime]) -> Optional[int]:
    if not onset_dt or not expires_dt:
        return None

    minutes = int((expires_dt - onset_dt).total_seconds() // 60)
    return minutes if minutes >= 0 else None


def time_since_minutes(now_dt: datetime, onset_dt: Optional[datetime]) -> Optional[int]:
    if not onset_dt:
        return None

    minutes = int((now_dt - onset_dt).total_seconds() // 60)
    return minutes if minutes >= 0 else None


def duration_bucket(minutes: Optional[int]) -> str:
    if minutes is None:
        return "Sem validade"

    h = minutes / 60

    if h <= 2:
        return "Até 2h"
    if h <= 6:
        return "2h a 6h"
    if h <= 12:
        return "6h a 12h"
    if h <= 24:
        return "12h a 24h"

    return "Mais de 24h"


def time_since_bucket(minutes: Optional[int]) -> str:
    if minutes is None:
        return "Sem data"

    h = minutes / 60

    if h <= 1:
        return "Até 1h"
    if h <= 3:
        return "1h a 3h"
    if h <= 6:
        return "3h a 6h"
    if h <= 12:
        return "6h a 12h"

    return "Mais de 12h"


def category_label(value: Optional[str]) -> str:
    txt = (value or "").strip()
    return txt if txt else "Sem categoria"


def event_category_fallback(event: Optional[str]) -> str:
    n = normalize_text(event)

    if any(k in n for k in ["CHUVA", "VENDAVAL", "TEMPESTADE", "GRANIZO", "FRENTE", "ONDA", "ESTIAGEM", "SECA", "UMIDADE"]):
        return "Met"

    if any(k in n for k in ["DESLIZ", "SOLO", "LAMA", "INUND", "ALAG", "ENXURR", "EROS", "ROCHA"]):
        return "Geo"

    if "INCENDIO" in n or "INCÊNDIO" in n:
        return "Fire"

    if "DOENC" in n or "SAUDE" in n or "SAÚDE" in n:
        return "Health"

    return "Safety"


def filter_window(alerts: List[Dict[str, Any]], window_hours: int, now_dt: datetime) -> List[Dict[str, Any]]:
    cutoff = now_dt.timestamp() - window_hours * 3600
    selected = []

    for a in alerts:
        ref_dt = parse_iso(a.get("onset") or a.get("sent"))
        if not ref_dt:
            continue
        if cutoff <= ref_dt.timestamp() <= now_dt.timestamp():
            selected.append(a)

    return selected


def make_latest_item(item: Dict[str, Any]) -> Dict[str, Any]:
    onset_dt = item.get("_onset_dt")

    return {
        "time": onset_dt.strftime("%H:%M") if onset_dt else "--:--",
        "date": onset_dt.strftime("%d/%m/%Y") if onset_dt else "--/--/----",
        "senderName": item.get("senderName") or "Emissor não informado",
        "senderNameShort": short_emitter(item.get("senderName"), 28),
        "event": item.get("event_short") or short_event(item.get("event")),
        "nivel": item.get("nivel") or "Indefinido",
        "location": item.get("location") or derive_location(item),
        "uf": item.get("uf") or "",
        "headline": item.get("headline") or item.get("description") or "",
        "status": item.get("status_vigencia") or "sem_validade",

        # Campos extras úteis no front.
        "identifier": item.get("identifier") or "",
        "entry_id": item.get("entry_id") or "",
        "sent": item.get("sent"),
        "onset": item.get("onset"),
        "expires": item.get("expires"),
        "category": item.get("category"),
        "severity": item.get("severity"),
        "urgency": item.get("urgency"),
        "certainty": item.get("certainty"),
        "responseType": item.get("responseType"),
        "channel_list": item.get("channel_list"),
    }



def slugify(value: Optional[str]) -> str:
    txt = (value or "").strip().lower()
    if not txt:
        return ""

    txt = unicodedata.normalize("NFD", txt)
    txt = "".join(c for c in txt if unicodedata.category(c) != "Mn")
    txt = re.sub(r"[^a-z0-9]+", "-", txt)
    return re.sub(r"-+", "-", txt).strip("-")


def extract_municipio_from_sender(sender_name: Optional[str]) -> Optional[tuple[str, str]]:
    sender = (sender_name or "").strip()
    if not sender:
        return None

    if re.search(r"Defesa\s+Civil\s+Estadual", sender, flags=re.IGNORECASE):
        return None

    patterns = [
        r"Defesa\s+Civil\s+Municipal\s+de\s+(.+?)\s*\(([A-Z]{2})\)",
        r"Defesa\s+Civil\s+do\s+Munic[ií]pio\s+de\s+(.+?)\s*\(([A-Z]{2})\)",
        r"Defesa\s+Civil\s+de\s+(.+?)\s*\(([A-Z]{2})\)",
        r"COMPDEC\s+de\s+(.+?)\s*\(([A-Z]{2})\)",
    ]

    for pat in patterns:
        m = re.search(pat, sender, flags=re.IGNORECASE)
        if m:
            city = re.sub(r"\s+", " ", m.group(1)).strip()
            uf = m.group(2).upper()
            if city and uf in UF_TO_REGION:
                return city, uf

    return None


def extract_municipio_from_location(location: Optional[str], uf_hint: Optional[str] = None) -> Optional[tuple[str, str]]:
    loc = (location or "").strip()
    if not loc:
        return None

    first = loc.split(",")[0].strip()

    patterns = [
        r"^(.+?)\s*/\s*([A-Z]{2})$",
        r"^(.+?)\s*-\s*([A-Z]{2})$",
        r"^(.+?)\s*\(([A-Z]{2})\)$",
    ]

    for pat in patterns:
        m = re.search(pat, first, flags=re.IGNORECASE)
        if m:
            city = re.sub(r"\s+", " ", m.group(1)).strip()
            uf = m.group(2).upper()
            if city and uf in UF_TO_REGION:
                return city, uf

    uf = (uf_hint or "").strip().upper()
    if uf in UF_TO_REGION and first and len(first) <= 60:
        state_name = STATE_NAME_TO_UF.get(normalize_text(first))
        if state_name == uf:
            return None
        return first, uf

    return None


def municipios_para_bandeiras(alerts: List[Dict[str, Any]]) -> list[tuple[str, str]]:
    found: set[tuple[str, str]] = set()

    for alert in alerts:
        sender_pair = extract_municipio_from_sender(alert.get("senderName"))
        if sender_pair:
            found.add(sender_pair)
            continue

        loc_pair = extract_municipio_from_location(
            alert.get("location") or alert.get("areaDesc"),
            alert.get("uf") or alert.get("uf_hint"),
        )
        if loc_pair:
            found.add(loc_pair)

    return sorted(found, key=lambda x: (x[1], slugify(x[0])))


def flag_failure_key(city: str, uf: str) -> str:
    return f"{uf.upper()}::{slugify(city)}"


def recently_failed(failures: Dict[str, Any], key: str, now_dt: datetime, retry_hours: int) -> bool:
    raw = failures.get(key)
    if not raw:
        return False

    dt = parse_iso(raw)
    if not dt:
        return False

    elapsed = (now_dt - dt).total_seconds() / 3600
    return elapsed < retry_hours


def request_get(session: Any, url: str, timeout: int = 45) -> Optional[Any]:
    try:
        response = session.get(url, timeout=timeout)

        if response.status_code == 404:
            return None

        if response.status_code in (403, 429):
            print(f"[WARN] Site de bandeiras limitou acesso ({response.status_code}). Parando tentativa nesta execução.")
            return "BLOCKED"

        response.raise_for_status()
        return response

    except Exception as exc:
        print(f"[WARN] Falha ao acessar {url}: {exc}")
        return None


def extrair_links_municipios(html: str, base_url: str) -> list[tuple[str, str]]:
    try:
        from bs4 import BeautifulSoup
    except Exception:
        print("[WARN] beautifulsoup4 não instalado. Pulando atualização de bandeiras.")
        return []

    soup = BeautifulSoup(html, "html.parser")
    links: list[tuple[str, str]] = []

    for a in soup.find_all("a", href=True):
        name = a.get_text(" ", strip=True)
        href = a.get("href") or ""

        if not name or "municipio-" not in href:
            continue

        links.append((name, urljoin(base_url, href)))

    return links


def extrair_url_bandeira(html: str, base_url: str) -> Optional[str]:
    try:
        from bs4 import BeautifulSoup
    except Exception:
        return None

    soup = BeautifulSoup(html, "html.parser")

    for img in soup.find_all("img"):
        src = img.get("src") or ""
        if "bandeira" in src.lower():
            return urljoin(base_url, src)

    return None


def atualizar_bandeiras_municipais(alerts: List[Dict[str, Any]], site_dir: Path, now_dt: datetime) -> None:
    enabled = os.getenv("FLAG_DOWNLOAD_ENABLED", DEFAULT_FLAG_DOWNLOAD_ENABLED).strip().lower()
    if enabled not in {"1", "true", "yes", "sim"}:
        print("[INFO] Download incremental de bandeiras desativado.")
        return

    try:
        import requests
    except Exception:
        print("[WARN] requests não instalado. Pulando atualização de bandeiras.")
        return

    base_url = os.getenv("FLAG_BASE_URL", DEFAULT_FLAG_BASE_URL).rstrip("/")
    max_downloads = int(os.getenv("FLAG_MAX_DOWNLOADS", str(DEFAULT_FLAG_MAX_DOWNLOADS)))
    min_sleep = float(os.getenv("FLAG_MIN_SLEEP", str(DEFAULT_FLAG_MIN_SLEEP)))
    max_sleep = float(os.getenv("FLAG_MAX_SLEEP", str(DEFAULT_FLAG_MAX_SLEEP)))
    retry_hours = int(os.getenv("FLAG_FAILURE_RETRY_HOURS", str(DEFAULT_FLAG_FAILURE_RETRY_HOURS)))

    flag_dir = Path(os.getenv("FLAG_DIR", str(site_dir / "assets" / "flags" / "municipios")))
    failures_path = Path(os.getenv("FLAG_FAILURES_PATH", DEFAULT_FLAG_FAILURES_PATH))

    failures = load_json(failures_path, {})
    if not isinstance(failures, dict):
        failures = {}

    municipios = municipios_para_bandeiras(alerts)
    if not municipios:
        print("[INFO] Nenhum município identificado para atualização de bandeiras.")
        return

    print(f"[INFO] Municípios candidatos a bandeira: {len(municipios)}")

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; IDAP-Dashboard/1.0; +https://www.gov.br/mdr)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    })

    estados_cache: dict[str, list[tuple[str, str]]] = {}
    downloads = 0

    for city, uf in municipios:
        if downloads >= max_downloads:
            break

        uf = uf.upper()
        if uf not in ESTADOS_MBI:
            continue

        city_slug = slugify(city)
        if not city_slug:
            continue

        target = flag_dir / uf.lower() / f"{city_slug}.jpg"
        if target.exists():
            continue

        key = flag_failure_key(city, uf)
        if recently_failed(failures, key, now_dt, retry_hours):
            continue

        try:
            if uf not in estados_cache:
                state_slug = ESTADOS_MBI[uf]
                state_url = f"{base_url}/mbi/biblioteca/simbolopedia/municipios-estado-{state_slug}-br/"

                print(f"[INFO] Carregando lista de municípios {uf}: {state_url}")
                resp = request_get(session, state_url)
                if resp == "BLOCKED":
                    break
                if not resp:
                    failures[key] = now_dt.isoformat()
                    continue

                estados_cache[uf] = extrair_links_municipios(resp.text, base_url)
                time.sleep(random.uniform(min_sleep, max_sleep))

            links = estados_cache.get(uf, [])
            city_url = None

            for name, url in links:
                if slugify(name) == city_slug:
                    city_url = url
                    break

            if not city_url:
                print(f"[WARN] Página municipal não encontrada: {city}/{uf}")
                failures[key] = now_dt.isoformat()
                continue

            print(f"[INFO] Baixando bandeira municipal: {city}/{uf}")
            resp_city = request_get(session, city_url)
            if resp_city == "BLOCKED":
                break
            if not resp_city:
                failures[key] = now_dt.isoformat()
                continue

            flag_url = extrair_url_bandeira(resp_city.text, base_url)
            if not flag_url:
                print(f"[WARN] URL da bandeira não encontrada: {city}/{uf}")
                failures[key] = now_dt.isoformat()
                continue

            time.sleep(random.uniform(min_sleep, max_sleep))

            resp_img = request_get(session, flag_url)
            if resp_img == "BLOCKED":
                break
            if not resp_img:
                failures[key] = now_dt.isoformat()
                continue

            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(resp_img.content)
            downloads += 1

            print(f"[INFO] Bandeira salva: {target}")

            failures.pop(key, None)
            time.sleep(random.uniform(min_sleep, max_sleep))

        except Exception as exc:
            print(f"[WARN] Falha ao baixar bandeira de {city}/{uf}: {exc}")
            failures[key] = now_dt.isoformat()

    save_json(failures_path, failures)
    print(f"[INFO] Bandeiras baixadas nesta execução: {downloads}")



def build_dash2(history_path: Path, site_dir: Path, window_hours: int) -> Dict[str, Any]:
    now_dt = datetime.now(TZ_BRASILIA)

    raw_history = load_json(history_path, [])
    if not isinstance(raw_history, list):
        raw_history = []

    window_alerts = filter_window(raw_history, window_hours, now_dt)

    enriched: List[Dict[str, Any]] = []

    for alert in window_alerts:
        if not isinstance(alert, dict):
            continue

        onset_dt = parse_iso(alert.get("onset") or alert.get("sent"))
        sent_dt = parse_iso(alert.get("sent"))
        expires_dt = parse_iso(alert.get("expires"))

        status_vigencia = classify_status(now_dt, onset_dt, expires_dt)
        uf = derive_uf(alert)
        region = (alert.get("region") or UF_TO_REGION.get(uf) or "").strip()
        dur_min = duration_minutes(onset_dt, expires_dt)
        since_min = time_since_minutes(now_dt, onset_dt)

        item = dict(alert)

        item.update({
            "uf": uf,
            "uf_hint": alert.get("uf_hint") or uf,
            "region": region,
            "location": derive_location(alert),
            "event_short": short_event(alert.get("event")),
            "category": alert.get("category") or event_category_fallback(alert.get("event")),
            "status_vigencia": status_vigencia,
            "status": status_vigencia,
            "date": onset_dt.strftime("%d/%m/%Y") if onset_dt else "--/--/----",
            "time": onset_dt.strftime("%H:%M") if onset_dt else "--:--",
            "sent_br": sent_dt.isoformat() if sent_dt else None,
            "onset_br": onset_dt.isoformat() if onset_dt else None,
            "expires_br": expires_dt.isoformat() if expires_dt else None,
            "duration_minutes": dur_min,
            "duration_hours": round(dur_min / 60, 2) if dur_min is not None else None,
            "duration_bucket": duration_bucket(dur_min),
            "time_since_minutes": since_min,
            "time_since_hours": round(since_min / 60, 2) if since_min is not None else None,
            "time_since_bucket": time_since_bucket(since_min),
            "is_active": status_vigencia == "vigente",
            "_onset_dt": onset_dt,
            "_expires_dt": expires_dt,
        })

        enriched.append(item)

    default_dt = datetime.min.replace(tzinfo=TZ_BRASILIA)
    enriched.sort(key=lambda a: a.get("_onset_dt") or default_dt, reverse=True)

    counter_emitters = Counter(a.get("senderName") or "Emissor não informado" for a in enriched)
    counter_levels = Counter(a.get("nivel") or "Indefinido" for a in enriched)
    counter_events = Counter(a.get("event_short") or short_event(a.get("event")) for a in enriched)
    counter_uf = Counter(a.get("uf") for a in enriched if a.get("uf"))
    counter_status = Counter(a.get("status_vigencia") or "sem_validade" for a in enriched)
    counter_category = Counter(category_label(a.get("category")) for a in enriched)
    counter_duration = Counter(a.get("duration_bucket") or "Sem validade" for a in enriched)
    counter_since = Counter(a.get("time_since_bucket") or "Sem data" for a in enriched)
    counter_channel = Counter(a.get("channel_list") or "Não informado" for a in enriched)

    top_events = counter_events.most_common(6)
    other_event_count = sum(counter_events.values()) - sum(count for _, count in top_events)
    if other_event_count > 0:
        top_events.append(("Outros", other_event_count))

    all_alerts = []
    for item in enriched:
        clean = dict(item)
        clean.pop("_onset_dt", None)
        clean.pop("_expires_dt", None)
        all_alerts.append(clean)

    cards = {
        "vigentes": counter_status.get("vigente", 0),
        "ultimas24h": len(enriched),
        "autoridadesAtivas": len(counter_emitters),
        "alertasExtremos": counter_levels.get("Extremo", 0),
        "alertasSeveros": counter_levels.get("Severo", 0),
        "alertasSeverosExtremos": counter_levels.get("Severo", 0) + counter_levels.get("Extremo", 0),
        "estadosComAlerta": len(counter_uf),
        "municipiosOuAreasComAlerta": len(set(a.get("location") for a in enriched if a.get("location"))),
    }

    return {
        "generated_at": now_dt.isoformat(),
        "source": str(history_path),
        "window_hours": window_hours,
        "cards": cards,

        "latest_alerts": [make_latest_item(a) for a in enriched[:10]],

        "top_emitters": [
            {
                "name": name,
                "short_name": short_emitter(name),
                "count": count,
            }
            for name, count in counter_emitters.most_common(10)
        ],

        "level_distribution": [
            {"label": level, "count": counter_levels.get(level, 0)}
            for level in NIVEL_ORDER
            if counter_levels.get(level, 0) > 0
        ],

        "event_distribution": [
            {"label": label, "count": count}
            for label, count in top_events
        ],

        "uf_distribution": [
            {"uf": uf, "count": count}
            for uf, count in counter_uf.most_common()
        ],

        "status_distribution": [
            {"label": label, "count": counter_status.get(label, 0)}
            for label in STATUS_ORDER
            if counter_status.get(label, 0) > 0
        ],

        "category_distribution": [
            {"label": label, "count": count}
            for label, count in counter_category.most_common()
        ],

        "duration_distribution": [
            {"label": label, "count": count}
            for label, count in counter_duration.most_common()
        ],

        "time_since_distribution": [
            {"label": label, "count": count}
            for label, count in counter_since.most_common()
        ],

        "channel_distribution": [
            {"label": label, "count": count}
            for label, count in counter_channel.most_common()
        ],

        "all_alerts": all_alerts,
    }


def main() -> None:
    history_path = Path(os.getenv("HISTORY_PATH", DEFAULT_HISTORY_PATH))
    site_dir = Path(os.getenv("SITE_DIR", DEFAULT_SITE_DIR))
    window_hours = int(os.getenv("WINDOW_HOURS", str(DEFAULT_WINDOW_HOURS)))

    geojson_source = Path(os.getenv("UF_GEOJSON_PATH", DEFAULT_GEOJSON_SOURCE))
    geojson_target = Path(os.getenv("DASHBOARD_GEOJSON_TARGET", DEFAULT_GEOJSON_TARGET))

    site_dir.mkdir(parents=True, exist_ok=True)

    data = build_dash2(history_path, site_dir, window_hours)
    out_path = site_dir / "dashboard_data2.json"
    save_json(out_path, data)

    # Atualização incremental e opcional das bandeiras municipais.
    # Se falhar, não quebra a geração do dashboard.
    try:
        atualizar_bandeiras_municipais(
            data.get("all_alerts", []),
            site_dir,
            parse_iso(data.get("generated_at")) or datetime.now(TZ_BRASILIA),
        )
    except Exception as exc:
        print(f"[WARN] Atualização de bandeiras ignorada: {exc}")

    if geojson_source.exists():
        geojson_target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(geojson_source, geojson_target)

    print("[INFO] dashboard_data2.json gerado com sucesso")
    print(f"[INFO] arquivo: {out_path}")
    print(f"[INFO] alertas no período: {len(data.get('all_alerts', []))}")

    if geojson_source.exists():
        print(f"[INFO] geojson copiado para: {geojson_target}")


if __name__ == "__main__":
    main()
