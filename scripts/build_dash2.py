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
import shutil
import unicodedata
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

TZ_BRASILIA = ZoneInfo("America/Sao_Paulo")

NIVEL_ORDER = ["Baixo", "Médio", "Alto", "Severo", "Extremo", "Indefinido"]
STATUS_ORDER = ["vigente", "futuro", "expirado", "sem_validade"]

UF_TO_REGION = {
    "AC": "N", "AP": "N", "AM": "N", "PA": "N", "RO": "N", "RR": "RR", "TO": "N",
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
