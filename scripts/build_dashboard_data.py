#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

DEFAULT_OUT_DIR = 'out'
DEFAULT_SITE_DIR = 'site'
DEFAULT_GEOJSON_SOURCE = 'resources/br_uf.geojson'
DEFAULT_GEOJSON_TARGET = 'site/data/br_uf.geojson'

TZ_BRASILIA = ZoneInfo("America/Sao_Paulo")

NIVEL_ORDER = ['Baixo', 'Médio', 'Alto', 'Severo', 'Extremo', 'Indefinido']
STATUS_ORDER = ['vigente', 'futuro', 'expirado', 'sem_validade']


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    txt = str(value).strip()
    if not txt:
        return None
    if txt.endswith('Z'):
        txt = txt[:-1] + '+00:00'
    try:
        dt = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(TZ_BRASILIA)
    except Exception:
        return None


def load_json(path: Path, default: Any):
    try:
        with path.open('r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def short_event(value: Optional[str]) -> str:
    txt = (value or '').strip()
    if not txt:
        return 'Não informado'
    if ' - ' in txt:
        txt = txt.split(' - ')[-1].strip()
    txt = txt.replace('TEMPESTADE LOCAL/CONVECTIVA', 'TEMPESTADE')
    txt = txt.replace('CORRIDA DE MASSA/SOLO/LAMA', 'CORRIDA DE MASSA')
    return txt.title()


def short_emitter(value: Optional[str], max_len: int = 32) -> str:
    txt = (value or 'Emissor não informado').strip()
    txt = txt.replace('Defesa Civil Estadual de ', 'DC Estadual de ')
    txt = txt.replace('Defesa Civil de ', 'DC de ')
    return txt if len(txt) <= max_len else txt[: max_len - 1].rstrip() + '…'


def derive_location(alert: Dict[str, Any]) -> str:
    area = (alert.get('areaDesc') or '').strip()
    uf = (alert.get('uf_hint') or '').strip()
    if area and len(area) <= 42:
        return area
    sender = (alert.get('senderName') or '').strip()
    if sender and len(sender) <= 42:
        return sender
    return uf or 'Não informado'


def classify_status(now_dt: datetime, onset_dt: Optional[datetime], expires_dt: Optional[datetime]) -> str:
    if onset_dt and onset_dt > now_dt:
        return 'futuro'
    if expires_dt:
        return 'vigente' if expires_dt >= now_dt else 'expirado'
    return 'sem_validade'


def latest_run_dir(out_dir: Path) -> Optional[Path]:
    if not out_dir.exists():
        return None
    runs = [p for p in out_dir.iterdir() if p.is_dir() and p.name.startswith('run_')]
    if not runs:
        return None
    runs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return runs[0]


def build_dashboard_data(out_dir: Path, site_dir: Path) -> Dict[str, Any]:
    run_dir = latest_run_dir(out_dir)
    alerts = []
    summary = {}

    if run_dir:
        alerts = load_json(run_dir / 'alerts_24h.json', [])
        summary = load_json(run_dir / 'resumo.json', {})

    now_dt = datetime.now(TZ_BRASILIA)

    enriched: List[Dict[str, Any]] = []
    for alert in alerts:
        onset_dt = parse_iso(alert.get('onset') or alert.get('sent'))
        expires_dt = parse_iso(alert.get('expires'))
        status = classify_status(now_dt, onset_dt, expires_dt)

        enriched.append({
            'identifier': alert.get('identifier') or alert.get('entry_id') or '',
            'entry_id': alert.get('entry_id') or '',
            'senderName': alert.get('senderName') or 'Emissor não informado',
            'event': alert.get('event') or 'Não informado',
            'event_short': short_event(alert.get('event')),
            'nivel': alert.get('nivel') or 'Indefinido',
            'headline': (alert.get('headline') or '').strip(),
            'description': (alert.get('description') or '').strip(),
            'areaDesc': (alert.get('areaDesc') or '').strip(),
            'uf': alert.get('uf_hint') or '',
            'region': alert.get('region') or '',
            'location': derive_location(alert),
            'onset': alert.get('onset'),
            'sent': alert.get('sent'),
            'expires': alert.get('expires'),
            'channel_list': alert.get('channel_list') or '',
            'status_vigencia': status,
            'onset_dt': onset_dt,
            'expires_dt': expires_dt,
        })

    default_dt = datetime.min.replace(tzinfo=TZ_BRASILIA)
    enriched.sort(key=lambda a: a['onset_dt'] or default_dt, reverse=True)

    latest5 = []
    for item in enriched[:5]:
        onset_dt = item['onset_dt']
        latest5.append({
            'time': onset_dt.strftime('%H:%M') if onset_dt else '--:--',
            'date': onset_dt.strftime('%d/%m/%Y') if onset_dt else '--/--/----',
            'senderName': item['senderName'],
            'senderNameShort': short_emitter(item['senderName'], 28),
            'event': item['event_short'],
            'nivel': item['nivel'],
            'location': item['location'],
            'uf': item['uf'],
            'headline': item['headline'] or item['description'],
            'status': item['status_vigencia'],
        })

    counter_emitters = Counter(item['senderName'] for item in enriched)
    top_emitters = [
        {
            'name': name,
            'short_name': short_emitter(name),
            'count': count,
        }
        for name, count in counter_emitters.most_common(10)
    ]

    counter_levels = Counter(item['nivel'] for item in enriched)
    levels = [
        {'label': level, 'count': counter_levels.get(level, 0)}
        for level in NIVEL_ORDER
        if counter_levels.get(level, 0) > 0
    ]

    counter_events = Counter(item['event_short'] for item in enriched)
    top_events = counter_events.most_common(6)
    other_count = sum(counter_events.values()) - sum(count for _, count in top_events)
    if other_count > 0:
        top_events.append(('Outros', other_count))
    event_distribution = [{'label': label, 'count': count} for label, count in top_events]

    counter_uf = Counter(item['uf'] for item in enriched if item['uf'])
    by_uf = [{'uf': uf, 'count': count} for uf, count in counter_uf.most_common()]

    counter_status = Counter(item['status_vigencia'] for item in enriched)
    status_distribution = [
        {'label': label, 'count': counter_status.get(label, 0)}
        for label in STATUS_ORDER
        if counter_status.get(label, 0) > 0
    ]

    all_alerts = []
    for item in enriched:
        onset_dt = item['onset_dt']
        all_alerts.append({
            'date': onset_dt.strftime('%d/%m/%Y') if onset_dt else '--/--/----',
            'time': onset_dt.strftime('%H:%M') if onset_dt else '--:--',
            'senderName': item['senderName'],
            'event': item['event_short'],
            'nivel': item['nivel'],
            'uf': item['uf'],
            'location': item['location'],
            'headline': item['headline'] or item['description'],
            'status': item['status_vigencia'],
            'onset': item['onset'],
            'expires': item['expires'],
        })

    cards = {
        'vigentes': counter_status.get('vigente', 0),
        'ultimas24h': len(enriched),
        'autoridadesAtivas': len(counter_emitters),
        'alertasExtremos': counter_levels.get('Extremo', 0),
    }

    generated_at = now_dt.isoformat()

    return {
        'generated_at': generated_at,
        'source_run_dir': run_dir.name if run_dir else None,
        'cards': cards,
        'latest_alerts': latest5,
        'top_emitters': top_emitters,
        'level_distribution': levels,
        'event_distribution': event_distribution,
        'uf_distribution': by_uf,
        'status_distribution': status_distribution,
        'all_alerts': all_alerts,
        'summary': summary,
    }


def main() -> None:
    out_dir = Path(os.getenv('OUT_DIR', DEFAULT_OUT_DIR))
    site_dir = Path(os.getenv('SITE_DIR', DEFAULT_SITE_DIR))
    geojson_source = Path(os.getenv('UF_GEOJSON_PATH', DEFAULT_GEOJSON_SOURCE))
    geojson_target = Path(os.getenv('DASHBOARD_GEOJSON_TARGET', DEFAULT_GEOJSON_TARGET))

    site_dir.mkdir(parents=True, exist_ok=True)
    geojson_target.parent.mkdir(parents=True, exist_ok=True)

    data = build_dashboard_data(out_dir, site_dir)

    with (site_dir / 'dashboard_data.json').open('w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    if geojson_source.exists():
        geojson_target.write_text(
            geojson_source.read_text(encoding='utf-8'),
            encoding='utf-8'
        )

    print('[INFO] dashboard_data.json gerado com sucesso')
    print(f"[INFO] arquivo: {site_dir / 'dashboard_data.json'}")
    if geojson_source.exists():
        print(f"[INFO] geojson copiado para: {geojson_target}")


if __name__ == '__main__':
    main()
