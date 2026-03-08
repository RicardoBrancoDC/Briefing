#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IDAP Daily Maps (versão estável + histórico 24h + logo + legenda colorida)

Saídas:
1) mapa_alertas_todos.png
2) mapa_alertas_chuva_temp_inund.png
3) mapa_alertas_deslizamento.png
4) mapa_alertas_outros.png
+ alerts_feed.json, alerts_24h.json, historico_alertas.json, errors.json, resumo.json, resumo.md

Regras:
- Varre o RSS completo a cada execução (sem filtrar por status).
- Mantém histórico local de alertas para contornar o limite de 12h do feed.
- Considera o campo onset como horário de geração do alerta.
- Deduplica pelo campo atom:id do entry RSS.
- Gera mapas e resumos com base nos alertas das últimas 24h.
- Cor por NIVEL calculado:
    Extremo, Severo, Alto, Médio, Baixo, Indefinido
- Mapas 2, 3 e 4 são filtros por EVENTO.
- Em cada mapa, coloca duas legendas:
    - inferior direita: contagem por nível (sem "Indefinido")
    - inferior esquerda: resumo de alertas por região
  na legenda por nível, a ordem é:
    Extremo, Severo, Alto, Médio, Baixo
- Logo (canto superior direito) se LOGO_PATH existir.
"""

import json
import os
import re
import time
import unicodedata
import urllib.request
import urllib.error
import http.client
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
from shapely.geometry.base import BaseGeometry

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.offsetbox import OffsetImage, AnnotationBbox


ATOM_NS = {"atom": "http://www.w3.org/2005/Atom", "dc": "http://purl.org/dc/elements/1.1/"}
CAP_NS = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}

DEFAULT_RSS_URL = "https://idapfile.mdr.gov.br/idap/api/rss/cap"
DEFAULT_UF_GEOJSON_PATH = "resources/br_uf.geojson"
DEFAULT_OUT_DIR = "out"
DEFAULT_STATE_PATH = ".cache/state.json"
DEFAULT_LOGO_PATH = ".cache/marca_sedec.png"
DEFAULT_HISTORY_PATH = ".cache/historico_alertas.json"
DEFAULT_WINDOW_HOURS = 24
DEFAULT_RETENTION_HOURS = 72

UF_TO_REGION = {
    "AC": "N", "AP": "N", "AM": "N", "PA": "N", "RO": "N", "RR": "N", "TO": "N",
    "AL": "NE", "BA": "NE", "CE": "NE", "MA": "NE", "PB": "NE", "PE": "NE",
    "PI": "NE", "RN": "NE", "SE": "NE",
    "DF": "CO", "GO": "CO", "MT": "CO", "MS": "CO",
    "ES": "SE", "MG": "SE", "RJ": "SE", "SP": "SE",
    "PR": "S", "RS": "S", "SC": "S",
}

NIVEL_COLORS = {
    "Extremo": "#6a0dad",
    "Severo":  "#d62728",
    "Alto":    "#ff7f0e",
    "Médio":   "#ffd92f",
    "Baixo":   "#2ca02c",
    "Indefinido": "#7f7f7f",
}

ALERT_ALPHA = 0.35
BORDER_ALPHA = 0.9


def calc_nivel(severity: str, urgency: str, certainty: str, response_type: str) -> str:
    s = (severity or "").strip()
    u = (urgency or "").strip()
    c = (certainty or "").strip()
    r = (response_type or "").strip()

    if s == "Extreme":
        return "Extremo"
    if s == "Moderate":
        return "Médio"
    if s == "Minor":
        return "Baixo"

    if s == "Severe":
        if (u == "Expected") and (c in {"Likely", "Observed"}) and (r in {"Execute", "Prepare"}):
            return "Severo"
        return "Alto"

    return "Indefinido"


def nivel_emoji(nivel: str) -> str:
    n = (nivel or "").strip()
    return {
        "Extremo": "🟣",
        "Severo": "🔴",
        "Alto": "🟠",
        "Médio": "🟡",
        "Baixo": "🟢",
        "Indefinido": "⚪",
    }.get(n, "⚪")


@dataclass
class AlertRecord:
    identifier: str
    entry_id: str
    sender: Optional[str]
    senderName: Optional[str]
    sent: Optional[str]
    status: Optional[str]
    msgType: Optional[str]
    category: Optional[str]
    event: Optional[str]
    responseType: Optional[str]
    urgency: Optional[str]
    severity: Optional[str]
    certainty: Optional[str]
    onset: Optional[str]
    expires: Optional[str]
    nivel: str
    headline: Optional[str]
    description: Optional[str]
    instruction: Optional[str]
    web: Optional[str]
    contact: Optional[str]
    channel_list: Optional[str]
    areaDesc: Optional[str]
    polygon_raw: Optional[str]
    polygon_points: int
    has_geocode: bool
    uf_hint: Optional[str]
    region: Optional[str]
    geometry_wkt: Optional[str]


def _now_sp() -> datetime:
    return datetime.now().astimezone()


_PT_MONTHS = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
    7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}


def _format_period_title() -> str:
    now_dt = _now_sp()
    return f"Alertas últimas 24h - Gerado em: {now_dt.day:02d} de {_PT_MONTHS[now_dt.month]} de {now_dt.year}"


def _parse_iso_any(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    txt = s.strip()
    if not txt:
        return None
    try:
        if txt.endswith("Z"):
            txt = txt[:-1] + "+00:00"
        dt = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone()
    except Exception:
        return None


def _safe_text(elem: Optional[ET.Element]) -> Optional[str]:
    if elem is None:
        return None
    txt = elem.text
    if txt is None:
        return None
    txt = txt.strip()
    return txt if txt != "" else None


def _first(elem: ET.Element, path: str, ns: Dict[str, str]) -> Optional[ET.Element]:
    try:
        return elem.find(path, ns)
    except Exception:
        return None


def _all(elem: ET.Element, path: str, ns: Dict[str, str]) -> List[ET.Element]:
    try:
        return elem.findall(path, ns) or []
    except Exception:
        return []


def _read_url(url: str, timeout: int = 30, retries: int = 3, backoff_s: float = 1.2) -> bytes:
    last_err: Optional[Exception] = None
    for i in range(retries):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "IDAP-Daily-Maps/1.5 (+github-actions)", "Accept": "*/*"},
                method="GET",
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except http.client.IncompleteRead as e:
            last_err = e
        except urllib.error.URLError as e:
            last_err = e
        except Exception as e:
            last_err = e
        time.sleep(backoff_s * (i + 1))
    raise last_err if last_err else RuntimeError("Falha ao baixar URL (erro desconhecido)")


def _normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.strip()
    if not s:
        return ""
    s2 = unicodedata.normalize("NFKD", s)
    s2 = "".join([c for c in s2 if not unicodedata.combining(c)])
    return s2.upper()


def _parse_polygon_str(poly_str: str) -> Optional[BaseGeometry]:
    if not poly_str:
        return None
    poly_str = poly_str.strip()
    if not poly_str:
        return None
    pts: List[Tuple[float, float]] = []
    for token in poly_str.split():
        if "," not in token:
            continue
        a, b = token.split(",", 1)
        try:
            lat = float(a)
            lon = float(b)
        except ValueError:
            continue
        pts.append((lon, lat))
    if len(pts) < 3:
        return None
    if pts[0] != pts[-1]:
        pts.append(pts[0])
    geom: BaseGeometry = Polygon(pts)
    if not geom.is_valid:
        geom = geom.buffer(0)
    if geom.is_empty:
        return None
    return geom


def _geom_points_count(geom: Optional[BaseGeometry]) -> int:
    try:
        if geom is None or geom.is_empty:
            return 0
        if geom.geom_type == "Polygon":
            return len(geom.exterior.coords) if geom.exterior else 0
        if geom.geom_type == "MultiPolygon":
            best = 0
            mp: MultiPolygon = geom  # type: ignore
            for g in mp.geoms:
                if g.exterior:
                    best = max(best, len(g.exterior.coords))
            return best
        return 0
    except Exception:
        return 0


STATE_NAME_TO_UF = {
    "ACRE": "AC",
    "ALAGOAS": "AL",
    "AMAPÁ": "AP",
    "AMAPA": "AP",
    "AMAZONAS": "AM",
    "BAHIA": "BA",
    "CEARÁ": "CE",
    "CEARA": "CE",
    "DISTRITO FEDERAL": "DF",
    "ESPÍRITO SANTO": "ES",
    "ESPIRITO SANTO": "ES",
    "GOIÁS": "GO",
    "GOIAS": "GO",
    "MARANHÃO": "MA",
    "MARANHAO": "MA",
    "MATO GROSSO": "MT",
    "MATO GROSSO DO SUL": "MS",
    "MINAS GERAIS": "MG",
    "PARÁ": "PA",
    "PARA": "PA",
    "PARAÍBA": "PB",
    "PARAIBA": "PB",
    "PARANÁ": "PR",
    "PARANA": "PR",
    "PERNAMBUCO": "PE",
    "PIAUÍ": "PI",
    "PIAUI": "PI",
    "RIO DE JANEIRO": "RJ",
    "RIO GRANDE DO NORTE": "RN",
    "RIO GRANDE DO SUL": "RS",
    "RONDÔNIA": "RO",
    "RONDONIA": "RO",
    "RORAIMA": "RR",
    "SANTA CATARINA": "SC",
    "SÃO PAULO": "SP",
    "SAO PAULO": "SP",
    "SERGIPE": "SE",
    "TOCANTINS": "TO",
}


def _guess_uf_from_text(text: Optional[str]) -> Optional[str]:
    txt = (text or "").strip().upper()
    if not txt:
        return None
    m = re.search(r"/([A-Z]{2})\b", txt)
    if m:
        return m.group(1)
    m = re.search(r"\(([A-Z]{2})\)", txt)
    if m:
        return m.group(1)
    m = re.search(r"\b([A-Z]{2})\b", txt)
    if m and m.group(1) in UF_TO_REGION:
        return m.group(1)
    for state_name, uf in sorted(STATE_NAME_TO_UF.items(), key=lambda x: -len(x[0])):
        if state_name in txt:
            return uf
    return None


def _guess_uf(area_desc: Optional[str], sender_name: Optional[str] = None) -> Optional[str]:
    uf = _guess_uf_from_text(area_desc)
    if uf:
        return uf
    return _guess_uf_from_text(sender_name)


def _uf_to_region(uf: Optional[str]) -> Optional[str]:
    if not uf:
        return None
    return UF_TO_REGION.get(uf.strip().upper())


def _cap_get_parameter(info_elem: ET.Element, value_name: str) -> Optional[str]:
    for p in _all(info_elem, "cap:parameter", CAP_NS):
        vn = _safe_text(_first(p, "cap:valueName", CAP_NS))
        if vn and vn.strip().upper() == value_name.strip().upper():
            return _safe_text(_first(p, "cap:value", CAP_NS))
    return None


def _extract_cap_xml_from_entry(entry: ET.Element) -> Optional[ET.Element]:
    content = _first(entry, "atom:content", ATOM_NS)
    if content is None:
        return None
    for child in list(content):
        if child.tag.endswith("alert"):
            return child
    raw = content.text
    if raw is None:
        return None
    raw = raw.strip()
    if not raw:
        return None
    try:
        root = ET.fromstring(raw)
        if root.tag.endswith("alert"):
            return root
    except Exception:
        pass
    raw2 = raw.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", '"').replace("&amp;", "&")
    try:
        root = ET.fromstring(raw2)
        if root.tag.endswith("alert"):
            return root
    except Exception:
        return None
    return None


def _parse_cap_from_entry(entry: ET.Element) -> Tuple[Optional[AlertRecord], Optional[str]]:
    try:
        cap_alert = _extract_cap_xml_from_entry(entry)
        if cap_alert is None:
            return None, "entry sem CAP <alert>"
        entry_id = _safe_text(_first(entry, "atom:id", ATOM_NS)) or "UNKNOWN"
        identifier = _safe_text(_first(cap_alert, "cap:identifier", CAP_NS)) or entry_id
        sender = _safe_text(_first(cap_alert, "cap:sender", CAP_NS))
        sent = _safe_text(_first(cap_alert, "cap:sent", CAP_NS))
        status = _safe_text(_first(cap_alert, "cap:status", CAP_NS))
        msgType = _safe_text(_first(cap_alert, "cap:msgType", CAP_NS))
        info = _first(cap_alert, "cap:info", CAP_NS)
        if info is None:
            infos = _all(cap_alert, "cap:info", CAP_NS)
            info = infos[0] if infos else None
        category = event = responseType = urgency = severity = certainty = onset = expires = None
        senderName = headline = description = instruction = web = contact = None
        channel_list = None
        areaDesc = None
        polygon_raw = None
        has_geocode = False
        geom: Optional[BaseGeometry] = None
        if info is not None:
            category = _safe_text(_first(info, "cap:category", CAP_NS))
            event = _safe_text(_first(info, "cap:event", CAP_NS))
            responseType = _safe_text(_first(info, "cap:responseType", CAP_NS))
            urgency = _safe_text(_first(info, "cap:urgency", CAP_NS))
            severity = _safe_text(_first(info, "cap:severity", CAP_NS))
            certainty = _safe_text(_first(info, "cap:certainty", CAP_NS))
            onset = _safe_text(_first(info, "cap:onset", CAP_NS))
            expires = _safe_text(_first(info, "cap:expires", CAP_NS))
            senderName = _safe_text(_first(info, "cap:senderName", CAP_NS))
            headline = _safe_text(_first(info, "cap:headline", CAP_NS))
            description = _safe_text(_first(info, "cap:description", CAP_NS))
            instruction = _safe_text(_first(info, "cap:instruction", CAP_NS))
            web = _safe_text(_first(info, "cap:web", CAP_NS))
            contact = _safe_text(_first(info, "cap:contact", CAP_NS))
            channel_list = _cap_get_parameter(info, "CHANNEL-LIST")
            area = _first(info, "cap:area", CAP_NS)
            if area is not None:
                areaDesc = _safe_text(_first(area, "cap:areaDesc", CAP_NS))
                polygon_raw = _safe_text(_first(area, "cap:polygon", CAP_NS))
                geocodes = _all(area, "cap:geocode", CAP_NS)
                has_geocode = len(geocodes) > 0
                if polygon_raw:
                    geom = _parse_polygon_str(polygon_raw)
        uf_hint = _guess_uf(areaDesc, senderName)
        region = _uf_to_region(uf_hint)
        nivel = calc_nivel(severity or "", urgency or "", certainty or "", responseType or "")
        rec = AlertRecord(
            identifier=identifier,
            entry_id=entry_id,
            sender=sender,
            senderName=senderName,
            sent=sent,
            status=status,
            msgType=msgType,
            category=category,
            event=event,
            responseType=responseType,
            urgency=urgency,
            severity=severity,
            certainty=certainty,
            onset=onset,
            expires=expires,
            nivel=nivel,
            headline=headline,
            description=description,
            instruction=instruction,
            web=web,
            contact=contact,
            channel_list=channel_list,
            areaDesc=areaDesc,
            polygon_raw=polygon_raw,
            polygon_points=_geom_points_count(geom),
            has_geocode=has_geocode,
            uf_hint=uf_hint,
            region=region,
            geometry_wkt=geom.wkt if geom is not None else None,
        )
        return rec, None
    except Exception as e:
        return None, f"erro parse CAP: {e}"


def _load_uf_gdf(path: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326", allow_override=True)
    else:
        try:
            gdf = gdf.to_crs("EPSG:4326")
        except Exception:
            pass
    return gdf


def _alerts_to_gdf(alerts: List[AlertRecord]) -> gpd.GeoDataFrame:
    geoms = []
    rows = []
    for a in alerts:
        if not a.geometry_wkt:
            continue
        try:
            geom = gpd.GeoSeries.from_wkt([a.geometry_wkt], crs="EPSG:4326").iloc[0]
        except Exception:
            continue
        geoms.append(geom)
        rows.append(a)
    if not rows:
        return gpd.GeoDataFrame(columns=["identifier"], geometry=[], crs="EPSG:4326")
    return gpd.GeoDataFrame([asdict(r) for r in rows], geometry=geoms, crs="EPSG:4326")


def _count_by(alerts: List[AlertRecord], key_fn) -> Dict[str, int]:
    d: Dict[str, int] = {}
    for a in alerts:
        k = key_fn(a) or "N/A"
        d[k] = d.get(k, 0) + 1
    return dict(sorted(d.items(), key=lambda x: (-x[1], x[0])))


def _make_summary(alerts: List[AlertRecord]) -> Dict[str, Any]:
    return {
        "total_alerts": len(alerts),
        "by_nivel": _count_by(alerts, lambda a: a.nivel),
        "by_channel_list": _count_by(alerts, lambda a: a.channel_list),
        "by_region": _count_by(alerts, lambda a: a.region),
    }


def _ensure_dirs(*paths: str) -> None:
    for p in paths:
        os.makedirs(p, exist_ok=True)


def _load_json_file(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json_file(path: str, data: Any) -> None:
    parent = os.path.dirname(path) or "."
    os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _load_state(path: str) -> Dict[str, Any]:
    return _load_json_file(path, {})


def _save_state(path: str, state: Dict[str, Any]) -> None:
    _save_json_file(path, state)


def _load_history(path: str) -> List[AlertRecord]:
    raw = _load_json_file(path, [])
    alerts: List[AlertRecord] = []
    if not isinstance(raw, list):
        return alerts
    for item in raw:
        try:
            if not isinstance(item, dict):
                continue
            if "entry_id" not in item:
                item["entry_id"] = item.get("identifier", "UNKNOWN")
            alerts.append(AlertRecord(**item))
        except Exception:
            continue
    return alerts


def _save_history(path: str, alerts: List[AlertRecord]) -> None:
    _save_json_file(path, [asdict(a) for a in alerts])


def _merge_history(existing: List[AlertRecord], new_alerts: List[AlertRecord]) -> Tuple[List[AlertRecord], int]:
    merged: Dict[str, AlertRecord] = {}
    for a in existing:
        key = (a.entry_id or a.identifier or "").strip()
        if key:
            merged[key] = a
    added = 0
    for a in new_alerts:
        key = (a.entry_id or a.identifier or "").strip()
        if not key:
            continue
        if key not in merged:
            added += 1
        merged[key] = a
    def _sort_key(a: AlertRecord) -> datetime:
        return _parse_iso_any(a.onset) or _parse_iso_any(a.sent) or datetime(1970, 1, 1, tzinfo=timezone.utc)
    items = list(merged.values())
    items.sort(key=_sort_key)
    return items, added


def _filter_recent_history(alerts: List[AlertRecord], retention_hours: int, ref_now: datetime) -> List[AlertRecord]:
    cutoff = ref_now - timedelta(hours=retention_hours)
    kept: List[AlertRecord] = []
    for a in alerts:
        ref_dt = _parse_iso_any(a.onset) or _parse_iso_any(a.sent)
        if ref_dt is None:
            continue
        if ref_dt >= cutoff:
            kept.append(a)
    return kept


def _filter_window(alerts: List[AlertRecord], window_hours: int, ref_now: datetime) -> List[AlertRecord]:
    cutoff = ref_now - timedelta(hours=window_hours)
    selected: List[AlertRecord] = []
    for a in alerts:
        ref_dt = _parse_iso_any(a.onset) or _parse_iso_any(a.sent)
        if ref_dt is None:
            continue
        if cutoff <= ref_dt <= ref_now:
            selected.append(a)
    return selected


def _write_resumo_md(path: str, resumo: Dict[str, Any]) -> None:
    lines = ["# Quadro geral", "", f"Total de alertas (últimas 24h): **{resumo.get('total_alerts', 0)}**", ""]
    def _block(title: str, d: Dict[str, int], emoji: bool = False):
        lines.append(f"## {title}")
        lines.append("")
        for k, v in d.items():
            lines.append(f"- {nivel_emoji(k)} {k}: {v}" if emoji else f"- {k}: {v}")
        lines.append("")
    _block("Nível (calculado)", resumo.get("by_nivel", {}), emoji=True)
    _block("Tipo (CHANNEL-LIST)", resumo.get("by_channel_list", {}), emoji=False)
    _block("Alertas por regiões do Brasil", resumo.get("by_region", {}), emoji=False)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def _is_chuva_temp_inund(event: Optional[str]) -> bool:
    n = _normalize_text(event)
    return ("CHUVA" in n and "INTENSA" in n) or ("TEMPESTADE" in n and "CONVECT" in n) or ("INUND" in n)


def _is_deslizamento(event: Optional[str]) -> bool:
    n = _normalize_text(event)
    return ("DESLIZ" in n) or ("MOVIMENTO DE MASSA" in n) or ("CORRIDA DE MASSA" in n)


def _nivel_color(n: str) -> str:
    return NIVEL_COLORS.get((n or "").strip(), NIVEL_COLORS["Indefinido"])


def _add_logo(ax, logo_path: str, width_frac: float = 0.04, x: float = 0.985, y: float = 0.985) -> None:
    try:
        if not logo_path or (not os.path.exists(logo_path)):
            return
        fig = ax.figure
        dpi = fig.dpi
        fig_w_px = fig.get_figwidth() * dpi
        img = plt.imread(logo_path)
        if img is None:
            return
        img_w = img.shape[1]
        if img_w <= 0:
            return
        desired_w_px = max(1.0, fig_w_px * width_frac)
        zoom = desired_w_px / float(img_w)
        oi = OffsetImage(img, zoom=zoom)
        ab = AnnotationBbox(oi, (x, y), xycoords=ax.transAxes, frameon=False, box_alignment=(1, 1), zorder=50)
        ax.add_artist(ab)
    except Exception:
        return


REGION_LABELS = {
    "N": "Norte",
    "NE": "Nordeste",
    "CO": "Centro-Oeste",
    "SE": "Sudeste",
    "S": "Sul",
    "N/A": "Não identificado",
}


def _add_region_legend(ax, alerts_gdf: gpd.GeoDataFrame, loc: str = "lower left") -> None:
    try:
        if alerts_gdf is None or len(alerts_gdf) == 0 or "region" not in alerts_gdf.columns:
            return
        order = ["N", "NE", "CO", "SE", "S", "N/A"]
        counts: Dict[str, int] = {}
        for r in alerts_gdf["region"].tolist():
            rr = (r or "N/A").strip()
            if rr not in order:
                rr = "N/A"
            counts[rr] = counts.get(rr, 0) + 1

        lines = ["Resumo por região"]
        for r in order:
            c = counts.get(r, 0)
            if c > 0:
                lines.append(f"{REGION_LABELS.get(r, r)}: {c}")

        if len(lines) == 1:
            return

        x = 0.015 if loc == "lower left" else 0.985
        ha = "left" if loc == "lower left" else "right"

        ax.text(
            x,
            0.03,
            "\n".join(lines),
            transform=ax.transAxes,
            ha=ha,
            va="bottom",
            fontsize=11,
            zorder=1000,
            bbox=dict(
                boxstyle="round,pad=0.45",
                facecolor="white",
                edgecolor="#666666",
                alpha=0.95,
            ),
        )
    except Exception:
        return


def _add_counts_legend(ax, alerts_gdf: gpd.GeoDataFrame, loc: str = "lower right") -> None:
    try:
        if alerts_gdf is None or len(alerts_gdf) == 0 or "nivel" not in alerts_gdf.columns:
            return
        order = ["Extremo", "Severo", "Alto", "Médio", "Baixo"]
        counts: Dict[str, int] = {}
        for n in alerts_gdf["nivel"].tolist():
            nn = (n or "").strip()
            if nn in order:
                counts[nn] = counts.get(nn, 0) + 1
        handles = []
        for n in order:
            c = counts.get(n, 0)
            if c <= 0:
                continue
            handles.append(mpatches.Patch(facecolor=_nivel_color(n), edgecolor=_nivel_color(n), label=f"{n}: {c}"))
        if not handles:
            return
        leg = ax.legend(
            handles=handles,
            loc=loc,
            fontsize=12,
            frameon=True,
            framealpha=0.92,
            borderpad=1.3,
            labelspacing=1.0,
            handlelength=2.0,
            handleheight=1.3,
        )
        leg.get_frame().set_linewidth(0.8)
    except Exception:
        return




def _plot_alerts_per_hour(alerts: List[AlertRecord], out_path: str, title: str) -> None:
    hourly_counts: Dict[datetime, int] = {}
    for a in alerts:
        dt = _parse_iso_any(a.onset)
        if dt is None:
            continue
        bucket = dt.replace(minute=0, second=0, microsecond=0)
        hourly_counts[bucket] = hourly_counts.get(bucket, 0) + 1

    if not hourly_counts:
        return

    buckets = sorted(hourly_counts.keys())
    start = buckets[0]
    end = buckets[-1]

    full_buckets: List[datetime] = []
    cur = start
    while cur <= end:
        full_buckets.append(cur)
        cur = cur + timedelta(hours=1)

    values = [hourly_counts.get(b, 0) for b in full_buckets]
    labels = [b.strftime('%d/%m - %H:%M') for b in full_buckets]

    fig = plt.figure(figsize=(14, 5), dpi=200)
    ax = plt.gca()
    ax.bar(range(len(full_buckets)), values)
    ax.set_title(title, fontsize=12)
    ax.set_ylabel('Quantidade de alertas')
    ax.set_xlabel('Hora de emissão (onset)')
    ax.set_xticks(range(len(full_buckets)))
    ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.grid(True, axis='y', alpha=0.3)
    plt.tight_layout()
    fig.savefig(out_path, dpi=200)
    plt.close(fig)

def _plot_alerts_map(
    uf_gdf: gpd.GeoDataFrame,
    alerts_gdf: gpd.GeoDataFrame,
    out_path: str,
    title_line1: str,
    title_line2: str,
    logo_path: str = "",
) -> None:
    fig = plt.figure(figsize=(12, 12), dpi=200)
    ax = plt.gca()
    uf_gdf.boundary.plot(ax=ax, linewidth=0.6, alpha=BORDER_ALPHA)
    if len(alerts_gdf) > 0:
        alerts_gdf = alerts_gdf.copy()
        alerts_gdf["_color"] = alerts_gdf["nivel"].apply(_nivel_color)
        alerts_gdf.plot(ax=ax, color=alerts_gdf["_color"], edgecolor=alerts_gdf["_color"], linewidth=0.8, alpha=ALERT_ALPHA)
    ax.set_title(f"{title_line1}\n{title_line2}", fontsize=12)
    ax.set_axis_off()
    if logo_path:
        _add_logo(ax, logo_path)
    _add_region_legend(ax, alerts_gdf, loc="lower left")
    _add_counts_legend(ax, alerts_gdf, loc="lower right")
    plt.tight_layout()
    fig.savefig(out_path, dpi=200)
    plt.close(fig)


def _tg_send_message(token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = json.dumps({"chat_id": chat_id, "text": text}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        _ = resp.read()


def _tg_send_photo(token: str, chat_id: str, photo_path: str, caption: str) -> None:
    import uuid
    boundary = f"----WebKitFormBoundary{uuid.uuid4().hex}"
    url = f"https://api.telegram.org/bot{token}/sendPhoto"
    with open(photo_path, "rb") as f:
        photo_bytes = f.read()
    def _part(name: str, value: str) -> bytes:
        return (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
            f"{value}\r\n"
        ).encode("utf-8")
    body = b""
    body += _part("chat_id", str(chat_id))
    if caption:
        body += _part("caption", caption)
    filename = os.path.basename(photo_path)
    body += (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="photo"; filename="{filename}"\r\n'
        f"Content-Type: image/png\r\n\r\n"
    ).encode("utf-8")
    body += photo_bytes
    body += b"\r\n"
    body += f"--{boundary}--\r\n".encode("utf-8")
    req = urllib.request.Request(url, data=body, headers={"Content-Type": f"multipart/form-data; boundary={boundary}"}, method="POST")
    with urllib.request.urlopen(req, timeout=60) as resp:
        _ = resp.read()


def main() -> int:
    rss_url = os.getenv("RSS_URL", DEFAULT_RSS_URL)
    uf_geojson_path = os.getenv("UF_GEOJSON_PATH", DEFAULT_UF_GEOJSON_PATH)
    out_dir = os.getenv("OUT_DIR", DEFAULT_OUT_DIR)
    state_path = os.getenv("STATE_PATH", DEFAULT_STATE_PATH)
    logo_path = os.getenv("LOGO_PATH", DEFAULT_LOGO_PATH).strip()
    history_path = os.getenv("HISTORY_PATH", DEFAULT_HISTORY_PATH)
    window_hours = int(os.getenv("WINDOW_HOURS", str(DEFAULT_WINDOW_HOURS)))
    retention_hours = int(os.getenv("RETENTION_HOURS", str(DEFAULT_RETENTION_HOURS)))
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    tg_chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    print(f"[INFO] RSS_URL={rss_url}")
    print(f"[INFO] UF_GEOJSON_PATH={uf_geojson_path}")
    print(f"[INFO] OUT_DIR={out_dir}")
    print(f"[INFO] HISTORY_PATH={history_path}")
    print(f"[INFO] WINDOW_HOURS={window_hours}")
    print(f"[INFO] RETENTION_HOURS={retention_hours}")

    run_ts = _now_sp().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(out_dir, f"run_{run_ts}")
    print(f"[INFO] RUN_DIR={run_dir}")
    print(f"[INFO] STATE_PATH={state_path}")

    _ensure_dirs(".cache", out_dir, run_dir)

    if logo_path and os.path.exists(logo_path):
        print(f"[INFO] LOGO_PATH={logo_path}")
    else:
        if logo_path:
            print(f"[WARN] LOGO_PATH não encontrado: {logo_path}")
        logo_path = ""

    state = _load_state(state_path)

    try:
        rss_bytes = _read_url(rss_url, timeout=45, retries=4)
    except Exception as e:
        print(f"[ERROR] Falha ao baixar RSS: {e}")
        return 2

    try:
        root = ET.fromstring(rss_bytes)
    except Exception as e:
        print(f"[ERROR] RSS inválido (XML): {e}")
        return 3

    entries = _all(root, "atom:entry", ATOM_NS)
    print(f"[INFO] Entradas no RSS (consideradas): {len(entries)}")

    feed_alerts: List[AlertRecord] = []
    errors: List[Dict[str, Any]] = []
    for entry in entries:
        a, err = _parse_cap_from_entry(entry)
        if a is None:
            errors.append({"error": err or "desconhecido"})
            continue
        feed_alerts.append(a)

    print(f"[INFO] CAPs parseados do feed: {len(feed_alerts)} | erros: {len(errors)}")

    history_before = _load_history(history_path)
    history_merged, added_count = _merge_history(history_before, feed_alerts)
    history_kept = _filter_recent_history(history_merged, retention_hours=retention_hours, ref_now=_now_sp())
    alerts = _filter_window(history_kept, window_hours=window_hours, ref_now=_now_sp())

    print(f"[INFO] Histórico anterior: {len(history_before)}")
    print(f"[INFO] Alertas novos inseridos no histórico: {added_count}")
    print(f"[INFO] Histórico após limpeza: {len(history_kept)}")
    print(f"[INFO] Alertas considerados nas últimas {window_hours}h: {len(alerts)}")

    _save_history(history_path, history_kept)
    _save_history(os.path.join(run_dir, "historico_alertas.json"), history_kept)

    with open(os.path.join(run_dir, "alerts_feed.json"), "w", encoding="utf-8") as f:
        json.dump([asdict(a) for a in feed_alerts], f, ensure_ascii=False, indent=2)
    with open(os.path.join(run_dir, "alerts_24h.json"), "w", encoding="utf-8") as f:
        json.dump([asdict(a) for a in alerts], f, ensure_ascii=False, indent=2)
    with open(os.path.join(run_dir, "errors.json"), "w", encoding="utf-8") as f:
        json.dump(errors, f, ensure_ascii=False, indent=2)

    resumo = _make_summary(alerts)
    with open(os.path.join(run_dir, "resumo.json"), "w", encoding="utf-8") as f:
        json.dump(resumo, f, ensure_ascii=False, indent=2)
    _write_resumo_md(os.path.join(run_dir, "resumo.md"), resumo)

    period_txt = _format_period_title()

    try:
        uf_gdf = _load_uf_gdf(uf_geojson_path)
    except Exception as e:
        print(f"[ERROR] Falha ao ler UF GeoJSON: {e}")
        return 4

    alerts_gdf_all = _alerts_to_gdf(alerts)
    title_line2 = period_txt

    graf_hora = os.path.join(run_dir, "grafico_alertas_por_hora_24h.png")
    try:
        _plot_alerts_per_hour(alerts, graf_hora, "Alertas emitidos por hora nas últimas 24h")
        if os.path.exists(graf_hora):
            print(f"[INFO] Gráfico gerado: {graf_hora}")
        else:
            graf_hora = ""
            print("[WARN] Gráfico por hora não gerado: nenhum alerta com onset válido")
    except Exception as e:
        graf_hora = ""
        print(f"[WARN] Falha ao gerar gráfico por hora: {e}")

    map1 = os.path.join(run_dir, "mapa_alertas_todos.png")
    if len(alerts_gdf_all) > 0:
        _plot_alerts_map(uf_gdf, alerts_gdf_all, map1, "Alertas IDAP - SEDEC/DPS/CGMA/CISDA", title_line2, logo_path=logo_path)
        print(f"[INFO] Mapa gerado: {map1}")
    else:
        map1 = ""
        print("[WARN] Mapa 1 não gerado: nenhum alerta com polygon")

    alerts_2 = [a for a in alerts if _is_chuva_temp_inund(a.event)]
    gdf_2 = _alerts_to_gdf(alerts_2)
    map2 = os.path.join(run_dir, "mapa_alertas_chuva_temp_inund.png")
    if len(gdf_2) > 0:
        _plot_alerts_map(uf_gdf, gdf_2, map2, "Alertas: Chuvas Intensas, Tempestades Convectivas, Inundações", title_line2, logo_path=logo_path)
        print(f"[INFO] Mapa gerado: {map2}")
    else:
        map2 = ""
        print("[WARN] Mapa 2 não gerado: nenhum alerta (filtro) com polygon")

    alerts_3 = [a for a in alerts if _is_deslizamento(a.event)]
    gdf_3 = _alerts_to_gdf(alerts_3)
    map3 = os.path.join(run_dir, "mapa_alertas_deslizamento.png")
    if len(gdf_3) > 0:
        _plot_alerts_map(uf_gdf, gdf_3, map3, "Alertas: Deslizamento", title_line2, logo_path=logo_path)
        print(f"[INFO] Mapa gerado: {map3}")
    else:
        map3 = ""
        print("[WARN] Mapa 3 não gerado: nenhum alerta de deslizamento com polygon")

    ids_2 = {a.entry_id for a in alerts_2}
    ids_3 = {a.entry_id for a in alerts_3}
    alerts_4 = [a for a in alerts if (a.entry_id not in ids_2) and (a.entry_id not in ids_3)]
    gdf_4 = _alerts_to_gdf(alerts_4)
    map4 = os.path.join(run_dir, "mapa_alertas_outros.png")
    if len(gdf_4) > 0:
        _plot_alerts_map(uf_gdf, gdf_4, map4, "Alertas: Outros tipos", title_line2, logo_path=logo_path)
        print(f"[INFO] Mapa gerado: {map4}")
    else:
        map4 = ""
        print("[WARN] Mapa 4 não gerado: nenhum alerta (outros) com polygon")

    if tg_token and tg_chat_id:
        by_nivel = resumo.get("by_nivel", {}) or {}
        by_reg = resumo.get("by_region", {}) or {}
        by_typ = resumo.get("by_channel_list", {}) or {}

        def _fmt_counts(d: Dict[str, int], with_emoji: bool = False) -> str:
            parts = []
            for k, v in list(d.items())[:10]:
                parts.append(f"{nivel_emoji(k)} {k}:{v}" if with_emoji else f"{k}:{v}")
            return ", ".join(parts)

        msg = (
            f"IDAP Daily Maps\n"
            f"{period_txt}\n"
            f"Entradas RSS atuais: {len(entries)}\n"
            f"CAPs parseados do feed: {len(feed_alerts)} | erros: {len(errors)}\n"
            f"Alertas válidos últimas {window_hours}h: {len(alerts)}\n"
            f"Histórico total salvo: {len(history_kept)}\n"
            f"Nível: {_fmt_counts(by_nivel, with_emoji=True)}\n"
            f"Tipo: {_fmt_counts(by_typ, with_emoji=False)}\n"
            f"Regiões: {_fmt_counts(by_reg, with_emoji=False)}\n"
        )
        try:
            _tg_send_message(tg_token, tg_chat_id, msg)
            print("[INFO] Telegram: mensagem enviada")
        except Exception as e:
            print(f"[WARN] Telegram: falha ao enviar mensagem: {e}")
        for pth, cap in [
            (graf_hora, "Gráfico: Alertas emitidos por hora nas últimas 24h"),
            (map1, "Mapa 1: Todas as Categorias"),
            (map2, "Mapa 2: Chuva, Tempestade e Inundação"),
            (map3, "Mapa 3: Deslizamentos"),
            (map4, "Mapa 4: Outras Categorias"),
        ]:
            if pth:
                try:
                    _tg_send_photo(tg_token, tg_chat_id, pth, cap)
                    print(f"[INFO] Telegram: enviado {os.path.basename(pth)}")
                except Exception as e:
                    print(f"[WARN] Telegram: falha ao enviar {os.path.basename(pth)}: {e}")
    else:
        print("[INFO] Telegram: não configurado (TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID vazios)")

    state["last_run_ts"] = run_ts
    state["last_run_iso"] = datetime.now(timezone.utc).isoformat()
    state["last_counts"] = {
        "entries": len(entries),
        "feed_alerts": len(feed_alerts),
        "window_alerts": len(alerts),
        "history_alerts": len(history_kept),
        "errors": len(errors),
    }
    state["history_path"] = history_path
    state["window_hours"] = window_hours
    state["retention_hours"] = retention_hours
    _save_state(state_path, state)

    print("[INFO] Finalizado.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
