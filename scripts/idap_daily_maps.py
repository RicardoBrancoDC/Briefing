#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IDAP Daily Maps (refinado)
- Lê RSS Atom da IDAP (CAP 1.2 dentro do <content type="text/xml">)
- Varredura completa a cada execução (sem filtrar por status)
- Gera 4 mapas:
  1) mapa_alertas_todos.png (todos os alertas com polygon, cor por severity)
  2) mapa_alertas_chuva_temp_inund.png (subset de eventos)
  3) mapa_ocorrencias_deslizamento.png (pontos)
  4) mapa_ocorrencias_outros.png (pontos)
- Gera quadro geral: resumo.json e resumo.md
- Envia resumo + mapas para Telegram (se configurado)
"""

import csv
import json
import os
import re
import sys
import unicodedata
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon, Point
from shapely.geometry.base import BaseGeometry
import matplotlib.pyplot as plt


# ----------------------------
# Config / Env
# ----------------------------

ATOM_NS = {"atom": "http://www.w3.org/2005/Atom", "dc": "http://purl.org/dc/elements/1.1/"}
CAP_NS = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}

DEFAULT_RSS_URL = "https://idapfile.mdr.gov.br/idap/api/rss/cap"
DEFAULT_UF_GEOJSON_PATH = "resources/br_uf.geojson"
DEFAULT_OUT_DIR = "out"
DEFAULT_STATE_PATH = ".cache/state.json"

# Arquivo opcional de ocorrências.
# Aceita:
# - CSV com colunas: lat, lon, tipo (ou type), e opcionalmente data/hora
# - GeoJSON com Point e um campo tipo/type
DEFAULT_OCCURRENCES_PATH = ""  # via env OCCURRENCES_PATH

# CAP severity -> cor
SEVERITY_COLORS = {
    "Extreme": "#6a0dad",    # roxo
    "Severe":  "#d62728",    # vermelho
    "Moderate": "#2ca02c",   # verde (você pode preferir laranja, mas deixei assim por leitura)
    "Minor":   "#ffd92f",    # amarelo
    "Unknown": "#7f7f7f",
    None:      "#7f7f7f",
    "":        "#7f7f7f",
}

ALERT_ALPHA = 0.35
BORDER_ALPHA = 0.9


# UF -> Região
UF_TO_REGION = {
    # Norte
    "AC": "N", "AP": "N", "AM": "N", "PA": "N", "RO": "N", "RR": "N", "TO": "N",
    # Nordeste
    "AL": "NE", "BA": "NE", "CE": "NE", "MA": "NE", "PB": "NE", "PE": "NE",
    "PI": "NE", "RN": "NE", "SE": "NE",
    # Centro-Oeste
    "DF": "CO", "GO": "CO", "MT": "CO", "MS": "CO",
    # Sudeste
    "ES": "SE", "MG": "SE", "RJ": "SE", "SP": "SE",
    # Sul
    "PR": "S", "RS": "S", "SC": "S",
}


# Eventos para o mapa 2 (normalizados)
TARGET_EVENTS_NORM = {
    "CHUVAS INTENSAS",
    "TEMPESTADE LOCAL CONVECTIVA",
    "TEMPESTADES CONVECTIVAS",
    "INUNDACOES",
    "INUNDAÇÕES",
}


# ----------------------------
# Modelos
# ----------------------------

@dataclass
class AlertRecord:
    identifier: str
    sender: Optional[str]
    senderName: Optional[str]
    sent: Optional[str]
    status: Optional[str]
    msgType: Optional[str]

    category: Optional[str]
    event: Optional[str]
    urgency: Optional[str]
    severity: Optional[str]
    certainty: Optional[str]
    onset: Optional[str]
    expires: Optional[str]

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


# ----------------------------
# Utilitários gerais
# ----------------------------

def _now_sp() -> datetime:
    return datetime.now().astimezone()


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


def _read_url(url: str, timeout: int = 30) -> bytes:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "IDAP-Daily-Maps/1.1 (+github-actions)", "Accept": "*/*"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.strip()
    if not s:
        return ""
    # remove acentos
    s2 = unicodedata.normalize("NFKD", s)
    s2 = "".join([c for c in s2 if not unicodedata.combining(c)])
    return s2.upper()


def _parse_polygon_str(poly_str: str) -> Optional[BaseGeometry]:
    """
    CAP polygon vem como "lat,lon lat,lon ..."
    Shapely espera (x,y) = (lon,lat)
    """
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


def _guess_uf(area_desc: Optional[str]) -> Optional[str]:
    txt = (area_desc or "").strip().upper()
    m = re.search(r"/([A-Z]{2})\b", txt)
    if m:
        return m.group(1)

    # fallback: tenta "MINAS GERAIS/MG"
    m = re.search(r"\b([A-Z]{2})\b", txt)
    if m:
        return m.group(1)

    return None


def _uf_to_region(uf: Optional[str]) -> Optional[str]:
    if not uf:
        return None
    uf2 = uf.strip().upper()
    return UF_TO_REGION.get(uf2)


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

        identifier = _safe_text(_first(cap_alert, "cap:identifier", CAP_NS)) or "UNKNOWN"
        sender = _safe_text(_first(cap_alert, "cap:sender", CAP_NS))
        sent = _safe_text(_first(cap_alert, "cap:sent", CAP_NS))
        status = _safe_text(_first(cap_alert, "cap:status", CAP_NS))
        msgType = _safe_text(_first(cap_alert, "cap:msgType", CAP_NS))

        info = _first(cap_alert, "cap:info", CAP_NS)
        if info is None:
            infos = _all(cap_alert, "cap:info", CAP_NS)
            info = infos[0] if infos else None

        category = event = urgency = severity = certainty = onset = expires = None
        senderName = headline = description = instruction = web = contact = None
        channel_list = None
        areaDesc = None
        polygon_raw = None
        has_geocode = False
        geom: Optional[BaseGeometry] = None

        if info is not None:
            category = _safe_text(_first(info, "cap:category", CAP_NS))
            event = _safe_text(_first(info, "cap:event", CAP_NS))
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

        uf_hint = _guess_uf(areaDesc)
        region = _uf_to_region(uf_hint)

        rec = AlertRecord(
            identifier=identifier,
            sender=sender,
            senderName=senderName,
            sent=sent,
            status=status,
            msgType=msgType,
            category=category,
            event=event,
            urgency=urgency,
            severity=severity,
            certainty=certainty,
            onset=onset,
            expires=expires,
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

    df = gpd.GeoDataFrame([asdict(r) for r in rows], geometry=geoms, crs="EPSG:4326")
    return df


def _count_by(alerts: List[AlertRecord], key_fn) -> Dict[str, int]:
    d: Dict[str, int] = {}
    for a in alerts:
        k = key_fn(a) or "N/A"
        d[k] = d.get(k, 0) + 1
    return dict(sorted(d.items(), key=lambda x: (-x[1], x[0])))


def _make_summary(alerts: List[AlertRecord]) -> Dict[str, Any]:
    by_severity = _count_by(alerts, lambda a: a.severity)
    by_channel = _count_by(alerts, lambda a: a.channel_list)
    by_region = _count_by(alerts, lambda a: a.region)

    by_event = _count_by(alerts, lambda a: a.event)

    # por região e severity (matriz simples)
    regions = ["N", "NE", "CO", "SE", "S", "N/A"]
    sev_keys = list(by_severity.keys()) if by_severity else []
    matrix: Dict[str, Dict[str, int]] = {r: {s: 0 for s in sev_keys} for r in regions}

    for a in alerts:
        r = a.region or "N/A"
        s = a.severity or "N/A"
        if r not in matrix:
            matrix[r] = {}
        if s not in matrix[r]:
            matrix[r][s] = 0
        matrix[r][s] += 1

    return {
        "total_alerts": len(alerts),
        "by_severity": by_severity,
        "by_channel_list": by_channel,
        "by_region": by_region,
        "by_event": dict(list(by_event.items())[:30]),
        "region_x_severity": matrix,
    }


def _plot_alerts_map(
    uf_gdf: gpd.GeoDataFrame,
    alerts_gdf: gpd.GeoDataFrame,
    out_path: str,
    title: str,
) -> None:
    fig = plt.figure(figsize=(12, 12))
    ax = plt.gca()

    uf_gdf.boundary.plot(ax=ax, linewidth=0.6, alpha=BORDER_ALPHA)

    if len(alerts_gdf) > 0:
        def sev_color(s):
            s = (s or "").strip()
            return SEVERITY_COLORS.get(s, SEVERITY_COLORS["Unknown"])

        alerts_gdf["_color"] = alerts_gdf["severity"].apply(sev_color)

        alerts_gdf.plot(
            ax=ax,
            color=alerts_gdf["_color"],
            edgecolor=alerts_gdf["_color"],
            linewidth=0.8,
            alpha=ALERT_ALPHA,
        )

    ax.set_title(title, fontsize=12)
    ax.set_axis_off()
    plt.tight_layout()
    fig.savefig(out_path, dpi=200)
    plt.close(fig)


def _load_occurrences(path: str) -> Optional[gpd.GeoDataFrame]:
    if not path or not os.path.exists(path):
        return None

    # tenta GeoJSON/qualquer coisa que o geopandas leia
    try:
        gdf = gpd.read_file(path)
        if gdf.crs is None:
            gdf = gdf.set_crs("EPSG:4326", allow_override=True)
        else:
            try:
                gdf = gdf.to_crs("EPSG:4326")
            except Exception:
                pass
        # garante que é point
        if "geometry" not in gdf.columns:
            return None
        return gdf
    except Exception:
        pass

    # fallback CSV
    try:
        rows = []
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                rows.append(r)
        if not rows:
            return None

        pts = []
        tipos = []
        for r in rows:
            lat = r.get("lat") or r.get("latitude")
            lon = r.get("lon") or r.get("lng") or r.get("longitude")
            tp = r.get("tipo") or r.get("type") or r.get("ocorrencia") or r.get("event") or ""
            if lat is None or lon is None:
                continue
            try:
                latf = float(str(lat).replace(",", "."))
                lonf = float(str(lon).replace(",", "."))
            except ValueError:
                continue
            pts.append(Point(lonf, latf))
            tipos.append(tp)

        if not pts:
            return None

        gdf = gpd.GeoDataFrame({"tipo": tipos}, geometry=pts, crs="EPSG:4326")
        return gdf
    except Exception:
        return None


def _is_landslide_occurrence(tipo: str) -> bool:
    t = _normalize_text(tipo)
    # aceita variações: DESLIZAMENTO, ESCORREGAMENTO, CORRIDA DE MASSA etc
    return any(k in t for k in ["DESLIZ", "ESCORREG", "CORRIDA DE MASSA", "MOVIMENTO DE MASSA"])


def _plot_occurrences_map(
    uf_gdf: gpd.GeoDataFrame,
    occ_gdf: gpd.GeoDataFrame,
    out_path: str,
    title: str,
) -> None:
    fig = plt.figure(figsize=(12, 12))
    ax = plt.gca()

    uf_gdf.boundary.plot(ax=ax, linewidth=0.6, alpha=BORDER_ALPHA)

    if len(occ_gdf) > 0:
        # pontos pretos simples
        occ_gdf.plot(ax=ax, markersize=12)

    ax.set_title(title, fontsize=12)
    ax.set_axis_off()
    plt.tight_layout()
    fig.savefig(out_path, dpi=200)
    plt.close(fig)


def _ensure_dirs(*paths: str) -> None:
    for p in paths:
        os.makedirs(p, exist_ok=True)


def _load_state(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(path: str, state: Dict[str, Any]) -> None:
    parent = os.path.dirname(path) or "."
    os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _send_telegram_message(token: str, chat_id: str, text: str) -> Tuple[bool, str]:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = json.dumps({"chat_id": chat_id, "text": text}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            _ = resp.read()
        return True, "ok"
    except Exception as e:
        return False, str(e)


def _send_telegram_photo(token: str, chat_id: str, photo_path: str, caption: str = "") -> Tuple[bool, str]:
    import uuid
    boundary = f"----WebKitFormBoundary{uuid.uuid4().hex}"
    url = f"https://api.telegram.org/bot{token}/sendPhoto"

    try:
        with open(photo_path, "rb") as f:
            photo_bytes = f.read()
    except Exception as e:
        return False, f"falha lendo foto: {e}"

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

    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            _ = resp.read()
        return True, "ok"
    except Exception as e:
        return False, str(e)


def _write_resumo_md(path: str, resumo: Dict[str, Any]) -> None:
    lines = []
    lines.append("# Quadro geral")
    lines.append("")
    lines.append(f"Total de alertas (RSS considerados): **{resumo.get('total_alerts', 0)}**")
    lines.append("")

    def _block(title: str, d: Dict[str, int]):
        lines.append(f"## {title}")
        lines.append("")
        for k, v in d.items():
            lines.append(f"- {k}: {v}")
        lines.append("")

    _block("Nível (severity)", resumo.get("by_severity", {}))
    _block("Tipo (CHANNEL-LIST)", resumo.get("by_channel_list", {}))
    _block("Alertas por regiões do Brasil", resumo.get("by_region", {}))

    lines.append("## Alertas por evento (top)")
    lines.append("")
    for k, v in resumo.get("by_event", {}).items():
        lines.append(f"- {k}: {v}")
    lines.append("")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    rss_url = os.getenv("RSS_URL", DEFAULT_RSS_URL)
    uf_geojson_path = os.getenv("UF_GEOJSON_PATH", DEFAULT_UF_GEOJSON_PATH)
    out_dir = os.getenv("OUT_DIR", DEFAULT_OUT_DIR)
    state_path = os.getenv("STATE_PATH", DEFAULT_STATE_PATH)
    occ_path = os.getenv("OCCURRENCES_PATH", DEFAULT_OCCURRENCES_PATH).strip()

    max_items_env = os.getenv("MAX_ITEMS", "").strip()
    max_items: Optional[int] = None
    if max_items_env:
        try:
            max_items = int(max_items_env)
        except ValueError:
            max_items = None

    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    tg_chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    print(f"[INFO] RSS_URL={rss_url}")
    print(f"[INFO] UF_GEOJSON_PATH={uf_geojson_path}")
    print(f"[INFO] OUT_DIR={out_dir}")
    print(f"[INFO] OCCURRENCES_PATH={(occ_path if occ_path else '(não informado)')}")

    run_ts = _now_sp().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(out_dir, f"run_{run_ts}")
    print(f"[INFO] RUN_DIR={run_dir}")
    print(f"[INFO] STATE_PATH={state_path}")
    print(f"[INFO] MAX_ITEMS={'(sem limite)' if max_items is None else max_items}")

    _ensure_dirs(".cache", out_dir, run_dir)

    state = _load_state(state_path)

    # baixa RSS
    try:
        rss_bytes = _read_url(rss_url, timeout=40)
    except urllib.error.URLError as e:
        print(f"[ERROR] Falha ao baixar RSS: {e}")
        return 2

    try:
        root = ET.fromstring(rss_bytes)
    except Exception as e:
        print(f"[ERROR] RSS inválido (XML): {e}")
        return 3

    entries = _all(root, "atom:entry", ATOM_NS)
    if max_items is not None:
        entries = entries[:max_items]

    print(f"[INFO] Entradas no RSS (consideradas): {len(entries)}")

    alerts: List[AlertRecord] = []
    errors: List[Dict[str, Any]] = []

    for entry in entries:
        a, err = _parse_cap_from_entry(entry)
        if a is None:
            errors.append({"error": err or "desconhecido"})
            continue
        alerts.append(a)

    print(f"[INFO] CAPs parseados: {len(alerts)} | erros: {len(errors)}")

    # salva dados brutos
    with open(os.path.join(run_dir, "alerts.json"), "w", encoding="utf-8") as f:
        json.dump([asdict(a) for a in alerts], f, ensure_ascii=False, indent=2)
    with open(os.path.join(run_dir, "errors.json"), "w", encoding="utf-8") as f:
        json.dump(errors, f, ensure_ascii=False, indent=2)

    # quadro geral
    resumo = _make_summary(alerts)
    resumo_json_path = os.path.join(run_dir, "resumo.json")
    resumo_md_path = os.path.join(run_dir, "resumo.md")

    with open(resumo_json_path, "w", encoding="utf-8") as f:
        json.dump(resumo, f, ensure_ascii=False, indent=2)
    _write_resumo_md(resumo_md_path, resumo)

    # base brasil
    try:
        uf_gdf = _load_uf_gdf(uf_geojson_path)
    except Exception as e:
        print(f"[ERROR] Falha ao ler UF GeoJSON: {e}")
        return 4

    # Mapa 1: todos alertas
    alerts_gdf = _alerts_to_gdf(alerts)
    map1 = os.path.join(run_dir, "mapa_alertas_todos.png")
    if len(alerts_gdf) > 0:
        _plot_alerts_map(
            uf_gdf,
            alerts_gdf,
            map1,
            f"Alertas IDAP (todos) | {run_ts}",
        )
        print(f"[INFO] Mapa gerado: {map1}")
    else:
        map1 = ""
        print("[WARN] Mapa 1 não gerado: nenhum alerta com polygon")

    # Mapa 2: subset eventos
    def _is_target_event(ev: Optional[str]) -> bool:
        n = _normalize_text(ev)
        # compatibiliza “Tempestade Local Convectiva - Chuvas Intensas”
        if "TEMPESTADE" in n and "CONVECT" in n:
            return True
        if "CHUVA" in n and "INTENSA" in n:
            return True
        if "INUND" in n:
            return True
        return n in { _normalize_text(x) for x in TARGET_EVENTS_NORM }

    alerts_subset = [a for a in alerts if _is_target_event(a.event)]
    subset_gdf = _alerts_to_gdf(alerts_subset)
    map2 = os.path.join(run_dir, "mapa_alertas_chuva_temp_inund.png")
    if len(subset_gdf) > 0:
        _plot_alerts_map(
            uf_gdf,
            subset_gdf,
            map2,
            f"Alertas: Chuvas Intensas, Tempestades Convectivas, Inundações | {run_ts}",
        )
        print(f"[INFO] Mapa gerado: {map2}")
    else:
        map2 = ""
        print("[WARN] Mapa 2 não gerado: nenhum alerta (subset) com polygon")

    # Ocorrências (opcional)
    map3 = os.path.join(run_dir, "mapa_ocorrencias_deslizamento.png")
    map4 = os.path.join(run_dir, "mapa_ocorrencias_outros.png")

    occ_gdf = _load_occurrences(occ_path) if occ_path else None
    if occ_gdf is None:
        map3 = ""
        map4 = ""
        print("[WARN] Ocorrências não carregadas. Mapas 3 e 4 não serão gerados.")
    else:
        # tenta descobrir o campo de tipo
        tipo_col = None
        for c in ["tipo", "type", "ocorrencia", "evento", "event"]:
            if c in occ_gdf.columns:
                tipo_col = c
                break
        if tipo_col is None:
            # se não tiver, cria vazio para não quebrar filtro
            occ_gdf["tipo"] = ""
            tipo_col = "tipo"

        occ_gdf["_tipo_norm"] = occ_gdf[tipo_col].apply(lambda x: _normalize_text(str(x) if x is not None else ""))

        occ_land = occ_gdf[occ_gdf[tipo_col].apply(lambda x: _is_landslide_occurrence(str(x) if x is not None else ""))].copy()
        occ_other = occ_gdf[~occ_gdf[tipo_col].apply(lambda x: _is_landslide_occurrence(str(x) if x is not None else ""))].copy()

        if len(occ_land) > 0:
            _plot_occurrences_map(uf_gdf, occ_land, map3, f"Ocorrências: Deslizamento | {run_ts}")
            print(f"[INFO] Mapa gerado: {map3}")
        else:
            map3 = ""
            print("[WARN] Mapa 3 não gerado: nenhuma ocorrência de deslizamento")

        if len(occ_other) > 0:
            _plot_occurrences_map(uf_gdf, occ_other, map4, f"Ocorrências: Outros tipos | {run_ts}")
            print(f"[INFO] Mapa gerado: {map4}")
        else:
            map4 = ""
            print("[WARN] Mapa 4 não gerado: nenhuma ocorrência em outros tipos")

    # Telegram
    if tg_token and tg_chat_id:
        by_sev = resumo.get("by_severity", {})
        by_reg = resumo.get("by_region", {})
        by_typ = resumo.get("by_channel_list", {})

        sev_line = ", ".join([f"{k}:{v}" for k, v in list(by_sev.items())[:6]]) if isinstance(by_sev, dict) else ""
        reg_line = ", ".join([f"{k}:{v}" for k, v in list(by_reg.items())[:6]]) if isinstance(by_reg, dict) else ""
        typ_line = ", ".join([f"{k}:{v}" for k, v in list(by_typ.items())[:6]]) if isinstance(by_typ, dict) else ""

        msg = (
            f"IDAP Daily Maps\n"
            f"Rodada: {run_ts}\n"
            f"Total (RSS considerados): {len(entries)}\n"
            f"CAPs parseados: {len(alerts)} | erros: {len(errors)}\n"
            f"Nível: {sev_line}\n"
            f"Tipo (CHANNEL-LIST): {typ_line}\n"
            f"Regiões: {reg_line}\n"
        )

        ok, detail = _send_telegram_message(tg_token, tg_chat_id, msg)
        if ok:
            print("[INFO] Telegram: mensagem enviada")
        else:
            print(f"[WARN] Telegram: falha ao enviar mensagem: {detail}")

        for pth, cap in [
            (map1, f"Mapa 1: todos | {run_ts}"),
            (map2, f"Mapa 2: chuva/temp/inund | {run_ts}"),
            (map3, f"Mapa 3: deslizamento | {run_ts}"),
            (map4, f"Mapa 4: outros | {run_ts}"),
        ]:
            if pth:
                ok2, detail2 = _send_telegram_photo(tg_token, tg_chat_id, pth, caption=cap)
                if ok2:
                    print(f"[INFO] Telegram: enviado {os.path.basename(pth)}")
                else:
                    print(f"[WARN] Telegram: falha ao enviar {os.path.basename(pth)}: {detail2}")
    else:
        print("[INFO] Telegram: não configurado (TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID vazios)")

    # state.json (só registro)
    state["last_run_ts"] = run_ts
    state["last_run_iso"] = datetime.now(timezone.utc).isoformat()
    state["last_counts"] = {"entries": len(entries), "alerts": len(alerts), "errors": len(errors)}
    _save_state(state_path, state)

    print("[INFO] Finalizado.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
