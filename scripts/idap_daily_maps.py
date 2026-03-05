#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IDAP Daily Maps
- Lê o RSS Atom da IDAP (CAP 1.2 dentro do <content type="text/xml">)
- Faz varredura COMPLETA a cada execução (não filtra por status)
- Parseia CAPs, extrai polygon/geocode, monta estatísticas
- Gera um mapa PNG (UFs + polígonos dos alertas)
- Envia resumo + mapa para Telegram (opcional, via env vars)
- Escreve saídas em out/run_YYYYMMDD_HHMMSS/
- Mantém .cache/state.json (mesmo que você não use para filtro)
"""

import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# Dependências: geopandas, shapely, matplotlib
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
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

SEVERITY_COLORS = {
    # CAP severity: Extreme, Severe, Moderate, Minor, Unknown
    "Extreme": "#6a0dad",   # roxo
    "Severe":  "#d62728",   # vermelho
    "Moderate": "#ff7f0e",  # laranja
    "Minor":   "#ffd92f",   # amarelo
    "Unknown": "#7f7f7f",   # cinza
    None:      "#7f7f7f",
    "":        "#7f7f7f",
}

# se quiser deixar mais visível:
ALERT_ALPHA = 0.35
BORDER_ALPHA = 0.9


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
    uf_hint: Optional[str]  # tentativa de UF (ex: GO)

    # geometria (não vai para JSON direto)
    geometry_wkt: Optional[str]


# ----------------------------
# Utilitários
# ----------------------------

def _now_sp() -> datetime:
    # sem depender de pytz/zoneinfo
    # a string do run dir só precisa ser estável
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
        headers={
            "User-Agent": "IDAP-Daily-Maps/1.0 (+github-actions)",
            "Accept": "*/*",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


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

    # fecha anel se necessário
    if pts[0] != pts[-1]:
        pts.append(pts[0])

    geom: BaseGeometry = Polygon(pts)

    # “conserta” polígonos inválidos; pode virar MultiPolygon
    if not geom.is_valid:
        geom = geom.buffer(0)

    if geom.is_empty:
        return None

    return geom


def _geom_points_count(geom: Optional[BaseGeometry]) -> int:
    """
    Conta pontos do contorno, funciona para Polygon e MultiPolygon.
    Esse é o ponto que estava quebrando no seu run.
    """
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


def _guess_uf(area_desc: Optional[str], identifier: str) -> Optional[str]:
    # exemplos comuns: "/GO", "MINAS GERAIS/MG", "GOIÁS/GO", "GO", etc
    txt = (area_desc or "").strip().upper()

    m = re.search(r"/([A-Z]{2})\b", txt)
    if m:
        return m.group(1)

    m = re.search(r"\b([A-Z]{2})\b", txt)
    if m:
        # cuidado: pode pegar coisas aleatórias, mas costuma funcionar se areaDesc for curto
        return m.group(1)

    # fallback: nada
    return None


def _cap_get_parameter(info_elem: ET.Element, value_name: str) -> Optional[str]:
    # <parameter><valueName>CHANNEL-LIST</valueName><value>Google</value></parameter>
    for p in _all(info_elem, "cap:parameter", CAP_NS):
        vn = _safe_text(_first(p, "cap:valueName", CAP_NS))
        if vn and vn.strip().upper() == value_name.strip().upper():
            return _safe_text(_first(p, "cap:value", CAP_NS))
    return None


def _extract_cap_xml_from_entry(entry: ET.Element) -> Optional[ET.Element]:
    """
    Pega o <content type="text/xml"> que contém <alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">...</alert>
    Alguns feeds colocam como elemento filho real, outros como texto.
    """
    content = _first(entry, "atom:content", ATOM_NS)
    if content is None:
        return None

    # Caso 1: o <alert> vem como filho dentro do content
    for child in list(content):
        if child.tag.endswith("alert"):
            return child

    # Caso 2: o conteúdo vem como texto (às vezes escapado)
    raw = content.text
    if raw is None:
        return None
    raw = raw.strip()
    if not raw:
        return None

    # Se vier como XML “normal”, parseia direto
    try:
        root = ET.fromstring(raw)
        if root.tag.endswith("alert"):
            return root
    except Exception:
        pass

    # Se vier escapado, tenta “desescapar” o mínimo
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

        identifier = _safe_text(_first(cap_alert, "cap:identifier", CAP_NS)) or _safe_text(_first(entry, "atom:id", ATOM_NS)) or "UNKNOWN"
        sender = _safe_text(_first(cap_alert, "cap:sender", CAP_NS))
        sent = _safe_text(_first(cap_alert, "cap:sent", CAP_NS))
        status = _safe_text(_first(cap_alert, "cap:status", CAP_NS))
        msgType = _safe_text(_first(cap_alert, "cap:msgType", CAP_NS))

        info = _first(cap_alert, "cap:info", CAP_NS)
        if info is None:
            # alguns CAP podem ter vários <info>, pega o primeiro
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

        uf_hint = _guess_uf(areaDesc, identifier)

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
            geometry_wkt=geom.wkt if geom is not None else None,
        )
        return rec, None

    except Exception as e:
        return None, f"erro parse CAP: {e}"


def _load_uf_gdf(path: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path)

    # garante CRS
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326", allow_override=True)
    else:
        try:
            gdf = gdf.to_crs("EPSG:4326")
        except Exception:
            # se der ruim, mantém
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


def _make_stats(alerts: List[AlertRecord]) -> Dict[str, Any]:
    def _count_by(key_fn):
        d: Dict[str, int] = {}
        for a in alerts:
            k = key_fn(a) or "N/A"
            d[k] = d.get(k, 0) + 1
        return dict(sorted(d.items(), key=lambda x: (-x[1], x[0])))

    stats = {
        "total_alerts": len(alerts),
        "by_status": _count_by(lambda a: a.status),
        "by_severity": _count_by(lambda a: a.severity),
        "by_category": _count_by(lambda a: a.category),
        "by_event": _count_by(lambda a: a.event),
        "by_channel_list": _count_by(lambda a: a.channel_list),
        "by_senderName": dict(list(_count_by(lambda a: a.senderName).items())[:15]),
        "by_uf_hint": _count_by(lambda a: a.uf_hint),
        "with_polygon": sum(1 for a in alerts if a.geometry_wkt),
        "with_geocode": sum(1 for a in alerts if a.has_geocode),
    }
    return stats


def _plot_map(
    uf_gdf: gpd.GeoDataFrame,
    alerts_gdf: gpd.GeoDataFrame,
    out_path: str,
    title: str,
) -> None:
    fig = plt.figure(figsize=(12, 12))
    ax = plt.gca()

    # base: estados
    uf_gdf.boundary.plot(ax=ax, linewidth=0.6, alpha=BORDER_ALPHA)

    if len(alerts_gdf) > 0:
        # cria uma coluna de cor
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


def _tg_api(method: str) -> str:
    return f"https://api.telegram.org/bot{method}"


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
    """
    Envia o PNG como multipart/form-data (sem requests, só stdlib).
    """
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


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    rss_url = os.getenv("RSS_URL", DEFAULT_RSS_URL)
    uf_geojson_path = os.getenv("UF_GEOJSON_PATH", DEFAULT_UF_GEOJSON_PATH)
    out_dir = os.getenv("OUT_DIR", DEFAULT_OUT_DIR)
    state_path = os.getenv("STATE_PATH", DEFAULT_STATE_PATH)

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

    run_ts = _now_sp().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(out_dir, f"run_{run_ts}")
    print(f"[INFO] RUN_DIR={run_dir}")
    print(f"[INFO] STATE_PATH={state_path}")
    print(f"[INFO] MAX_ITEMS={'(sem limite)' if max_items is None else max_items}")

    _ensure_dirs(".cache", out_dir, run_dir)

    # estado (não filtra, só registra que rodou)
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

    # salva JSONs
    alerts_json_path = os.path.join(run_dir, "alerts.json")
    errors_json_path = os.path.join(run_dir, "errors.json")
    stats_json_path = os.path.join(run_dir, "stats.json")

    with open(alerts_json_path, "w", encoding="utf-8") as f:
        json.dump([asdict(a) for a in alerts], f, ensure_ascii=False, indent=2)

    with open(errors_json_path, "w", encoding="utf-8") as f:
        json.dump(errors, f, ensure_ascii=False, indent=2)

    stats = _make_stats(alerts)
    with open(stats_json_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    # mapa
    map_path = os.path.join(run_dir, "mapa_alertas.png")

    alerts_gdf = _alerts_to_gdf(alerts)
    if len(alerts_gdf) == 0:
        print("[WARN] Mapa não gerado: nenhum alerta com polygon para plotar")
        map_path = ""  # não envia foto
    else:
        try:
            uf_gdf = _load_uf_gdf(uf_geojson_path)
        except Exception as e:
            print(f"[ERROR] Falha ao ler UF GeoJSON: {e}")
            return 4

        title = f"Alertas IDAP (varredura completa) | {run_ts}"
        _plot_map(uf_gdf, alerts_gdf, map_path, title)
        print(f"[INFO] Mapa gerado: {map_path}")

    # Telegram
    if tg_token and tg_chat_id:
        sev_counts = stats.get("by_severity", {})
        top_sev = ", ".join([f"{k}:{v}" for k, v in list(sev_counts.items())[:4]]) if isinstance(sev_counts, dict) else ""
        msg = (
            f"IDAP Daily Maps\n"
            f"Rodada: {run_ts}\n"
            f"Total CAPs no RSS (considerados): {len(entries)}\n"
            f"CAPs parseados: {len(alerts)} | erros: {len(errors)}\n"
            f"Com polygon: {stats.get('with_polygon', 0)} | com geocode: {stats.get('with_geocode', 0)}\n"
            f"Severidade (top): {top_sev}\n"
        )

        ok, detail = _send_telegram_message(tg_token, tg_chat_id, msg)
        if ok:
            print("[INFO] Telegram: mensagem enviada")
        else:
            print(f"[WARN] Telegram: falha ao enviar mensagem: {detail}")

        if map_path:
            ok2, detail2 = _send_telegram_photo(tg_token, tg_chat_id, map_path, caption=f"Mapa | {run_ts}")
            if ok2:
                print("[INFO] Telegram: mapa enviado")
            else:
                print(f"[WARN] Telegram: falha ao enviar mapa: {detail2}")

    else:
        print("[INFO] Telegram: não configurado (TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID vazios)")

    # atualiza state.json (só log de execução)
    state["last_run_ts"] = run_ts
    state["last_run_iso"] = datetime.now(timezone.utc).isoformat()
    state["last_counts"] = {
        "entries": len(entries),
        "alerts": len(alerts),
        "errors": len(errors),
    }
    _save_state(state_path, state)

    print("[INFO] Finalizado.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
