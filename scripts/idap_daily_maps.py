#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import io
import json
import os
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import xml.etree.ElementTree as ET

# deps
import pandas as pd
import matplotlib.pyplot as plt

import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
from shapely import wkt

try:
    import requests
except Exception:
    requests = None


ATOM_NS = {"atom": "http://www.w3.org/2005/Atom", "dc": "http://purl.org/dc/elements/1.1/"}
CAP_NS = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}


def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else v


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_filename(s: str) -> str:
    keep = []
    for ch in s:
        if ch.isalnum() or ch in ("-", "_", ".", " "):
            keep.append(ch)
        else:
            keep.append("_")
    out = "".join(keep).strip().replace(" ", "_")
    return out[:120] if len(out) > 120 else out


def _parse_iso(dt_str: str) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        # aceita Z e offsets
        if dt_str.endswith("Z"):
            return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return datetime.fromisoformat(dt_str)
    except Exception:
        return None


def _http_get_text(url: str, timeout: int = 30) -> str:
    if requests is None:
        import urllib.request
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.read().decode("utf-8", errors="replace")
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    r.encoding = "utf-8"
    return r.text


def _mkdirp(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _read_json(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: str, obj: Any) -> None:
    _mkdirp(os.path.dirname(path) or ".")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _telegram_send_message(token: str, chat_id: str, text: str) -> bool:
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    try:
        if requests is None:
            import urllib.parse, urllib.request
            data = urllib.parse.urlencode(payload).encode("utf-8")
            req = urllib.request.Request(url, data=data, method="POST")
            with urllib.request.urlopen(req, timeout=30) as r:
                _ = r.read()
            return True
        r = requests.post(url, data=payload, timeout=30)
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"[WARN] Telegram: falha ao enviar mensagem: {e}")
        return False


def _telegram_send_document(token: str, chat_id: str, file_path: str, caption: str = "") -> bool:
    """
    Envia arquivo como documento (melhor para PNG grande, sem compressão agressiva).
    Requer requests. Se requests não estiver disponível, retorna False.
    """
    if not token or not chat_id:
        return False
    if not os.path.exists(file_path):
        return False
    if requests is None:
        print("[WARN] Telegram: requests não disponível, não dá para enviar arquivo.")
        return False

    url = f"https://api.telegram.org/bot{token}/sendDocument"
    try:
        with open(file_path, "rb") as f:
            files = {"document": (os.path.basename(file_path), f)}
            data = {"chat_id": chat_id, "caption": caption} if caption else {"chat_id": chat_id}
            r = requests.post(url, data=data, files=files, timeout=60)
            r.raise_for_status()
        return True
    except Exception as e:
        print(f"[WARN] Telegram: falha ao enviar arquivo: {e}")
        return False


def _cap_text(el: Optional[ET.Element]) -> str:
    if el is None or el.text is None:
        return ""
    return el.text.strip()


def _parse_polygon_str(poly_str: str) -> Optional[Polygon]:
    """
    CAP polygon: "lat,lon lat,lon lat,lon"
    shapely Polygon espera (x,y) = (lon,lat)
    """
    if not poly_str:
        return None
    pts = []
    for part in poly_str.strip().split():
        if "," not in part:
            continue
        a, b = part.split(",", 1)
        try:
            lat = float(a)
            lon = float(b)
            pts.append((lon, lat))
        except Exception:
            continue

    if len(pts) < 3:
        return None

    # fecha anel se necessário
    if pts[0] != pts[-1]:
        pts.append(pts[0])

    try:
        p = Polygon(pts)
        if not p.is_valid or p.is_empty:
            p = p.buffer(0)
        if p.is_empty:
            return None
        return p
    except Exception:
        return None


@dataclass
class ParsedAlert:
    identifier: str
    sent: str
    expires: str
    status: str
    msgType: str
    senderName: str
    event: str
    category: str
    urgency: str
    severity: str
    certainty: str
    areaDesc: str
    channels: List[str]
    polygon_wkt: str  # salvar WKT para JSON
    polygon_points: int


def _parse_cap_from_entry(entry_el: ET.Element) -> Tuple[Optional[ParsedAlert], Optional[str]]:
    """
    Extrai CAP XML de <content type="text/xml"> ... </content> dentro do Atom.
    """
    content_el = entry_el.find("atom:content", ATOM_NS)
    if content_el is None:
        return None, "entry sem content"

    # O <content> contém um nó <alert xmlns="urn:oasis...">
    # Em ElementTree, isso fica como child.
    alert_el = None
    for child in list(content_el):
        # procura a tag que termina com 'alert'
        if child.tag.endswith("alert"):
            alert_el = child
            break

    if alert_el is None:
        return None, "content sem alert CAP"

    ident = _cap_text(alert_el.find("cap:identifier", CAP_NS))
    sent = _cap_text(alert_el.find("cap:sent", CAP_NS))
    status = _cap_text(alert_el.find("cap:status", CAP_NS))
    msgType = _cap_text(alert_el.find("cap:msgType", CAP_NS))

    info_el = alert_el.find("cap:info", CAP_NS)
    if info_el is None:
        return None, f"{ident or 'sem-id'}: sem info"

    category = _cap_text(info_el.find("cap:category", CAP_NS))
    event = _cap_text(info_el.find("cap:event", CAP_NS))
    urgency = _cap_text(info_el.find("cap:urgency", CAP_NS))
    severity = _cap_text(info_el.find("cap:severity", CAP_NS))
    certainty = _cap_text(info_el.find("cap:certainty", CAP_NS))
    expires = _cap_text(info_el.find("cap:expires", CAP_NS))
    senderName = _cap_text(info_el.find("cap:senderName", CAP_NS))

    channels = []
    for p in info_el.findall("cap:parameter", CAP_NS):
        vn = _cap_text(p.find("cap:valueName", CAP_NS))
        vv = _cap_text(p.find("cap:value", CAP_NS))
        if vn.upper() == "CHANNEL-LIST" and vv:
            # pode vir "SMS, Google" etc
            for it in vv.replace(";", ",").split(","):
                it2 = it.strip()
                if it2:
                    channels.append(it2)

    area_el = info_el.find("cap:area", CAP_NS)
    if area_el is None:
        return None, f"{ident or 'sem-id'}: sem area"

    areaDesc = _cap_text(area_el.find("cap:areaDesc", CAP_NS))
    polygon_str = _cap_text(area_el.find("cap:polygon", CAP_NS))
    poly = _parse_polygon_str(polygon_str)
    if poly is None:
        return None, f"{ident or 'sem-id'}: polygon inválido/ausente"

    return ParsedAlert(
        identifier=ident,
        sent=sent,
        expires=expires,
        status=status,
        msgType=msgType,
        senderName=senderName,
        event=event,
        category=category,
        urgency=urgency,
        severity=severity,
        certainty=certainty,
        areaDesc=areaDesc,
        channels=channels,
        polygon_wkt=poly.wkt,
        polygon_points=len(poly.exterior.coords) if poly.exterior else 0,
    ), None


def _load_rss_entries(rss_xml: str) -> List[ET.Element]:
    root = ET.fromstring(rss_xml)
    entries = root.findall("atom:entry", ATOM_NS)
    return entries


def _load_uf_gdf(path: str) -> Optional[gpd.GeoDataFrame]:
    if not path:
        return None
    if not os.path.exists(path):
        print(f"[WARN] UF_GEOJSON_PATH não encontrado: {path}")
        return None
    try:
        gdf = gpd.read_file(path)
        # tenta achar coluna de nome/UF
        return gdf
    except Exception as e:
        print(f"[WARN] Falha ao ler UF geojson: {e}")
        return None


def _alerts_to_gdf(alerts: List[ParsedAlert]) -> gpd.GeoDataFrame:
    rows = []
    geoms = []
    for a in alerts:
        try:
            geom = wkt.loads(a.polygon_wkt)
        except Exception:
            continue
        rows.append(asdict(a))
        geoms.append(geom)
    gdf = gpd.GeoDataFrame(rows, geometry=geoms, crs="EPSG:4326")
    return gdf


def _severity_color(sev: str) -> str:
    """
    CAP severity típico: Extreme, Severe, Moderate, Minor, Unknown
    """
    s = (sev or "").strip().lower()
    if s == "extreme":
        return "#6a1b9a"  # roxo
    if s == "severe":
        return "#d32f2f"  # vermelho
    if s == "moderate":
        return "#fbc02d"  # amarelo
    if s == "minor":
        return "#388e3c"  # verde
    return "#1565c0"      # azul fallback


def _plot_map(uf_gdf: Optional[gpd.GeoDataFrame], alerts_gdf: gpd.GeoDataFrame, out_png: str) -> bool:
    if alerts_gdf.empty:
        return False

    fig, ax = plt.subplots(figsize=(11, 11))
    ax.set_title("Alertas CAP (IDAP) - polígonos do RSS", fontsize=12)

    if uf_gdf is not None and not uf_gdf.empty:
        try:
            uf_gdf = uf_gdf.to_crs("EPSG:4326")
            uf_gdf.boundary.plot(ax=ax, linewidth=0.6)
        except Exception:
            pass

    # plota por severidade, em camadas
    for sev in ["Extreme", "Severe", "Moderate", "Minor", "Unknown"]:
        sub = alerts_gdf[alerts_gdf["severity"].fillna("") == sev]
        if not sub.empty:
            sub.plot(ax=ax, facecolor=_severity_color(sev), edgecolor="black", linewidth=0.5, alpha=0.35)

    # pega o resto que não bateu
    rest = alerts_gdf[~alerts_gdf["severity"].isin(["Extreme", "Severe", "Moderate", "Minor", "Unknown"])]
    if not rest.empty:
        rest.plot(ax=ax, facecolor=_severity_color(""), edgecolor="black", linewidth=0.5, alpha=0.35)

    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")

    # legenda simples com contagem
    counts = alerts_gdf["severity"].fillna("Unknown").value_counts().to_dict()
    legend_lines = []
    for k in ["Extreme", "Severe", "Moderate", "Minor", "Unknown"]:
        if k in counts:
            legend_lines.append(f"{k}: {counts[k]}")
    ax.text(
        0.01, 0.01,
        "\n".join(legend_lines),
        transform=ax.transAxes,
        fontsize=10,
        verticalalignment="bottom",
        bbox=dict(boxstyle="round", facecolor="white", alpha=0.85)
    )

    _mkdirp(os.path.dirname(out_png) or ".")
    fig.tight_layout()
    fig.savefig(out_png, dpi=160)
    plt.close(fig)
    return True


def main() -> int:
    rss_url = _env("RSS_URL", "https://idapfile.mdr.gov.br/idap/api/rss/cap")
    uf_geojson_path = _env("UF_GEOJSON_PATH", "resources/br_uf.geojson")
    out_dir = _env("OUT_DIR", "out")
    state_path = _env("STATE_PATH", ".cache/state.json")
    max_items_env = _env("MAX_ITEMS", "").strip()
    max_items = int(max_items_env) if max_items_env.isdigit() else None

    tg_token = _env("TELEGRAM_BOT_TOKEN", "").strip()
    tg_chat_id = _env("TELEGRAM_CHAT_ID", "").strip()

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(out_dir, f"run_{run_id}")

    print(f"[INFO] RSS_URL={rss_url}")
    print(f"[INFO] UF_GEOJSON_PATH={uf_geojson_path}")
    print(f"[INFO] OUT_DIR={out_dir}")
    print(f"[INFO] RUN_DIR={run_dir}")
    print(f"[INFO] STATE_PATH={state_path}")
    print(f"[INFO] MAX_ITEMS={'(sem limite)' if max_items is None else max_items}")

    _mkdirp(run_dir)
    _mkdirp(os.path.dirname(state_path) or ".")

    # baixa RSS
    try:
        rss_xml = _http_get_text(rss_url, timeout=40)
    except Exception as e:
        print(f"[ERROR] Falha ao baixar RSS: {e}")
        _telegram_send_message(tg_token, tg_chat_id, f"IDAP Daily Maps: falha ao baixar RSS.\n{e}")
        return 2

    # parse entries
    try:
        entries = _load_rss_entries(rss_xml)
    except Exception as e:
        print(f"[ERROR] RSS inválido: {e}")
        _telegram_send_message(tg_token, tg_chat_id, f"IDAP Daily Maps: RSS inválido.\n{e}")
        return 3

    if max_items is not None:
        entries = entries[:max_items]

    print(f"[INFO] Entradas no RSS (consideradas): {len(entries)}")

    alerts: List[ParsedAlert] = []
    errors: List[Dict[str, Any]] = []

    for i, entry in enumerate(entries, start=1):
        a, err = _parse_cap_from_entry(entry)
        if a is not None:
            alerts.append(a)
        else:
            errors.append({"idx": i, "error": err or "erro desconhecido"})

    print(f"[INFO] CAPs parseados: {len(alerts)} | erros: {len(errors)}")

    # salva erros, mesmo se vazio
    _write_json(os.path.join(run_dir, "errors.json"), errors)

    # monta estatística completa do momento do run (você pediu isso)
    stats = {
        "run_id": run_id,
        "run_utc": _now_utc_iso(),
        "rss_url": rss_url,
        "entries_considered": len(entries),
        "caps_parsed": len(alerts),
        "caps_errors": len(errors),
        "by_severity": {},
        "by_senderName": {},
        "by_event": {},
        "by_channel": {},
    }

    if alerts:
        df = pd.DataFrame([asdict(a) for a in alerts])
        stats["by_severity"] = df["severity"].fillna("Unknown").value_counts().to_dict()
        stats["by_senderName"] = df["senderName"].fillna("Unknown").value_counts().head(20).to_dict()
        stats["by_event"] = df["event"].fillna("Unknown").value_counts().head(20).to_dict()

        # canais: explode
        ch = []
        for a in alerts:
            if a.channels:
                ch.extend(a.channels)
        if ch:
            stats["by_channel"] = pd.Series(ch).value_counts().to_dict()

        # CSV de resumo
        csv_path = os.path.join(run_dir, "alerts_summary.csv")
        cols = ["identifier", "sent", "expires", "severity", "urgency", "certainty", "event", "senderName", "status", "msgType", "areaDesc"]
        df[cols].to_csv(csv_path, index=False, encoding="utf-8")

    _write_json(os.path.join(run_dir, "stats.json"), stats)

    # salva alerts.json (sem shapely dentro, só WKT)
    _write_json(os.path.join(run_dir, "alerts.json"), [asdict(a) for a in alerts])

    # carrega base do mapa
    uf_gdf = _load_uf_gdf(uf_geojson_path)

    # plota mapa
    map_path = os.path.join(run_dir, "mapa_alertas.png")
    map_ok = False
    if alerts:
        gdf_alerts = _alerts_to_gdf(alerts)
        map_ok = _plot_map(uf_gdf, gdf_alerts, map_path)

    if not map_ok:
        print("[WARN] Mapa não gerado: nenhum alerta com polygon para plotar")
    else:
        print(f"[INFO] Mapa gerado: {map_path}")

    # grava/atualiza state.json só para histórico simples (não controla filtro)
    state_obj = {
        "last_run_id": run_id,
        "last_run_utc": _now_utc_iso(),
        "last_entries_considered": len(entries),
        "last_caps_parsed": len(alerts),
    }
    _write_json(state_path, state_obj)

    # Telegram: manda resumo + manda mapa (arquivo)
    if tg_token and tg_chat_id:
        sev = stats.get("by_severity", {})
        line_sev = ", ".join([f"{k}:{v}" for k, v in sev.items()]) if sev else "sem dados"

        txt = (
            f"IDAP Daily Maps\n"
            f"Run: {run_id}\n"
            f"Entradas RSS: {len(entries)}\n"
            f"CAPs parseados: {len(alerts)} | erros: {len(errors)}\n"
            f"Severidade: {line_sev}"
        )
        ok_msg = _telegram_send_message(tg_token, tg_chat_id, txt)

        ok_file = False
        if map_ok and os.path.exists(map_path):
            ok_file = _telegram_send_document(
                tg_token,
                tg_chat_id,
                map_path,
                caption=f"Mapa de alertas (run {run_id})"
            )

        if ok_msg and (ok_file or not map_ok):
            if ok_file:
                print("[INFO] Telegram: mensagem + mapa enviados")
            else:
                print("[INFO] Telegram: mensagem enviada")
        else:
            print("[WARN] Telegram: não consegui enviar mensagem e/ou arquivo")

    print("[INFO] Finalizado.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
