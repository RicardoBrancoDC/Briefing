#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
cemaden_watch.py

O que faz:
- Lê feed RSS (CAP em Atom) da IDAP.
- Para itens novos, baixa o CAP (XML).
- Envia resumo no Telegram.
- Se NÃO houver geocode no CAP e for severidade Severo/Extremo,
  plota um mapa com o contorno do estado + polígono do alerta e envia a imagem.

Requisitos (pip):
  geopandas
  shapely
  matplotlib

Em GitHub Actions, você normalmente precisa também de:
  fiona
  pyproj
  gdal
(Depende do runner e da forma que você instala os pacotes.)
"""

import hashlib
import json
import os
import re
import sys
import time
import urllib.request
import urllib.parse
import urllib.error
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple, List

# Dependências de mapa
try:
    import geopandas as gpd
    import matplotlib.pyplot as plt
    from shapely.geometry import Polygon
except Exception:
    gpd = None
    plt = None
    Polygon = None


# -----------------------------
# Config
# -----------------------------

DEFAULT_FEED_URL = "https://idapfile.mdr.gov.br/idap/api/rss/cap"
DEFAULT_STATE = "state.json"

# Seu arquivo enviado aqui, mas no repo recomendo colocar em resources/
DEFAULT_UF_GEOJSON = os.environ.get("UF_GEOJSON_PATH", "resources/estadosBrasil2.json")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

# Só plota quando:
PLOT_ONLY_IF_NO_GEOCODE = True
PLOT_ONLY_SEVERO_EXTREMO = True

# Severidade CAP costuma vir em "Extreme", "Severe", etc.
SEVERITY_MAP = {
    "extreme": "Extremo",
    "severe": "Severo",
    "moderate": "Moderada",
    "minor": "Baixa",
    "unknown": "Desconhecida",
}

# Cor simples para o preenchimento do polígono no mapa
SEVERITY_COLOR = {
    "Extremo": "#6f2dbd",   # roxo
    "Severo": "#d00000",    # vermelho
    "Alta": "#f4a261",      # laranja
    "Moderada": "#2a9d8f",  # verde
    "Baixa": "#457b9d",     # azul
    "Desconhecida": "#666666",
}

UF_NAME_NORMALIZE = {
    # quando o nome do CAP vier diferente do seu geojson, dá pra ajustar aqui
    # exemplo:
    # "DISTRITO FEDERAL": "DISTRITO FEDERAL",
}


# -----------------------------
# Utils
# -----------------------------

def _http_get(url: str, timeout: int = 30) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "cemaden_watch/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def _load_state(path: str) -> Dict:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"seen": {}}


def _save_state(path: str, state: Dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _cap_text(root: ET.Element, tag: str) -> str:
    # tags do CAP não têm namespace fixo na prática aqui
    el = root.find(f".//{tag}")
    return (el.text or "").strip() if el is not None else ""


def _find_all(root: ET.Element, tag: str) -> List[ET.Element]:
    return root.findall(f".//{tag}")


def _normalize_uf_name(area_desc: str) -> Optional[str]:
    """
    areaDesc vem tipo: "MINAS GERAIS/MG" ou "SANTA CATARINA/SC"
    retorna nome_uf em caixa alta, sem /UF
    """
    if not area_desc:
        return None
    s = area_desc.strip().upper()
    # pega antes do /XX
    if "/" in s:
        s = s.split("/", 1)[0].strip()
    s = re.sub(r"\s+", " ", s)
    s = UF_NAME_NORMALIZE.get(s, s)
    return s or None


def _has_geocode(cap_root: ET.Element) -> bool:
    # CAP 1.2: <geocode><valueName>...</valueName><value>...</value></geocode>
    geocodes = cap_root.findall(".//geocode")
    for g in geocodes:
        val = g.findtext("value", default="").strip()
        if val:
            return True
    return False


def _parse_polygon_str(poly_str: str) -> Optional[List[Tuple[float, float]]]:
    """
    CAP polygon: "lat,lon lat,lon ..."
    Retorna lista [(lon, lat), ...] para shapely/matplotlib
    """
    poly_str = (poly_str or "").strip()
    if not poly_str:
        return None

    coords = []
    for token in poly_str.split():
        if "," not in token:
            continue
        lat_s, lon_s = token.split(",", 1)
        try:
            lat = float(lat_s)
            lon = float(lon_s)
        except ValueError:
            continue
        coords.append((lon, lat))

    # Fecha o polígono se necessário
    if len(coords) >= 3 and coords[0] != coords[-1]:
        coords.append(coords[0])

    return coords if len(coords) >= 4 else None


def _telegram_send_message(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = urllib.parse.urlencode({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }).encode("utf-8")

    req = urllib.request.Request(url, data=payload, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        _ = resp.read()


def _telegram_send_photo(photo_path: str, caption: str = "") -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    if not os.path.exists(photo_path):
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"

    # multipart simples na unha (stdlib)
    boundary = "----cemadenwatchboundary"
    with open(photo_path, "rb") as f:
        photo_bytes = f.read()

    def part(name: str, value: str) -> bytes:
        return (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
            f"{value}\r\n"
        ).encode("utf-8")

    head = b"".join([
        part("chat_id", TELEGRAM_CHAT_ID),
        part("caption", caption),
        (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="photo"; filename="{os.path.basename(photo_path)}"\r\n'
            f"Content-Type: image/png\r\n\r\n"
        ).encode("utf-8"),
        photo_bytes,
        b"\r\n",
        f"--{boundary}--\r\n".encode("utf-8"),
    ])

    req = urllib.request.Request(
        url,
        data=head,
        method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        _ = resp.read()


# -----------------------------
# Mapa
# -----------------------------

def _load_uf_gdf(path: str) -> gpd.GeoDataFrame:
    if gpd is None:
        raise RuntimeError("geopandas não disponível. Instale geopandas + shapely + matplotlib.")

    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo UF GeoJSON não encontrado: {path}")

    gdf = gpd.read_file(path)

    # Seu arquivo tem "nome_uf" e "uf_05". Vamos usar "nome_uf".
    if "nome_uf" not in gdf.columns:
        raise RuntimeError("Coluna 'nome_uf' não encontrada no geojson de UFs.")

    # normaliza
    gdf["nome_uf_norm"] = gdf["nome_uf"].astype(str).str.upper().str.replace(r"\s+", " ", regex=True).str.strip()
    return gdf


def _plot_alert_polygon_on_state(
    uf_gdf: "gpd.GeoDataFrame",
    uf_name: str,
    polygon_coords_lonlat: List[Tuple[float, float]],
    severity_pt: str,
    out_png: str,
) -> str:
    if plt is None or Polygon is None:
        raise RuntimeError("matplotlib/shapely não disponíveis.")

    uf_name = (uf_name or "").strip().upper()
    if not uf_name:
        raise ValueError("UF vazia para plot.")

    sel = uf_gdf[uf_gdf["nome_uf_norm"] == uf_name]
    if sel.empty:
        # tenta "contains" (às vezes vem algo tipo "MATO GROSSO DO SUL", etc)
        sel = uf_gdf[uf_gdf["nome_uf_norm"].str.contains(uf_name, na=False)]

    if sel.empty:
        raise ValueError(f"Não encontrei a UF no geojson: {uf_name}")

    poly = Polygon(polygon_coords_lonlat)

    ax = sel.boundary.plot(figsize=(10, 8), linewidth=1)
    sel.plot(ax=ax, alpha=0.05)

    color = SEVERITY_COLOR.get(severity_pt, "#666666")
    gpd.GeoSeries([poly], crs="EPSG:4326").plot(
        ax=ax,
        color=color,
        alpha=0.35,
        edgecolor=color,
        linewidth=2,
    )

    ax.set_title(f"{uf_name} | Polígono do alerta ({severity_pt})")
    ax.set_axis_off()

    os.makedirs(os.path.dirname(out_png) or ".", exist_ok=True)
    plt.savefig(out_png, dpi=160, bbox_inches="tight")
    plt.close()
    return out_png


# -----------------------------
# Feed parsing
# -----------------------------

def _parse_atom_feed(feed_xml: bytes) -> List[Dict[str, str]]:
    """
    Retorna lista de entries com:
      - id
      - title
      - updated
      - link_href (se existir)
    """
    root = ET.fromstring(feed_xml)

    # Atom tem namespace, mas a IDAP às vezes vem sem, então eu trato sem ser chato demais.
    # Procura por 'entry' tanto com quanto sem namespace.
    entries = []
    for entry in root.findall(".//{http://www.w3.org/2005/Atom}entry") + root.findall(".//entry"):
        eid = entry.findtext("{http://www.w3.org/2005/Atom}id") or entry.findtext("id") or ""
        title = entry.findtext("{http://www.w3.org/2005/Atom}title") or entry.findtext("title") or ""
        updated = entry.findtext("{http://www.w3.org/2005/Atom}updated") or entry.findtext("updated") or ""

        link_href = ""
        link = entry.find("{http://www.w3.org/2005/Atom}link") or entry.find("link")
        if link is not None and "href" in link.attrib:
            link_href = link.attrib.get("href", "").strip()

        entries.append({
            "id": eid.strip(),
            "title": title.strip(),
            "updated": updated.strip(),
            "link": link_href,
        })

    return entries


def _pick_cap_url(entry: Dict[str, str]) -> Optional[str]:
    # Em muitos feeds o link já é o arquivo CAP. Se não tiver, usa o id como fallback.
    url = (entry.get("link") or "").strip()
    if url:
        return url
    eid = (entry.get("id") or "").strip()
    if eid.startswith("http"):
        return eid
    return None


def _format_alert_message(cap_root: ET.Element) -> str:
    # Campos mais úteis
    identifier = _cap_text(cap_root, "identifier")
    sent = _cap_text(cap_root, "sent")
    sender = _cap_text(cap_root, "sender")
    status = _cap_text(cap_root, "status")
    msg_type = _cap_text(cap_root, "msgType")
    scope = _cap_text(cap_root, "scope")

    info = cap_root.find(".//info")
    severity_raw = ""
    event = ""
    area_desc = ""
    expires = ""
    polygon = ""

    if info is not None:
        severity_raw = (info.findtext("severity", default="") or "").strip()
        event = (info.findtext("event", default="") or "").strip()
        expires = (info.findtext("expires", default="") or "").strip()
        area = info.find(".//area")
        if area is not None:
            area_desc = (area.findtext("areaDesc", default="") or "").strip()
            polygon = (area.findtext("polygon", default="") or "").strip()

    sev_pt = SEVERITY_MAP.get(severity_raw.lower(), severity_raw or "Desconhecida")

    lines = []
    lines.append(f"<b>Alerta CAP</b>")
    if event:
        lines.append(f"<b>Evento:</b> {event}")
    lines.append(f"<b>Severidade:</b> {sev_pt}")
    if area_desc:
        lines.append(f"<b>Área:</b> {area_desc}")
    if expires:
        lines.append(f"<b>Expira:</b> {expires}")
    if sent:
        lines.append(f"<b>Sent:</b> {sent}")
    if identifier:
        lines.append(f"<b>ID:</b> {identifier}")
    if status or msg_type or scope:
        lines.append(f"<b>Status:</b> {status} | <b>Tipo:</b> {msg_type} | <b>Scope:</b> {scope}")
    if sender:
        lines.append(f"<b>Sender:</b> {sender}")

    if polygon:
        lines.append("<i>Polígono presente</i>")

    return "\n".join(lines)


def _should_plot(cap_root: ET.Element) -> bool:
    info = cap_root.find(".//info")
    if info is None:
        return False

    severity_raw = (info.findtext("severity", default="") or "").strip().lower()
    sev_pt = SEVERITY_MAP.get(severity_raw, severity_raw or "unknown")

    if PLOT_ONLY_SEVERO_EXTREMO and sev_pt not in ("Extremo", "Severo"):
        return False

    if PLOT_ONLY_IF_NO_GEOCODE and _has_geocode(cap_root):
        return False

    # precisa de polygon e areaDesc
    area = info.find(".//area")
    if area is None:
        return False
    poly = (area.findtext("polygon", default="") or "").strip()
    area_desc = (area.findtext("areaDesc", default="") or "").strip()
    if not poly or not area_desc:
        return False

    return True


def _extract_plot_inputs(cap_root: ET.Element) -> Tuple[str, List[Tuple[float, float]], str]:
    info = cap_root.find(".//info")
    if info is None:
        raise ValueError("Sem <info> no CAP.")

    severity_raw = (info.findtext("severity", default="") or "").strip().lower()
    sev_pt = SEVERITY_MAP.get(severity_raw, severity_raw or "unknown")

    area = info.find(".//area")
    if area is None:
        raise ValueError("Sem <area> no CAP.")

    area_desc = (area.findtext("areaDesc", default="") or "").strip()
    polygon_str = (area.findtext("polygon", default="") or "").strip()

    uf_name = _normalize_uf_name(area_desc)
    coords = _parse_polygon_str(polygon_str)
    if not uf_name or not coords:
        raise ValueError("Não consegui extrair UF/polígono para plot.")

    return uf_name, coords, sev_pt


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    feed_url = os.environ.get("FEED_URL", DEFAULT_FEED_URL).strip()
    state_path = os.environ.get("STATE_PATH", DEFAULT_STATE).strip()

    uf_geojson_path = DEFAULT_UF_GEOJSON

    state = _load_state(state_path)
    seen = state.get("seen", {})

    # Carrega geojson das UFs uma vez
    uf_gdf = None
    if gpd is not None and os.path.exists(uf_geojson_path):
        try:
            uf_gdf = _load_uf_gdf(uf_geojson_path)
        except Exception as e:
            print(f"[WARN] Falha carregando UF geojson: {e}", file=sys.stderr)

    feed_xml = _http_get(feed_url)
    entries = _parse_atom_feed(feed_xml)

    # processa do mais antigo para o mais novo, pra ficar mais natural
    entries = list(reversed(entries))

    new_count = 0
    for entry in entries:
        cap_url = _pick_cap_url(entry)
        if not cap_url:
            continue

        key = _sha1(cap_url)
        if key in seen:
            continue

        try:
            cap_xml = _http_get(cap_url)
            cap_root = ET.fromstring(cap_xml)
        except Exception as e:
            print(f"[WARN] Erro baixando/parsing CAP: {cap_url} | {e}", file=sys.stderr)
            # marca como visto pra não ficar preso em item quebrado
            seen[key] = {"url": cap_url, "ts": datetime.now(timezone.utc).isoformat()}
            continue

        msg = _format_alert_message(cap_root)
        _telegram_send_message(msg)
        new_count += 1

        # Plot se aplicável
        if uf_gdf is not None and _should_plot(cap_root):
            try:
                uf_name, coords, sev_pt = _extract_plot_inputs(cap_root)

                identifier = _cap_text(cap_root, "identifier") or key[:10]
                safe_id = re.sub(r"[^a-zA-Z0-9_-]+", "_", identifier)
                out_png = os.path.join("out", f"map_{safe_id}.png")

                _plot_alert_polygon_on_state(
                    uf_gdf=uf_gdf,
                    uf_name=uf_name,
                    polygon_coords_lonlat=coords,
                    severity_pt=sev_pt,
                    out_png=out_png,
                )

                _telegram_send_photo(out_png, caption=f"{uf_name} | {sev_pt} | sem geocode")
            except Exception as e:
                print(f"[WARN] Falha no plot/envio de imagem: {e}", file=sys.stderr)

        # marca como visto
        seen[key] = {"url": cap_url, "ts": datetime.now(timezone.utc).isoformat()}

    state["seen"] = seen
    _save_state(state_path, state)

    print(f"OK. Novos alertas processados: {new_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
