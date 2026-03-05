#!/usr/bin/env python3
# scripts/idap_daily_maps.py

import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
import matplotlib.pyplot as plt


# -----------------------------
# Config
# -----------------------------
RSS_URL = os.getenv("RSS_URL", "https://idapfile.mdr.gov.br/idap/api/rss/cap")
UF_GEOJSON_PATH = os.getenv("UF_GEOJSON_PATH", "resources/br_uf.geojson")
OUT_DIR = os.getenv("OUT_DIR", "out")
STATE_PATH = os.getenv("STATE_PATH", ".cache/state.json")
MAX_ITEMS = os.getenv("MAX_ITEMS", "").strip()  # vazio = sem limite


# -----------------------------
# Helpers
# -----------------------------
def _now_tag() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _http_get(url: str, timeout: int = 30) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "idap-daily-maps/1.0 (+github-actions)",
            "Accept": "*/*",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _clean_xml_bytes(b: bytes) -> bytes:
    # remove BOM se existir e espaços estranhos no início
    if b.startswith(b"\xef\xbb\xbf"):
        b = b[3:]
    return b.lstrip()


def _try_parse_xml(b: bytes) -> ET.Element:
    b = _clean_xml_bytes(b)
    return ET.fromstring(b)


def _strip_ns(tag: str) -> str:
    # "{namespace}tag" -> "tag"
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _find_first_text(root: ET.Element, path_tags: List[str]) -> Optional[str]:
    """
    Busca um caminho simples ignorando namespace.
    Ex: ["info","severity"] pega o primeiro <info><severity>...</severity>
    """
    cur = root
    for t in path_tags:
        found = None
        for ch in list(cur):
            if _strip_ns(ch.tag) == t:
                found = ch
                break
        if found is None:
            return None
        cur = found
    return (cur.text or "").strip() if cur is not None else None


def _find_all_elements(root: ET.Element, tag_name: str) -> List[ET.Element]:
    out = []
    stack = [root]
    while stack:
        node = stack.pop()
        if _strip_ns(node.tag) == tag_name:
            out.append(node)
        stack.extend(list(node))
    return out


def _parse_atom_links(feed_root: ET.Element) -> List[str]:
    """
    Lê Atom/RSS com tolerância:
    - Atom: <entry><link href="..."/>
    - RSS: <item><link>...</link>
    - Alguns feeds colocam URL no <id> do entry
    """
    urls: List[str] = []

    # Atom entries
    entries = [el for el in list(feed_root) if _strip_ns(el.tag) == "entry"]
    if entries:
        for e in entries:
            # 1) link href
            for ch in list(e):
                if _strip_ns(ch.tag) == "link":
                    href = ch.attrib.get("href", "").strip()
                    if href.startswith("http"):
                        urls.append(href)
                        break
            else:
                # 2) <id>http...</id>
                id_txt = None
                for ch in list(e):
                    if _strip_ns(ch.tag) == "id":
                        id_txt = (ch.text or "").strip()
                        break
                if id_txt and id_txt.startswith("http"):
                    urls.append(id_txt)

        return urls

    # RSS items
    items = [el for el in list(feed_root) if _strip_ns(el.tag) == "channel"]
    if items:
        channel = items[0]
        for it in list(channel):
            if _strip_ns(it.tag) != "item":
                continue
            link_txt = None
            for ch in list(it):
                if _strip_ns(ch.tag) == "link":
                    link_txt = (ch.text or "").strip()
                    break
            if link_txt and link_txt.startswith("http"):
                urls.append(link_txt)

    return urls


def _parse_polygon_text(poly_text: str) -> Optional[Polygon]:
    """
    CAP polygon é algo tipo:
    "-21.611,-44.386 -21.603,-44.373 ..."
    (lat,lon) separados por espaço.
    Shapely usa (x,y) = (lon,lat).
    """
    poly_text = (poly_text or "").strip()
    if not poly_text:
        return None

    pts = []
    for token in re.split(r"\s+", poly_text):
        token = token.strip()
        if not token:
            continue
        if "," not in token:
            continue
        a, b = token.split(",", 1)
        try:
            lat = float(a)
            lon = float(b)
            pts.append((lon, lat))
        except ValueError:
            continue

    if len(pts) < 3:
        return None

    # fecha polígono se necessário
    if pts[0] != pts[-1]:
        pts.append(pts[0])

    try:
        return Polygon(pts)
    except Exception:
        return None


def _severity_color(sev: str) -> str:
    s = (sev or "").strip().lower()
    # você pode ajustar aqui se quiser “muito alta” virar vermelho etc.
    if s in ["extreme", "extremo"]:
        return "#7B2CBF"  # roxo
    if s in ["severe", "severo"]:
        return "#D00000"  # vermelho
    if s in ["very high", "muito alta", "muito alto"]:
        return "#D00000"
    if s in ["high", "alta", "alto"]:
        return "#FFB703"  # amarelo/laranja
    if s in ["moderate", "moderada", "moderado"]:
        return "#2A9D8F"  # verde
    if s in ["minor", "baixa", "baixo"]:
        return "#6C757D"  # cinza
    return "#1D3557"     # azul escuro fallback


@dataclass
class CapAlert:
    cap_url: str
    identifier: str
    sender: str
    sent: str
    status: str
    msg_type: str
    event: str
    severity: str
    urgency: str
    certainty: str
    area_desc: str
    polygons: List[Polygon]


def _parse_cap_xml(cap_xml: bytes, cap_url: str) -> CapAlert:
    root = _try_parse_xml(cap_xml)

    # CAP 1.2 normalmente tem raiz <alert> e <info>...
    identifier = _find_first_text(root, ["identifier"]) or ""
    sender = _find_first_text(root, ["sender"]) or ""
    sent = _find_first_text(root, ["sent"]) or ""
    status = _find_first_text(root, ["status"]) or ""
    msg_type = _find_first_text(root, ["msgType"]) or ""

    # pega o primeiro <info> (se tiver vários)
    info_nodes = [n for n in list(root) if _strip_ns(n.tag) == "info"]
    info = info_nodes[0] if info_nodes else root

    def info_text(tag: str) -> str:
        for ch in list(info):
            if _strip_ns(ch.tag) == tag:
                return (ch.text or "").strip()
        return ""

    event = info_text("event")
    severity = info_text("severity")
    urgency = info_text("urgency")
    certainty = info_text("certainty")

    # pega areaDesc e polygons (pode ter múltiplos <area>)
    area_desc = ""
    polygons: List[Polygon] = []

    for area in [n for n in list(info) if _strip_ns(n.tag) == "area"]:
        for ch in list(area):
            t = _strip_ns(ch.tag)
            if t == "areaDesc" and not area_desc:
                area_desc = (ch.text or "").strip()
            if t == "polygon":
                poly = _parse_polygon_text(ch.text or "")
                if poly is not None and poly.is_valid:
                    polygons.append(poly)

    return CapAlert(
        cap_url=cap_url,
        identifier=identifier,
        sender=sender,
        sent=sent,
        status=status,
        msg_type=msg_type,
        event=event,
        severity=severity,
        urgency=urgency,
        certainty=certainty,
        area_desc=area_desc,
        polygons=polygons,
    )


def _safe_write_json(path: str, obj: dict) -> None:
    _ensure_dir(os.path.dirname(path) or ".")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _load_state(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _plot_map(uf_path: str, alerts: List[CapAlert], out_png: str) -> bool:
    polys = []
    rows = []
    for a in alerts:
        for p in a.polygons:
            polys.append(p)
            rows.append(
                {
                    "identifier": a.identifier,
                    "event": a.event,
                    "severity": a.severity,
                    "status": a.status,
                    "sent": a.sent,
                    "cap_url": a.cap_url,
                }
            )

    if not polys:
        print("[WARN] Mapa não gerado: nenhum alerta com polygon para plotar")
        return False

    try:
        uf = gpd.read_file(uf_path)
    except Exception as e:
        print(f"[ERROR] Falha ao ler UF_GEOJSON_PATH={uf_path}: {e}")
        raise

    gdf = gpd.GeoDataFrame(rows, geometry=polys, crs="EPSG:4326")

    fig = plt.figure(figsize=(12, 10))
    ax = plt.gca()

    # estados como base
    uf.boundary.plot(ax=ax, linewidth=0.8)

    # plot por severidade, mantendo simples
    # (fazemos em camadas para não misturar)
    severities = list(pd.unique(gdf["severity"].fillna("").astype(str)))
    for sev in severities:
        sub = gdf[gdf["severity"].astype(str) == sev]
        if sub.empty:
            continue
        sub.plot(
            ax=ax,
            facecolor=_severity_color(sev),
            edgecolor=_severity_color(sev),
            alpha=0.35,
            linewidth=1.0,
        )

    ax.set_title("IDAP CAP, polígonos do RSS (varredura completa)")
    ax.set_axis_off()

    _ensure_dir(os.path.dirname(out_png) or ".")
    plt.savefig(out_png, dpi=200, bbox_inches="tight")
    plt.close(fig)
    return True


def main() -> int:
    _ensure_dir(".cache")
    _ensure_dir(OUT_DIR)

    run_tag = _now_tag()
    run_dir = os.path.join(OUT_DIR, f"run_{run_tag}")
    _ensure_dir(run_dir)

    print(f"[INFO] RSS_URL={RSS_URL}")
    print(f"[INFO] UF_GEOJSON_PATH={UF_GEOJSON_PATH}")
    print(f"[INFO] OUT_DIR={OUT_DIR}")
    print(f"[INFO] RUN_DIR={run_dir}")
    print(f"[INFO] STATE_PATH={STATE_PATH}")
    print(f"[INFO] MAX_ITEMS={(MAX_ITEMS or '(sem limite)')}")

    # garante state.json sempre
    state = _load_state(STATE_PATH)
    state["last_run_tag"] = run_tag
    state["last_run_iso"] = datetime.now().isoformat(timespec="seconds")
    _safe_write_json(STATE_PATH, state)

    # baixa RSS
    try:
        rss_bytes = _http_get(RSS_URL, timeout=40)
    except Exception as e:
        print(f"[ERROR] Falha ao baixar RSS: {e}")
        return 2

    try:
        feed_root = _try_parse_xml(rss_bytes)
    except Exception as e:
        print(f"[ERROR] RSS retornou algo que não parece XML: {e}")
        return 2

    cap_urls = _parse_atom_links(feed_root)

    # remove duplicadas mantendo ordem
    seen = set()
    cap_urls_unique = []
    for u in cap_urls:
        if u in seen:
            continue
        seen.add(u)
        cap_urls_unique.append(u)

    if MAX_ITEMS:
        try:
            n = int(MAX_ITEMS)
            cap_urls_unique = cap_urls_unique[: max(0, n)]
        except ValueError:
            pass

    print(f"[INFO] Entradas no RSS (consideradas): {len(cap_urls_unique)}")

    alerts: List[CapAlert] = []
    errors: List[Dict[str, str]] = []

    for i, cap_url in enumerate(cap_urls_unique, start=1):
        try:
            cap_xml = _http_get(cap_url, timeout=40)
            alert = _parse_cap_xml(cap_xml, cap_url=cap_url)
            alerts.append(alert)
        except urllib.error.HTTPError as e:
            errors.append({"cap_url": cap_url, "error": f"HTTPError {e.code}"})
        except urllib.error.URLError as e:
            errors.append({"cap_url": cap_url, "error": f"URLError {e.reason}"})
        except ET.ParseError as e:
            errors.append({"cap_url": cap_url, "error": f"XML ParseError: {e}"})
        except Exception as e:
            errors.append({"cap_url": cap_url, "error": f"Exception: {e}"})

        # uma pequena pausa ajuda em alguns hosts
        time.sleep(0.2)

    print(f"[INFO] CAPs parseados: {len(alerts)} | erros: {len(errors)}")

    # salva erros
    if errors:
        _safe_write_json(os.path.join(run_dir, "errors.json"), {"errors": errors})

    # dataframe para estatísticas (inclui todos status)
    rows = []
    for a in alerts:
        rows.append(
            {
                "identifier": a.identifier,
                "sender": a.sender,
                "sent": a.sent,
                "status": a.status,
                "msgType": a.msg_type,
                "event": a.event,
                "severity": a.severity,
                "urgency": a.urgency,
                "certainty": a.certainty,
                "areaDesc": a.area_desc,
                "cap_url": a.cap_url,
                "polygon_count": len(a.polygons),
            }
        )

    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(run_dir, "alerts.csv"), index=False, encoding="utf-8")

    stats = {
        "run_tag": run_tag,
        "rss_url": RSS_URL,
        "count_rss_items": len(cap_urls_unique),
        "count_parsed": len(alerts),
        "count_errors": len(errors),
        "by_status": df["status"].value_counts(dropna=False).to_dict() if not df.empty else {},
        "by_severity": df["severity"].value_counts(dropna=False).to_dict() if not df.empty else {},
        "by_event": df["event"].value_counts(dropna=False).head(30).to_dict() if not df.empty else {},
        "with_polygon": int((df["polygon_count"] > 0).sum()) if not df.empty else 0,
    }
    _safe_write_json(os.path.join(run_dir, "stats.json"), stats)

    # mapa
    out_png = os.path.join(run_dir, "mapa_alertas.png")
    try:
        _plot_map(UF_GEOJSON_PATH, alerts, out_png)
    except Exception:
        # já logou erro específico, não derruba tudo
        pass

    print("[INFO] Finalizado.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
