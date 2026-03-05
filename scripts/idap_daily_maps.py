#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import os
import sys
import time
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

# -----------------------------
# Config
# -----------------------------

RSS_URL = os.getenv("RSS_URL", "https://idapfile.mdr.gov.br/idap/api/rss/cap")
UF_GEOJSON_PATH = os.getenv("UF_GEOJSON_PATH", "resources/br_uf.geojson")
OUT_DIR = os.getenv("OUT_DIR", "out")
STATE_PATH = os.getenv("STATE_PATH", ".cache/state.json")

# Limite opcional só para debug (0/ vazio = sem limite)
MAX_ITEMS_ENV = os.getenv("MAX_ITEMS", "").strip()
MAX_ITEMS: Optional[int] = int(MAX_ITEMS_ENV) if MAX_ITEMS_ENV.isdigit() and int(MAX_ITEMS_ENV) > 0 else None

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "25"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "3"))
SLEEP_BETWEEN = float(os.getenv("SLEEP_BETWEEN", "0.2"))

TZ = ZoneInfo("America/Sao_Paulo")


# -----------------------------
# Helpers HTTP
# -----------------------------

def _http_get(url: str, timeout: float = HTTP_TIMEOUT) -> bytes:
    headers = {
        "User-Agent": "idap-daily-maps/1.0 (github-actions)",
        "Accept": "*/*",
    }
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def http_get_with_retries(url: str, retries: int = HTTP_RETRIES) -> bytes:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            return _http_get(url)
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(0.8 * attempt)
                continue
            raise
    raise last_err if last_err else RuntimeError("HTTP error (unknown)")


# -----------------------------
# CAP parsing
# -----------------------------

NS_ATOM = {"a": "http://www.w3.org/2005/Atom"}
# CAP 1.2
NS_CAP = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}

def _text(el: Optional[ET.Element]) -> str:
    if el is None or el.text is None:
        return ""
    return el.text.strip()


def _find(el: ET.Element, path: str, ns: Dict[str, str]) -> Optional[ET.Element]:
    return el.find(path, ns)


def _findall(el: ET.Element, path: str, ns: Dict[str, str]) -> List[ET.Element]:
    return el.findall(path, ns)


def parse_rss_entries(rss_xml: bytes) -> List[Dict[str, str]]:
    """
    Retorna lista de itens do RSS com campos básicos.
    Tenta capturar o link do CAP (xml) de cada entry.
    """
    root = ET.fromstring(rss_xml)
    entries: List[Dict[str, str]] = []

    for entry in root.findall("a:entry", NS_ATOM):
        title = _text(entry.find("a:title", NS_ATOM))
        entry_id = _text(entry.find("a:id", NS_ATOM))

        # links possíveis
        links = entry.findall("a:link", NS_ATOM)
        hrefs = [l.attrib.get("href", "").strip() for l in links if l.attrib.get("href")]
        hrefs = [h for h in hrefs if h]

        # heurística: preferir link que termine com .xml ou contenha "cap" e "xml"
        cap_url = ""
        for h in hrefs:
            hl = h.lower()
            if hl.endswith(".xml"):
                cap_url = h
                break
        if not cap_url:
            for h in hrefs:
                hl = h.lower()
                if "cap" in hl and "xml" in hl:
                    cap_url = h
                    break
        if not cap_url and hrefs:
            cap_url = hrefs[0]

        updated = _text(entry.find("a:updated", NS_ATOM))
        published = _text(entry.find("a:published", NS_ATOM))

        entries.append(
            {
                "title": title,
                "entry_id": entry_id,
                "cap_url": cap_url,
                "updated": updated,
                "published": published,
            }
        )

    return entries


@dataclass
class CapAlert:
    identifier: str
    sender: str
    sent: str
    status: str
    msgType: str
    scope: str

    senderName: str
    contact: str

    event: str
    category: str
    severity: str
    urgency: str
    certainty: str
    headline: str
    description: str
    instruction: str

    areaDesc: str
    polygon: str
    geocodes: str  # "key=value; key=value"
    source_cap_url: str


def _cap_first_info(alert_root: ET.Element) -> Optional[ET.Element]:
    infos = alert_root.findall("cap:info", NS_CAP)
    return infos[0] if infos else None


def _join_geocodes(area_el: ET.Element) -> str:
    pairs: List[str] = []
    for geocode in area_el.findall("cap:geocode", NS_CAP):
        name = _text(geocode.find("cap:valueName", NS_CAP))
        value = _text(geocode.find("cap:value", NS_CAP))
        if name or value:
            pairs.append(f"{name}={value}")
    return "; ".join(pairs)


def parse_cap_xml(cap_xml: bytes, source_cap_url: str) -> CapAlert:
    root = ET.fromstring(cap_xml)

    identifier = _text(_find(root, "cap:identifier", NS_CAP))
    sender = _text(_find(root, "cap:sender", NS_CAP))
    sent = _text(_find(root, "cap:sent", NS_CAP))
    status = _text(_find(root, "cap:status", NS_CAP))
    msgType = _text(_find(root, "cap:msgType", NS_CAP))
    scope = _text(_find(root, "cap:scope", NS_CAP))

    senderName = _text(_find(root, "cap:senderName", NS_CAP))
    contact = _text(_find(root, "cap:contact", NS_CAP))

    info = _cap_first_info(root)
    if info is None:
        # CAP estranho, mas vamos manter um objeto "vazio" sem quebrar
        return CapAlert(
            identifier=identifier,
            sender=sender,
            sent=sent,
            status=status,
            msgType=msgType,
            scope=scope,
            senderName=senderName,
            contact=contact,
            event="",
            category="",
            severity="",
            urgency="",
            certainty="",
            headline="",
            description="",
            instruction="",
            areaDesc="",
            polygon="",
            geocodes="",
            source_cap_url=source_cap_url,
        )

    # alguns campos podem aparecer várias vezes (ex: category), então juntamos
    category_list = [_text(x) for x in info.findall("cap:category", NS_CAP)]
    category = "; ".join([c for c in category_list if c])

    event = _text(_find(info, "cap:event", NS_CAP))
    severity = _text(_find(info, "cap:severity", NS_CAP))
    urgency = _text(_find(info, "cap:urgency", NS_CAP))
    certainty = _text(_find(info, "cap:certainty", NS_CAP))
    headline = _text(_find(info, "cap:headline", NS_CAP))
    description = _text(_find(info, "cap:description", NS_CAP))
    instruction = _text(_find(info, "cap:instruction", NS_CAP))

    # Pega primeira área por padrão, mas se quiser depois dá pra expandir
    area = info.find("cap:area", NS_CAP)
    areaDesc = _text(area.find("cap:areaDesc", NS_CAP)) if area is not None else ""
    polygon = _text(area.find("cap:polygon", NS_CAP)) if area is not None else ""
    geocodes = _join_geocodes(area) if area is not None else ""

    return CapAlert(
        identifier=identifier,
        sender=sender,
        sent=sent,
        status=status,
        msgType=msgType,
        scope=scope,
        senderName=senderName,
        contact=contact,
        event=event,
        category=category,
        severity=severity,
        urgency=urgency,
        certainty=certainty,
        headline=headline,
        description=description,
        instruction=instruction,
        areaDesc=areaDesc,
        polygon=polygon,
        geocodes=geocodes,
        source_cap_url=source_cap_url,
    )


# -----------------------------
# Stats
# -----------------------------

def _norm(s: str) -> str:
    return (s or "").strip()


def compute_stats(alerts: List[CapAlert]) -> Dict[str, Any]:
    def count_by(key_fn):
        out: Dict[str, int] = {}
        for a in alerts:
            k = _norm(key_fn(a)) or "(vazio)"
            out[k] = out.get(k, 0) + 1
        # ordenar por contagem desc
        return dict(sorted(out.items(), key=lambda kv: (-kv[1], kv[0])))

    stats = {
        "total_alerts": len(alerts),
        "by_status": count_by(lambda a: a.status),
        "by_msgType": count_by(lambda a: a.msgType),
        "by_severity": count_by(lambda a: a.severity),
        "by_event": count_by(lambda a: a.event),
        "by_senderName": count_by(lambda a: a.senderName),
        "by_areaDesc": count_by(lambda a: a.areaDesc),
    }
    return stats


# -----------------------------
# Map plotting (optional)
# -----------------------------

def _parse_cap_polygon(polygon_text: str):
    """
    CAP polygon: "lat,lon lat,lon ..."
    Retorna shapely Polygon em EPSG:4326.
    """
    from shapely.geometry import Polygon  # type: ignore

    pts = []
    raw = (polygon_text or "").strip()
    if not raw:
        return None

    # CAP separa pontos por espaço
    parts = raw.split()
    for p in parts:
        p = p.strip()
        if not p:
            continue
        if "," not in p:
            continue
        lat_s, lon_s = p.split(",", 1)
        try:
            lat = float(lat_s)
            lon = float(lon_s)
        except ValueError:
            continue
        # shapely usa (x, y) = (lon, lat)
        pts.append((lon, lat))

    if len(pts) < 3:
        return None

    # às vezes o CAP repete o primeiro ponto no final, ok
    try:
        return Polygon(pts)
    except Exception:
        return None


def plot_map(alerts: List[CapAlert], uf_geojson_path: str, out_png: str) -> Tuple[bool, str]:
    """
    Plota mapa do Brasil (UF) + polígonos dos alertas que tiverem polygon.
    Se geopandas/shapely/matplotlib não existirem, retorna False.
    """
    try:
        import geopandas as gpd  # type: ignore
        import matplotlib.pyplot as plt  # type: ignore
        from shapely.geometry import Polygon  # noqa
    except Exception as e:
        return False, f"dependências do mapa não disponíveis ({e})"

    if not os.path.exists(uf_geojson_path):
        return False, f"UF_GEOJSON_PATH não encontrado: {uf_geojson_path}"

    # carregar UF
    try:
        uf_gdf = gpd.read_file(uf_geojson_path)
    except Exception as e:
        return False, f"falha lendo geojson de UF ({e})"

    # montar gdf de polígonos de alertas
    polys = []
    meta = []
    for a in alerts:
        poly = _parse_cap_polygon(a.polygon)
        if poly is None:
            continue
        polys.append(poly)
        meta.append(
            {
                "identifier": a.identifier,
                "severity": a.severity,
                "event": a.event,
                "areaDesc": a.areaDesc,
            }
        )

    if not polys:
        return False, "nenhum alerta com polygon para plotar"

    try:
        gdf_alerts = gpd.GeoDataFrame(meta, geometry=polys, crs="EPSG:4326")
    except Exception as e:
        return False, f"falha criando GeoDataFrame dos alertas ({e})"

    # plot
    try:
        fig = plt.figure(figsize=(12, 10))
        ax = fig.add_subplot(1, 1, 1)

        uf_gdf.plot(ax=ax, linewidth=0.6, edgecolor="black", facecolor="none")
        gdf_alerts.plot(ax=ax, alpha=0.35)  # sem forçar cores aqui

        ax.set_title("IDAP CAP: polígonos presentes no RSS (execução do horário)", fontsize=12)
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")

        fig.tight_layout()
        fig.savefig(out_png, dpi=150)
        plt.close(fig)
        return True, "ok"
    except Exception as e:
        return False, f"falha plotando mapa ({e})"


# -----------------------------
# IO
# -----------------------------

def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def write_csv(alerts: List[CapAlert], path: str) -> None:
    ensure_parent_dir(path)
    fieldnames = list(asdict(alerts[0]).keys()) if alerts else [
        "identifier","sender","sent","status","msgType","scope","senderName","contact",
        "event","category","severity","urgency","certainty","headline","description","instruction",
        "areaDesc","polygon","geocodes","source_cap_url"
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for a in alerts:
            w.writerow(asdict(a))


def write_json(obj: Any, path: str) -> None:
    ensure_parent_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def write_text(text: str, path: str) -> None:
    ensure_parent_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def update_state(state_path: str, run_meta: Dict[str, Any]) -> None:
    """
    Não filtra nada. Só grava um registro simples para você saber que o job
    rodou e em que horário, e quantos CAPs conseguiu ler.
    """
    ensure_parent_dir(state_path)
    state = {}
    if os.path.exists(state_path):
        try:
            with open(state_path, "r", encoding="utf-8") as f:
                state = json.load(f) or {}
        except Exception:
            state = {}

    state["last_run"] = run_meta
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    os.makedirs(OUT_DIR, exist_ok=True)

    now_sp = datetime.now(TZ)
    run_id = now_sp.strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(OUT_DIR, f"run_{run_id}")
    os.makedirs(run_dir, exist_ok=True)

    print(f"[INFO] RSS_URL={RSS_URL}")
    print(f"[INFO] UF_GEOJSON_PATH={UF_GEOJSON_PATH}")
    print(f"[INFO] OUT_DIR={OUT_DIR}")
    print(f"[INFO] RUN_DIR={run_dir}")
    print(f"[INFO] STATE_PATH={STATE_PATH}")
    print(f"[INFO] MAX_ITEMS={MAX_ITEMS if MAX_ITEMS else '(sem limite)'}")

    # 1) RSS
    try:
        rss_xml = http_get_with_retries(RSS_URL)
    except Exception as e:
        print(f"[ERROR] Falha baixando RSS: {e}", file=sys.stderr)
        return 2

    rss_path = os.path.join(run_dir, "rss.xml")
    write_text(rss_xml.decode("utf-8", errors="replace"), rss_path)

    entries = parse_rss_entries(rss_xml)
    if MAX_ITEMS is not None:
        entries = entries[:MAX_ITEMS]

    print(f"[INFO] Entradas no RSS (consideradas): {len(entries)}")

    alerts: List[CapAlert] = []
    errors: List[Dict[str, str]] = []

    # 2) baixar e parsear cada CAP
    for i, it in enumerate(entries, start=1):
        cap_url = it.get("cap_url", "")
        if not cap_url:
            errors.append({"where": "rss_entry", "error": "cap_url vazio", "entry_id": it.get("entry_id", "")})
            continue

        try:
            cap_xml = http_get_with_retries(cap_url)
            alert = parse_cap_xml(cap_xml, cap_url)
            alerts.append(alert)
        except Exception as e:
            errors.append({"where": "cap_download_or_parse", "cap_url": cap_url, "error": str(e)})
        finally:
            if SLEEP_BETWEEN > 0:
                time.sleep(SLEEP_BETWEEN)

        if i % 25 == 0:
            print(f"[INFO] Processadas {i}/{len(entries)} entradas...")

    print(f"[INFO] CAPs parseados: {len(alerts)} | erros: {len(errors)}")

    # 3) salvar tabela completa
    csv_path = os.path.join(run_dir, "alerts_full.csv")
    write_csv(alerts, csv_path)

    # 4) stats completas (sem filtrar status)
    stats = compute_stats(alerts)
    stats_path = os.path.join(run_dir, "stats.json")
    write_json(stats, stats_path)

    # 5) relatório simples em markdown
    lines = []
    lines.append(f"# IDAP Daily Maps\n")
    lines.append(f"- Data/hora (São Paulo): {now_sp.strftime('%d/%m/%Y %H:%M:%S')}\n")
    lines.append(f"- Total de alertas no RSS (considerados): {len(entries)}\n")
    lines.append(f"- CAPs parseados com sucesso: {len(alerts)}\n")
    lines.append(f"- Erros: {len(errors)}\n")

    def top_n(d: Dict[str, int], n: int = 12) -> str:
        items = list(d.items())[:n]
        return "\n".join([f"  - {k}: {v}" for k, v in items]) if items else "  - (vazio)"

    lines.append("\n## Por severidade\n")
    lines.append(top_n(stats.get("by_severity", {})))

    lines.append("\n\n## Por evento\n")
    lines.append(top_n(stats.get("by_event", {})))

    lines.append("\n\n## Por status\n")
    lines.append(top_n(stats.get("by_status", {})))

    lines.append("\n\n## Por órgão (senderName)\n")
    lines.append(top_n(stats.get("by_senderName", {})))

    md_path = os.path.join(run_dir, "stats.md")
    write_text("\n".join(lines) + "\n", md_path)

    # 6) erros (se houver)
    if errors:
        errors_path = os.path.join(run_dir, "errors.json")
        write_json(errors, errors_path)

    # 7) mapa (opcional)
    map_png = os.path.join(run_dir, "map_alert_polygons.png")
    ok_map, msg_map = plot_map(alerts, UF_GEOJSON_PATH, map_png)
    if ok_map:
        print(f"[INFO] Mapa gerado: {map_png}")
    else:
        print(f"[WARN] Mapa não gerado: {msg_map}")

    # 8) state.json: não filtra nada, só registra a execução
    run_meta = {
        "run_id": run_id,
        "timestamp_sp": now_sp.isoformat(),
        "rss_url": RSS_URL,
        "entries_considered": len(entries),
        "alerts_parsed": len(alerts),
        "errors": len(errors),
        "run_dir": run_dir,
    }
    try:
        update_state(STATE_PATH, run_meta)
        # também copia uma versão dentro do run_dir só pra você ver no artifact
        write_json(run_meta, os.path.join(run_dir, "run_meta.json"))
    except Exception as e:
        print(f"[WARN] Falha atualizando STATE_PATH: {e}")

    print("[INFO] Finalizado.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
