#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import csv
import gzip
import urllib.request
import urllib.error
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import geopandas as gpd
from shapely.geometry import Polygon
import matplotlib.pyplot as plt

ATOM_NS = {"a": "http://www.w3.org/2005/Atom"}


def _now_tag() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _read_env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        if default is None:
            raise SystemExit(f"[ERRO] Variável de ambiente ausente: {name}")
        return default
    return v.strip()


def _http_get(url: str, timeout: int = 30) -> Tuple[int, bytes, Dict[str, str]]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "idap-daily-maps/1.2 (GitHub Actions)",
            "Accept": "application/xml,text/xml,*/*;q=0.8",
            "Accept-Encoding": "gzip",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, "status", 200)
            hdrs = {k.lower(): v for k, v in resp.headers.items()}
            raw = resp.read()

        if hdrs.get("content-encoding", "").lower() == "gzip":
            raw = gzip.decompress(raw)

        return status, raw, hdrs

    except urllib.error.HTTPError as e:
        body = e.read() if hasattr(e, "read") else b""
        return int(e.code), body, {}
    except Exception:
        raise


def _safe_text(el: Optional[ET.Element]) -> str:
    if el is None or el.text is None:
        return ""
    return el.text.strip()


def _strip_ns(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _find_first_by_localname(root: ET.Element, local: str) -> Optional[ET.Element]:
    for el in root.iter():
        if _strip_ns(el.tag) == local:
            return el
    return None


def _parse_polygon(poly_str: str) -> Optional[Polygon]:
    s = (poly_str or "").strip()
    if not s:
        return None

    coords = []
    for part in s.split():
        if "," not in part:
            continue
        a, b = part.split(",", 1)
        try:
            lat = float(a)
            lon = float(b)
            coords.append((lon, lat))  # shapely = (x,y) = (lon,lat)
        except Exception:
            continue

    if len(coords) < 3:
        return None

    if coords[0] != coords[-1]:
        coords.append(coords[0])

    try:
        return Polygon(coords)
    except Exception:
        return None


def _cap_value(root: ET.Element, name: str) -> str:
    el = _find_first_by_localname(root, name)
    return _safe_text(el)


def _cap_info_block(root: ET.Element) -> Optional[ET.Element]:
    for el in root.iter():
        if _strip_ns(el.tag) == "info":
            return el
    return None


def _cap_area_block(info: ET.Element) -> Optional[ET.Element]:
    for el in info.iter():
        if _strip_ns(el.tag) == "area":
            return el
    return None


def _parse_cap_alert_element(alert_el: ET.Element) -> Dict[str, Any]:
    root = alert_el

    alert: Dict[str, Any] = {
        "identifier": _cap_value(root, "identifier"),
        "sender": _cap_value(root, "sender"),
        "sent": _cap_value(root, "sent"),
        "status": _cap_value(root, "status"),
        "msgType": _cap_value(root, "msgType"),
        "scope": _cap_value(root, "scope"),
        "event": "",
        "severity": "",
        "urgency": "",
        "certainty": "",
        "effective": "",
        "onset": "",
        "expires": "",
        "areaDesc": "",
        "polygon_str": "",
        "geocodes": [],
        # geom fica só em memória, não vai para JSON
        "polygon_geom": None,
    }

    info = _cap_info_block(root)
    if info is None:
        return alert

    def info_val(local: str) -> str:
        for el in info.iter():
            if _strip_ns(el.tag) == local:
                return _safe_text(el)
        return ""

    alert["event"] = info_val("event")
    alert["severity"] = info_val("severity")
    alert["urgency"] = info_val("urgency")
    alert["certainty"] = info_val("certainty")
    alert["effective"] = info_val("effective")
    alert["onset"] = info_val("onset")
    alert["expires"] = info_val("expires")

    area = _cap_area_block(info)
    if area is not None:
        for el in area.iter():
            if _strip_ns(el.tag) == "areaDesc":
                alert["areaDesc"] = _safe_text(el)
                break

        poly = ""
        for el in area.iter():
            if _strip_ns(el.tag) == "polygon":
                poly = _safe_text(el)
                break
        alert["polygon_str"] = poly
        alert["polygon_geom"] = _parse_polygon(poly)

        geocodes = []
        for g in area.iter():
            if _strip_ns(g.tag) == "geocode":
                vn = ""
                vv = ""
                for c in list(g):
                    if _strip_ns(c.tag) == "valueName":
                        vn = _safe_text(c)
                    elif _strip_ns(c.tag) == "value":
                        vv = _safe_text(c)
                if vn or vv:
                    geocodes.append({"valueName": vn, "value": vv})
        alert["geocodes"] = geocodes

    return alert


def _parse_cap_xml_bytes(cap_xml: bytes) -> Dict[str, Any]:
    root = ET.fromstring(cap_xml)

    if _strip_ns(root.tag) == "alert":
        return _parse_cap_alert_element(root)

    for el in root.iter():
        if _strip_ns(el.tag) == "alert":
            return _parse_cap_alert_element(el)

    return {"identifier": "", "sender": "", "sent": "", "status": "", "msgType": "", "scope": ""}


def _extract_cap_from_entry(entry_el: ET.Element) -> Optional[bytes]:
    content_el = entry_el.find("a:content", ATOM_NS)
    if content_el is None:
        for x in entry_el.iter():
            if _strip_ns(x.tag) == "content":
                content_el = x
                break

    if content_el is None:
        return None

    for el in content_el.iter():
        if _strip_ns(el.tag) == "alert":
            return ET.tostring(el, encoding="utf-8", xml_declaration=True)

    return None


def _parse_atom_entries(feed_xml: bytes) -> List[Dict[str, Any]]:
    root = ET.fromstring(feed_xml)

    entries = root.findall("a:entry", ATOM_NS)
    out: List[Dict[str, Any]] = []

    if entries:
        for e in entries:
            eid = _safe_text(e.find("a:id", ATOM_NS))

            href = ""
            link_el = e.find("a:link", ATOM_NS)
            if link_el is not None:
                href = (link_el.attrib.get("href") or "").strip()

            cap_bytes = _extract_cap_from_entry(e)

            out.append({"id": eid, "link": href, "cap_embedded": cap_bytes})
        return out

    items = root.findall(".//item")
    for it in items:
        link = _safe_text(it.find("link"))
        guid = _safe_text(it.find("guid"))
        out.append({"id": guid, "link": link, "cap_embedded": None})
    return out


def _severity_style(sev: str) -> Dict[str, Any]:
    s = (sev or "").strip().lower()
    if s in ("extreme", "extremo"):
        return {"lw": 2.2, "alpha": 0.70}
    if s in ("severe", "severo"):
        return {"lw": 2.0, "alpha": 0.60}
    if s in ("moderate", "moderada"):
        return {"lw": 1.6, "alpha": 0.55}
    if s in ("minor", "baixa", "baixo"):
        return {"lw": 1.4, "alpha": 0.50}
    return {"lw": 1.6, "alpha": 0.55}


def _plot_map(uf_geojson_path: str, alerts: List[Dict[str, Any]], out_png: str, title: str) -> None:
    uf_gdf = gpd.read_file(uf_geojson_path)

    poly_rows = []
    for a in alerts:
        geom = a.get("polygon_geom")
        if geom is None or getattr(geom, "is_empty", False):
            continue
        poly_rows.append(
            {
                "identifier": a.get("identifier", ""),
                "event": a.get("event", ""),
                "severity": a.get("severity", ""),
                "geometry": geom,
            }
        )

    if not poly_rows:
        raise ValueError("nenhum alerta com polygon válido para plotar")

    agdf = gpd.GeoDataFrame(poly_rows, geometry="geometry", crs="EPSG:4326")

    fig = plt.figure(figsize=(12, 12))
    ax = plt.gca()

    uf_gdf.plot(ax=ax, linewidth=0.6, edgecolor="black", facecolor="white")

    for _, row in agdf.iterrows():
        style = _severity_style(row.get("severity", ""))
        gpd.GeoSeries([row.geometry], crs="EPSG:4326").plot(
            ax=ax,
            linewidth=style["lw"],
            alpha=style["alpha"],
            edgecolor="black",
            facecolor="none",
        )

    ax.set_title(title)
    ax.set_axis_off()
    plt.tight_layout()
    fig.savefig(out_png, dpi=200)
    plt.close(fig)


def _telegram_send(token: str, chat_id: str, text: str) -> Tuple[bool, str]:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode(
        {"chat_id": chat_id, "text": text, "disable_web_page_preview": "true"}
    ).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8", errors="replace")
        return True, body
    except Exception as e:
        return False, str(e)


def _json_safe_alert(a: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove coisas não serializáveis (Polygon) e adiciona um WKT opcional pra debug.
    """
    out = dict(a)
    geom = out.pop("polygon_geom", None)

    # ajuda a debugar sem quebrar o json
    out["has_polygon"] = bool(geom is not None)
    try:
        out["polygon_wkt"] = geom.wkt if geom is not None else ""
    except Exception:
        out["polygon_wkt"] = ""

    return out


def main() -> int:
    RSS_URL = _read_env("RSS_URL", "https://idapfile.mdr.gov.br/idap/api/rss/cap")
    UF_GEOJSON_PATH = _read_env("UF_GEOJSON_PATH", "resources/br_uf.geojson")
    OUT_DIR = _read_env("OUT_DIR", "out")
    CACHE_DIR = _read_env("CACHE_DIR", ".cache")
    MAX_ITEMS = os.getenv("MAX_ITEMS", "").strip()

    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    SEND_TELEGRAM = os.getenv("SEND_TELEGRAM", "1").strip()

    _ensure_dir(OUT_DIR)
    _ensure_dir(CACHE_DIR)

    run_tag = _now_tag()
    run_dir = os.path.join(OUT_DIR, f"run_{run_tag}")
    _ensure_dir(run_dir)

    state_path = os.path.join(CACHE_DIR, "state.json")

    print(f"[INFO] RSS_URL={RSS_URL}")
    print(f"[INFO] UF_GEOJSON_PATH={UF_GEOJSON_PATH}")
    print(f"[INFO] OUT_DIR={OUT_DIR}")
    print(f"[INFO] RUN_DIR={run_dir}")
    print(f"[INFO] STATE_PATH={state_path}")
    print(f"[INFO] MAX_ITEMS={(MAX_ITEMS if MAX_ITEMS else '(sem limite)')}")

    st, feed_bytes, _ = _http_get(RSS_URL, timeout=40)
    if st < 200 or st >= 300:
        preview = feed_bytes[:300].decode("utf-8", errors="replace")
        print(f"[ERRO] Falha ao baixar RSS. HTTP {st}. Preview: {preview}")
        return 2

    try:
        entries = _parse_atom_entries(feed_bytes)
    except Exception as e:
        preview = feed_bytes[:600].decode("utf-8", errors="replace")
        print(f"[ERRO] Não consegui parsear o RSS/Atom. Erro: {e}")
        print(f"[DEBUG] Feed preview:\n{preview}")
        return 3

    if not entries:
        preview = feed_bytes[:600].decode("utf-8", errors="replace")
        print("[ERRO] Feed baixado, mas nenhuma entrada encontrada.")
        print(f"[DEBUG] Primeiros 600 chars do feed:\n{preview}")
        return 4

    if MAX_ITEMS:
        try:
            lim = int(MAX_ITEMS)
            entries = entries[:lim]
        except Exception:
            pass

    print(f"[INFO] Entradas no RSS (consideradas): {len(entries)}")

    alerts: List[Dict[str, Any]] = []
    errors: List[Dict[str, str]] = []

    for it in entries:
        eid = (it.get("id") or "").strip()
        link = (it.get("link") or "").strip()
        cap_embedded: Optional[bytes] = it.get("cap_embedded")

        try:
            if cap_embedded:
                a = _parse_cap_xml_bytes(cap_embedded)
                a["cap_source"] = "embedded"
                a["cap_url"] = ""
            else:
                if not link:
                    errors.append({"id": eid, "link": link, "error": "entrada sem CAP embutido e sem link"})
                    continue
                st2, cap_bytes, _ = _http_get(link, timeout=40)
                if st2 < 200 or st2 >= 300:
                    errors.append({"id": eid, "link": link, "error": f"HTTP {st2} ao baixar CAP"})
                    continue
                a = _parse_cap_xml_bytes(cap_bytes)
                a["cap_source"] = "link"
                a["cap_url"] = link

            if not a.get("identifier"):
                a["identifier"] = eid

            alerts.append(a)

        except Exception as e:
            errors.append({"id": eid, "link": link, "error": str(e)})

    print(f"[INFO] CAPs parseados: {len(alerts)} | erros: {len(errors)}")

    # salva versão "json safe"
    alerts_json = [_json_safe_alert(a) for a in alerts]
    with open(os.path.join(run_dir, "alerts.json"), "w", encoding="utf-8") as f:
        json.dump(alerts_json, f, ensure_ascii=False, indent=2)

    with open(os.path.join(run_dir, "errors.json"), "w", encoding="utf-8") as f:
        json.dump(errors, f, ensure_ascii=False, indent=2)

    by_sev: Dict[str, int] = {}
    by_event: Dict[str, int] = {}
    n_with_polygon = 0

    for a in alerts:
        sev = (a.get("severity") or "").strip() or "(vazio)"
        evt = (a.get("event") or "").strip() or "(vazio)"
        by_sev[sev] = by_sev.get(sev, 0) + 1
        by_event[evt] = by_event.get(evt, 0) + 1
        if a.get("polygon_geom") is not None:
            n_with_polygon += 1

    stats = {
        "run_tag": run_tag,
        "rss_url": RSS_URL,
        "entries_considered": len(entries),
        "caps_parsed": len(alerts),
        "caps_errors": len(errors),
        "caps_with_polygon": n_with_polygon,
        "by_severity": dict(sorted(by_sev.items(), key=lambda x: (-x[1], x[0]))),
        "by_event": dict(sorted(by_event.items(), key=lambda x: (-x[1], x[0]))),
    }

    with open(os.path.join(run_dir, "stats.json"), "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    with open(os.path.join(run_dir, "alerts_summary.csv"), "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["identifier", "sent", "status", "msgType", "event", "severity", "urgency", "certainty", "areaDesc", "cap_source", "has_polygon"])
        for a in alerts_json:
            w.writerow([
                a.get("identifier", ""),
                a.get("sent", ""),
                a.get("status", ""),
                a.get("msgType", ""),
                a.get("event", ""),
                a.get("severity", ""),
                a.get("urgency", ""),
                a.get("certainty", ""),
                a.get("areaDesc", ""),
                a.get("cap_source", ""),
                a.get("has_polygon", False),
            ])

    polys = [a for a in alerts if a.get("polygon_geom") is not None]
    if not polys:
        print("[WARN] Mapa não gerado: nenhum alerta com polygon para plotar")
    else:
        out_png = os.path.join(run_dir, "mapa_alertas.png")
        try:
            title = f"IDAP CAP (RSS) | {run_tag} | polígonos: {len(polys)}"
            _plot_map(UF_GEOJSON_PATH, polys, out_png, title)
            print(f"[INFO] Mapa gerado: {out_png}")
        except Exception as e:
            print(f"[WARN] Falha ao gerar mapa: {e}")

    state = {
        "last_run_utc": datetime.now(timezone.utc).isoformat(),
        "run_tag": run_tag,
        "entries_considered": len(entries),
        "caps_parsed": len(alerts),
        "caps_errors": len(errors),
    }
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

    if SEND_TELEGRAM == "1" and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        lines = []
        lines.append(f"IDAP Daily Maps {run_tag}")
        lines.append(f"Entradas RSS: {len(entries)}")
        lines.append(f"CAPs parseados: {len(alerts)} | erros: {len(errors)}")
        lines.append(f"Com polygon: {n_with_polygon}")
        top_events = list(stats["by_event"].items())[:3]
        if top_events:
            lines.append("Top eventos:")
            for k, v in top_events:
                lines.append(f"- {k}: {v}")

        ok, resp = _telegram_send(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, "\n".join(lines))
        if ok:
            print("[INFO] Telegram: mensagem enviada")
        else:
            print(f"[WARN] Telegram: falha ao enviar: {resp}")

    print("[INFO] Finalizado.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
