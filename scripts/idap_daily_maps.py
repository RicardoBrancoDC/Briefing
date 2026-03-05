#!/usr/bin/env python3
import os
import re
import time
import urllib.request
import urllib.error
import http.client
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
from shapely.ops import unary_union
import matplotlib.pyplot as plt


# =========================
# Config (env vars)
# =========================
RSS_URL = os.environ.get("RSS_URL", "https://idapfile.mdr.gov.br/idap/api/rss/cap").strip()
UF_GEOJSON_PATH = os.environ.get("UF_GEOJSON_PATH", "resources/br_uf.geojson").strip()
OUT_DIR = os.environ.get("OUT_DIR", "out").strip()

HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "45"))
HTTP_RETRIES = int(os.environ.get("HTTP_RETRIES", "3"))
HTTP_RETRY_SLEEP_SECONDS = float(os.environ.get("HTTP_RETRY_SLEEP_SECONDS", "2.0"))


# =========================
# Helpers: HTTP
# =========================
def http_get(url: str, timeout: int = HTTP_TIMEOUT) -> bytes:
    last_err: Optional[Exception] = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "github-actions-idap-daily-maps/1.1"},
                method="GET",
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except http.client.IncompleteRead as e:
            last_err = e
            print(f"[http_get] IncompleteRead {attempt}/{HTTP_RETRIES}: {e}")
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="ignore")
            except Exception:
                pass
            last_err = RuntimeError(f"HTTPError {e.code} ao baixar {url}. Body: {body[:400]}")
            print(f"[http_get] HTTPError {attempt}/{HTTP_RETRIES}: {e.code}")
            if 400 <= e.code < 500:
                break
        except urllib.error.URLError as e:
            last_err = e
            print(f"[http_get] URLError {attempt}/{HTTP_RETRIES}: {e}")
        except Exception as e:
            last_err = e
            print(f"[http_get] Erro {attempt}/{HTTP_RETRIES}: {e}")

        if attempt < HTTP_RETRIES:
            time.sleep(HTTP_RETRY_SLEEP_SECONDS * attempt)

    raise RuntimeError(f"Falha ao baixar {url} após {HTTP_RETRIES} tentativas. Último erro: {last_err}") from last_err


# =========================
# XML helpers (namespace-safe)
# =========================
def localname(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def first_child(elem: ET.Element, name: str) -> Optional[ET.Element]:
    for ch in list(elem):
        if localname(ch.tag) == name:
            return ch
    return None


def children(elem: ET.Element, name: str) -> List[ET.Element]:
    out: List[ET.Element] = []
    for ch in list(elem):
        if localname(ch.tag) == name:
            out.append(ch)
    return out


def child_text(elem: ET.Element, name: str) -> str:
    ch = first_child(elem, name)
    return (ch.text or "").strip() if ch is not None else ""


# =========================
# Nível do alerta (sua regra)
# =========================
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


def normalize_event(event: str) -> str:
    e = (event or "").strip()
    if not e:
        return "-"
    e_low = e.lower()

    # seu ajuste que já vinha usando
    if "tempestade local convectiva" in e_low and "chuvas intensas" in e_low:
        return "Chuvas Intensas"

    return e


# =========================
# Parse Atom feed + embedded CAP
# =========================
def parse_atom_feed(feed_xml: bytes) -> List[Dict]:
    root = ET.fromstring(feed_xml)
    out: List[Dict] = []

    for entry in list(root):
        if localname(entry.tag) != "entry":
            continue

        entry_id = child_text(entry, "id").strip()
        entry_updated = child_text(entry, "updated").strip()

        content = first_child(entry, "content")
        if content is None:
            continue

        alert = None
        for ch in list(content):
            if localname(ch.tag) == "alert":
                alert = ch
                break
        if alert is None:
            continue

        info = first_child(alert, "info")

        def info_text(name: str) -> str:
            return child_text(info, name) if info is not None else ""

        areas_payload: List[Dict] = []
        if info is not None:
            for area in children(info, "area"):
                area_desc = child_text(area, "areaDesc")
                polygons = [ (p.text or "").strip() for p in children(area, "polygon") if (p.text or "").strip() ]
                areas_payload.append({"areaDesc": (area_desc or "").strip(), "polygons": polygons})

        out.append(
            {
                "entry_id": entry_id,
                "entry_updated": entry_updated,
                "onset": info_text("onset"),
                "senderName": info_text("senderName"),
                "event": normalize_event(info_text("event")),
                "headline": info_text("headline"),
                "severity": info_text("severity"),
                "urgency": info_text("urgency"),
                "certainty": info_text("certainty"),
                "responseType": info_text("responseType"),
                "areas": areas_payload,
            }
        )

    return out


# =========================
# Geometry parsing
# CAP polygon: "lat,lon lat,lon ..."
# shapely: (lon,lat)
# =========================
def parse_cap_polygon(poly_text: str) -> Optional[Polygon]:
    txt = (poly_text or "").strip()
    if not txt:
        return None

    pts: List[Tuple[float, float]] = []
    for token in txt.split():
        if "," not in token:
            continue
        lat_s, lon_s = token.split(",", 1)
        try:
            lat = float(lat_s)
            lon = float(lon_s)
        except ValueError:
            continue
        pts.append((lon, lat))

    if len(pts) < 3:
        return None

    if pts[0] != pts[-1]:
        pts.append(pts[0])

    try:
        p = Polygon(pts)
        if not p.is_valid:
            p = p.buffer(0)
        if p.is_empty:
            return None
        return p
    except Exception:
        return None


def guess_ufs_from_areadesc(area_desc: str) -> Set[str]:
    """
    Tenta extrair UF(s) de areaDesc.
    Exemplos típicos: "MINAS GERAIS/MG" ou listas com "MG, RJ" etc.
    """
    s = (area_desc or "").upper()

    # pega o padrão /UF no final
    m = re.search(r"/\s*([A-Z]{2})\s*$", s)
    if m:
        return {m.group(1)}

    # fallback: caça quaisquer tokens UF isolados
    cand = set(re.findall(r"\b(AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|RS|RO|RR|SC|SP|SE|TO)\b", s))
    return cand


UF_TO_REGIAO = {
    "AC":"Norte","AP":"Norte","AM":"Norte","PA":"Norte","RO":"Norte","RR":"Norte","TO":"Norte",
    "AL":"Nordeste","BA":"Nordeste","CE":"Nordeste","MA":"Nordeste","PB":"Nordeste","PE":"Nordeste","PI":"Nordeste","RN":"Nordeste","SE":"Nordeste",
    "DF":"Centro-Oeste","GO":"Centro-Oeste","MT":"Centro-Oeste","MS":"Centro-Oeste",
    "ES":"Sudeste","MG":"Sudeste","RJ":"Sudeste","SP":"Sudeste",
    "PR":"Sul","SC":"Sul","RS":"Sul",
}


LEVEL_COLOR = {
    "Extremo": "#6f2dbd",
    "Severo":  "#d00000",
    "Alto":    "#ffba08",
    "Médio":   "#2d6a4f",
    "Baixo":   "#457b9d",
    "Indefinido": "#6c757d",
}


# =========================
# Build GeoDataFrame de alertas
# - geometry: união de polígonos CAP
# - fallback: união de UFs citadas no areaDesc
# =========================
def build_alerts_gdf(entries: List[Dict], uf_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    uf_index = {str(r["sigla"]).upper(): r["geometry"] for _, r in uf_gdf.iterrows()}

    rows = []
    for e in entries:
        nivel = calc_nivel(e.get("severity",""), e.get("urgency",""), e.get("certainty",""), e.get("responseType",""))
        event = e.get("event") or "-"
        sender = e.get("senderName") or "-"
        onset = (e.get("onset") or e.get("entry_updated") or "").strip()
        entry_id = (e.get("entry_id") or "").strip()

        geoms = []
        ufs_found: Set[str] = set()

        for area in e.get("areas", []) or []:
            area_desc = area.get("areaDesc") or ""
            ufs_found |= guess_ufs_from_areadesc(area_desc)

            for poly_txt in area.get("polygons", []) or []:
                p = parse_cap_polygon(poly_txt)
                if p is not None:
                    geoms.append(p)

        geom = None
        if geoms:
            geom = unary_union(geoms)
        else:
            fallback = [uf_index[uf] for uf in sorted(list(ufs_found)) if uf in uf_index]
            if fallback:
                geom = unary_union(fallback)

        if geom is None or getattr(geom, "is_empty", True):
            continue

        # regiões (conta em todas se tiver múltiplas UFs)
        regioes = sorted({UF_TO_REGIAO.get(uf, "-") for uf in ufs_found if uf in UF_TO_REGIAO})
        if not regioes:
            regioes = ["-"]

        rows.append({
            "entry_id": entry_id,
            "onset": onset,
            "sender": sender,
            "event": event,
            "nivel": nivel,
            "ufs": ",".join(sorted(list(ufs_found))) if ufs_found else "",
            "regioes": ",".join(regioes),
            "geometry": geom,
        })

    return gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")


# =========================
# Plot helpers
# =========================
def plot_base(ax, uf_gdf: gpd.GeoDataFrame, title: str) -> None:
    uf_gdf.boundary.plot(ax=ax, linewidth=0.6)
    ax.set_title(title, fontsize=12)
    ax.set_axis_off()


def plot_alerts_map(uf_gdf: gpd.GeoDataFrame, alerts_gdf: gpd.GeoDataFrame, out_png: str, title: str) -> None:
    fig = plt.figure(figsize=(11, 10))
    ax = plt.gca()
    plot_base(ax, uf_gdf, title)

    for nivel, color in LEVEL_COLOR.items():
        sub = alerts_gdf[alerts_gdf["nivel"] == nivel]
        if len(sub) == 0:
            continue
        sub.plot(ax=ax, color=color, alpha=0.55, linewidth=0.2, edgecolor="black", label=nivel)

    ax.legend(loc="lower left", fontsize=9, frameon=True)
    os.makedirs(os.path.dirname(out_png) or ".", exist_ok=True)
    plt.tight_layout()
    plt.savefig(out_png, dpi=200)
    plt.close(fig)


# =========================
# Summary markdown
# =========================
def explode_regioes(alerts_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    if alerts_gdf.empty:
        return pd.DataFrame(columns=["entry_id", "regiao"])

    tmp = alerts_gdf[["entry_id", "regioes"]].copy()
    tmp["regiao"] = tmp["regioes"].fillna("").apply(lambda s: [r.strip() for r in s.split(",") if r.strip()] or ["-"])
    tmp = tmp.explode("regiao")
    return tmp[["entry_id", "regiao"]]


def write_summary_md(alerts_gdf: gpd.GeoDataFrame, out_md: str) -> None:
    now_local = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    total = int(len(alerts_gdf))

    by_level = alerts_gdf["nivel"].value_counts(dropna=False).to_dict() if total else {}
    by_event = alerts_gdf["event"].value_counts(dropna=False).head(20).to_dict() if total else {}

    reg = explode_regioes(alerts_gdf)
    by_region = reg["regiao"].value_counts(dropna=False).to_dict() if not reg.empty else {}

    lines = []
    lines.append("# Resumo diário IDAP")
    lines.append("")
    lines.append(f"Gerado em: {now_local}")
    lines.append(f"Total de alertas plotados: **{total}**")
    lines.append("")

    lines.append("## Alertas por nível")
    if by_level:
        for k, v in by_level.items():
            lines.append(f"- {k}: {v}")
    else:
        lines.append("- (sem dados)")

    lines.append("")
    lines.append("## Alertas por região")
    if by_region:
        for k, v in by_region.items():
            lines.append(f"- {k}: {v}")
    else:
        lines.append("- (sem dados)")

    lines.append("")
    lines.append("## Top eventos (até 20)")
    if by_event:
        for k, v in by_event.items():
            lines.append(f"- {k}: {v}")
    else:
        lines.append("- (sem dados)")

    os.makedirs(os.path.dirname(out_md) or ".", exist_ok=True)
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).strip() + "\n")


# =========================
# Regras de grupos (mapas 2, 3, 4)
# =========================
FOCUS_MAP2 = {"Chuvas Intensas", "Tempestades Convectivas", "Inundações"}

def is_deslizamento(event: str) -> bool:
    e = (event or "").lower()
    # aqui dá pra ajustar fino, mas já cobre bem:
    return ("desliz" in e) or ("corrida de massa" in e) or ("movimento de massa" in e)


# =========================
# Main
# =========================
def main() -> int:
    if not os.path.exists(UF_GEOJSON_PATH):
        raise RuntimeError(f"UF_GEOJSON_PATH não existe: {UF_GEOJSON_PATH}")

    os.makedirs(OUT_DIR, exist_ok=True)

    print(f"Baixando feed: {RSS_URL}")
    feed_xml = http_get(RSS_URL)
    entries = parse_atom_feed(feed_xml)
    print(f"Entries no feed: {len(entries)}")

    print(f"Lendo UFs: {UF_GEOJSON_PATH}")
    uf_gdf = gpd.read_file(UF_GEOJSON_PATH)

    if "sigla" not in uf_gdf.columns:
        for cand in ["SIGLA", "uf", "UF", "sigla_uf"]:
            if cand in uf_gdf.columns:
                uf_gdf = uf_gdf.rename(columns={cand: "sigla"})
                break

    if "sigla" not in uf_gdf.columns:
        raise RuntimeError("Seu GeoJSON de UFs precisa ter uma coluna com a sigla da UF (ex: 'sigla').")

    uf_gdf["sigla"] = uf_gdf["sigla"].astype(str).str.upper()

    alerts_gdf = build_alerts_gdf(entries, uf_gdf)
    print(f"Alertas com geometria (polygon ou fallback UF): {len(alerts_gdf)}")

    # 1) Todos os alertas
    plot_alerts_map(
        uf_gdf,
        alerts_gdf,
        os.path.join(OUT_DIR, "mapa_1_todos_alertas.png"),
        "Mapa 1. Todos os alertas (IDAP) por nível",
    )

    # 2) Chuvas/Tempestades/Inundações
    sub2 = alerts_gdf[alerts_gdf["event"].isin(FOCUS_MAP2)]
    plot_alerts_map(
        uf_gdf,
        sub2,
        os.path.join(OUT_DIR, "mapa_2_chuva_tempestade_inundacao.png"),
        "Mapa 2. Chuvas Intensas, Tempestades Convectivas, Inundações",
    )

    # 3) Deslizamentos
    sub3 = alerts_gdf[alerts_gdf["event"].apply(is_deslizamento)]
    plot_alerts_map(
        uf_gdf,
        sub3,
        os.path.join(OUT_DIR, "mapa_3_deslizamentos.png"),
        "Mapa 3. Alertas de deslizamentos e movimentos de massa",
    )

    # 4) Demais eventos
    mask4 = (~alerts_gdf["event"].isin(FOCUS_MAP2)) & (~alerts_gdf["event"].apply(is_deslizamento))
    sub4 = alerts_gdf[mask4]
    plot_alerts_map(
        uf_gdf,
        sub4,
        os.path.join(OUT_DIR, "mapa_4_demais_eventos.png"),
        "Mapa 4. Demais tipos de alertas",
    )

    # Summary
    write_summary_md(alerts_gdf, os.path.join(OUT_DIR, "SUMMARY.md"))

    print("OK. Saídas geradas em:", OUT_DIR)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
