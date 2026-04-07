const AUTO_REFRESH_MS = 300000;
let dashboardCarregando = false;
let dashboardUltimaLeitura = null;
let refreshSecondsRemaining = Math.floor(AUTO_REFRESH_MS / 1000);
let refreshIntervalId = null;

async function carregarDashboard() {
  if (dashboardCarregando) return;
  dashboardCarregando = true;

  try {
    const cacheBuster = `_ts=${Date.now()}`;
    const response = await fetch(`dashboard_data.json?${cacheBuster}`, {
      cache: "no-store"
    });

    if (!response.ok) {
      throw new Error(`Falha ao carregar dashboard_data.json: ${response.status}`);
    }

    const data = await response.json();
    dashboardUltimaLeitura = new Date();
    resetRefreshCountdown();

    preencherCabecalho(data);
    preencherCards(data);
    renderUltimosAlertas(data.ultimos_alertas || data.latest_alerts || []);
    renderTopAutoridades(data.top5_autoridades || data.top_emitters || []);
    renderTabelaAlertas(
      data.all_alerts ||
      data.tabela_alertas ||
      data.ultimos_alertas ||
      data.latest_alerts ||
      []
    );
    await renderMapaUF(data);
    renderGraficos(data);
  } catch (error) {
    console.error("Erro geral do dashboard:", error);
    renderErroGeral(error);
  } finally {
    dashboardCarregando = false;
  }
}

function preencherCabecalho(data) {
  const atualizadoOrigem = formatarDataHora(
    data.atualizado_em ||
    data.gerado_em ||
    data.generated_at
  );

  const atualizadoLeitura = dashboardUltimaLeitura
    ? dashboardUltimaLeitura.toLocaleString("pt-BR", {
        timeZone: "America/Sao_Paulo",
        day: "2-digit",
        month: "2-digit",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit"
      })
    : "--/--/---- --:--:--";

  setText("meta-atualizado", `${atualizadoLeitura}`);
  setText("meta-execucao", data.execucao || data.run_id || data.source_run_dir || "--");
}

function preencherCards(data) {
  setText("card-vigentes", numero(
    data.cards?.vigentes ??
    data.resumo?.vigentes ??
    0
  ));

  setText("card-24h", numero(
    data.cards?.ultimas_24h ??
    data.cards?.ultimas24h ??
    data.resumo?.ultimas_24h ??
    data.summary?.total_alerts ??
    0
  ));

  setText("card-autoridades", numero(
    data.cards?.autoridades_ativas ??
    data.cards?.autoridadesAtivas ??
    data.resumo?.autoridades_ativas ??
    0
  ));

  setText("card-tipos-evento", numero(contarTiposEvento(
    data.eventos ||
    data.alertas_por_evento ||
    data.event_distribution ||
    {}
  )));
}

function renderUltimosAlertas(alertas) {
  const container = document.getElementById("ultimos-alertas");
  if (!container) return;

  container.innerHTML = "";

  if (!alertas.length) {
    container.innerHTML = `<div class="empty-state">Nenhum alerta recente disponível.</div>`;
    return;
  }

  alertas.slice(0, 2).forEach((alerta) => {
    const item = document.createElement("div");
    item.className = "recent-item";

    const hora = alerta.time || obterHoraAlerta(alerta);
    const dataAlerta = alerta.date || obterDataAlerta(alerta);
    const emissor = alerta.emissor || alerta.senderName || alerta.sender || "Sem emissor";
    const local = alerta.location || montarLocal(alerta);
    const evento = alerta.evento || alerta.event || "Sem evento";
    const descricao = truncar(
      alerta.descricao_curta ||
      alerta.descricao ||
      alerta.description ||
      alerta.headline ||
      "Sem descrição disponível.",
      160
    );
    const nivel = normalizarNivel(
      alerta.nivel ||
      alerta.nivel_calculado ||
      alerta.severidade_label ||
      alerta.severity_label ||
      "Indefinido"
    );

    item.innerHTML = `
      <div class="recent-time">
        <div class="recent-time-hour">${esc(hora)}</div>
        <div class="recent-time-date">${esc(dataAlerta)}</div>
      </div>

      <div class="recent-emissor">
        <div class="recent-emissor-name" title="${escAttr(emissor)}">${esc(emissor)}</div>
        <div class="recent-emissor-loc">${esc(local)}</div>
      </div>

      <div class="recent-content">
        <div class="recent-evento">${esc(evento)}</div>
        <div class="recent-desc">${esc(descricao)}</div>
      </div>

      <div class="recent-tag ${classeNivel(nivel)}">${esc(nivel)}</div>
    `;

    container.appendChild(item);
  });
}

function renderTopAutoridades(items) {
  const container = document.getElementById("top5-autoridades");
  if (!container) return;

  container.innerHTML = "";

  if (!items.length) {
    container.innerHTML = `<div class="empty-state">Nenhuma autoridade emissora encontrada.</div>`;
    return;
  }

  const cores = ["top-blue", "top-green", "top-orange", "top-red", "top-purple"];
  const maxValor = Math.max(...items.map((item) => Number(item.valor ?? item.total ?? item.count ?? 0)), 1);

  items.slice(0, 10).forEach((item, index) => {
    const nome =
      item.short_name ||
      item.nome ||
      item.name ||
      item.autoridade ||
      item.emissor ||
      "Sem nome";

    const valor = Number(item.valor ?? item.total ?? item.count ?? 0);
    const largura = Math.max((valor / maxValor) * 100, valor > 0 ? 8 : 0);
    const cor = cores[index % cores.length];

    const div = document.createElement("div");
    div.className = "top-item";

    div.innerHTML = `
      <div class="top-item-head">
        <div class="top-item-name" title="${escAttr(nome)}">${esc(nome)}</div>
        <div class="top-item-value">${numero(valor)}</div>
      </div>
      <div class="top-track">
        <div class="top-fill ${cor}" style="width: ${largura}%;"></div>
      </div>
    `;

    container.appendChild(div);
  });
}

function renderTabelaAlertas(alertas) {
  const tbody = document.getElementById("tabela-alertas-body");
  if (!tbody) return;

  tbody.innerHTML = "";

  if (!alertas.length) {
    tbody.innerHTML = `
      <tr>
        <td colspan="6" class="empty-state">Nenhum alerta disponível para a tabela.</td>
      </tr>
    `;
    return;
  }

  alertas.forEach((alerta) => {
    const tr = document.createElement("tr");

    const dataHora =
      alerta.date && alerta.time
        ? `${alerta.date} ${alerta.time}`
        : formatarDataHoraCurta(
            alerta.data ||
            alerta.onset ||
            alerta.sent ||
            alerta.inicio ||
            alerta.timestamp
          );

    const emissor = alerta.emissor || alerta.senderName || alerta.sender || "-";
    const evento = alerta.evento || alerta.event || "-";
    const severidade = normalizarNivel(
      alerta.nivel ||
      alerta.nivel_calculado ||
      alerta.severidade_label ||
      alerta.severity_label ||
      alerta.severity ||
      "-"
    );
    const uf = alerta.uf || alerta.estado || extrairUF(alerta.areaDesc || alerta.local || alerta.location || "") || "-";
    const municipio = alerta.municipio || alerta.cidade || extrairMunicipio(alerta.areaDesc || alerta.local || alerta.location || "") || "-";

    tr.innerHTML = `
      <td>${esc(dataHora)}</td>
      <td title="${escAttr(emissor)}">${esc(emissor)}</td>
      <td title="${escAttr(evento)}">${esc(evento)}</td>
      <td>${esc(severidade)}</td>
      <td>${esc(uf)}</td>
      <td title="${escAttr(municipio)}">${esc(municipio)}</td>
    `;

    tbody.appendChild(tr);
  });
}

async function renderMapaUF(data) {
  const container = document.getElementById("mapa-uf");
  if (!container) return;

  const listaUF = data.alertas_por_uf || data.ufs || data.uf_distribution || [];
  if (!Array.isArray(listaUF) || !listaUF.length) {
    container.innerHTML = `<div class="empty-state">Mapa por UF não disponível nesta execução.</div>`;
    return;
  }

  try {
    const geojsonResp = await fetch(`data/br_uf.geojson?_ts=${Date.now()}`, { cache: "no-store" });
    if (!geojsonResp.ok) {
      throw new Error(`Falha ao carregar GeoJSON: ${geojsonResp.status}`);
    }

    const geojson = await geojsonResp.json();
    const ufMap = new Map();

    listaUF.forEach((item) => {
      const uf = String(item.uf || item.nome || item.label || "").trim().toUpperCase();
      const valor = Number(item.valor ?? item.total ?? item.count ?? 0);
      if (uf) ufMap.set(uf, valor);
    });

    const svg = montarSvgMapaBrasil(geojson, ufMap);
    container.innerHTML = `
      <div class="map-inner">
        <div class="map-svg-wrap">${svg}</div>
      </div>
    `;
  } catch (error) {
    console.error("Erro ao renderizar mapa por UF:", error);
    const resumo = listaUF
      .map((item) => {
        const uf = item.uf || item.nome || item.label || "--";
        const valor = item.valor ?? item.total ?? item.count ?? 0;
        return `<strong>${esc(uf)}</strong>: ${numero(valor)}`;
      })
      .join(" &nbsp;&nbsp; ");

    container.innerHTML = `<div class="empty-state">${resumo}</div>`;
  }
}

function montarSvgMapaBrasil(geojson, ufMap) {
  const width = 1200;
  const height = 760;
  const padding = 6;

  const allPoints = [];
  for (const feature of geojson.features || []) {
    coletarPontosFeature(feature, allPoints);
  }

  if (!allPoints.length) {
    return `<div class="empty-state">Não foi possível montar o mapa.</div>`;
  }

  let minX = Infinity;
  let minY = Infinity;
  let maxX = -Infinity;
  let maxY = -Infinity;

  allPoints.forEach(([lon, lat]) => {
    if (lon < minX) minX = lon;
    if (lon > maxX) maxX = lon;
    if (lat < minY) minY = lat;
    if (lat > maxY) maxY = lat;
  });

  const dataWidth = maxX - minX || 1;
  const dataHeight = maxY - minY || 1;
  const drawWidth = width - padding * 2;
  const drawHeight = height - padding * 2;
  const scale = Math.min(drawWidth / dataWidth, drawHeight / dataHeight) * 1.1;

  const projectedWidth = dataWidth * scale;
  const projectedHeight = dataHeight * scale;
  const offsetX = (width - projectedWidth) / 2;
  const offsetY = (height - projectedHeight) / 2 + 35;

  function project([lon, lat]) {
    const x = offsetX + (lon - minX) * scale;
    const y = height - (offsetY + (lat - minY) * scale);
    return [x, y];
  }

  const valores = Array.from(ufMap.values());
  const maxValor = Math.max(...valores, 0);

  const paths = [];
  const labels = [];

  for (const feature of geojson.features || []) {
    const uf = extrairSiglaUF(feature);
    const valor = ufMap.get(uf) || 0;
    const fill = corMapa(valor, maxValor);
    const pathD = geometryToPath(feature.geometry, project);

    if (!pathD) continue;

    const centroide = featureCentroid(feature.geometry);
    let cx = null;
    let cy = null;

    if (centroide) {
      [cx, cy] = project(centroide);
    }

    paths.push(`
      <path
        d="${pathD}"
        fill="${fill}"
        stroke="#ffffff"
        stroke-width="1.4"
      >
        <title>${esc(uf || "UF")}: ${numero(valor)} alerta(s)</title>
      </path>
    `);

    if (cx !== null && cy !== null && valor > 0) {
      labels.push(`
        <text
          x="${cx}"
          y="${cy - 4}"
          text-anchor="middle"
          dominant-baseline="middle"
          font-size="24"
          font-weight="800"
          fill="#1f2a44"
        >
          ${esc(uf)}
        </text>
        <text
          x="${cx}"
          y="${cy + 21}"
          text-anchor="middle"
          dominant-baseline="middle"
          font-size="20"
          font-weight="700"
          fill="#1f2a44"
        >
          ${esc(String(valor))}
        </text>
      `);
    }
  }

  return `
    <svg viewBox="0 0 ${width} ${height}" style="width:100%; height:100%; display:block;">
      <rect x="0" y="0" width="${width}" height="${height}" fill="transparent"></rect>
      ${paths.join("\n")}
      ${labels.join("\n")}
    </svg>
  `;
}

function coletarPontosFeature(feature, bucket) {
  if (!feature || !feature.geometry) return;
  coletarPontosGeometry(feature.geometry, bucket);
}

function coletarPontosGeometry(geometry, bucket) {
  if (!geometry) return;

  if (geometry.type === "Polygon") {
    geometry.coordinates.forEach((ring) => {
      ring.forEach((point) => bucket.push(point));
    });
    return;
  }

  if (geometry.type === "MultiPolygon") {
    geometry.coordinates.forEach((polygon) => {
      polygon.forEach((ring) => {
        ring.forEach((point) => bucket.push(point));
      });
    });
  }
}

function geometryToPath(geometry, project) {
  if (!geometry) return "";

  if (geometry.type === "Polygon") {
    return polygonToPath(geometry.coordinates, project);
  }

  if (geometry.type === "MultiPolygon") {
    return geometry.coordinates
      .map((polygon) => polygonToPath(polygon, project))
      .join(" ");
  }

  return "";
}

function polygonToPath(polygonCoords, project) {
  return polygonCoords
    .map((ring) => {
      if (!ring.length) return "";
      const [x0, y0] = project(ring[0]);
      const rest = ring
        .slice(1)
        .map((pt) => {
          const [x, y] = project(pt);
          return `L ${x.toFixed(2)} ${y.toFixed(2)}`;
        })
        .join(" ");
      return `M ${x0.toFixed(2)} ${y0.toFixed(2)} ${rest} Z`;
    })
    .join(" ");
}

function featureCentroid(geometry) {
  const points = [];
  coletarPontosGeometry(geometry, points);
  if (!points.length) return null;

  let sumX = 0;
  let sumY = 0;

  points.forEach(([x, y]) => {
    sumX += x;
    sumY += y;
  });

  return [sumX / points.length, sumY / points.length];
}

function extrairSiglaUF(feature) {
  const p = feature?.properties || {};
  const candidatos = [
    p.uf_05,
    p.UF_05,
    p.sigla,
    p.SIGLA,
    p.uf,
    p.UF,
    p.id,
    p.ID,
    p.sigla_uf,
    p.SIGLA_UF,
    p.estado,
    p.ESTADO,
    p.cd_uf,
    p.CD_UF
  ];

  for (const c of candidatos) {
    if (c && String(c).trim().length === 2) {
      return String(c).trim().toUpperCase();
    }
  }

  return "";
}

function corMapa(valor, maxValor) {
  if (!valor || maxValor <= 0) return "#dfe5ef";

  const ratio = valor / maxValor;
  if (ratio >= 0.8) return "#d9362c";
  if (ratio >= 0.6) return "#f08c24";
  if (ratio >= 0.4) return "#d2be45";
  if (ratio >= 0.2) return "#4caf50";
  return "#8ec5ff";
}

function renderGraficos(data) {
  renderChartSeveridade(data.severidade || data.alertas_por_severidade || data.level_distribution || {});
  renderChartEventos(data.eventos || data.alertas_por_evento || data.event_distribution || {});
  renderChartHoras(
    data.all_alerts ||
    data.tabela_alertas ||
    data.ultimos_alertas ||
    data.latest_alerts ||
    [],
    data.atualizado_em || data.gerado_em || data.generated_at || null
  );
}

function renderChartSeveridade(severidadeData) {
  const container = document.getElementById("severity-breakdown");
  if (!container) return;

  container.innerHTML = "";

  const ordem = ["Baixo", "Médio", "Alto", "Severo", "Extremo"];
  const mapa = normalizarColecaoParaMapa(severidadeData);

  const itens = ordem.map((nivel) => ({
    nome: nivel,
    valor: Number(
      mapa[nivel] ??
      mapa[nivel.toLowerCase()] ??
      0
    )
  }));

  const maxValor = Math.max(...itens.map((item) => item.valor), 1);

  const classes = {
    "Baixo": "sev-c1",
    "Médio": "sev-c2",
    "Alto": "sev-c3",
    "Severo": "sev-c4",
    "Extremo": "sev-c5"
  };

  itens.forEach((item) => {
    const pct = Math.max((item.valor / maxValor) * 100, item.valor > 0 ? 2 : 0);
    const classe = classes[item.nome] || "sev-c2";

    const row = document.createElement("div");
    row.className = "severity-row";
    row.innerHTML = `
      <div class="severity-row-head">
        <div class="severity-row-name">${esc(item.nome)}</div>
        <div class="severity-row-value">${numero(item.valor)}</div>
      </div>
      <div class="severity-track">
        <div class="severity-fill ${classe}" style="width:${pct}%;"></div>
      </div>
    `;
    container.appendChild(row);
  });
}

function renderChartEventos(eventosData) {
  const container = document.getElementById("event-breakdown");
  if (!container) return;

  container.innerHTML = "";

  const mapa = normalizarColecaoParaMapa(eventosData);
  const entries = Object.entries(mapa)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5);

  if (!entries.length) {
    container.innerHTML = `<div class="empty-state">Nenhum tipo de evento disponível.</div>`;
    return;
  }

  const classes = ["event-c1", "event-c2", "event-c3", "event-c4", "event-c5"];
  const maxValor = Math.max(...entries.map(([, v]) => Number(v || 0)), 1);

  entries.forEach(([nome, valor], index) => {
    const classe = classes[index] || "event-c5";
    const pct = Math.max((Number(valor || 0) / maxValor) * 100, valor > 0 ? 8 : 0);

    const row = document.createElement("div");
    row.className = "event-row";
    row.innerHTML = `
      <div class="event-row-head">
        <div class="event-row-name" title="${escAttr(nome)}">${esc(nome)}</div>
        <div class="event-row-value">${numero(valor)}</div>
      </div>
      <div class="event-track">
        <div class="event-fill ${classe}" style="width:${pct}%;"></div>
      </div>
    `;
    container.appendChild(row);
  });
}
function renderChartHoras(alertas, referenciaIso) {
  const container = document.getElementById("hourly-breakdown");
  if (!container) return;

  container.innerHTML = "";

  const referencia = obterDataValida(referenciaIso) || obterMaiorDataAlertas(alertas) || new Date();
  const buckets = montarBuckets24h(referencia);
  const counts = new Map(buckets.map((b) => [b.key, 0]));

  (alertas || []).forEach((alerta) => {
    const d = obterDataAlertaCompleta(alerta);
    if (!d) return;
    const key = chaveHoraSaoPaulo(d);
    if (counts.has(key)) {
      counts.set(key, counts.get(key) + 1);
    }
  });

  const itens = buckets.map((bucket) => ({
    ...bucket,
    valor: counts.get(bucket.key) || 0
  }));

  const maxValor = Math.max(...itens.map((item) => item.valor), 1);

  const grid = `<div class="hourly-grid">${Array.from({ length: 4 }).map(() => '<div class="hourly-grid-line"></div>').join("")}</div>`;

  const bars = itens.map((item) => {
    const height = item.valor > 0 ? Math.max((item.valor / maxValor) * 100, 6) : 0;
    return `
      <div class="hourly-bar-col" title="${esc(item.tooltip)}: ${numero(item.valor)} alerta(s)">
        <div class="hourly-bar-value">${item.valor > 0 ? esc(String(item.valor)) : ""}</div>
        <div class="hourly-bar" style="height:${height}%; opacity:${item.valor > 0 ? 1 : 0.14};"></div>
      </div>
    `;
  }).join("");

  const labels = itens.map((item, index) => {
    const classe = index % 2 === 0 ? "hourly-label" : "hourly-label muted";
    return `<div class="${classe}">${esc(item.label)}</div>`;
  }).join("");

  container.innerHTML = `
    <div class="hourly-chart">
      ${grid}
      <div class="hourly-bars">${bars}</div>
      <div class="hourly-labels">${labels}</div>
    </div>
  `;
}

function obterDataAlertaCompleta(alerta) {
  return obterDataValida(
    alerta?.data ||
    alerta?.onset ||
    alerta?.sent ||
    alerta?.inicio ||
    alerta?.timestamp ||
    alerta?.expires
  );
}

function obterDataValida(valor) {
  if (!valor) return null;
  const d = new Date(valor);
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

function obterMaiorDataAlertas(alertas) {
  let maior = null;
  (alertas || []).forEach((alerta) => {
    const d = obterDataAlertaCompleta(alerta);
    if (!d) return;
    if (!maior || d > maior) maior = d;
  });
  return maior;
}

function montarBuckets24h(referencia) {
  const itens = [];
  for (let i = 23; i >= 0; i -= 1) {
    const d = new Date(referencia.getTime() - i * 60 * 60 * 1000);
    itens.push({
      key: chaveHoraSaoPaulo(d),
      label: d.toLocaleTimeString("pt-BR", {
        timeZone: "America/Sao_Paulo",
        hour: "2-digit",
        hour12: false
      }),
      tooltip: d.toLocaleString("pt-BR", {
        timeZone: "America/Sao_Paulo",
        day: "2-digit",
        month: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false
      })
    });
  }
  return itens;
}

function chaveHoraSaoPaulo(date) {
  return date.toLocaleString("sv-SE", {
    timeZone: "America/Sao_Paulo",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    hour12: false
  }).replace(" ", "T");
}

function renderErroGeral(error) {
  console.error("Erro geral do dashboard:", error);

  const ids = [
    "ultimos-alertas",
    "top5-autoridades",
    "mapa-uf"
  ];

  ids.forEach((id) => {
    const el = document.getElementById(id);
    if (el) {
      el.innerHTML = `<div class="empty-state">Não foi possível carregar os dados do painel.</div>`;
    }
  });
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value ?? "--";
}

function contarTiposEvento(origem) {
  const mapa = normalizarColecaoParaMapa(origem);
  return Object.entries(mapa).filter(([, valor]) => Number(valor || 0) > 0).length;
}

function numero(value) {
  return Number(value || 0).toLocaleString("pt-BR");
}

function truncar(texto, limite) {
  const t = String(texto || "");
  if (t.length <= limite) return t;
  return `${t.slice(0, limite - 3)}...`;
}

function esc(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function escAttr(value) {
  return esc(value);
}

function normalizarNivel(valor) {
  const v = String(valor || "").trim().toLowerCase();

  if (v === "baixo" || v === "minor") return "Baixo";
  if (v === "médio" || v === "medio" || v === "moderate") return "Médio";
  if (v === "alto" || v === "severe") return "Alto";
  if (v === "severo") return "Severo";
  if (v === "extremo" || v === "extreme") return "Extremo";
  if (!v || v === "indefinido") return "Indefinido";

  return valor;
}

function classeNivel(nivel) {
  const n = String(nivel || "").toLowerCase();
  if (n === "baixo") return "baixo";
  if (n === "médio" || n === "medio") return "medio";
  if (n === "alto") return "alto";
  if (n === "severo") return "severo";
  if (n === "extremo") return "extremo";
  return "medio";
}

function formatarDataHora(iso) {
  if (!iso) return "--/--/---- --:--:--";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);

  return d.toLocaleString("pt-BR", {
    timeZone: "America/Sao_Paulo",
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  });
}

function obterHoraAlerta(alerta) {
  const valor = alerta.data || alerta.onset || alerta.sent || alerta.inicio || alerta.timestamp;
  if (!valor) return "--:--";

  const d = new Date(valor);
  if (Number.isNaN(d.getTime())) return "--:--";

  return d.toLocaleTimeString("pt-BR", {
    timeZone: "America/Sao_Paulo",
    hour: "2-digit",
    minute: "2-digit"
  });
}

function obterDataAlerta(alerta) {
  const valor = alerta.data || alerta.onset || alerta.sent || alerta.inicio || alerta.timestamp;
  if (!valor) return "--/--/----";

  const d = new Date(valor);
  if (Number.isNaN(d.getTime())) return "--/--/----";

  return d.toLocaleDateString("pt-BR", {
    timeZone: "America/Sao_Paulo",
    day: "2-digit",
    month: "2-digit",
    year: "numeric"
  });
}

function formatarDataHoraCurta(iso) {
  if (!iso) return "-";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);

  return d.toLocaleString("pt-BR", {
    timeZone: "America/Sao_Paulo",
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit"
  });
}

function montarLocal(alerta) {
  const municipio = alerta.municipio || alerta.cidade || "";
  const uf = alerta.uf || alerta.estado || extrairUF(alerta.areaDesc || alerta.local || alerta.location || "") || "";
  const areaDesc = alerta.areaDesc || alerta.local || alerta.location || "";

  if (municipio && uf) return `${municipio}/${uf}`.toUpperCase();
  if (municipio) return municipio.toUpperCase();
  if (uf) return uf.toUpperCase();
  if (areaDesc) return truncar(areaDesc.toUpperCase(), 40);
  return "LOCAL NÃO INFORMADO";
}

function extrairUF(texto) {
  const t = String(texto || "");
  const match = t.match(/\(([A-Z]{2})\)$/) || t.match(/\b([A-Z]{2})\b/);
  return match ? match[1] : "";
}

function extrairMunicipio(texto) {
  const t = String(texto || "").trim();
  if (!t) return "";
  const parts = t.split("/");
  if (parts.length > 1) return parts[0].trim();
  return t;
}

function normalizarColecaoParaMapa(origem) {
  if (!origem) return {};

  if (Array.isArray(origem)) {
    const mapa = {};
    origem.forEach((item) => {
      const chave =
        item.label ||
        item.nome ||
        item.name ||
        item.evento ||
        item.nivel ||
        item.uf ||
        item.key ||
        item.status ||
        "Sem nome";

      const valor = Number(item.valor ?? item.total ?? item.count ?? 0);
      mapa[chave] = valor;
    });
    return mapa;
  }

  if (typeof origem === "object") {
    return Object.fromEntries(
      Object.entries(origem).map(([k, v]) => [k, Number(v || 0)])
    );
  }

  return {};
}

function contarUFs(origem) {
  if (!origem) return 0;
  if (Array.isArray(origem)) return origem.length;
  if (typeof origem === "object") return Object.keys(origem).length;
  return 0;
}

function resetRefreshCountdown() {
  refreshSecondsRemaining = Math.floor(AUTO_REFRESH_MS / 1000);
  atualizarTextoCountdown();
}

function atualizarTextoCountdown() {
  const el = document.getElementById("refresh-countdown");
  if (!el) return;
  const min = String(Math.floor(refreshSecondsRemaining / 60)).padStart(2, "0");
  const sec = String(refreshSecondsRemaining % 60).padStart(2, "0");
  el.textContent = `${min}:${sec}`;
}

function iniciarTimerRefresh() {
  if (refreshIntervalId) clearInterval(refreshIntervalId);
  resetRefreshCountdown();
  refreshIntervalId = setInterval(() => {
    refreshSecondsRemaining -= 1;
    if (refreshSecondsRemaining < 0) {
      refreshSecondsRemaining = Math.floor(AUTO_REFRESH_MS / 1000);
    }
    atualizarTextoCountdown();
  }, 1000);
}

document.addEventListener("DOMContentLoaded", () => {
  carregarDashboard();
  iniciarTimerRefresh();

  setInterval(() => {
    carregarDashboard();
  }, AUTO_REFRESH_MS);
});
