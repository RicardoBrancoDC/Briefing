const AUTO_REFRESH_MS = 300000; // 5 minutos
let dashboardCarregando = false;
let dashboardUltimaLeitura = null;

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

    preencherCabecalho(data);
    preencherCards(data);
    renderUltimosAlertas(data.ultimos_alertas || data.latest_alerts || []);
    renderTopAutoridades(data.top5_autoridades || data.top_emitters || []);
    renderResumoOperacional(data);
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
    console.error(error);
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

  setText("meta-atualizado", `${atualizadoOrigem} | leitura: ${atualizadoLeitura}`);
  setText("meta-base", data.base || "últimas 24h");
  setText("meta-fonte", data.fonte || "CAP processado pelo workflow atual");
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

  setText("card-extremos", numero(
    data.cards?.extremos ??
    data.cards?.alertasExtremos ??
    data.resumo?.extremos ??
    data.summary?.by_nivel?.Extremo ??
    0
  ));
}

function renderUltimosAlertas(alertas) {
  const container = document.getElementById("ultimos-alertas");
  if (!container) return;

  container.innerHTML = "";

  if (!alertas.length) {
    container.innerHTML = `<div class="empty-state">Nenhum alerta recente disponível.</div>`;
    return;
  }

  alertas.slice(0, 5).forEach((alerta) => {
    const item = document.createElement("div");
    item.className = "alert-item";

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
      150
    );
    const nivel = normalizarNivel(
      alerta.nivel ||
      alerta.nivel_calculado ||
      alerta.severidade_label ||
      alerta.severity_label ||
      "Indefinido"
    );

    item.innerHTML = `
      <div class="alert-time">
        <div class="alert-time-hour">${esc(hora)}</div>
        <div class="alert-time-date">${esc(dataAlerta)}</div>
      </div>

      <div class="alert-emissor">
        <div class="alert-emissor-name" title="${escAttr(emissor)}">${esc(emissor)}</div>
        <div class="alert-emissor-loc">${esc(local)}</div>
      </div>

      <div class="alert-desc">
        <div class="alert-desc-evento">${esc(evento)}</div>
        <div class="alert-desc-texto">${esc(descricao)}</div>
      </div>

      <div class="alert-tag-wrap">
        <span class="alert-tag ${classeNivel(nivel)}">${esc(nivel)}</span>
      </div>
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

  const cores = ["top5-blue", "top5-green", "top5-orange", "top5-red", "top5-purple"];
  const maxValor = Math.max(...items.map((item) => Number(item.valor ?? item.total ?? item.count ?? 0)), 1);

  items.slice(0, 10).forEach((item, index) => {
    const nome =
      item.nome ||
      item.name ||
      item.autoridade ||
      item.emissor ||
      "Sem nome";

    const valor = Number(item.valor ?? item.total ?? item.count ?? 0);
    const largura = Math.max((valor / maxValor) * 100, valor > 0 ? 8 : 0);
    const cor = cores[index % cores.length];

    const div = document.createElement("div");
    div.className = "top5-item";

    div.innerHTML = `
      <div class="top5-item-header">
        <div class="top5-item-name" title="${escAttr(nome)}">${esc(nome)}</div>
        <div class="top5-item-value">${numero(valor)}</div>
      </div>
      <div class="top5-bar-track">
        <div class="top5-bar-fill ${cor}" style="width: ${largura}%;"></div>
      </div>
    `;

    container.appendChild(div);
  });
}

function renderResumoOperacional(data) {
  const container = document.getElementById("resumo-operacional");
  if (!container) return;

  const vigentes = data.cards?.vigentes ?? 0;
  const ultimas24h = data.cards?.ultimas_24h ?? data.cards?.ultimas24h ?? data.summary?.total_alerts ?? 0;
  const autoridades = data.cards?.autoridades_ativas ?? data.cards?.autoridadesAtivas ?? 0;
  const extremos = data.cards?.extremos ?? data.cards?.alertasExtremos ?? data.summary?.by_nivel?.Extremo ?? 0;

  const statusMap = normalizarColecaoParaMapa(
    data.vigencia ||
    data.status_vigencia ||
    data.status_distribution ||
    {}
  );

  const expirados = statusMap.expirado ?? statusMap.Expirado ?? 0;
  const futuros = statusMap.futuro ?? statusMap.Futuro ?? 0;

  const ufs = contarUFs(data.alertas_por_uf || data.ufs || data.uf_distribution || []);

  container.innerHTML = `
    <div class="resumo-box">
      <div class="resumo-box-label">Alertas vigentes</div>
      <div class="resumo-box-value">${numero(vigentes)}</div>
    </div>

    <div class="resumo-box">
      <div class="resumo-box-label">Alertas nas últimas 24h</div>
      <div class="resumo-box-value">${numero(ultimas24h)}</div>
    </div>

    <div class="resumo-box">
      <div class="resumo-box-label">Alertas expirados</div>
      <div class="resumo-box-value">${numero(expirados)}</div>
    </div>

    <div class="resumo-box">
      <div class="resumo-box-label">Alertas futuros</div>
      <div class="resumo-box-value">${numero(futuros)}</div>
    </div>

    <div class="resumo-box">
      <div class="resumo-box-label">UFs com alertas</div>
      <div class="resumo-box-value">${numero(ufs)}</div>
    </div>

    <div class="resumo-box">
      <div class="resumo-box-label">Autoridades ativas</div>
      <div class="resumo-box-value">${numero(autoridades)}</div>
    </div>

    <div class="resumo-box">
      <div class="resumo-box-label">Alertas extremos</div>
      <div class="resumo-box-value">${numero(extremos)}</div>
    </div>
  `;
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

  const mapaImg = data.mapa_uf_png || data.mapa_png || data.imagem_mapa || null;

  if (mapaImg) {
    const mapaComCacheBuster = `${mapaImg}${mapaImg.includes("?") ? "&" : "?"}_ts=${Date.now()}`;
    container.innerHTML = `
      <img
        src="${escAttr(mapaComCacheBuster)}"
        alt="Mapa de alertas por UF"
        style="max-width: 100%; width: 100%; height: auto; display: block; border-radius: 18px;"
      />
    `;
    return;
  }

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

    console.log("UF distribution do dashboard:", listaUF);
    console.log("Primeira feature do geojson:", geojson.features?.[0]);
    console.log("Properties da primeira feature:", geojson.features?.[0]?.properties);

    const svg = montarSvgMapaBrasil(geojson, ufMap);
    container.innerHTML = svg;
  } catch (error) {
    console.error("Erro ao renderizar mapa por UF:", error);

    const resumo = listaUF
      .slice(0, 10)
      .map((item) => {
        const uf = item.uf || item.nome || item.label || "--";
        const valor = item.valor ?? item.total ?? item.count ?? 0;
        return `<strong>${esc(uf)}</strong>: ${numero(valor)}`;
      })
      .join(" &nbsp;&nbsp; ");

    container.innerHTML = `<div style="padding:24px; text-align:center;">${resumo}</div>`;
  }
}

function montarSvgMapaBrasil(geojson, ufMap) {
  const width = 980;
  const height = 520;
  const padding = 20;

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
  const scale = Math.min(
    (width - padding * 2) / dataWidth,
    (height - padding * 2) / dataHeight
  );

  const offsetX = (width - dataWidth * scale) / 2;
  const offsetY = (height - dataHeight * scale) / 2;

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
    const uf = extrairSiglaUF(feature).toUpperCase();
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
        stroke-width="1.2"
      >
        <title>${esc(uf || "UF")} : ${numero(valor)} alerta(s)</title>
      </path>
    `);

    if (cx !== null && cy !== null && valor > 0) {
      labels.push(`
        <text
          x="${cx}"
          y="${cy}"
          text-anchor="middle"
          dominant-baseline="middle"
          font-size="16"
          font-weight="800"
          fill="#1f2a44"
        >
          ${esc(uf)}
        </text>
        <text
          x="${cx}"
          y="${cy + 18}"
          text-anchor="middle"
          dominant-baseline="middle"
          font-size="14"
          font-weight="700"
          fill="#1f2a44"
        >
          ${esc(String(valor))}
        </text>
      `);
    }
  }

  return `
    <div style="width:100%; overflow:hidden;">
      <svg viewBox="0 0 ${width} ${height}" style="width:100%; height:auto; display:block;">
        <rect x="0" y="0" width="${width}" height="${height}" fill="transparent"></rect>
        ${paths.join("\n")}
        ${labels.join("\n")}
      </svg>
    </div>
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
  renderChartVigencia(data.vigencia || data.status_vigencia || data.status_distribution || {});
}

function renderChartSeveridade(severidadeData) {
  const canvas = document.getElementById("chart-severidade");
  if (!canvas) return;

  const ctx = canvas.getContext("2d");
  if (!ctx) return;

  const ordem = ["Baixo", "Médio", "Alto", "Severo", "Extremo"];
  const valoresMap = normalizarColecaoParaMapa(severidadeData);

  const labels = ordem.filter((label) => (valoresMap[label] ?? 0) > 0);
  const valores = labels.map((label) => valoresMap[label] ?? 0);

  destruirGraficoAnterior(canvas);

  new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [{
        data: valores,
        backgroundColor: ["#4caf50", "#d2be45", "#f08c24", "#db3d34", "#6a43d9"],
        borderRadius: 8
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      plugins: {
        legend: { display: false }
      },
      scales: {
        x: {
          ticks: { color: "#33415f", font: { weight: "700" } },
          grid: { display: false }
        },
        y: {
          beginAtZero: true,
          ticks: { color: "#667085" },
          grid: { color: "rgba(102, 112, 133, 0.16)" }
        }
      }
    }
  });
}

function renderChartEventos(eventosData) {
  const canvas = document.getElementById("chart-eventos");
  if (!canvas) return;

  const ctx = canvas.getContext("2d");
  if (!ctx) return;

  const mapa = normalizarColecaoParaMapa(eventosData);
  const entries = Object.entries(mapa)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 6);

  const labels = entries.map(([k]) => k);
  const valores = entries.map(([, v]) => v);

  destruirGraficoAnterior(canvas);

  new Chart(ctx, {
    type: "doughnut",
    data: {
      labels,
      datasets: [{
        data: valores,
        backgroundColor: ["#f08c24", "#db3d34", "#6a43d9", "#2382ea", "#4caf50", "#c8ccd6"],
        borderColor: "#f3f4f6",
        borderWidth: 3
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      cutout: "58%",
      plugins: {
        legend: {
          position: "bottom",
          labels: {
            color: "#33415f",
            padding: 16,
            boxWidth: 14,
            font: { size: 12 }
          }
        }
      }
    }
  });
}

function renderChartVigencia(vigenciaData) {
  const canvas = document.getElementById("chart-vigencia");
  if (!canvas) return;

  const ctx = canvas.getContext("2d");
  if (!ctx) return;

  const mapa = normalizarColecaoParaMapa(vigenciaData);
  const vigentes = Number(mapa.vigente ?? mapa.Vigente ?? mapa.vigentes ?? 0);
  const expirados = Number(mapa.expirado ?? mapa.Expirado ?? mapa.expirados ?? 0);
  const futuros = Number(mapa.futuro ?? mapa.Futuro ?? mapa.futuros ?? 0);

  destruirGraficoAnterior(canvas);

  new Chart(ctx, {
    type: "doughnut",
    data: {
      labels: ["Vigentes", "Expirados", "Futuros"],
      datasets: [{
        data: [vigentes, expirados, futuros],
        backgroundColor: ["#2382ea", "#c8ccd6", "#6a43d9"],
        borderColor: "#f3f4f6",
        borderWidth: 3
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      cutout: "58%",
      plugins: {
        legend: {
          position: "bottom",
          labels: {
            color: "#33415f",
            padding: 16,
            boxWidth: 14,
            font: { size: 12 }
          }
        }
      }
    }
  });
}

function renderErroGeral(error) {
  console.error("Erro geral do dashboard:", error);

  const ids = [
    "ultimos-alertas",
    "top5-autoridades",
    "resumo-operacional",
    "mapa-uf"
  ];

  ids.forEach((id) => {
    const el = document.getElementById(id);
    if (el) {
      el.innerHTML = `<div class="empty-state">Não foi possível carregar os dados do painel.</div>`;
    }
  });

  const tbody = document.getElementById("tabela-alertas-body");
  if (tbody) {
    tbody.innerHTML = `
      <tr>
        <td colspan="6" class="empty-state">Não foi possível carregar os dados da tabela.</td>
      </tr>
    `;
  }
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value ?? "--";
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

function destruirGraficoAnterior(canvasEl) {
  if (!canvasEl) return;
  const chart = Chart.getChart(canvasEl);
  if (chart) chart.destroy();
}

document.addEventListener("DOMContentLoaded", () => {
  carregarDashboard();

  setInterval(() => {
    carregarDashboard();
  }, AUTO_REFRESH_MS);
});
