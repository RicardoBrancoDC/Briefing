const LEVEL_COLORS = {
  'Baixo': '#2ca02c',
  'Médio': '#ffd92f',
  'Alto': '#ff7f0e',
  'Severo': '#d62728',
  'Extremo': '#6a0dad',
  'Indefinido': '#94a3b8'
};

const STATUS_LABELS = {
  vigente: 'Vigente',
  futuro: 'Futuro',
  expirado: 'Expirado',
  sem_validade: 'Sem validade'
};

const STATUS_COLORS = {
  vigente: '#1e88e5',
  futuro: '#7c3aed',
  expirado: '#94a3b8',
  sem_validade: '#cbd5e1'
};

function fmtDateTime(value) {
  if (!value) return 'não disponível';
  const dt = new Date(value);
  return dt.toLocaleString('pt-BR', { dateStyle: 'short', timeStyle: 'medium' });
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function setHTML(id, value) {
  const el = document.getElementById(id);
  if (el) el.innerHTML = value;
}

function renderCards(cards) {
  setText('card-vigentes', cards?.vigentes ?? 0);
  setText('card-24h', cards?.ultimas24h ?? 0);
  setText('card-autoridades', cards?.autoridadesAtivas ?? 0);
  setText('card-extremos', cards?.alertasExtremos ?? 0);
}

function renderLatestAlerts(items = []) {
  const el = document.getElementById('latest-alerts');
  if (!el) return;
  if (!items.length) {
    el.innerHTML = '<div class="empty-state">Nenhum alerta disponível para o período processado.</div>';
    return;
  }
  el.innerHTML = items.map(item => `
    <div class="alert-item">
      <div>
        <div class="alert-time">${item.time}</div>
        <span class="alert-date">${item.date}</span>
      </div>
      <div class="alert-main">
        <strong>${item.senderName}</strong>
        <div>${item.location || item.uf || 'Local não informado'}</div>
      </div>
      <div class="alert-event">
        <strong>${item.event}</strong>
        <div>${item.headline || 'Sem headline disponível'}</div>
      </div>
      <div>
        <span class="badge ${item.nivel}">${item.nivel}</span>
      </div>
    </div>
  `).join('');
}

function renderTopEmitters(items = []) {
  const el = document.getElementById('top-emitters');
  if (!el) return;
  if (!items.length) {
    el.innerHTML = '<div class="empty-state">Sem dados de autoridades emissoras.</div>';
    return;
  }
  const max = Math.max(...items.map(item => item.count), 1);
  el.innerHTML = items.map(item => {
    const width = Math.max(18, (item.count / max) * 100);
    return `
      <div class="emitter-row">
        <div class="emitter-bar-wrap">
          <div class="emitter-bar" style="width:${width}%">${item.short_name}</div>
        </div>
        <div class="emitter-count">${item.count}</div>
      </div>
    `;
  }).join('');
}

function renderCharts(data) {
  const levelCtx = document.getElementById('chart-levels');
  const eventCtx = document.getElementById('chart-events');
  const statusCtx = document.getElementById('chart-status');

  const levelLabels = (data.level_distribution || []).map(item => item.label);
  const levelValues = (data.level_distribution || []).map(item => item.count);
  const levelColors = levelLabels.map(label => LEVEL_COLORS[label] || '#94a3b8');

  new Chart(levelCtx, {
    type: 'bar',
    data: {
      labels: levelLabels,
      datasets: [{
        data: levelValues,
        backgroundColor: levelColors,
        borderRadius: 8,
        borderSkipped: false
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        y: { beginAtZero: true, ticks: { precision: 0 } },
        x: { grid: { display: false } }
      }
    }
  });

  const eventLabels = (data.event_distribution || []).map(item => item.label);
  const eventValues = (data.event_distribution || []).map(item => item.count);
  new Chart(eventCtx, {
    type: 'doughnut',
    data: {
      labels: eventLabels,
      datasets: [{
        data: eventValues,
        backgroundColor: ['#5b6dee', '#2ea8df', '#4caf50', '#ffb300', '#ef5350', '#7e57c2', '#90a4ae'],
        borderColor: '#ffffff',
        borderWidth: 2
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: 'right' }
      },
      cutout: '54%'
    }
  });

  const statusLabels = (data.status_distribution || []).map(item => STATUS_LABELS[item.label] || item.label);
  const statusValues = (data.status_distribution || []).map(item => item.count);
  const statusColors = (data.status_distribution || []).map(item => STATUS_COLORS[item.label] || '#94a3b8');
  new Chart(statusCtx, {
    type: 'bar',
    data: {
      labels: statusLabels,
      datasets: [{
        label: 'Quantidade',
        data: statusValues,
        backgroundColor: statusColors,
        borderRadius: 8,
        borderSkipped: false
      }]
    },
    options: {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { beginAtZero: true, ticks: { precision: 0 } },
        y: { grid: { display: false } }
      }
    }
  });
}

function renderTable(items = []) {
  const el = document.getElementById('table-body');
  if (!el) return;
  if (!items.length) {
    el.innerHTML = '<tr><td colspan="6" class="empty-state">Nenhum alerta disponível.</td></tr>';
    return;
  }
  el.innerHTML = items.map(item => `
    <tr>
      <td><strong>${item.date}</strong><div class="small-muted">${item.time}</div></td>
      <td>${item.senderName}</td>
      <td>${item.event}</td>
      <td><span class="badge ${item.nivel}">${item.nivel}</span></td>
      <td>${item.uf || '-'}</td>
      <td>${item.location || '-'}</td>
    </tr>
  `).join('');
}

async function renderMap(ufDistribution = []) {
  const container = document.getElementById('uf-map');
  if (!container) return;
  container.innerHTML = '';

  const [geojson, topo] = await Promise.all([
    fetch('./data/br_uf.geojson?ts=' + Date.now(), { cache: 'no-store' }).then(r => r.json()),
    Promise.resolve(ufDistribution)
  ]);

  const counts = new Map(topo.map(item => [item.uf, item.count]));
  const maxValue = Math.max(...topo.map(item => item.count), 1);

  const width = container.clientWidth || 900;
  const height = container.clientHeight || 420;
  const svg = d3.select(container).append('svg')
    .attr('viewBox', `0 0 ${width} ${height}`)
    .attr('preserveAspectRatio', 'xMidYMid meet');

  const projection = d3.geoMercator().fitSize([width - 20, height - 20], geojson);
  const path = d3.geoPath(projection);
  const color = d3.scaleSequential().domain([0, maxValue]).interpolator(d3.interpolateTurbo);

  svg.append('g')
    .attr('transform', 'translate(10,10)')
    .selectAll('path')
    .data(geojson.features)
    .join('path')
    .attr('d', path)
    .attr('fill', d => {
      const uf = d.properties.uf_05;
      const val = counts.get(uf) || 0;
      return val > 0 ? color(val) : '#dbe3ee';
    })
    .attr('stroke', '#ffffff')
    .attr('stroke-width', 1.2)
    .append('title')
    .text(d => `${d.properties.nome_uf} (${d.properties.uf_05}): ${counts.get(d.properties.uf_05) || 0}`);

  svg.append('g')
    .attr('transform', 'translate(10,10)')
    .selectAll('text')
    .data(geojson.features.filter(d => (counts.get(d.properties.uf_05) || 0) > 0))
    .join('text')
    .attr('transform', d => `translate(${path.centroid(d)})`)
    .attr('text-anchor', 'middle')
    .attr('font-size', 16)
    .attr('font-weight', 800)
    .attr('fill', '#ffffff')
    .style('paint-order', 'stroke')
    .style('stroke', 'rgba(31,42,68,0.30)')
    .style('stroke-width', 4)
    .text(d => counts.get(d.properties.uf_05));
}

function renderSummary(data) {
  setText('generated-at', fmtDateTime(data.generated_at));
  setText('run-dir', data.source_run_dir || 'não disponível');
}

async function init() {
  try {
    const response = await fetch('./dashboard_data.json?ts=' + Date.now(), { cache: 'no-store' });
    if (!response.ok) throw new Error('dashboard_data.json não encontrado');
    const data = await response.json();

    renderSummary(data);
    renderCards(data.cards || {});
    renderLatestAlerts(data.latest_alerts || []);
    renderTopEmitters(data.top_emitters || []);
    renderCharts(data);
    renderTable(data.latest_alerts || []);
    await renderMap(data.uf_distribution || []);
  } catch (error) {
    console.error(error);
    setHTML('app-error', '<div class="empty-state">Não foi possível carregar os dados do dashboard.</div>');
  }
}

document.addEventListener('DOMContentLoaded', init);
