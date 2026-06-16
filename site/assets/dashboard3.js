(() => {
  "use strict";

  const JSON_URL = "sms_brasil_estoque_atual_por_municipio.json";
  const REFRESH_INTERVAL_MS = 3000;

  let smsBase = null;
  let indiceNome = {};
  let municipios = {};
  let totaisUF = {};
  let processamentoPendente = false;

  function normalizarTexto(texto) {
    return String(texto || "")
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .toUpperCase()
      .trim();
  }

  function formatarNumero(valor) {
    if (valor === null || valor === undefined || Number.isNaN(Number(valor))) return "--";
    return Number(valor).toLocaleString("pt-BR");
  }

  function limparArea(area) {
    return String(area || "")
      .replace(/\s+/g, " ")
      .trim();
  }

  function montarTotaisUF() {
    totaisUF = {};
    Object.values(municipios).forEach((mun) => {
      const uf = mun.uf;
      const total = Number(mun.terminais_aptos_sms || 0);
      totaisUF[uf] = (totaisUF[uf] || 0) + total;
    });
  }

  async function carregarBaseSMS() {
    try {
      const resp = await fetch(JSON_URL, { cache: "no-store" });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      smsBase = await resp.json();
      indiceNome = smsBase.indice_nome || {};
      municipios = smsBase.municipios || {};
      montarTotaisUF();
      processarTabela();
    } catch (erro) {
      console.warn("Dashboard3: não foi possível carregar o JSON de estoque SMS.", erro);
      atualizarKPI(null, "JSON de estoque SMS não carregado");
    }
  }

  function extrairMunicipiosDaArea(areaTexto) {
    const area = limparArea(areaTexto);
    if (!area) return [];

    const partes = area
      .split(/\s*(?:,|;|\||\/\s*\/|\s+E\s+)\s*/i)
      .map((p) => p.trim())
      .filter(Boolean);

    // Mantém também a área completa, pois muitas linhas vêm como "Município/UF".
    const candidatos = [area, ...partes];
    const encontrados = [];
    const vistos = new Set();

    candidatos.forEach((candidatoOriginal) => {
      let candidato = limparArea(candidatoOriginal);
      const match = candidato.match(/^(.+?)\s*\/\s*([A-Z]{2})$/i);

      if (match) {
        const nome = normalizarTexto(match[1]);
        const uf = normalizarTexto(match[2]);

        // Caso seja alerta estadual, tipo "MINAS GERAIS/MG" ou "AMAZONAS/AM".
        const totalUF = totaisUF[uf];
        const nomeUF = Object.values(municipios).find((m) => m.uf === uf)?.nome_uf;
        if (nomeUF && normalizarTexto(nomeUF) === nome && totalUF !== undefined) {
          const chaveUF = `UF-${uf}`;
          if (!vistos.has(chaveUF)) {
            vistos.add(chaveUF);
            encontrados.push({ tipo: "uf", uf, total: totalUF });
          }
          return;
        }

        const chave = `${nome}-${uf}`;
        const cdMun = indiceNome[chave];
        if (cdMun && municipios[cdMun] && !vistos.has(cdMun)) {
          vistos.add(cdMun);
          encontrados.push({ tipo: "municipio", cd_mun: cdMun, municipio: municipios[cdMun] });
        }
      }
    });

    return encontrados;
  }

  function calcularMensagens(areaTexto) {
    const encontrados = extrairMunicipiosDaArea(areaTexto);
    if (!encontrados.length) return { total: null, tipo: "unknown", quantidadeLocais: 0 };

    const total = encontrados.reduce((soma, item) => {
      if (item.tipo === "uf") return soma + Number(item.total || 0);
      return soma + Number(item.municipio.terminais_aptos_sms || 0);
    }, 0);

    return { total, tipo: encontrados.some((e) => e.tipo === "uf") ? "uf" : "municipio", quantidadeLocais: encontrados.length };
  }

  function obterSpansDaLinha(linha) {
    return Array.from(linha.children).filter((el) => el.tagName && el.tagName.toLowerCase() === "span");
  }

  function inserirOuAtualizarCelula(linha, resultado) {
    let celula = linha.querySelector(".messages-cell");
    const spans = obterSpansDaLinha(linha);

    if (!celula) {
      celula = document.createElement("span");
      celula.className = "messages-cell";
      // Inserir antes da coluna "Expira em", que no layout original era o penúltimo span.
      const referencia = spans[6] || null;
      linha.insertBefore(celula, referencia);
    }

    celula.classList.remove("zero", "unknown");

    if (resultado.total === null) {
      celula.classList.add("unknown");
      celula.innerHTML = "--<small>não localizado</small>";
      return;
    }

    if (resultado.total === 0) celula.classList.add("zero");

    const detalhe = resultado.tipo === "uf" ? "SMS Enviados" : "SMS Enviados";
    celula.innerHTML = `${formatarNumero(resultado.total)}<small>${detalhe}</small>`;
  }

  function processarTabela() {
    if (!smsBase) return;
    if (processamentoPendente) return;

    processamentoPendente = true;

    window.setTimeout(() => {
      processamentoPendente = false;

      const corpo = document.getElementById("alerts-table-body");
      if (!corpo) return;

      const linhas = Array.from(corpo.querySelectorAll(".tr"));
      let totalVisivel = 0;
      let linhasComValor = 0;

      linhas.forEach((linha) => {
        const spans = obterSpansDaLinha(linha);
        // No dashboard2: 0 hora, 1 emissor, 2 evento, 3 área afetada, 4 nível, 5 categoria, 6 expira, 7 status.
        const areaTexto = spans[3]?.textContent || "";
        const resultado = calcularMensagens(areaTexto);
        inserirOuAtualizarCelula(linha, resultado);

        if (resultado.total !== null) {
          totalVisivel += resultado.total;
          linhasComValor += 1;
        }
      });

      atualizarKPI(totalVisivel, linhasComValor ? "Soma estimada dos últimos alertas exibidos" : "Aguardando alertas com área identificável");
    }, 120);
  }

  function atualizarKPI(valor, subtitulo) {
    const el = document.getElementById("kpi-alcance-potencial");
    const sub = document.getElementById("kpi-alcance-sub");

    if (el) el.textContent = valor === null ? "--" : formatarNumero(valor);
    if (sub && subtitulo) sub.textContent = subtitulo;
  }

  function observarTabela() {
    const corpo = document.getElementById("alerts-table-body");
    if (!corpo) return;

    const observer = new MutationObserver(() => processarTabela());
    observer.observe(corpo, { childList: true, subtree: true, characterData: true });
  }

  document.addEventListener("DOMContentLoaded", () => {
    carregarBaseSMS();
    observarTabela();
    window.setInterval(processarTabela, REFRESH_INTERVAL_MS);
  });
})();
