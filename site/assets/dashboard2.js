const AUTO_REFRESH_MS = 60000;
const LEVEL_COLORS = {"Extremo":"#8457e6","Severo":"#ef3f34","Alto":"#ff861a","Médio":"#2f8cff","Baixo":"#52b34d","Indefinido":"#aebccc"};
const UF_NOME = {AC:"Acre",AL:"Alagoas",AP:"Amapá",AM:"Amazonas",BA:"Bahia",CE:"Ceará",DF:"Distrito Federal",ES:"Espírito Santo",GO:"Goiás",MA:"Maranhão",MT:"Mato Grosso",MS:"Mato Grosso do Sul",MG:"Minas Gerais",PA:"Pará",PB:"Paraíba",PR:"Paraná",PE:"Pernambuco",PI:"Piauí",RJ:"Rio de Janeiro",RN:"Rio Grande do Norte",RS:"Rio Grande do Sul",RO:"Rondônia",RR:"Roraima",SC:"Santa Catarina",SP:"São Paulo",SE:"Sergipe",TO:"Tocantins"};
const ESTADO_TO_UF = Object.fromEntries(Object.entries(UF_NOME).map(([uf,n])=>[slug(n),uf]));
let countdown=300, timer=null;

// Rotação automática da tabela de últimos alertas
const TABLE_PAGE_SIZE = 6;
const TABLE_ROTATE_MS = 10000;
let tableAlerts = [];
let tablePage = 0;
let tableTimer = null;
let lastTopAlertKey = null;

document.addEventListener('DOMContentLoaded',()=>{loadDashboard(); timer=setInterval(tick,1000); setInterval(loadDashboard,AUTO_REFRESH_MS)});
function tick(){countdown=Math.max(0,countdown-1); const m=String(Math.floor(countdown/60)).padStart(2,'0'),s=String(countdown%60).padStart(2,'0'); setText('refresh-countdown2',`${m}:${s}`); if(countdown===0) countdown=60;}
async function loadDashboard(){
  try{
    let res = await fetch(`dashboard_data2.json?_=${Date.now()}`,{cache:'no-store'});

    if(!res.ok){
      console.warn('dashboard_data2.json não encontrado. Tentando dashboard_data.json...');
      res = await fetch(`dashboard_data.json?_=${Date.now()}`,{cache:'no-store'});
    }

    if(!res.ok) throw new Error('Nenhum arquivo de dados encontrado');

    const data = await res.json();
    countdown = 60;
    renderAll(data);
  }catch(e){
    console.error(e);
  }
}
function renderAll(data){const alerts=(data.all_alerts||data.latest_alerts||[]).map(normalizeAlert); const latest=(data.latest_alerts||alerts).map(normalizeAlert); const total=alerts.length || num(data.summary?.total_alerts)||num(data.cards?.ultimas24h)||0; const vigentes=alerts.filter(a=>isVigente(a)).length || num(data.cards?.vigentes)||0; const levels=aggregate(alerts,'nivel'); const ufAgg=aggregate(alerts,'uf'); const topUfs=Object.entries(ufAgg)
    .filter(([uf])=>uf && uf !== 'Sem informação')
    .map(([uf,count])=>[String(uf).toUpperCase(),count])
    .sort((a,b)=>b[1]-a[1]);
  const statesCount=topUfs.length; const last=alerts.slice().sort((a,b)=>dateValue(b.sent)-dateValue(a.sent))[0] || latest[0] || {};
  setText('top-date', formatNow()); setText('kpi-24h', total); setText('kpi-vigentes', vigentes); setText('kpi-graves', (levels['Severo']||0)+(levels['Extremo']||0)); setText('kpi-municipios', estimateMunicipios(alerts)); setText('kpi-estados', statesCount); setText('kpi-estados-sub',`em ${statesCount} estados`); setText('kpi-ultimo', timeAgoShort(last.sent)); setText('kpi-ultimo-hora', last.time || formatTime(last.sent));
  renderLevelDonut(levels,total); renderCategory(alerts,data); renderAvgDuration(alerts); renderHourly(alerts,last.sent||data.generated_at); renderEmitters(data.top_emitters||[],alerts); renderEvents(data.event_distribution||[],alerts); renderActiveLevels(alerts); renderStates(topUfs,total); renderRegion(alerts); renderSince(alerts); renderTable(latest.length?latest:alerts); renderMap(topUfs,alerts);
}
function normalizeAlert(a){
  const senderName = a.senderName || a.emissor || a.sender || '';
  const uf = (
    a.uf ||
    extractUf(a.location) ||
    extractUf(a.areaDesc) ||
    extractUfFromSender(senderName) ||
    a.uf_hint ||
    ''
  ).toUpperCase();

  const sent = a.sent || a.onset || a.sent_iso || a.datetime || a.data_hora_iso;

  return {
    ...a,
    senderName,
    uf,
    nivel: normalizeNivel(a.nivel || a.nivel_calculado || a.severidade_label || levelFromCap(a)),
    event: a.event || a.evento || 'Sem evento',
    location: a.location || a.areaDesc || '',
    sent,
    expires: a.expires || a.expira || a.expires_iso,
    category: a.category || a.categoria || inferCategory(a.event || a.evento)
  };
}
function renderLevelDonut(levels,total){const order=['Extremo','Severo','Alto','Médio','Baixo']; renderDonut('donut-level',order.map(k=>levels[k]||0),order.map(k=>LEVEL_COLORS[k]),total); setText('donut-level-total',total); const el=byId('level-legend'); el.innerHTML=order.map(k=>legendLine(k,levels[k]||0,total,LEVEL_COLORS[k])).join('')}
function renderCategory(alerts,data){const raw=data.category_distribution || null; let obj={}; if(Array.isArray(raw)) raw.forEach(x=>obj[x.label||x.category||'Sem categoria']=num(x.count)); if(!Object.keys(obj).length) alerts.forEach(a=>{const c=categoryLabel(a.category||inferCategory(a.event)); obj[c]=(obj[c]||0)+1}); const total=Object.values(obj).reduce((a,b)=>a+b,0)||1; const colors=['#2f8cff','#52b34d','#8457e6','#ef3f34','#f6c218','#1a9cc2']; byId('category-list').innerHTML=Object.entries(obj).sort((a,b)=>b[1]-a[1]).slice(0,6).map(([k,v],i)=>barLine(k,v,total,colors[i%colors.length])).join('')}
function renderAvgDuration(alerts){const ds=alerts.map(durationHours).filter(x=>x>0&&isFinite(x)); if(!ds.length){setText('avg-duration','--');return} const avg=ds.reduce((a,b)=>a+b,0)/ds.length; const h=Math.floor(avg), m=Math.round((avg-h)*60); setText('avg-duration',`${h}h ${String(m).padStart(2,'0')}min`)}
function renderHourly(alerts,refIso){const ref=refIso?new Date(refIso):new Date(); const buckets=[]; for(let i=23;i>=0;i--){const d=new Date(ref.getTime()-i*3600000); buckets.push({h:d.getHours(),label:String(d.getHours()).padStart(2,'0')+'h',count:0})} alerts.forEach(a=>{const d=new Date(a.sent||0); if(isNaN(d)) return; const diff=(ref-d)/3600000; if(diff>=0&&diff<24){const idx=23-Math.floor(diff); if(buckets[idx]) buckets[idx].count++}}); const max=Math.max(...buckets.map(b=>b.count),1); byId('hourly-chart2').innerHTML=buckets.map(b=>`<div class="hour-col"><div class="hour-val">${b.count||''}</div><div class="hour-bar" style="height:${Math.max(2,b.count/max*86)}px"></div><div class="hour-label">${b.label}</div></div>`).join('')}
function renderEmitters(top,alerts){let arr=top.map(x=>({name:x.short_name||x.name||x.senderName||'Sem emissor',count:num(x.count||x.valor||x.total)})); if(!arr.length){const obj=aggregate(alerts,'senderName'); arr=Object.entries(obj).map(([name,count])=>({name,count})).sort((a,b)=>b.count-a.count)} const max=Math.max(...arr.map(x=>x.count),1); const colors=['#ef3f34','#ff861a','#f6c218','#52b34d','#2f8cff','#4b8be7','#3aa968','#ef3f34','#8db9df']; byId('emitters-list').innerHTML=arr.slice(0,10).map((x,i)=>`<div class="rank-item"><div class="rank-top"><span class="truncate">${esc(x.name)}</span><strong>${x.count}</strong></div><div class="track"><div class="fill" style="width:${x.count/max*100}%;background:${colors[i%colors.length]}"></div></div></div>`).join('')}
function renderEvents(dist,alerts){let arr=(dist||[]).map(x=>({label:x.label||x.event||'Sem evento',count:num(x.count)})); if(!arr.length){const o=aggregate(alerts,'event'); arr=Object.entries(o).map(([label,count])=>({label,count})).sort((a,b)=>b.count-a.count)} const icons=['🌧️','💨','🌊','⛰️','⛈️','◆']; const colors=['#ef3f34','#ff861a','#f6c218','#52b34d','#2f8cff','#4b8be7']; byId('events-list2').innerHTML=arr.slice(0,5).map((x,i)=>`<div class="event-row"><span class="event-icon">${icons[i]||'◆'}</span><span>${esc(x.label)}</span><strong class="event-count" style="color:${colors[i%colors.length]}">${x.count}</strong></div>`).join('')}
function renderActiveLevels(alerts){const vig=alerts.filter(isVigente), o=aggregate(vig,'nivel'), order=['Extremo','Severo','Alto','Médio','Baixo']; byId('active-level-list').innerHTML=order.map(k=>`<div class="mini-row"><i class="sw" style="background:${LEVEL_COLORS[k]}"></i><span>${k}</span><strong>${o[k]||0}</strong></div>`).join(''); setText('active-total',vig.length)}
function renderStates(topUfs,total){const rows=topUfs.slice(0,7).map(([uf,c])=>`<div class="mini-row"><span></span><span>${esc(UF_NOME[uf]||uf)}</span><strong>${c}</strong></div>`).join(''); const other=topUfs.slice(7).reduce((a,b)=>a+b[1],0); byId('state-list').innerHTML=rows+(other?`<div class="mini-row"><span></span><span>Outros ${topUfs.length-7} estados</span><strong>${other}</strong></div>`:'')}
function renderRegion(alerts){
  const regions=[
    ['Norte',0,'#2f8cff'],
    ['Nordeste',0,'#52b34d'],
    ['Centro-Oeste',0,'#f6c218'],
    ['Sudeste',0,'#ff861a'],
    ['Sul',0,'#8457e6']
  ];

  const regionMap={
    AC:'Norte', AP:'Norte', AM:'Norte', PA:'Norte', RO:'Norte', RR:'Norte', TO:'Norte',
    AL:'Nordeste', BA:'Nordeste', CE:'Nordeste', MA:'Nordeste', PB:'Nordeste', PE:'Nordeste',
    PI:'Nordeste', RN:'Nordeste', SE:'Nordeste',
    DF:'Centro-Oeste', GO:'Centro-Oeste', MT:'Centro-Oeste', MS:'Centro-Oeste',
    ES:'Sudeste', MG:'Sudeste', RJ:'Sudeste', SP:'Sudeste',
    PR:'Sul', RS:'Sul', SC:'Sul'
  };

  const regionCodeMap={
    N:'Norte',
    NE:'Nordeste',
    CO:'Centro-Oeste',
    SE:'Sudeste',
    S:'Sul'
  };

  alerts.forEach(a=>{
    const uf=String(a.uf||a.uf_hint||'').toUpperCase();
    const rawRegion=String(a.region||'').toUpperCase();

    const reg=
      regionCodeMap[rawRegion] ||
      regionMap[uf];

    const item=regions.find(r=>r[0]===reg);
    if(item) item[1]++;
  });

  const total=regions.reduce((a,b)=>a+b[1],0);

  renderDonut(
    'donut-duration',
    regions.map(r=>r[1]),
    regions.map(r=>r[2]),
    total
  );

  setText('donut-duration-total',total);

  byId('duration-legend').innerHTML=regions
    .filter(r=>r[1]>0)
    .map(r=>legendLine(r[0],r[1],total,r[2],true))
    .join('');
}
function renderSince(alerts){
  const bins=[
    ['Até 6h',0],
    ['Até 12h',0],
    ['Até 24h',0],
    ['Mais de 24h',0]
  ];

  const now=new Date();

  alerts.forEach(a=>{
    const d=new Date(a.sent||a.onset||0);
    if(isNaN(d)) return;

    const h=(now-d)/3600000;
    if(h<0) return;

    if(h<=6) bins[0][1]++;
    else if(h<=12) bins[1][1]++;
    else if(h<=24) bins[2][1]++;
    else bins[3][1]++;
  });

  const total=bins.reduce((a,b)=>a+b[1],0)||1;

  byId('since-list').innerHTML=bins.map(b=>`
    <div class="since-row">
      <span class="since-icon">◷</span>
      <span>${b[0]}</span>
      <strong>${b[1]} (${pct(b[1],total)})</strong>
    </div>
  `).join('');
}
function renderTable(alerts){
  tableAlerts = Array.isArray(alerts) ? alerts : [];

  if(!tableAlerts.length){
    const body = byId('alerts-table-body');
    if(body) body.innerHTML = '';
    return;
  }

  const currentTopKey = alertKey(tableAlerts[0]);
  const hasNewAlert = Boolean(lastTopAlertKey && currentTopKey && currentTopKey !== lastTopAlertKey);

  if(currentTopKey) lastTopAlertKey = currentTopKey;

  tablePage = 0;
  renderTablePage(hasNewAlert);

  if(!tableTimer){
    tableTimer = setInterval(rotateTablePage, TABLE_ROTATE_MS);
  }
}

function rotateTablePage(){
  if(!tableAlerts || tableAlerts.length <= TABLE_PAGE_SIZE) return;

  const body = byId('alerts-table-body');
  if(!body) return;

  body.classList.add('table-fade-out');

  setTimeout(()=>{
    const totalPages = Math.ceil(tableAlerts.length / TABLE_PAGE_SIZE);
    tablePage = (tablePage + 1) % totalPages;

    renderTablePage(false);
    body.classList.remove('table-fade-out');
  }, 500);
}

function renderTablePage(highlightFirst=false){
  const body = byId('alerts-table-body');
  if(!body) return;

  const start = tablePage * TABLE_PAGE_SIZE;
  let page = tableAlerts.slice(start, start + TABLE_PAGE_SIZE);

  if(!page.length){
    tablePage = 0;
    page = tableAlerts.slice(0, TABLE_PAGE_SIZE);
  }

  const rows = page.map((a,i)=>{
    const flag=flagMarkup(a.senderName,a.uf,a.location||a.areaDesc);
    const status=isVigente(a)?'Vigente':'Expirado';
    const extraClass = highlightFirst && i===0 ? ' alert-new' : '';

    return `<div class="tr table-body-row${extraClass}">
      <span>
        <div class="time-main">${esc(a.time||formatTime(a.sent))}</div>
        <div class="time-sub">${esc(a.date||formatDate(a.sent))}</div>
      </span>
      <span class="emitter-cell">
        ${flag}
        <span class="truncate">
          <div class="emitter-name">${esc(a.senderNameShort||a.senderName)}</div>
          <div class="emitter-code">${siglaEmissor(a.senderName,a.uf)}</div>
        </span>
      </span>
      <span>${esc(title(a.event))}</span>
      <span class="truncate">${esc(a.location||a.areaDesc||a.uf||'')}</span>
      <span><b class="nivel-badge nivel-${a.nivel}">${esc(a.nivel)}</b></span>
      <span class="cat-cell"><span>🌧️</span>${esc(categoryLabel(a.category))}</span>
      <span><strong>${formatTime(a.expires)}</strong><br><small>${formatDate(a.expires)}</small></span>
      <span><i class="status-dot"></i>${status}</span>
    </div>`;
  }).join('');

  body.innerHTML = rows;
}

function alertKey(a){
  return String(
    a?.identifier ||
    a?.entry_id ||
    a?.id ||
    `${a?.senderName||''}|${a?.event||''}|${a?.sent||''}|${a?.location||''}`
  );
}

async function renderMap(topUfs,alerts){
  const holder=byId('map-holder2');

  try{
    const r=await fetch('data/br_uf.geojson');
    const geo=await r.json();

    const ufCount=Object.fromEntries(topUfs);
    holder.innerHTML=mapSvg(geo,ufCount);
    renderMapQuantityLegend();

  }catch(e){
    holder.innerHTML='<div class="empty">Mapa indisponível</div>';
  }
}
function mapSvg(geo,ufCount){
  const feats=geo.features||[];
  let pts=[];

  feats.forEach(f=>collectPts(f.geometry,pts));

  const xs=pts.map(p=>p[0]), ys=pts.map(p=>p[1]);
  const minX=Math.min(...xs), maxX=Math.max(...xs), minY=Math.min(...ys), maxY=Math.max(...ys);

  const W=620,H=460,pad=30;
const scale=Math.min((W-pad*2)/(maxX-minX),(H-pad*2)/(maxY-minY)) * 1.18;
  const project=([lon,lat])=>[(lon-minX)*scale+pad,(maxY-lat)*scale+pad];

  const paths=feats.map(f=>{
    const uf=featureUf(f);
    const count=Number(ufCount[uf]||0);
    const color=colorByAlertCount(count);
    const d=geoPath(f.geometry,project);
    const cen=centroid(f.geometry);
    const [cx,cy]=project(cen);

    return `<path class="uf-shape" d="${d}" fill="${color}" opacity="${count?'.95':'.78'}"></path>${count?`<text class="uf-label" x="${cx}" y="${cy}">${uf}<tspan x="${cx}" dy="16">${count}</tspan></text>`:''}`;
  }).join('');

  return `<svg viewBox="0 0 ${W} ${H}" role="img">${paths}</svg>`;
}

function colorByAlertCount(count){
  if(!count || count<=0) return '#aebccc';      // 0
  if(count<=10) return '#2f8cff';              // 1 a 10
  if(count<=20) return '#52b34d';              // 11 a 20
  if(count<=30) return '#ff861a';              // 21 a 30
  if(count<=40) return '#ef3f34';              // 31 a 40
  return '#8457e6';                            // mais de 40
}

function renderMapQuantityLegend(){
  const el=document.querySelector('.map-legend');
  if(!el) return;

  const items=[
    ['0 alertas','#aebccc'],
    ['1 a 10','#2f8cff'],
    ['11 a 20','#52b34d'],
    ['21 a 30','#ff861a'],
    ['31 a 40','#ef3f34'],
    ['Mais de 40','#8457e6']
  ];

  el.innerHTML=[
    '<strong>QUANTIDADE DE ALERTAS</strong>',
    ...items.map(([label,color])=>`<span><i style="background:${color}"></i>${label}</span>`)
  ].join('');
}

function flagMarkup(sender,uf,location){
  const paths = flagPaths(sender,uf,location);
  const encoded = paths.map(p => encodeURIComponent(p)).join('|');
  const first = paths[0] || 'assets/flags/default.svg';

  return `<img class="flag-img"
    src="${first}"
    data-paths="${encoded}"
    data-index="0"
    data-uf="${esc((uf||'BR').toUpperCase())}"
    onerror="cycleFlag(this)"
    alt="Bandeira do emissor">`;
}

function cycleFlag(img){
  const paths = (img.dataset.paths || '')
    .split('|')
    .map(decodeURIComponent)
    .filter(Boolean);

  let index = Number(img.dataset.index || 0) + 1;

  if(index < paths.length){
    img.dataset.index = String(index);
    img.src = paths[index];
    return;
  }

  const uf = img.dataset.uf || 'BR';
  img.outerHTML = `<span class="flag-badge">${uf}</span>`;
}

function flagPaths(sender,uf,location){
  const fallbackDefault = 'assets/flags/default.svg';

  const cityFromSender = extractCityFromSender(sender);
  const cityFromLocation = extractCityFromLocation(location);
  const chosen = cityFromSender || cityFromLocation;

  if(chosen && chosen.city && chosen.uf){
    const citySlug = slug(chosen.city);
    const cityUf = chosen.uf.toLowerCase();

    return [
      `assets/flags/municipios/${cityUf}/${citySlug}.jpg`,
      `assets/flags/municipios/${cityUf}/${citySlug}.jpeg`,
      `assets/flags/municipios/${cityUf}/${citySlug}.png`,
      `assets/flags/estados/${cityUf}.svg`,
      `assets/flags/estados/${cityUf}.png`,
      fallbackDefault
    ];
  }

  const euf = (uf || extractUfFromSender(sender) || '').toLowerCase();

  if(euf){
    return [
      `assets/flags/estados/${euf}.svg`,
      `assets/flags/estados/${euf}.png`,
      fallbackDefault
    ];
  }

  return [fallbackDefault];
}

function extractCityFromSender(sender){
  const s = String(sender || '').trim();
  if(!s) return null;

  const patterns = [
    /Defesa\s+Civil\s+Municipal\s+de\s+(.+?)\s*\(([A-Z]{2})\)/i,
    /Defesa\s+Civil\s+do\s+Munic[ií]pio\s+de\s+(.+?)\s*\(([A-Z]{2})\)/i,
    /Defesa\s+Civil\s+de\s+(.+?)\s*\(([A-Z]{2})\)/i,
    /Defesa\s+Civil\s+(.+?)\s*\(([A-Z]{2})\)/i,
    /COMPDEC\s+de\s+(.+?)\s*\(([A-Z]{2})\)/i
  ];

  for(const p of patterns){
    const m = s.match(p);
    if(m){
      const city = cleanCityName(m[1]);
      const upper = city.toUpperCase();
      if(upper.startsWith('ESTADUAL')) return null;
      return {city, uf:m[2].toUpperCase()};
    }
  }

  return null;
}

function extractCityFromLocation(location){
  const s = String(location || '').trim();
  if(!s) return null;

  const first = s.split(',')[0].trim();

  const patterns = [
    /^(.+?)\s*\/\s*([A-Z]{2})$/i,
    /^(.+?)\s*-\s*([A-Z]{2})$/i,
    /^(.+?)\s*\(([A-Z]{2})\)$/i
  ];

  for(const p of patterns){
    const m = first.match(p);
    if(m){
      const city = cleanCityName(m[1]);
      const uf = m[2].toUpperCase();

      const stateName = UF_NOME[uf] || '';
      if(slug(city) === slug(stateName)) return null;

      return {city, uf};
    }
  }

  return null;
}

function cleanCityName(city){
  return String(city || '')
    .replace(/\s+/g,' ')
    .replace(/^de\s+/i,'')
    .replace(/\s+Municipal$/i,'')
    .trim();
}

function renderDonut(id,values,colors,total){let acc=0; const sum=values.reduce((a,b)=>a+b,0)||1; const parts=values.map((v,i)=>{const a=acc/sum*100; acc+=v; const b=acc/sum*100; return `${colors[i]} ${a}% ${b}%`}).join(','); const el=byId(id); if(el) el.style.background=`conic-gradient(${parts})`}
function legendLine(label,count,total,color,twoLine=false){return `<div class="legend-item"><i class="sw" style="background:${color}"></i><span>${esc(label)}</span>${twoLine?`<span>${count} (${pct(count,total)})</span>`:`<strong>${count} (${pct(count,total)})</strong>`}</div>`}
function barLine(label,count,total,color){return `<div class="bar-item"><div class="bar-top"><span>${esc(label)}</span><strong>${count} (${pct(count,total)})</strong></div><div class="track"><div class="fill" style="width:${count/total*100}%;background:${color}"></div></div></div>`}
function aggregate(arr,key){const o={}; arr.forEach(a=>{const k=(typeof key==='function'?key(a):a[key])||'Sem informação'; o[k]=(o[k]||0)+1}); return o}
function normalizeNivel(n){n=String(n||'').toLowerCase(); if(n.includes('extremo'))return'Extremo'; if(n.includes('severo'))return'Severo'; if(n.includes('alto'))return'Alto'; if(n.includes('médio')||n.includes('medio'))return'Médio'; if(n.includes('baixo'))return'Baixo'; return 'Indefinido'}
function levelFromCap(a){const s=String(a.severity||'').toLowerCase(),u=String(a.urgency||'').toLowerCase(); if(s==='extreme') return u==='immediate'?'Extremo':'Severo'; if(s==='severe') return 'Alto'; if(s==='moderate') return 'Médio'; if(s==='minor') return 'Baixo'; return 'Indefinido'}
function moreSevere(a,b){const rank={Baixo:1,Médio:2,Alto:3,Severo:4,Extremo:5,Indefinido:0,Sem:0}; return (rank[b]||0)>(rank[a]||0)?b:a}
function inferCategory(event){const e=slug(event||''); if(/chuva|vendaval|tempest|granizo|frente|onda|estiagem|seca|umidade/.test(e))return'Met'; if(/desliz|lama|solo|inund|alag|enxurr|eros|rocha/.test(e))return'Geo'; if(/incendio/.test(e))return'Fire'; if(/doenc|saude/.test(e))return'Health'; return 'Safety'}
function categoryLabel(c){c=String(c||'').trim(); const m={Met:'Met',Geo:'Geo',Fire:'Fire',Health:'Health',Safety:'Safety',Transport:'Transport'}; return m[c]||c||'Sem categoria'}
function durationHours(a){const s=new Date(a.sent||0), e=new Date(a.expires||0); if(isNaN(s)||isNaN(e)) return 0; return (e-s)/3600000}
function isVigente(a){
  const st = String(a.status || a.status_vigencia || '').toLowerCase();
  if(st.includes('vigente')) return true;
  if(st.includes('expirado')) return false;

  const e = new Date(a.expires || 0);
  return !isNaN(e) && e > new Date();
}
function timeAgoShort(iso){const d=new Date(iso||0); if(isNaN(d)) return '--'; const min=Math.max(0,Math.floor((new Date()-d)/60000)); if(min<60) return `${min} min`; return `${Math.floor(min/60)}h`}
function estimateMunicipios(alerts){const vals=new Set(); alerts.forEach(a=>{const loc=(a.location||a.areaDesc||'').split(';')[0].trim(); if(loc) vals.add(loc)}); return vals.size||alerts.length}
function featureUf(f){
  const p = f.properties || {};

  const direct =
    p.sigla ||
    p.SIGLA ||
    p.sigla_uf ||
    p.SIGLA_UF ||
    p.uf ||
    p.UF ||
    p.uf_sigla ||
    p.UF_SIGLA ||
    '';

  if(direct && String(direct).length === 2){
    return String(direct).toUpperCase();
  }

  const nome =
    p.nome ||
    p.NOME ||
    p.name ||
    p.NAME ||
    p.estado ||
    p.ESTADO ||
    p.nm_uf ||
    p.NM_UF ||
    p.NM_ESTADO ||
    '';

  if(nome){
    const key = slug(nome);
    for(const [stateSlug, uf] of Object.entries(ESTADO_TO_UF)){
      if(key === stateSlug || key.includes(stateSlug) || stateSlug.includes(key)){
        return uf;
      }
    }
  }

  // último recurso: procura qualquer valor textual com UF ou nome de estado
  for(const value of Object.values(p)){
    const txt = String(value || '');
    if(/^[A-Z]{2}$/.test(txt) && UF_NOME[txt]){
      return txt;
    }

    const key = slug(txt);
    for(const [stateSlug, uf] of Object.entries(ESTADO_TO_UF)){
      if(key === stateSlug){
        return uf;
      }
    }
  }

  return '';
}
function collectPts(g,out){if(!g)return; if(g.type==='Polygon')g.coordinates.flat(1).forEach(p=>out.push(p)); else if(g.type==='MultiPolygon')g.coordinates.flat(2).forEach(p=>out.push(p))}
function geoPath(g,project){if(g.type==='Polygon')return poly(g.coordinates,project); if(g.type==='MultiPolygon')return g.coordinates.map(p=>poly(p,project)).join(''); return ''}
function poly(coords,project){return coords.map(r=>r.map((p,i)=>{const [x,y]=project(p);return`${i?'L':'M'}${x.toFixed(1)},${y.toFixed(1)}`}).join('')+'Z').join('')}
function centroid(g){const pts=[]; collectPts(g,pts); return [pts.reduce((a,p)=>a+p[0],0)/pts.length,pts.reduce((a,p)=>a+p[1],0)/pts.length]}
function extractUf(t){const m=String(t||'').match(/\b([A-Z]{2})\b\s*$/)||String(t||'').match(/\/([A-Z]{2})\b/)||String(t||'').match(/-([A-Z]{2})\b/); return m?m[1]:''}
function extractUfFromSender(s){const txt=slug(s||''); for(const [name,uf] of Object.entries(ESTADO_TO_UF)){if(txt.includes(name))return uf} const m=String(s||'').match(/\(([A-Z]{2})\)/); return m?m[1]:''}
function siglaEmissor(sender,uf){if(!sender)return uf||''; const m=sender.match(/Defesa Civil Estadual.*?([A-Z][a-záéíóúãõç]+)/); return (uf?`DC${uf}`:'DC')}
function title(s){return String(s||'').toLowerCase().replace(/(^|\s)\S/g,t=>t.toUpperCase())}
function slug(s){return String(s||'').normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/^-+|-+$/g,'')}
function esc(s){return String(s??'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]))}
function pct(v,t){return t?`${(v/t*100).toFixed(1).replace('.',',')}%`:'0,0%'}
function num(v){return Number(v)||0} function byId(id){return document.getElementById(id)} function setText(id,v){const e=byId(id); if(e)e.textContent=v}
function dateValue(iso){const d=new Date(iso||0); return isNaN(d)?0:d.getTime()} function formatTime(iso){const d=new Date(iso||0);return isNaN(d)?'--:--':d.toLocaleTimeString('pt-BR',{timeZone:'America/Sao_Paulo',hour:'2-digit',minute:'2-digit'})} function formatDate(iso){const d=new Date(iso||0);return isNaN(d)?'--/--':d.toLocaleDateString('pt-BR',{timeZone:'America/Sao_Paulo',day:'2-digit',month:'2-digit'})} function formatNow(){return new Date().toLocaleString('pt-BR',{timeZone:'America/Sao_Paulo',day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit',second:'2-digit'}).replace(',','')}
