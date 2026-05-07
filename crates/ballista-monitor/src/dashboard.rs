/// Single-file HTML dashboard for Ballista monitoring.
/// Served as `const &str` and embedded in the binary.
pub const DASHBOARD_HTML: &str = r##"<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Ballista Monitor</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; scrollbar-gutter: stable; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; overflow-y: scroll; }

.header { background: #1e293b; padding: 12px 24px; display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid #334155; }
.header h1 { font-size: 18px; color: #38bdf8; }
.header-actions { display: flex; gap: 12px; align-items: center; }
.btn { padding: 6px 14px; border-radius: 6px; border: 1px solid #475569; background: #1e293b; color: #e2e8f0; cursor: pointer; font-size: 13px; }
.btn:hover { background: #334155; }
.btn-primary { background: #2563eb; border-color: #2563eb; }
.btn-primary:hover { background: #1d4ed8; }

.tabs { background: #1e293b; padding: 0 24px; display: flex; gap: 0; border-bottom: 2px solid #334155; overflow-x: auto; overflow-y: hidden; }
.tab { padding: 10px 20px; cursor: pointer; font-size: 13px; border-bottom: 2px solid transparent; margin-bottom: -2px; white-space: nowrap; color: #94a3b8; }
.tab:hover { color: #e2e8f0; }
.tab.active { color: #38bdf8; border-bottom-color: #38bdf8; }

.content { padding: 20px 24px; }

.overview-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 16px; }
.node-card { background: #1e293b; border-radius: 10px; padding: 16px; border: 1px solid #334155; cursor: pointer; }
.node-card:hover { border-color: #475569; }
.node-card.offline { border-color: #ef4444; opacity: 0.7; }
.node-card-title { font-size: 14px; font-weight: 600; margin-bottom: 12px; display: flex; justify-content: space-between; align-items: center; }
.node-card-role { font-size: 11px; padding: 2px 8px; border-radius: 10px; background: #334155; }
.node-card-role.scheduler { background: #1e3a5f; color: #60a5fa; }
.node-card-role.executor { background: #1a332e; color: #34d399; }
.node-card-stats { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
.stat { font-size: 12px; }
.stat-label { color: #64748b; }
.stat-value { font-size: 16px; font-weight: 600; }
.node-card-error { color: #ef4444; font-size: 11px; margin-top: 8px; }

.detail-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 24px; }
.metric-card { background: #1e293b; border-radius: 8px; padding: 14px; border: 1px solid #334155; }
.metric-card .label { font-size: 12px; color: #64748b; margin-bottom: 4px; }
.metric-card .value { font-size: 22px; font-weight: 700; }

.charts-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 24px; }
.chart-box { background: #1e293b; border-radius: 8px; padding: 14px; border: 1px solid #334155; }
.chart-box h3 { font-size: 13px; margin-bottom: 8px; color: #94a3b8; }
.chart-container { position: relative; height: 200px; }

.section { background: #1e293b; border-radius: 8px; padding: 14px; border: 1px solid #334155; margin-bottom: 16px; }
.section h3 { font-size: 14px; margin-bottom: 12px; color: #94a3b8; }

table { width: 100%; border-collapse: collapse; font-size: 12px; }
th { text-align: left; color: #64748b; padding: 6px 8px; border-bottom: 1px solid #334155; }
td { padding: 6px 8px; border-bottom: 1px solid #1e293b; }
tr.clickable { cursor: pointer; }
tr.clickable:hover { background: #334155; }

.info-tip { display: inline-block; width: 16px; height: 16px; line-height: 16px; text-align: center; border-radius: 50%; background: #334155; color: #94a3b8; font-size: 11px; cursor: help; vertical-align: middle; margin-left: 2px; }

.proc-detail-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
.proc-detail-item { }
.proc-detail-label { font-size: 11px; color: #64748b; }
.proc-detail-value { font-size: 14px; font-weight: 600; }
.proc-stage-bar { display: flex; height: 24px; border-radius: 4px; overflow: hidden; margin: 8px 0; background: #1e293b; }
.proc-stage-seg { display: flex; align-items: center; justify-content: center; font-size: 10px; color: #fff; min-width: 30px; }

.proc-group { margin-bottom: 16px; border: 1px solid #334155; border-radius: 8px; overflow: hidden; }
.proc-group-header { padding: 10px 14px; font-size: 13px; font-weight: 600; background: #1e293b; cursor: pointer; user-select: none; border-bottom: 1px solid #334155; }
.proc-group-header:hover { background: #334155; }
.proc-group table { margin: 0; }
.proc-group.collapsed table { display: none; }

.modal-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.6); z-index: 100; align-items: center; justify-content: center; }
.modal-overlay.show { display: flex; }
.modal { background: #1e293b; border-radius: 12px; padding: 24px; width: min(90vw, 520px); max-height: 80vh; overflow-y: auto; overflow-x: hidden; border: 1px solid #334155; }
.modal h2 { font-size: 16px; margin-bottom: 16px; }
.node-entry { display: flex; gap: 6px; margin-bottom: 6px; align-items: center; }
.node-entry input { padding: 5px 8px; border-radius: 5px; border: 1px solid #475569; background: #0f172a; color: #e2e8f0; font-size: 13px; min-width: 0; }
.node-entry .cfg-name { width: 100px; flex-shrink: 0; }
.node-entry .cfg-url { flex: 1; min-width: 0; }
.node-entry select { padding: 5px 4px; border-radius: 5px; border: 1px solid #475569; background: #0f172a; color: #e2e8f0; font-size: 13px; flex-shrink: 0; }
.node-entry .btn-remove { color: #64748b; cursor: pointer; padding: 2px 6px; font-size: 13px; flex-shrink: 0; }
.node-entry .btn-remove:hover { color: #ef4444; }

.status-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }
.status-dot.online { background: #34d399; }
.status-dot.offline { background: #ef4444; }
.status-dot.checking { background: #fbbf24; }
</style>
</head>
<body>

<div class="header">
  <h1>Ballista Monitor</h1>
  <div class="header-actions">
    <span id="refresh-status" style="font-size:12px;color:#64748b;">Connecting...</span>
    <button class="btn" onclick="openConfig()">&#9881; Nodes</button>
  </div>
</div>

<div class="tabs" id="tabs"></div>

<div class="content" id="content"></div>

<!-- Config Modal -->
<div class="modal-overlay" id="config-modal">
  <div class="modal">
    <h2>Node Configuration</h2>
    <div id="node-list"></div>
    <div style="margin-top:12px;display:flex;gap:8px;">
      <button class="btn" onclick="addNodeEntry()">+ Add Node</button>
      <button class="btn btn-primary" onclick="saveConfig()">Save</button>
      <button class="btn" onclick="closeConfig()">Cancel</button>
    </div>
  </div>
</div>

<!-- Processor Detail Modal -->
<div class="modal-overlay" id="proc-modal">
  <div class="modal" style="width:min(90vw,600px);max-height:none;overflow:visible">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
      <h2 id="proc-modal-title" style="margin:0">Processor Detail</h2>
      <button class="btn" onclick="closeProcDetail()">Close</button>
    </div>
    <div id="proc-modal-body"></div>
  </div>
</div>

<script>
// ---- State ----
let nodes = [];
let nodeData = {};
let nodeStatus = {};
let currentView = 'overview';
let charts = {};
let detailBuilt = false;  // whether the detail DOM structure exists
let lastDetailView = null; // track which node index we're viewing

function fetchWithTimeout(url, timeoutMs) {
  const controller = new AbortController();
  const tid = setTimeout(() => controller.abort(), timeoutMs);
  return fetch(url, { signal: controller.signal }).then(r => { clearTimeout(tid); return r; }).catch(e => { clearTimeout(tid); throw e; });
}

// ---- Config ----
function loadConfig() {
  const saved = localStorage.getItem('ballista_monitor_nodes');
  if (saved) { try { nodes = JSON.parse(saved); } catch(e) { nodes = []; } }
  if (nodes.length === 0) {
    nodes = [{ name: 'This Node', role: 'executor', url: location.protocol + '//' + location.host }];
  }
}
function saveConfigToStorage() { localStorage.setItem('ballista_monitor_nodes', JSON.stringify(nodes)); }

// ---- Tabs ----
function switchTab(view) {
  currentView = view;
  if (view !== lastDetailView) detailBuilt = false; // force rebuild on node switch
  rebuildTabs();
  buildContent();
}

function rebuildTabs() {
  const el = document.getElementById('tabs');
  let html = '<div class="tab' + (currentView === 'overview' ? ' active' : '') + '" onclick="switchTab(\'overview\')">Overview</div>';
  nodes.forEach((n, i) => {
    const s = nodeStatus[n.url];
    let dc = 'checking';
    if (s) dc = s.online ? 'online' : 'offline';
    html += '<div class="tab' + (currentView === 'node-' + i ? ' active' : '') + '" onclick="switchTab(\'node-' + i + '\')"><span class="status-dot ' + dc + '"></span>' + n.name + '</div>';
  });
  el.innerHTML = html;
}

// ---- Data Fetching ----
async function fetchNodeData(url) {
  try {
    const r = await fetchWithTimeout(url + '/api/overview', 5000);
    if (!r.ok) throw new Error('HTTP ' + r.status);
    nodeData[url] = await r.json();
    if (!nodeStatus[url]) nodeStatus[url] = { online: false, failCount: 0, lastError: '' };
    nodeStatus[url].online = true;
    nodeStatus[url].failCount = 0;
    nodeStatus[url].lastError = '';
  } catch(e) {
    if (!nodeStatus[url]) nodeStatus[url] = { online: false, failCount: 0, lastError: '' };
    nodeStatus[url].failCount++;
    nodeStatus[url].lastError = e.message || String(e);
    if (nodeStatus[url].failCount >= 2) nodeStatus[url].online = false;
  }
}

async function refreshData() {
  await Promise.all(nodes.map(n => fetchNodeData(n.url)));
  rebuildTabs();
  updateContent();
  const onlineCount = Object.values(nodeStatus).filter(s => s.online).length;
  document.getElementById('refresh-status').textContent = onlineCount + '/' + nodes.length + ' online - ' + new Date().toLocaleTimeString();
}

// ---- Helpers ----
function fmtB(b) { if(b==null) return 'N/A'; if(b<1024) return b+' B'; if(b<1048576) return (b/1024).toFixed(1)+' KB'; if(b<1073741824) return (b/1048576).toFixed(1)+' MB'; return (b/1073741824).toFixed(2)+' GB'; }
function fmtP(v) { return v==null ? 'N/A' : v.toFixed(1)+'%'; }
function fmtU(s) { if(!s) return 'N/A'; const h=Math.floor(s/3600), m=Math.floor((s%3600)/60); return h>0 ? h+'h '+m+'m' : m+'m'; }
function fmtMs(v) { return v < 1000 ? v.toFixed(3)+' ms' : (v/1000).toFixed(3)+' s'; }
function gmv(metrics, name) { const m = metrics.find(x => x.name === name); return m ? m.value : null; }

// ---- Content: build (full render) vs update (patch values) ----
function buildContent() {
  detailBuilt = false;
  const c = document.getElementById('content');
  if (currentView === 'overview') {
    buildOverview(c);
  } else {
    const idx = parseInt(currentView.replace('node-', ''));
    buildDetail(c, idx);
  }
}

function updateContent() {
  if (currentView === 'overview') {
    updateOverview();
  } else {
    const idx = parseInt(currentView.replace('node-', ''));
    updateDetail(idx);
  }
}

// ---- Overview ----
function buildOverview(container) {
  let html = '<div class="overview-grid">';
  nodes.forEach((n, i) => {
    html += '<div class="node-card" id="nc-' + i + '" onclick="switchTab(\'node-' + i + '\')">';
    html += '<div class="node-card-title">' + n.name + ' <span class="node-card-role ' + n.role + '">' + n.role + '</span></div>';
    html += '<div class="node-card-stats">';
    html += '<div class="stat"><div class="stat-label">CPU</div><div class="stat-value" id="nc-cpu-' + i + '">--</div></div>';
    html += '<div class="stat"><div class="stat-label">Memory</div><div class="stat-value" id="nc-mem-' + i + '">--</div></div>';
    html += '<div class="stat"><div class="stat-label">Disk Used</div><div class="stat-value" id="nc-disk-' + i + '">--</div></div>';
    html += '<div class="stat"><div class="stat-label">SO Processors</div><div class="stat-value" id="nc-proc-' + i + '">--</div></div>';
    html += '<div class="stat"><div class="stat-label">Uptime</div><div class="stat-value" id="nc-up-' + i + '">--</div></div>';
    html += '<div class="stat"><div class="stat-label">Concurrent</div><div class="stat-value" id="nc-tasks-' + i + '">--</div></div>';
    html += '</div>';
    html += '<div class="node-card-error" id="nc-err-' + i + '"></div>';
    html += '</div>';
  });
  html += '</div>';
  container.innerHTML = html;
  updateOverview();
}

function updateOverview() {
  nodes.forEach((n, i) => {
    const card = document.getElementById('nc-' + i);
    const data = nodeData[n.url];
    const status = nodeStatus[n.url];
    const online = status && status.online;

    if (card) card.className = 'node-card' + (online ? '' : ' offline');

    if (!data || !online) {
      const err = status ? status.lastError : 'Not connected';
      setText('nc-cpu-' + i, '--');
      setText('nc-mem-' + i, '--');
      setText('nc-disk-' + i, '--');
      // Hide SO Processors & Concurrent for scheduler nodes even when offline
      const procEl = document.getElementById('nc-proc-' + i);
      const tasksEl = document.getElementById('nc-tasks-' + i);
      if (procEl && procEl.parentElement) {
        procEl.parentElement.style.display = n.role === 'scheduler' ? 'none' : '';
      }
      if (tasksEl && tasksEl.parentElement) {
        tasksEl.parentElement.style.display = n.role === 'scheduler' ? 'none' : '';
      }
      if (n.role !== 'scheduler') {
        setText('nc-proc-' + i, '--');
        setText('nc-tasks-' + i, '--');
      }
      setText('nc-up-' + i, '--');
      setText('nc-err-' + i, 'Offline: ' + err + ' (' + n.url + ')');
      return;
    }

    const m = data.metrics || [];
    setText('nc-cpu-' + i, fmtP(gmv(m, 'process_cpu_usage')));
    setText('nc-mem-' + i, fmtB(gmv(m, 'process_mem_rss_bytes')));
    const da = gmv(m, 'disk_available_bytes'), dt = gmv(m, 'disk_total_bytes');
    setText('nc-disk-' + i, dt ? ((1 - da / dt) * 100).toFixed(1) + '%' : 'N/A');
    // Hide SO Processors & Concurrent for scheduler nodes
    const procEl = document.getElementById('nc-proc-' + i);
    const tasksEl = document.getElementById('nc-tasks-' + i);
    if (procEl && procEl.parentElement) {
      procEl.parentElement.style.display = data.role === 'scheduler' ? 'none' : '';
    }
    if (tasksEl && tasksEl.parentElement) {
      tasksEl.parentElement.style.display = data.role === 'scheduler' ? 'none' : '';
    }
    if (data.role !== 'scheduler') {
      setText('nc-proc-' + i, data.processor_count + '/' + data.processor_total);
      setText('nc-tasks-' + i, data.concurrent_tasks > 0 ? String(data.concurrent_tasks) : '-');
    }
    setText('nc-up-' + i, fmtU(data.uptime_secs));
    setText('nc-err-' + i, '');
  });
}

function setText(id, val) {
  const el = document.getElementById(id);
  if (el && el.textContent !== val) el.textContent = val;
}

// ---- Detail ----
function buildDetail(container, idx) {
  const node = nodes[idx];
  if (!node) { container.innerHTML = '<p>Node not found</p>'; return; }
  const url = node.url;
  const status = nodeStatus[url];
  if (!(status && status.online) || !nodeData[url]) {
    container.innerHTML = '<div class="section"><h3>Node Offline</h3><p style="color:#ef4444">Cannot connect to ' + url + '</p><p style="color:#94a3b8;font-size:12px;margin-top:8px">Error: ' + (status ? status.lastError : 'Unknown') + '</p></div>';
    return;
  }

  let html = '';
  html += '<div class="detail-grid">';
  html += '<div class="metric-card"><div class="label">CPU</div><div class="value" id="d-cpu">--</div></div>';
  html += '<div class="metric-card"><div class="label">Memory (RSS)</div><div class="value" id="d-mem">--</div></div>';
  html += '<div class="metric-card"><div class="label">Disk Used</div><div class="value" id="d-disk">--</div></div>';
  html += '<div class="metric-card"><div class="label">Network</div><div class="value" id="d-net">--</div></div>';
  html += '</div>';

  html += '<div class="charts-grid">';
  html += '<div class="chart-box"><h3>CPU Usage</h3><div class="chart-container"><canvas id="chart-cpu"></canvas></div></div>';
  html += '<div class="chart-box"><h3>Memory</h3><div class="chart-container"><canvas id="chart-mem"></canvas></div></div>';
  html += '</div>';

  html += '<div class="section" id="d-top10-section"><h3>Top 10 Slowest Processors</h3><div id="d-top10"></div></div>';
  html += '<div class="section" id="d-procs-section"><h3>Processors</h3><div id="d-procs">Loading...</div></div>';

  // Hide .so-specific sections for scheduler
  if (node.role === 'scheduler') {
    html += '<style>#d-top10-section,#d-procs-section{display:none}</style>';
  }

  container.innerHTML = html;
  detailBuilt = true;
  lastDetailView = currentView;

  // Create charts once
  createCharts(url);
  // Initial detail update
  updateDetail(idx);
}

async function updateDetail(idx) {
  const node = nodes[idx];
  if (!node) return;
  const url = node.url;
  const data = nodeData[url];
  const status = nodeStatus[url];

  if (!(status && status.online) || !data) {
    if (!detailBuilt) buildDetail(document.getElementById('content'), idx);
    return;
  }

  // Update metric cards (no DOM rebuild)
  const m = data.metrics || [];
  const cpu = gmv(m, 'process_cpu_usage') || 0;
  const memRss = gmv(m, 'process_mem_rss_bytes') || 0;
  const da = gmv(m, 'disk_available_bytes') || 0, dt = gmv(m, 'disk_total_bytes') || 1;
  const diskPct = dt ? ((1 - da / dt) * 100).toFixed(1) : 'N/A';
  const ns = gmv(m, 'net_bytes_sent'), nr = gmv(m, 'net_bytes_recv');

  setText('d-cpu', cpu.toFixed(1) + '%');
  setText('d-mem', fmtB(memRss));
  setText('d-disk', diskPct + '%');
  setText('d-net', fmtB(nr) + ' / ' + fmtB(ns));

  // Update chart data (in-place, no destroy/recreate)
  updateCharts(url);

  // Update processors (only for executor nodes)
  if (node.role !== 'scheduler') {
    fetchProcessors(url).then(procs => {
      window._cachedProcs = procs;

      // Top 10 slowest
      const topEl = document.getElementById('d-top10');
      if (topEl) {
        const ranked = procs.map((p, i) => {
          const ss = p.stage_stats || {};
          const stageTotal = Object.values(ss).reduce((s, v) => s + (v.duration_ms || 0), 0);
          return { p, i, stageTotal };
        }).sort((a, b) => b.stageTotal - a.stageTotal).slice(0, 10);
        if (ranked.length === 0) {
          topEl.innerHTML = '<div style="text-align:center;color:#64748b;padding:8px">No processors yet</div>';
        } else {
          let th = '<table><tr><th>#</th><th>Job</th><th>SO</th><th>Function</th><th>Partition</th><th>Stage</th><th>Stages Total</th><th>Lifecycle</th></tr>';
          ranked.forEach((r, rank) => {
            const lc = r.p.finished_at != null ? (r.p.finished_at - r.p.created_at) : (Date.now() - r.p.created_at);
            const soName = (r.p.so_path||'').split('/').pop() || r.p.so_path;
            th += '<tr class="clickable" onclick="showProcDetail('+r.i+')"><td>'+(rank+1)+'</td><td title="'+r.p.job_id+'">'+(r.p.job_id||'-').slice(0,8)+'</td><td title="'+r.p.so_path+'">'+soName+'</td><td>'+r.p.fn_name+'</td><td>'+r.p.partition+'</td><td>'+r.p.stage+'</td><td style="color:#fbbf24;font-weight:600">'+r.stageTotal.toFixed(3)+' ms</td><td>'+fmtMs(lc)+'</td></tr>';
          });
          th += '</table>';
          topEl.innerHTML = th;
        }
      }

      if (!window._expandedGroups) window._expandedGroups = {};
      const el = document.getElementById('d-procs');
      if (!el) return;
      let h = '';
      if (procs.length === 0) {
        h = '<div style="text-align:center;color:#64748b;padding:12px">No active processors</div>';
      } else {
        // Group by job_id
        const groups = {};
        procs.forEach((p, i) => {
          const jid = p.job_id || '_unknown';
          if (!groups[jid]) groups[jid] = [];
          groups[jid].push({ p, i });
        });
        const jids = Object.keys(groups);
        jids.forEach(jid => {
          const g = groups[jid];
          const active = g.filter(x => x.p.stage !== 'done').length;
          const total = g.length;
          const earliest = Math.min(...g.map(x => x.p.created_at));
          const jobCreated = new Date(earliest).toLocaleTimeString();
          const isExpanded = !!window._expandedGroups[jid];
          h += '<div class="proc-group'+(isExpanded ? '' : ' collapsed')+'" data-jid="'+jid+'">';
          h += '<div class="proc-group-header" onclick="toggleProcGroup(\''+jid.replace(/'/g,"\\'")+'\')">Job '+jid.slice(0,8)+' <span style="color:#64748b;font-weight:400">'+jobCreated+' | '+active+' active / '+total+' total</span></div>';
          h += '<table><tr><th>SO</th><th>Function</th><th>Partition</th><th>Key</th><th>Stage</th><th>Created</th><th>Finished</th><th>Rows In</th><th>Rows Out</th><th>Bytes In</th><th>Bytes Out</th></tr>';
          g.forEach(({ p, i }) => {
            const soName = (p.so_path||'').split('/').pop() || p.so_path;
            const created = new Date(p.created_at).toLocaleTimeString();
            const finished = p.finished_at != null ? new Date(p.finished_at).toLocaleTimeString() : '-';
            h += '<tr class="clickable" onclick="showProcDetail('+i+')"><td title="'+p.so_path+'">'+soName+'</td><td>'+p.fn_name+'</td><td>'+p.partition+'</td><td>'+(p.key||'-')+'</td><td>'+p.stage+'</td><td>'+created+'</td><td>'+finished+'</td><td>'+p.rows_in+'</td><td>'+p.rows_out+'</td><td>'+fmtB(p.bytes_in)+'</td><td>'+fmtB(p.bytes_out)+'</td></tr>';
          });
          h += '</table></div>';
        });
      }
      el.innerHTML = h;
    });
  }
}

function toggleProcGroup(jid) {
  if (!window._expandedGroups) window._expandedGroups = {};
  window._expandedGroups[jid] = !window._expandedGroups[jid];
  const el = document.querySelector('.proc-group[data-jid="'+jid+'"]');
  if (el) el.classList.toggle('collapsed');
}

function showProcDetail(idx) {
  const p = (window._cachedProcs || [])[idx];
  if (!p) return;
  document.getElementById('proc-modal-title').textContent = 'Processor ' + p.id;
  const ss = p.stage_stats || {};
  const stages = ['init','feed','execute','fetch','finish'];
  const stageColors = {'init':'#fbbf24','feed':'#38bdf8','execute':'#a78bfa','fetch':'#34d399','finish':'#f87171'};
  function dur(k) { return (ss[k]||{}).duration_ms || 0; }
  function calls(k) { return (ss[k]||{}).calls || 0; }
  const stageTotal = stages.reduce((s, k) => s + dur(k), 0);
  const lifecycleMs = p.finished_at != null ? (p.finished_at - p.created_at) : (Date.now() - p.created_at);
  const waitMs = lifecycleMs - stageTotal;

  const soName = (p.so_path||'').split('/').pop() || p.so_path;

  let body = '<div class="proc-detail-grid">';
  body += '<div class="proc-detail-item"><div class="proc-detail-label">Job ID</div><div class="proc-detail-value" title="'+p.job_id+'">'+(p.job_id||'-')+'</div></div>';
  body += '<div class="proc-detail-item"><div class="proc-detail-label">SO</div><div class="proc-detail-value" title="'+p.so_path+'">'+soName+'</div></div>';
  body += '<div class="proc-detail-item"><div class="proc-detail-label">Function</div><div class="proc-detail-value">'+(p.fn_name||'-')+'</div></div>';
  body += '<div class="proc-detail-item"><div class="proc-detail-label">Partition</div><div class="proc-detail-value">'+p.partition+'</div></div>';
  body += '<div class="proc-detail-item"><div class="proc-detail-label">Key</div><div class="proc-detail-value">'+(p.key||'-')+'</div></div>';
  body += '<div class="proc-detail-item"><div class="proc-detail-label">Stage</div><div class="proc-detail-value">'+p.stage+'</div></div>';
  body += '<div class="proc-detail-item"><div class="proc-detail-label">Created At</div><div class="proc-detail-value">'+new Date(p.created_at).toLocaleString()+'</div></div>';
  body += '<div class="proc-detail-item"><div class="proc-detail-label">Finished At</div><div class="proc-detail-value">'+(p.finished_at != null ? new Date(p.finished_at).toLocaleString() : '-')+'</div></div>';
  body += '<div class="proc-detail-item"><div class="proc-detail-label">Lifecycle <span class="info-tip" title="从 init 创建到 finish 完成的挂钟时间，包含调度等待、I/O 等所有耗时">&#9432;</span></div><div class="proc-detail-value">'+fmtMs(lifecycleMs)+'</div></div>';
  body += '<div class="proc-detail-item"><div class="proc-detail-label">Stages Total <span class="info-tip" title="5 个 stage 的 CPU 耗时之和（init+feed+execute+fetch+finish），不含等待和调度开销">&#9432;</span></div><div class="proc-detail-value">'+fmtMs(stageTotal)+'</div></div>';
  if (waitMs > 0.001) {
    body += '<div class="proc-detail-item" style="grid-column:1/-1"><div class="proc-detail-label">Wait / Overhead <span class="info-tip" title="Lifecycle - Stages Total，即调度排队、I/O 等待等非 stage 计算耗时">&#9432;</span></div><div class="proc-detail-value" style="color:#fbbf24">'+fmtMs(waitMs)+' ('+((waitMs/lifecycleMs)*100).toFixed(1)+'%)</div></div>';
  }
  body += '</div>';

  // Stage duration bar
  if (stageTotal > 0) {
    body += '<div style="margin:12px 0 4px;font-size:12px;color:#64748b">Stage Breakdown</div>';
    body += '<div class="proc-stage-bar">';
    stages.forEach(k => {
      const v = dur(k);
      if (v <= 0) return;
      const pct = (v / stageTotal * 100);
      body += '<div class="proc-stage-seg" style="width:'+pct+'%;background:'+stageColors[k]+'" title="'+k+': '+v.toFixed(3)+'ms"></div>';
    });
    body += '</div>';
  }

  // Stage details table
  body += '<table style="margin-top:12px"><tr><th>Stage</th><th>Calls</th><th>Duration (ms)</th><th>Avg (ms)</th><th>%</th></tr>';
  stages.forEach(k => {
    const d = dur(k), c = calls(k);
    const avg = c > 0 ? (d / c).toFixed(3) : '-';
    const pct = stageTotal > 0 ? (d / stageTotal * 100).toFixed(1) : '0.0';
    body += '<tr><td style="color:'+(stageColors[k]||'#e2e8f0')+'">'+k+'</td><td>'+(c > 0 ? c : '-')+'</td><td>'+(c > 0 ? d.toFixed(3) : '-')+'</td><td>'+avg+'</td><td>'+pct+'%</td></tr>';
  });
  body += '</table>';

  // I/O stats
  body += '<div style="margin-top:16px;font-size:12px;color:#64748b">I/O Statistics</div>';
  body += '<table><tr><th></th><th>Rows</th><th>Bytes</th></tr>';
  body += '<tr><td>Input</td><td>'+p.rows_in+'</td><td>'+fmtB(p.bytes_in)+'</td></tr>';
  body += '<tr><td>Output</td><td>'+p.rows_out+'</td><td>'+fmtB(p.bytes_out)+'</td></tr>';
  body += '</table>';

  document.getElementById('proc-modal-body').innerHTML = body;
  document.getElementById('proc-modal').classList.add('show');
}

function closeProcDetail() { document.getElementById('proc-modal').classList.remove('show'); }

async function fetchProcessors(url) { try { return await (await fetchWithTimeout(url+'/api/processors',5000)).json(); } catch(e) { return []; } }

// ---- Charts: create once, then update data in-place ----
const chartOpts = {
  responsive: true, maintainAspectRatio: false, animation: false,
  scales: { x: { display: false }, y: { ticks: { color: '#64748b' }, grid: { color: '#1e293b' } } },
  plugins: { legend: { labels: { color: '#94a3b8', font: { size: 11 } } } }
};

function makeDataset(label, color) {
  return { label, data: [], borderColor: color, borderWidth: 1.5, pointRadius: 0, fill: false };
}

function createCharts(url) {
  Object.values(charts).forEach(c => c.destroy());
  charts = {};

  const mk = (id, datasets) => {
    const ctx = document.getElementById(id);
    if (!ctx) return null;
    return new Chart(ctx, { type: 'line', data: { labels: [], datasets }, options: { ...chartOpts } });
  };

  charts.cpu = mk('chart-cpu', [makeDataset('Process CPU %', '#38bdf8')]);
  charts.mem = mk('chart-mem', [makeDataset('RSS (MB)', '#a78bfa')]);
}

async function updateCharts(url) {
  const since = Date.now() - 5 * 60 * 1000;

  async function fh(name) {
    try { return await (await fetchWithTimeout(url + '/api/metrics/' + name + '/history?since=' + since, 5000)).json(); }
    catch(e) { return []; }
  }

  const [cpuD, memD] = await Promise.all([
    fh('process_cpu_usage'), fh('process_mem_rss_bytes'),
  ]);

  function labels(d) { return d.map(x => new Date(x.timestamp).toLocaleTimeString()); }
  function vals(d) { return d.map(x => x.value); }

  function patch(chart, lbls, datasets) {
    if (!chart) return;
    chart.data.labels = lbls;
    datasets.forEach((d, i) => { if (chart.data.datasets[i]) chart.data.datasets[i].data = d; });
    chart.update('none'); // skip animation
  }

  patch(charts.cpu, labels(cpuD), [vals(cpuD)]);
  patch(charts.mem, labels(memD), [vals(memD).map(v => v / 1048576)]);
}

// ---- Config Modal ----
function openConfig() { document.getElementById('config-modal').classList.add('show'); renderNodeList(); }
function closeConfig() { document.getElementById('config-modal').classList.remove('show'); }

function renderNodeList() {
  let html = '';
  nodes.forEach((n, i) => {
    html += '<div class="node-entry">';
    html += '<input type="text" class="cfg-name" value="'+n.name+'" id="cfg-n-'+i+'" placeholder="Name">';
    html += '<select id="cfg-r-'+i+'"><option value="scheduler"'+(n.role==='scheduler'?' selected':'')+'>Scheduler</option><option value="executor"'+(n.role==='executor'?' selected':'')+'>Executor</option></select>';
    html += '<input type="text" class="cfg-url" value="'+n.url+'" id="cfg-u-'+i+'" placeholder="http://host:port">';
    html += '<span class="btn-remove" onclick="removeNode('+i+')">&#10005;</span>';
    html += '</div>';
  });
  document.getElementById('node-list').innerHTML = html;
}
function addNodeEntry() { nodes.push({name:'',role:'executor',url:''}); renderNodeList(); }
function removeNode(i) { nodes.splice(i,1); renderNodeList(); }
function saveConfig() {
  const nl = [];
  nodes.forEach((n,i) => {
    let u = document.getElementById('cfg-u-'+i).value.trim();
    if (u && !u.startsWith('http')) u = 'http://' + u;
    const nm = document.getElementById('cfg-n-'+i).value.trim();
    const rl = document.getElementById('cfg-r-'+i).value;
    if (nm && u) nl.push({name:nm, role:rl, url:u});
  });
  nodes = nl; saveConfigToStorage(); closeConfig(); rebuildTabs(); refreshData();
}

// ---- Init ----
loadConfig();
rebuildTabs();
buildContent();
refreshData();

// Auto refresh data only (no DOM rebuild)
setInterval(refreshData, 5000);
</script>
</body>
</html>"##;
