// Polls /api/summary and updates KPIs, charts, alerts and batch reports in place.
const REFRESH_MS = 3000;
const PALETTE = ["#4f9cf9", "#3fb950", "#d29922", "#a371f7", "#f85149", "#39c5cf", "#ff9bce", "#8b949e",
                 "#79c0ff", "#56d364", "#e3b341", "#bc8cff"];
Chart.defaults.color = "#8b949e";
Chart.defaults.borderColor = "#262d38";
Chart.defaults.font.family = "system-ui, -apple-system, Segoe UI, Roboto, sans-serif";

const charts = {};
let timelineChart = null;
let seenAlerts = new Set();
let firstLoad = true;
let activeReport = null;

const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const compact = (v) => Math.abs(v) >= 1e7 ? (v / 1e7).toFixed(2) + " Cr" : Math.abs(v) >= 1e5 ? (v / 1e5).toFixed(2) + " L"
  : Math.abs(v) >= 1e3 ? (v / 1e3).toFixed(1) + "k" : String(Math.round(v * 100) / 100);

function clusterLinks() {
  const h = location.hostname;
  const links = [["HDFS", 9870], ["Spark", 8080], ["Spark job", 4040], ["HBase", 16010]];
  $("links").innerHTML = links.map(([n, p]) => `<a href="http://${h}:${p}" target="_blank">${n} UI</a>`).join("");
}

function renderKpis(kpis) {
  $("kpis").innerHTML = kpis.map((k) => `
    <div class="kpi ${k.label === "Alerts" ? "alert" : ""}">
      <div class="label">${esc(k.label)}</div><div class="value">${esc(k.value)}</div><div class="subv">${esc(k.sub)}</div>
    </div>`).join("");
}

function ensureChart(spec) {
  if (charts[spec.id]) return charts[spec.id];
  const card = document.createElement("div");
  card.className = "card";
  card.innerHTML = `<h2>${esc(spec.title)}</h2><div class="chart-box"><canvas id="c-${spec.id}"></canvas></div>`;
  $("charts").appendChild(card);
  const isDoughnut = spec.type === "doughnut";
  const horizontal = spec.type === "hbar";
  charts[spec.id] = new Chart($(`c-${spec.id}`), {
    type: isDoughnut ? "doughnut" : "bar",
    data: { labels: [], datasets: [{ data: [], backgroundColor: isDoughnut ? PALETTE : PALETTE[0], borderWidth: isDoughnut ? 0 : 0, borderRadius: 4 }] },
    options: {
      maintainAspectRatio: false, animation: { duration: 400 }, indexAxis: horizontal ? "y" : "x",
      plugins: { legend: { display: isDoughnut, position: "right", labels: { boxWidth: 12 } } },
      scales: isDoughnut ? {} : {
        [horizontal ? "x" : "y"]: { beginAtZero: true, ticks: { callback: (v) => compact(v) } },
        [horizontal ? "y" : "x"]: { grid: { display: false } },
      },
      cutout: isDoughnut ? "62%" : undefined,
    },
  });
  return charts[spec.id];
}

function renderCharts(specs) {
  specs.forEach((spec) => {
    const c = ensureChart(spec);
    c.data.labels = spec.labels;
    c.data.datasets[0].data = spec.values;
    c.update();
  });
}

function renderTimeline(t, valueLabel) {
  if (!timelineChart) {
    timelineChart = new Chart($("timeline"), {
      type: "line",
      data: { labels: [], datasets: [
        { label: "Events / min", data: [], borderColor: PALETTE[0], backgroundColor: "rgba(79,156,249,.15)", fill: true, tension: .3, yAxisID: "y", pointRadius: 0 },
        { label: valueLabel, data: [], borderColor: PALETTE[2], tension: .3, yAxisID: "y1", pointRadius: 0, borderDash: [4, 3] },
      ] },
      options: {
        maintainAspectRatio: false, animation: { duration: 300 }, interaction: { mode: "index", intersect: false },
        scales: {
          y: { beginAtZero: true, position: "left", ticks: { callback: (v) => compact(v) } },
          y1: { beginAtZero: true, position: "right", grid: { display: false }, ticks: { callback: (v) => compact(v) } },
          x: { grid: { display: false } },
        },
        plugins: { legend: { labels: { boxWidth: 12 } } },
      },
    });
  }
  timelineChart.data.labels = t.labels;
  timelineChart.data.datasets[0].data = t.counts;
  timelineChart.data.datasets[1].data = t.values;
  timelineChart.update();
}

function pillClass(type, types) {
  const i = Object.keys(types).sort().indexOf(type);
  return i === 1 ? "pill t1" : i === 2 ? "pill t2" : "pill";
}

function renderAlerts(alerts, cols, types) {
  $("alert-types").innerHTML = Object.entries(types).sort()
    .map(([k, v]) => `<span class="${pillClass(k, types)}">${esc(k)}: ${v}</span>`).join("");
  $("alerts").querySelector("thead").innerHTML = `<tr>${cols.map((c) => `<th>${esc(c.replace("_", " "))}</th>`).join("")}</tr>`;
  const body = alerts.map((a) => {
    const key = (a.id || "") + (a.ts_ms || "");
    const isNew = !firstLoad && !seenAlerts.has(key);
    seenAlerts.add(key);
    return `<tr class="${isNew ? "new" : ""}">${cols.map((c) => {
      if (c === "alert_type") return `<td><span class="${pillClass(a[c], types)}">${esc(a[c])}</span></td>`;
      if (c === "amount") return `<td class="num">₹${Number(a[c] || 0).toLocaleString("en-IN")}</td>`;
      return `<td>${esc(a[c])}</td>`;
    }).join("")}</tr>`;
  }).join("");
  $("alerts").querySelector("tbody").innerHTML = body || `<tr><td colspan="${cols.length}" class="muted">No alerts yet</td></tr>`;
}

function renderReports(reports) {
  const names = Object.keys(reports).filter((n) => n !== "_meta");
  $("report-empty").classList.toggle("hidden", names.length > 0);
  if (!names.length) return;
  const meta = (reports._meta || [{}])[0];
  $("report-meta").textContent = meta.events_analysed ? `(${Number(meta.events_analysed).toLocaleString("en-IN")} events · generated ${meta.generated_at || ""})` : "";
  if (!activeReport || !names.includes(activeReport)) activeReport = names[0];
  $("report-tabs").innerHTML = names.map((n) => `<button class="${n === activeReport ? "active" : ""}" data-r="${esc(n)}">${esc(n.replaceAll("_", " "))}</button>`).join("");
  $("report-tabs").querySelectorAll("button").forEach((b) => b.onclick = () => { activeReport = b.dataset.r; renderReports(reports); });
  const rows = reports[activeReport] || [];
  const cols = rows.length ? Object.keys(rows[0]) : [];
  $("report").querySelector("thead").innerHTML = `<tr>${cols.map((c) => `<th>${esc(c.replaceAll("_", " "))}</th>`).join("")}</tr>`;
  $("report").querySelector("tbody").innerHTML = rows.map((r) => `<tr>${cols.map((c) =>
    typeof r[c] === "number" ? `<td class="num">${r[c].toLocaleString("en-IN")}</td>` : `<td>${esc(r[c])}</td>`).join("")}</tr>`).join("");
}

async function refresh() {
  try {
    const res = await fetch("/api/summary");
    const d = await res.json();
    if (!d.ok) throw new Error(d.error);
    $("error").classList.add("hidden");
    renderKpis(d.kpis);
    renderTimeline(d.timeline, d.timeline_value_label);
    renderCharts(d.charts);
    renderAlerts(d.alerts, d.alert_columns, d.alert_types);
    renderReports(d.reports);
    $("live").className = "dot on";
    $("status-text").textContent = `live · updated ${d.server_time} · HBase query ${d.query_ms} ms`;
    firstLoad = false;
  } catch (e) {
    $("live").className = "dot off";
    $("status-text").textContent = "waiting for data…";
    $("error").textContent = "Cannot read from HBase yet: " + e.message;
    $("error").classList.remove("hidden");
  }
}

clusterLinks();
refresh();
setInterval(refresh, REFRESH_MS);
